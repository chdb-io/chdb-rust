//! Lease acquisition, cold creation and release (contract §5.2, §5.7).
//!
//! These run before an object exists, which is why they are functions rather
//! than methods: an open that fails while taking the lease has nothing to hang
//! state on, and the unwind has to work anyway.

use std::time::{Instant, SystemTime, UNIX_EPOCH};

use uuid::Uuid;

use super::backend::{Backend, PutOutcome, ReplaceOutcome};
use super::errors::{err, Category, Result};
use super::head::{cold_head, parse_head, serialize_head, HeadSnapshot};
use super::keys::HEAD_KEY;
use super::negotiate::{assert_engine_compatible, assert_readable, assert_writable, RunningEngine};
use super::object::Tuning;
use super::types::Lease;

/// Identifies one live instance of a writer.
///
/// The owner name is for humans and may repeat — two replicas of the same
/// deployment, a restarted process with the same name. The instance is what the
/// fence compares, so it has to be unique per live handle.
pub(crate) fn new_instance_id() -> String {
    Uuid::new_v4().simple().to_string()
}

/// The current time as epoch seconds, the form a lease records.
pub(crate) fn now_seconds() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| since.as_secs_f64())
        // A clock before the epoch cannot produce a usable expiry, and treating
        // it as zero at least makes every lease look long expired rather than
        // eternally held.
        .unwrap_or(0.0)
}

/// Reads and validates the head, if there is one.
pub(crate) fn read_head(backend: &dyn Backend) -> Result<Option<HeadSnapshot>> {
    let Some(tagged) = backend.get_bytes_with_etag(HEAD_KEY)? else {
        return Ok(None);
    };
    let (head, raw) = parse_head(&tagged.data)?;
    Ok(Some(HeadSnapshot {
        head,
        etag: tagged.etag,
        raw,
    }))
}

pub(crate) struct LeaseParams<'a> {
    pub(crate) instance: &'a str,
    pub(crate) owner: &'a str,
    pub(crate) tuning: &'a Tuning,
    pub(crate) force: bool,
}

pub(crate) struct ColdParams<'a> {
    pub(crate) id: &'a str,
    pub(crate) database: &'a str,
    pub(crate) running: &'a RunningEngine,
    pub(crate) instance: &'a str,
    pub(crate) owner: &'a str,
    pub(crate) tuning: &'a Tuning,
}

/// Atomically creates a cold object and takes generation 1 in the same write.
pub(crate) fn create_cold(backend: &dyn Backend, params: ColdParams<'_>) -> Result<HeadSnapshot> {
    let head = cold_head(
        params.database,
        &params.running.version,
        params.running.backup_format,
        Lease {
            generation: 1,
            owner: Some(params.owner.to_string()),
            instance: Some(params.instance.to_string()),
            expires_at: Some(now_seconds() + params.tuning.lease_ttl.as_secs_f64()),
        },
    );
    let outcome = backend.put_bytes_if_absent(HEAD_KEY, &serialize_head(&head, None)?)?;

    // Read back on every path, including success. The ETag of what was created
    // is what the next commit has to present, and a conditional create does not
    // always report one.
    let fresh = read_head(backend)?;

    if outcome == PutOutcome::Created {
        return fresh.ok_or_else(|| {
            err(
                Category::Corrupt,
                format!(
                    "durable: created {HEAD_KEY} at {} but it cannot be read back",
                    backend.describe()
                ),
            )
        });
    }

    // Lost the race, or the response was lost. Either way the answer is in the
    // object: if the head that is there names this instance, the create landed.
    let Some(fresh) = fresh else {
        return Err(err(
            Category::CommitAmbiguous,
            format!(
                "durable: could not determine whether object {} was created at {}",
                params.id,
                backend.describe()
            ),
        )
        .with_key(HEAD_KEY));
    };
    if fresh.head.lease.instance_is(params.instance) {
        return Ok(fresh);
    }

    // Someone else created it. It is now an existing object, so it goes through
    // the same gates any existing object would.
    assert_readable(&fresh.head)?;
    assert_engine_compatible(&fresh.head, params.running)?;
    assert_writable(&fresh.head)?;
    acquire_lease(
        backend,
        fresh,
        LeaseParams {
            instance: params.instance,
            owner: params.owner,
            tuning: params.tuning,
            force: false,
        },
    )
}

/// Takes the writer lease by compare-and-swap.
///
/// An unheld lease is free. A held one is only takeable once its recorded expiry
/// is behind us by more than the clock-skew allowance — the allowance is there
/// because the two writers' clocks are not the same clock, and a lease that
/// looks expired by a second might not be. Taking a lease that has not expired
/// is possible, but only as an explicit force, never as a retry.
pub(crate) fn acquire_lease(
    backend: &dyn Backend,
    start: HeadSnapshot,
    params: LeaseParams<'_>,
) -> Result<HeadSnapshot> {
    let mut current = start;
    let deadline = Instant::now() + params.tuning.commit_deadline;

    for attempt in 1.. {
        let lease = current.head.lease.clone();
        if lease.is_held() && !params.force {
            let takeable_at = lease.expires_at.unwrap_or(f64::INFINITY)
                + params.tuning.clock_skew_allowance.as_secs_f64();
            if now_seconds() < takeable_at {
                let owner = lease.owner.clone().unwrap_or_default();
                return Err(err(
                    Category::LeaseHeld,
                    format!(
                        "durable: {owner:?} holds the writer lease (generation {})",
                        lease.generation
                    ),
                )
                .with_owner(owner));
            }
        }

        let mut candidate = current.head.clone();
        candidate.lease = Lease {
            generation: lease.generation + 1,
            owner: Some(params.owner.to_string()),
            instance: Some(params.instance.to_string()),
            expires_at: Some(now_seconds() + params.tuning.lease_ttl.as_secs_f64()),
        };
        let body = serialize_head(&candidate, Some(&current.raw))?;

        if let ReplaceOutcome::Done { etag } =
            backend.replace_if_match(HEAD_KEY, &body, &current.etag)?
        {
            return Ok(HeadSnapshot {
                head: candidate,
                etag,
                raw: current.raw,
            });
        }

        let fresh = read_head(backend)?.ok_or_else(|| {
            err(
                Category::Corrupt,
                format!(
                    "durable: {HEAD_KEY} disappeared from {} while taking the lease",
                    backend.describe()
                ),
            )
        })?;
        if fresh.head.lease.instance_is(params.instance) {
            // The write landed and its response was lost.
            return Ok(fresh);
        }

        if attempt >= params.tuning.max_commit_attempts || Instant::now() >= deadline {
            let mut held = err(
                Category::LeaseHeld,
                format!(
                    "durable: could not take the writer lease after {attempt} attempts; \
                     generation is now {}",
                    fresh.head.lease.generation
                ),
            );
            if let Some(owner) = fresh.head.lease.owner.clone() {
                held = held.with_owner(owner);
            }
            return Err(held);
        }
        current = fresh;
    }
    unreachable!("the attempt loop returns from inside")
}

/// Gives the lease back, best effort, when an open failed part-way through.
///
/// A failure after the lease was taken must not strand it until the TTL
/// expires, but the release has to be conditional on still owning it: a head
/// that has moved on belongs to someone else, and writing over it would undo
/// their acquisition.
pub(crate) fn release_lease(backend: &dyn Backend, instance: &str) -> Result<()> {
    let Some(fresh) = read_head(backend)? else {
        return Ok(());
    };
    if !fresh.head.lease.instance_is(instance) {
        return Ok(());
    }
    let mut candidate = fresh.head.clone();
    candidate.lease = Lease::released(fresh.head.lease.generation);
    let body = serialize_head(&candidate, Some(&fresh.raw))?;
    backend.replace_if_match(HEAD_KEY, &body, &fresh.etag)?;
    Ok(())
}
