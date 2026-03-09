//! Tests for Arrow streaming functionality.
//!
//! Note: These tests focus on error handling and API correctness since we don't have
//! actual Arrow handles to test with. The Arrow handles would typically come from
//! Arrow C++ or other Arrow implementations.

use chdb_rust::arrow_stream::{ArrowArray, ArrowSchema, ArrowStream};
use chdb_rust::connection::Connection;
use chdb_rust::error::Error;

#[test]
fn test_arrow_stream_wrapper() {
    // Test creating ArrowStream from null pointer
    let null_ptr = std::ptr::null_mut();
    let stream = unsafe { ArrowStream::from_raw(null_ptr) };
    assert!(stream.as_raw().is_null());

    // Test that we can clone and copy
    let stream2 = stream;
    assert_eq!(stream.as_raw(), stream2.as_raw());
}

#[test]
fn test_arrow_schema_wrapper() {
    // Test creating ArrowSchema from null pointer
    let null_ptr = std::ptr::null_mut();
    let schema = unsafe { ArrowSchema::from_raw(null_ptr) };
    assert!(schema.as_raw().is_null());

    // Test that we can clone and copy
    let schema2 = schema;
    assert_eq!(schema.as_raw(), schema2.as_raw());
}

#[test]
fn test_arrow_array_wrapper() {
    // Test creating ArrowArray from null pointer
    let null_ptr = std::ptr::null_mut();
    let array = unsafe { ArrowArray::from_raw(null_ptr) };
    assert!(array.as_raw().is_null());

    // Test that we can clone and copy
    let array2 = array;
    assert_eq!(array.as_raw(), array2.as_raw());
}

#[test]
fn test_register_arrow_stream_invalid_table_name() {
    let conn = Connection::open_in_memory().expect("Failed to create connection");
    let null_stream = unsafe { ArrowStream::from_raw(std::ptr::null_mut()) };

    // Test with null byte in table name (should fail)
    let result = conn.register_arrow_stream("table\0name", &null_stream);
    assert!(result.is_err());
    match result {
        Err(Error::Nul(_)) => {}
        Err(e) => panic!("Expected Nul error, got {:?}", e),
        Ok(_) => panic!("Expected error for null byte in table name"),
    }
}

#[test]
fn test_register_arrow_stream_with_null_handle() {
    let conn = Connection::open_in_memory().expect("Failed to create connection");
    let null_stream = unsafe { ArrowStream::from_raw(std::ptr::null_mut()) };

    // This will likely fail because the stream handle is null
    // The actual behavior depends on the chDB library, but we should handle it gracefully
    let result = conn.register_arrow_stream("test_table", &null_stream);
    // The function may succeed or fail depending on chDB's validation
    // We just ensure it doesn't panic
    let _ = result;
}

#[test]
fn test_register_arrow_array_invalid_table_name() {
    let conn = Connection::open_in_memory().expect("Failed to create connection");
    let null_schema = unsafe { ArrowSchema::from_raw(std::ptr::null_mut()) };
    let null_array = unsafe { ArrowArray::from_raw(std::ptr::null_mut()) };

    // Test with null byte in table name (should fail)
    let result = conn.register_arrow_array("table\0name", &null_schema, &null_array);
    assert!(result.is_err());
    match result {
        Err(Error::Nul(_)) => {}
        Err(e) => panic!("Expected Nul error, got {:?}", e),
        Ok(_) => panic!("Expected error for null byte in table name"),
    }
}

#[test]
fn test_register_arrow_array_with_null_handles() {
    let conn = Connection::open_in_memory().expect("Failed to create connection");
    let null_schema = unsafe { ArrowSchema::from_raw(std::ptr::null_mut()) };
    let null_array = unsafe { ArrowArray::from_raw(std::ptr::null_mut()) };

    // This will likely fail because the handles are null
    // The actual behavior depends on the chDB library, but we should handle it gracefully
    let result = conn.register_arrow_array("test_table", &null_schema, &null_array);
    // The function may succeed or fail depending on chDB's validation
    // We just ensure it doesn't panic
    let _ = result;
}

#[test]
fn test_unregister_arrow_table_invalid_table_name() {
    let conn = Connection::open_in_memory().expect("Failed to create connection");

    // Test with null byte in table name (should fail)
    let result = conn.unregister_arrow_table("table\0name");
    assert!(result.is_err());
    match result {
        Err(Error::Nul(_)) => {}
        Err(e) => panic!("Expected Nul error, got {:?}", e),
        Ok(_) => panic!("Expected error for null byte in table name"),
    }
}

#[test]
fn test_unregister_nonexistent_table() {
    let conn = Connection::open_in_memory().expect("Failed to create connection");

    // Try to unregister a table that was never registered
    // The chDB library may succeed silently or return an error
    // We just ensure it doesn't panic
    let result = conn.unregister_arrow_table("nonexistent_table_12345");
    // Accept either success or error - both are valid behaviors
    match result {
        Ok(_) => {
            // Library allows unregistering nonexistent tables
        }
        Err(Error::QueryError(_)) => {
            // Library returns an error for nonexistent tables
        }
        Err(e) => panic!("Unexpected error type: {:?}", e),
    }
}

#[test]
fn test_arrow_stream_send() {
    // Test that ArrowStream implements Send
    use std::thread;
    let null_ptr = std::ptr::null_mut();
    let stream = unsafe { ArrowStream::from_raw(null_ptr) };

    thread::spawn(move || {
        let _ = stream;
    })
    .join()
    .expect("Thread should complete");
}

#[test]
fn test_arrow_schema_send() {
    // Test that ArrowSchema implements Send
    use std::thread;
    let null_ptr = std::ptr::null_mut();
    let schema = unsafe { ArrowSchema::from_raw(null_ptr) };

    thread::spawn(move || {
        let _ = schema;
    })
    .join()
    .expect("Thread should complete");
}

#[test]
fn test_arrow_array_send() {
    // Test that ArrowArray implements Send
    use std::thread;
    let null_ptr = std::ptr::null_mut();
    let array = unsafe { ArrowArray::from_raw(null_ptr) };

    thread::spawn(move || {
        let _ = array;
    })
    .join()
    .expect("Thread should complete");
}

#[test]
fn test_arrow_stream_valid_table_names() {
    let conn = Connection::open_in_memory().expect("Failed to create connection");
    let null_stream = unsafe { ArrowStream::from_raw(std::ptr::null_mut()) };

    // Test various valid table names (they may fail due to null handle, but shouldn't fail on name validation)
    let valid_names = [
        "simple_table",
        "table_with_underscores",
        "TableWithCamelCase",
        "table123",
        "a",
        "very_long_table_name_with_many_characters_that_should_still_be_valid",
    ];

    for name in &valid_names {
        let result = conn.register_arrow_stream(name, &null_stream);
        // May fail due to null handle, but shouldn't fail on name validation
        let _ = result;
    }
}

#[test]
fn test_arrow_array_valid_table_names() {
    let conn = Connection::open_in_memory().expect("Failed to create connection");
    let null_schema = unsafe { ArrowSchema::from_raw(std::ptr::null_mut()) };
    let null_array = unsafe { ArrowArray::from_raw(std::ptr::null_mut()) };

    // Test various valid table names
    let valid_names = [
        "simple_table",
        "table_with_underscores",
        "TableWithCamelCase",
        "table123",
    ];

    for name in &valid_names {
        let result = conn.register_arrow_array(name, &null_schema, &null_array);
        // May fail due to null handles, but shouldn't fail on name validation
        let _ = result;
    }
}

#[test]
fn test_connection_methods_consistency() {
    // Test that all three methods use consistent error handling
    let conn = Connection::open_in_memory().expect("Failed to create connection");
    let null_stream = unsafe { ArrowStream::from_raw(std::ptr::null_mut()) };
    let null_schema = unsafe { ArrowSchema::from_raw(std::ptr::null_mut()) };
    let null_array = unsafe { ArrowArray::from_raw(std::ptr::null_mut()) };

    // All should handle null bytes consistently
    let result1 = conn.register_arrow_stream("test\0", &null_stream);
    let result2 = conn.register_arrow_array("test\0", &null_schema, &null_array);
    let result3 = conn.unregister_arrow_table("test\0");

    assert!(result1.is_err());
    assert!(result2.is_err());
    assert!(result3.is_err());

    // All should be Nul errors
    match (result1, result2, result3) {
        (Err(Error::Nul(_)), Err(Error::Nul(_)), Err(Error::Nul(_))) => {}
        _ => panic!("All should return Nul errors for null bytes"),
    }
}
