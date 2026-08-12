# 静态链接:待验证事项与理由

配套 `chdb-go-static-link-findings.md`(Linux 实测结论)与 `chdb-go-static-link-deadlock.md`(原始复现)。

**为什么需要这份文档**:chdb-rust 在 macOS arm64 上静态链接 `libchdb.a` **实测通过**
(54 test + 36 doctest,`otool -L` 显示 libc++ 确实在进程里)。这否掉了「macOS 上静态必挂、与语言无关」
这个读法,也意味着原始 Go 复现的归因**不成立或不完整**。下面每一项都是为了把归因钉死,
而不是为了再确认一遍现象。

---

## 一、已确证的事实(不需要再验)

先划掉不必重复的部分,避免浪费机器时间。

| 事实 | 证据 |
| --- | --- |
| Linux x86_64 上 raw `libchdb.a` + cgo 可用,无死锁 | `chdb_connect` 0.06 s 返回;16000 次并发查询 0 失败;Go runtime 事后健康 |
| Linux 上默认只有一份 C++ runtime | 加 `-lc++` 后 `--as-needed` 直接丢弃依赖,`ldd` 看不到 |
| 两份共存 + 通道打开 + 方向混合,在 Linux 上仍不出错 | `--no-as-needed` 后 2052 个 mangled 符号进 `.dynsym`;`LD_PRELOAD` 下观测到 409 个反向绑定;仍全过 |
| Linux 静态包**没有 jemalloc** | `je_`/`_rjem_` 符号 0 个;`malloc/free` 全部 `U ...@GLIBC` |
| macOS 静态包**有 jemalloc** | `chdb-go-static-link-deadlock.md` 的成员清点:jemalloc* 5 个 |
| `.so` 的导出契约 = 48 个符号、0 个 mangled | `nm -D --defined-only libchdb.so` |
| 方案 B(预链接 + objcopy 本地化)与 COMDAT 去重冲突 | 链接 chdb-go 测试套件时 `defined in discarded section`,vtable 指向被丢弃的析构 |
| COMMON 符号会逃出封装,`ld -r -d` 可修,**lld 静默忽略 `-d`** | 最小例子上双向验证 |
| 封装后体积零代价 | 641,744,064 vs raw 642,180,760 字节 |

---

## 二、当前假设与证据矩阵

| 假设 | 与「Rust on macOS 通过」是否兼容 | 状态 |
| --- | --- | --- |
| **H1**:Go runtime 与 libchdb 的信号处理器冲突(issue #30 的静态版) | ✅ 兼容 —— Rust 不依赖信号,同样的覆盖对它无害 | **领先假设,未验证** |
| **H2**:cgo 显式引入的第二份 C++ runtime(`-lc++ -lc++abi`) | ✅ 兼容 —— 前提是 Rust 侧没加 | 未验证 |
| **H3**:C++ runtime / allocator 串扰是根因 | ❌ 不兼容 —— Rust 会一样挂 | 已被削弱 |
| **H4**:纯平台问题(macOS vs Linux) | ❌ 已被 Rust 结果否掉 | 已否 |

H1 的机制:`chdb_set_signal_handlers_enabled(0)` 会把那批信号**打回 SIG_DFL**
(经验证,`chdb.h` 未提及此副作用),而 **SIGURG 的默认动作是忽略** —— Go 的异步抢占信号被丢弃 →
某个 goroutine 永不让出 → stop-the-world 永远完不成 → **所有线程停在 `_pthread_cond_wait`**。
这比「locale 锁」更贴合原始现象:六个线程、十二帧全在 `_pthread_cond_wait`、60 秒无输出。

**H1 若成立,影响很大**:静态方案不需要方案 B、不需要编译期 visibility、不需要链接参数隔离,
只需要把 chdb-go 现有的 #30 保护接到静态路径上 —— 而 `spike/static-cgo` 分支里**已经带了这段逻辑**。

---

## 三、待验证实验(按信息量排序)

**建议执行顺序**:先做 **E4**(只读现成产物,不需要重新链接,几分钟,信息量最高),
再做 **E1**(决定性),然后按 E1 的结果决定要不要做 E2 / E3 / E6。

### E1 —— chdb-go 全套测试 + 静态,在 macOS 上【最决定性,需要 mac】

**理由**:这是唯一一个**只改一个变量**的对比。原始复现同时变了两件事(链接方式 + 缺 #30 保护),
所以它证明不了链接方式有问题。`spike/static-cgo` 分支把「怎么拿到引擎」抽成 `loadEngine()`
的两份实现,上层代码、信号保护、测试全部一致 —— **切换 build tag 就只改链接方式。**

```bash
# 在 mac 上
git fetch && git checkout spike/static-cgo   # 若未推送:见文末「分支状态」

mkdir -p /tmp/chdb-static && cd /tmp/chdb-static
curl -sL -o s.tgz https://github.com/chdb-io/chdb-core/releases/download/v26.5.0/macos-arm64-libchdb-static.tar.gz
tar xzf s.tgz && rm s.tgz          # 得到 libchdb.a (1.07 GiB)、chdb.h、chdb.hpp

cd <chdb-go repo>
export CGO_CFLAGS="-I/tmp/chdb-static"
export CGO_LDFLAGS="/tmp/chdb-static/libchdb.a -lc++ -lc++abi -lz -lbz2 -llzma -lm -ldl -lpthread -liconv \
  -framework CoreFoundation -framework Security -framework SystemConfiguration -framework IOKit"

go test -tags chdb_static -count=1 -p 1 -timeout 900s ./chdb/
go test -tags chdb_static -count=1 -p 1 -timeout 900s ./chdb/driver/
go test -tags chdb_static -count=1 -p 1 -race -timeout 900s ./chdb/
```

| 结果 | 判读 | 后续 |
| --- | --- | --- |
| 全过 | **H1 成立**:原死锁是缺 #30 保护,不是链接方式。静态在 mac 上对 Go 也可用 | 方案 B / 路 1 / 路 2 **全部不必要**;静态变成一个正常的构建选项,剩下的只是 cgo 的固有代价 |
| 挂在 `chdb_connect` | H1 不成立(保护已就位仍挂) | 走 E2、E3 归因 |
| 过了但 `-race` 挂 | 信号/内存问题只在高压下暴露 | 抓 `sample` + 记录,按 issue #30 的路径继续 |

**成本**:一次链接约 4 分钟 × 3,加 220 MB 下载。磁盘峰值需 ~4 GB。

---

### E2 —— 给原始复现程序补上 #30 保护【归因,需要 mac】

**理由**:E1 用的是完整 chdb-go,变量多。E2 在**最小程序**上只加信号保护这一项,
直接测 H1。若 E1 全过而 E2 也从挂变成过,归因就闭环了。

在 `chdb-go-static-link-deadlock.md` 的复现程序上加这段(纯 C,不需要 purego):

```go
/*
#cgo CFLAGS: -I${SRCDIR}
#cgo LDFLAGS: ${SRCDIR}/libchdb.a -lc++ -lc++abi -lz -lbz2 -llzma -lm -ldl -lpthread -liconv
#cgo LDFLAGS: -framework CoreFoundation -framework Security -framework SystemConfiguration -framework IOKit
#include <stdlib.h>
#include <signal.h>
#include "chdb.h"

// Go runtime 依赖这批信号:SIGSEGV 用于栈增长与 nil 检查,SIGURG 用于异步抢占。
// chdb_set_signal_handlers_enabled(0) 会把它们打回 SIG_DFL(头文件未提及此副作用),
// 而 SIGURG 的默认动作是「忽略」,抢占信号一旦被丢弃,stop-the-world 就再也完不成。
static struct sigaction saved[6];
static int prot[6] = {SIGILL, SIGABRT, SIGFPE, SIGBUS, SIGSEGV, SIGURG};
static void snap(void) { for (int i = 0; i < 6; i++) sigaction(prot[i], NULL, &saved[i]); }
static void rest(void) { for (int i = 0; i < 6; i++) sigaction(prot[i], &saved[i], NULL); }
*/
import "C"
```

```go
// 1) 关掉 libchdb 自己装处理器,并把 Go 的处理器放回去
C.snap()
C.chdb_set_signal_handlers_enabled(0)
C.rest()

// 2) connect 首次调用仍会重置那批信号,所以这里也要包一层
C.snap()
conn := C.chdb_connect(1, (**C.char)(unsafe.Pointer(&argv[0])))
C.rest()
```

想进一步细分归因,可以拆成两个变体各跑一次:

- **E2a**:只做「保存 / 恢复」,**不调** `chdb_set_signal_handlers_enabled(0)` → 测「是不是 connect 自己弄坏的」
- **E2b**:只调 `set_enabled(0)`,**不做**保存恢复 → 测「是不是这个调用的副作用弄坏的」

| 结果 | 判读 |
| --- | --- |
| 加了保护就不挂 | **H1 确立**,根因是信号,与 C++ 串扰无关 |
| 仍然挂 | H1 被否;此时 E3 的权重上升 |
| E2a 过 / E2b 挂 | 是 `set_enabled(0)` 的副作用为主 |
| E2a 挂 / E2b 过 | 是 `chdb_connect` 自身重置处理器为主 |

**成本**:每个变体一次链接约 4 分钟。

---

### E3 —— 去掉 `-lc++ -lc++abi`【归因 H2,需要 mac】

**理由**:原复现的 LDFLAGS 显式引了第二份 C++ runtime。archive 自带一整份,这两个 flag 本就多余。
Rust 侧大概没加(**需要确认,见 E4**),这就是两个实验之间的另一处差异。

```bash
# 复现程序的 LDFLAGS 去掉 -lc++ -lc++abi,其余不变
go build -o b-nocxx . && ./b-nocxx
```

| 结果 | 判读 |
| --- | --- |
| 不再挂 | H2 成立:cgo 默认引入的第二份 runtime 是原因之一。修法极小(chdb-go 的 cgo LDFLAGS 不要加这两个) |
| 一堆 undefined symbol | 说明 archive 并不自足,本身就是有用信息 —— 记录缺哪些符号 |
| 仍然挂 | H2 被否 |

---

### E4 —— Rust 那边到底是怎么链的【关键对照,需要 mac】

**理由**:Rust 通过而 Go 挂,差异一定落在**链接命令**或**runtime 行为**上。E4 把链接命令的差异读出来。

```bash
cd <chdb-rust repo>
cargo build --features static -v 2>&1 | grep -A2 'rustc-link' | head -40   # build.rs 发了哪些 link 指令
cargo rustc --features static -- --print link-args 2>&1 | tr ' ' '\n' | grep -E 'lc\+\+|hidden|exported|libchdb|framework'
```

具体要看三件事:

1. **有没有 `-lc++` / `-lc++abi`**。没有 → 支持 H2。
2. **有没有 `-hidden-l` / `-load_hidden` / `-exported_symbols_list`**。有 → 路 2 的隔离在 Rust 侧被**意外满足**了,那反过来证明修法方向正确。
3. **两个二进制的符号归属对比**:

```bash
nm -m <go-bin>   | grep -E '__Znwm|__ZdlPvm|__ZNSt3__16localeD1Ev'
nm -m <rust-bin> | grep -E '__Znwm|__ZdlPvm|__ZNSt3__16localeD1Ev'
otool -L <go-bin>; otool -L <rust-bin>
```

关键差别看 `external` vs `private extern`:

| 观察 | 含义 |
| --- | --- |
| Rust 侧是 `private extern`,Go 侧是 `external` | 通道在 Rust 侧关着、在 Go 侧开着 → **H3 复活**,且路 1/路 2 就是正解 |
| 两侧都是 `external` | 通道两侧都开,但只有 Go 挂 → 差异在 runtime,**H1/H2 胜出** |

这一项**信息量最高、成本最低**(不需要重新链接,只读现成产物)。**建议第一个做。**

---

### E5 —— chdb-go 全套测试 + 静态,在 Linux 上【✅ 已完成,通过】

**理由**:填矩阵里空着的一格。此前 Linux 上只跑过最小程序;封装版在链接期就撞了 COMDAT,
所以「chdb-go 全套 + 静态」从没跑过。用 raw archive + `spike/static-cgo` 的 cgo 绑定
(含 issue #30 信号保护)。

| 包 | 结果 |
| --- | --- |
| `./chdb/` | ok 1.03s |
| `./chdb/driver/` | ok 3.47s |
| `./chdb/` `-race` | ok 1.49s |
| `./chdb-purego/` | ok 11.79s |

起初 `./chdb-purego/` 有两个失败,都是**动态路径专属断言**,不是引擎问题
(`CHDB_LIB_PATH` 在静态下没有可覆盖的对象;`LoadedLibraryPath` 返回的是一句说明而非文件路径)。
已加 `staticallyLinked` 常量让它们在静态下自我跳过 —— **所以在 mac 上跑 E1 时不会再看到这两个失败,
如果看到了,说明是别的问题。**

**判读**:Go 侧的静态路径本身没有问题。矩阵里只剩 macOS 一格待定,
且它与「Rust 在 macOS 通过」并列,进一步把归因推向 H1/H2 而不是平台。

---

### E6 —— 路 2 的 macOS 半边:`-hidden-lchdb`【条件性,需要 mac】

**只在 E1/E2 判定 H1 不成立时才需要。** 若 H1 成立,隔离根本不必做。

```bash
# 复现程序改用 -hidden-l 加载 archive,让它的全局符号全部 hidden
#cgo LDFLAGS: -L${SRCDIR} -hidden-lchdb ...
# 或 Xcode 15+:
#cgo LDFLAGS: -Wl,-load_hidden,${SRCDIR}/libchdb.a ...
```

| 结果 | 判读 |
| --- | --- |
| 不再挂 | **路 2 成立且今天可用**;路 1 是同机制的更彻底版本,提 chdb-core 时有实证 |
| 仍然挂 | 条件②不是唯一原因,才轮到怀疑两份 libc++ 的 ABI 配置差异 |

---

### E7 —— Rust + 静态,在 Linux 上【低优先,我这边可做】

**理由**:只为矩阵对称性。预期通过(Linux 上 Go 已通过,且 Rust 更不依赖 runtime 行为)。
**若意外失败,信息量极大** —— 那说明差异根本不在平台也不在信号,而在链接细节。

---

## 四、结果如何改变决策

| 若…… | 则 |
| --- | --- |
| E1 或 E2 显示 H1 成立 | **静态成为可选构建方式**;方案 B、路 1、路 2 全部作废;`spike/static-cgo` 从 spike 升级为候选特性;向 chdb-core 提的只剩一条文档级建议:在 `chdb.h` 里写明 `chdb_set_signal_handlers_enabled(0)` 会把信号打回 SIG_DFL |
| E3 显示 H2 成立 | 修法是「cgo LDFLAGS 不要加 `-lc++ -lc++abi`」,同样不需要 chdb-core 改动 |
| E4 显示 Rust 侧符号是 private extern | H3 复活,**路 1(编译期 `-fvisibility=hidden`)是正解**,向 chdb-core 提构建配置变更 |
| E6 显示 `-hidden-l` 有效 | 路 2 立即可用,由 chdb-core 产品化链接参数(pkg-config / CMake config / cgo 片段) |
| 以上全否 | 静态继续搁置,`.so` 是唯一方案(它今天在所有平台所有语言都工作) |

**无论结果如何,有一条不依赖任何实验**:静态必须回到 cgo,代价是用户侧要 C 工具链、
不能 `CGO_ENABLED=0`、交叉编译从一条命令变成每目标一套工具链、C++ 崩溃 Go 的 `recover` 抓不到。
而 issue #19 恰好是用户要求摆脱 cgo。**这部分是产品决策,不是技术问题。**

---

## 五、记录要求

每个实验请记录这四项,缺一项归因就会含糊:

1. **完整链接命令**(`go build -x` 或 `cargo rustc -- --print link-args` 的输出)
2. **`otool -L` 与 `nm -m | grep` 的原始输出**,而不是「有/没有」的结论
3. **挂住时的 `sample $(pgrep -f ...) 5 -mayDie -f stack.txt`**,全部线程,不只主线程
4. **环境三元组**:macOS 版本、Xcode/clang 版本、Go/rustc 版本

结果回填到 `chdb-go-static-link-findings.md`,并把该文档里「macOS 与语言无关」的段落按新证据改写
—— 那一段现在已知是错的。

---

## 六、相关分支与文件状态

| | |
| --- | --- |
| `feat/dynamic-engine` | PR #44,CI 6/6 绿。**待一次 `git push --force-with-lease`**(force-push 被权限分类器拦截) |
| `spike/static-cgo` | cgo 绑定,**未提交、未推送**。E1/E5 依赖它。要在 mac 上跑 E1,得先推上去 |
| `chdb-purego/binding_cgo.go` | `//go:build chdb_static`,cgo 实现,含 #30 信号保护 |
| `chdb-purego/binding_purego.go` | `//go:build !chdb_static`,原 purego 实现 |
| `~/static-lab/`(本机) | 验收检查器 `verify-static.sh`、变体驱动 `run-variant.sh`、最小程序、符号清单。`libchdb.a` 1.59 GiB 尚在 |
