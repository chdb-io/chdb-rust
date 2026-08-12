# libchdb.a 静态链接实测结论

`chdb-go-static-link-deadlock.md` 提出的任务的执行结果。那份文档记录了 macOS arm64 上
静态链接 `libchdb.a` 后在 `chdb_connect` 里死锁,并给出「两份 C++ runtime + 两个 allocator」
的假设。本文是在 Linux x86_64 上把这个假设逐条验证之后的结论。

**核心结论三句话:**

1. **Linux 上静态链接开箱可用,没有死锁。** raw `libchdb.a` + cgo,`chdb_connect` 0.06 秒返回,
   16000 次并发查询通过。macOS 的失败不是「静态链接不行」,是 macOS 特有的。
2. **失败需要三个条件同时成立**,而 Linux 只满足第一个的一半。根因不是「进程里有两份 runtime」,
   而是「两份之间解析方向不一致,且两份都在执行代码」。
3. **修法在「可见性」这一层,不在「绑定」这一层。** 事后用 objcopy 把符号改成 local(方案 B)
   会和 C++ 的 COMDAT 去重打架,实测在链接 chdb-go 测试套件时失败。

所有静态结论都是 **Linux x86_64** 上的。macOS 那一半**未经验证**,只有基于机制的预测。

---

## 一、动态链接部分的状态(PR #44)

与静态无关,但同一会话完成,记录在此以免遗漏。

`chdb-io/chdb-go#44`(draft,基于 #42、含 #43 的 commit),CI **6/6 全绿**:
`build_linux`、`build_mac`、`consume_as_published_module`(ubuntu / macos-14)、
`embedded_engine`(ubuntu / macos-14)。后者是 #43 从未跑过的任务,首次运行即在两个平台通过。

修掉四个问题:

| 问题 | 性质 |
| --- | --- |
| 12 个进程并发冷启动各写自己的 507 MiB 临时副本,4.7 GiB 空闲的机器上全部 ENOSPC | 真 bug。加了按 digest 命名的 flock,仍是纯优化:不支持 flock 或持有者卡住都退回无锁提取 |
| `git archive` 没排除嵌套模块,`go get` 报 `go.mod file not in module root directory` | 验证任务在一个用户拿不到的 archive 上失败 |
| packaging 恒用 zstd -19,runner 上要跑几十分钟 | 压缩比是发版属性,不是该任务要验的东西。改 `ZSTD_LEVEL`,验证用 3 → 1m31s |
| `macos-13` 标签已下线,release 任务等 24 小时后被取消,v2.0.0 / v2.0.1 都没发出 macOS 产物 | 改 macos-14,产物名同时改成 `macos-arm64` |

**待办**:分支 `feat/dynamic-engine` 需要一次 `git push --force-with-lease`
(本地已 rebase,修正了作者身份 —— 这是 `license/cla` 一直 pending 的原因)。

---

## 二、静态链接实测

环境:Ubuntu 24.04 / x86_64,Go 1.24.5,gcc 13.3,binutils 2.42,lld 21,chdb-core `v26.5.0`。
`linux-x86_64-libchdb-static.tar.gz` 解出的 `libchdb.a` 为 **1.59 GiB / 8551 个成员**。

### 2.1 raw archive 直接可用

最小 cgo 程序,LDFLAGS 只有 `libchdb.a -lm -ldl -lpthread -lrt`:

| | |
| --- | --- |
| 链接耗时 | 57 秒(macOS 上那次记录是约 4 分钟) |
| 二进制 | 642,180,760 字节(613 MiB,未 strip) |
| `chdb_connect` | **0.06 秒返回**,无死锁 |
| 查询 | `SELECT 41+1` = 42;聚合、`formatDateTime`、`groupArray`、`GROUP BY` 混合负载通过 |
| 并发 | 16 worker × 200 轮 = 3200 次查询,0 失败;加压到 16000 次仍 0 失败 |
| Go runtime | 深递归 + 分配(触发栈增长信号)在 connect 之后仍正常 |

符号层面同样干净:`ldd` 只有 libm / libc,**进程里没有第二份 C++ runtime**;13 个「必须唯一」的
符号(`operator new/delete` 全套、`__cxa_*`、unwinder)各有且仅有 1 个全局定义,且**都不在动态符号表里**;
动态导出的 mangled C++ 符号 **0 个**。

### 2.2 三条件模型

对着假设逐个开关,得到失败需要的三个条件:

| 条件 | Linux 实测 | macOS |
| --- | --- | --- |
| **① 进程里有两份同款 C++ runtime** | 默认没有。加 `-lc++ -lc++abi` 后 ELF 的 `--as-needed` 发现 libc++.so.1 一个符号都没被用到,**直接删掉这个依赖**,`ldd` 里看不见 | Apple 框架(CoreFoundation / Security / IOKit)自己链着 libc++.1.dylib。**躲不掉** |
| **② 两份之间有绑定通道** | `--no-as-needed` 强制后通道确实打开:2052 个 mangled 符号进了 `.dynsym`,检查器把 12 个单例符号判为 FAIL。**但方向一致** —— ELF 规定可执行文件在查找顺序里永远第一,所以全场都绑到同一份,两份塌缩成一份 | Mach-O 的 two-level namespace + weak 定义合并,选中的是**先加载的 image**,而这**可以逐符号不同**。死锁栈就是证据:`ios_base::~ios_base` 与 `locale::~locale` 在系统 libc++ 执行,`operator delete` 解析回二进制内部 |
| **③ 两份都真的在执行代码** | 那份 .so **空转**。用 `LD_PRELOAD` 把系统 libc++ 塞到最前面,观测到它有 409 个引用绑进了可执行文件、455 个绑回自己 —— 依然跑通,因为没人调用它的代码 | Apple 框架**自己就在用**系统 libc++,会构造/析构 string、iostream、locale。于是「A 造 B 拆」真实发生 |

**结论**:Linux 的链接器和加载器帮你把第二份去掉了(或者让它闲着);macOS 既去不掉、又让它真的参与工作、
而且参与方式逐符号不同。同一个 `.a`,在 Linux 上是干净的单 runtime,在 macOS 上是两份交叉执行。

顺带解释了 `.so` 为什么到处都能用:它只对外露 **48 个符号、0 个 mangled**(实测
`nm -D --defined-only libchdb.so`),自带的 libc++ 从外面根本看不见,条件 ② 被从根上掐断。

### 2.3 archive 的事实(推翻了几个先前假设)

- **自带 libc++**:`std::__1` 前缀符号 69528 个;libstdc++ 风格 0 个。
- **没有 jemalloc**:`je_` / `_rjem_` 前缀符号 **0 个**;`malloc` / `free` / `calloc` 全部是
  `U ...@GLIBC_2.2.5`(外部引用)。内部只定义 `operator new/delete`。
  → 原假设里的「jemalloc 半接管」在 Linux 静态包上**无对象**,「关掉 jemalloc」这个选项
  在这里没有性能代价可谈,因为没东西可关。
- **带 `-ffunction-sections`**:section 名形如 `.text._ZNSt3__17collateIcED2Ev`。
- **libc++ 符号大多是 DEFAULT 可见性**:抽查 `libcxx__locale.cpp.o` 得 993 DEFAULT / 217 HIDDEN。
  → `.so` 的隔离是靠**链接期的 version script**,不是编译期 visibility。这一点决定了后面的修法选择。

### 2.4 方案 B 实测:结构目标达成,但撞上 COMDAT

方案 B = 预链接成一个 relocatable object,再把除 C API 之外的符号全部降级为 local。

**为什么必须先预链接**:成员 A 引用 `operator new`,定义在成员 B 里。按成员本地化会让 A 绑不上 B,
直接链接失败。只有合成**一个链接单元**后内部引用已解析完毕,才能降级符号。这也是它必然产生
单体 `.o` 的原因。

配方与耗时:

```bash
ld.lld-21 -r --whole-archive libchdb.a -o sealed.o          # 13.5 秒,1.31 GiB
objcopy --keep-global-symbols=EXPORTS.txt sealed.o sealed-local.o   # 4m35s,1.20 GiB
```

> `ld -r`(bfd)在这个 archive 上跑了 12 分钟仍未写出输出,被换成 lld。ClickHouse 自己就用 lld 构建,
> 所以对 chdb-core 侧的脚本来说 lld 是自然选择 —— 但注意 2.4.2 里 lld 的一个坑。

**达成的(全部实测):**

| 指标 | 结果 |
| --- | --- |
| 全局定义符号 | **563,473 → 47** |
| 导出面 vs `.so` 的 48 个契约 | 几乎完全一致。少的 2 个(`__start_pb_defaults` / `__stop_pb_defaults`)是链接器为 section 合成的封装符号,只存在于最终 `.so` 链接里,archive 中本就没有;多的 1 个见下 |
| 强未定义符号 | 475 个,其中 **471 个**由 glibc / libm / pthread / dl / rt / ld.so 满足;剩 4 个是 crt 与链接器生成的(`__dso_handle`、`atexit`、`__start/__stop_pb_defaults`)→ **零真实外部依赖** |
| 13 个单例符号 | 全部变成 **LOCAL、0 个全局、都不在 `.dynsym`**。此时进程里 libc++.so.1 / libc++abi.so.1 / libunwind.so.1 是真实加载的 → **两份共存、零通道** |
| 体积 | 641,744,064 字节,比 raw 版本**小 0.4 MB**。先前预测的膨胀不存在,因为 archive 带 `-ffunction-sections` |
| 最小程序 | 标准跑、16×1000 加压跑、`LD_PRELOAD` 跑,全过 |

#### 2.4.1 撞墙:`defined in discarded section`

用同一个 `sealed-local.o` 链接 **chdb-go 真实测试套件**时失败,几十条同形错误:

```
`_ZN2DB4PackD0Ev' referenced in section `.data.rel.ro._ZTVN2DB4PackE'
  ... defined in discarded section `.text._ZN2DB4PackD0Ev'
```

`_ZTVN2DB4PackE` 是 `DB::Pack` 的虚函数表,`_ZN2DB4PackD0Ev` 是它的析构 —— **vtable 指向了一个被丢弃的函数。**

机制:C++ 的 inline 函数、模板实例、vtable、typeinfo 会在每个用到它的编译单元各生成一份相同副本,
打包成 section group(COMDAT),链接时只保留一份、其余整组丢弃 —— 这是「同一个类的 vtable 全进程
只有一个地址」的保证。把符号降级为 local 之后,链接器做组去重时无法再把被丢弃组里的 local 符号
匹配到保留的那份,引用就悬空了。

**特别值得警惕**:最小程序**没有**触发这个错误,只有真实测试套件触发。也就是说
**B 的问题会随着用到多少引擎功能而逐步暴露,不是一个确定性的开关。**

#### 2.4.2 顺带抓到的两个同类缺陷

- **`OPENSSL_ia32cap_P` 逃出封装**。`nm` 类型为 `C`(COMMON,存储尚未分配),objcopy 无法把它变 local。
  在最小例子上验证:`ld -r -d`(强制分配 common)后变成 `b`(本地 BSS),封住了。
- **lld 接受 `-d` 但静默忽略**。同一个最小例子里,`ld.lld-21 -r -d` 后符号仍是 `C`。
  静默失效,正是 chdb-core PR #76 那种坑的形状。所以配方若要用 `-d`,必须走 bfd
  (可两阶段:lld 快速合并 → bfd 对单个对象再做一次 `-r -d`)。

#### 2.4.3 有一个「能让 B 链接通过」的办法,但不建议

`--force-group-allocation` 强制解散所有 group、副本全部保留。链接能过,但代价是**同一个类的
vtable / typeinfo 在进程里有多个地址**。而 `catch (const DB::Exception&)`、`dynamic_cast`
是靠 typeinfo 的**地址**判断类型匹配的(libc++abi 通常按指针比较,而能这么做的前提正是符号唯一)。
地址不唯一 → 异常可能匹配不上本该匹配的 handler。

**这是把一个链接期的明确错误,换成一个运行期的隐蔽风险。** 交换方向是错的。

### 2.5 chdb-go 的 cgo 绑定(分支 `spike/static-cgo`,未提交)

为了让 chdb-go **真实测试套件**能覆盖静态构建而写的最小改动:只替换「符号绑定」这一层。

- `chdb-purego/binding.go`:`ensureLoaded()` 改为调用 `loadEngine()`。
- `chdb-purego/binding_purego.go`(`//go:build !chdb_static`):原 `bindSymbols` 移入,
  `loadEngine()` 走 `openLibrary()` + purego。
- `chdb-purego/binding_cgo.go`(`//go:build chdb_static`):`loadEngine()` 用 cgo 填同一批
  函数变量,并做同样的 issue #30 信号处理保护。

上层 sessions / `database/sql` driver / streaming **零改动**。`go vet -tags chdb_static` 干净通过。
引擎路径不写死在代码里,构建时传入:

```bash
CGO_CFLAGS="-I<headers>" \
CGO_LDFLAGS="<engine.o or .a> -lm -ldl -lpthread -lrt" \
go build -tags chdb_static ./...
```

实现里有两个非显然的点,都在文件注释里说明了:`chdb_connect` 的 argv 是 Go 指针数组,
直接传给 C 会触发 cgo 的指针规则检查而中止进程,必须拷进 C 内存;`chdb_result_error`
在 purego 下把 NULL 转成空字符串,调用方按空字符串判断,cgo 侧必须一致。

---

## 三、对其他语言的影响

**与语言基本无关。** 决定因素两个都不在语言侧:产物自带一份 DEFAULT 可见性的 libc++;
平台的链接/加载规则。Go 那侧唯一的「贡献」是 cgo 默认加 `-lc++ -lc++abi`,但就算不加,
macOS 上第二份也躲不掉。

| 消费方式 | macOS | Linux |
| --- | --- | --- |
| Go(cgo)+ `.a` | 死锁(已复现) | 实测正常 |
| Rust(build.rs)+ `.a` | 同样风险、同样机制 | 正常(前提见下) |
| C / C++ 宿主 + `.a` | 同样风险 | **前提可能不成立** |
| 任何语言 + `.so` | 正常 | 正常 |

佐证:chdb-core PR #76 的 abseil/protobuf mutex 死锁没有 Go 参与,是纯 C++ 消费方,同一形状。
反过来 Python wheel 与 chdb-node 从未遇到 —— 它们用的是 `.so`。

**一个需要单独提醒的扩展(未实测,基于已验证机制的预测)**:Linux 的安全靠条件 ③ 不成立
—— 那份系统 libc++ 空转。这对 Go / Rust 成立(跨边界只走 C API,自己不用 std::)。
但对一个**自己就在用 C++ 标准库的宿主程序**,静态定义优先于动态定义,它那些 `std::string`、
`std::locale` 调用会绑到 **archive 里那份 libc++ 的实现**,而头文件来自系统 libc++。
两份编译配置若不同(ABI 宏、hardening),就是「按 A 的图纸写、用 B 的代码读」。

→ **在静态包里打包一份 C++ runtime,这个决定会传染,它约束了下游能怎么用 C++。**

---

## 四、修法分层

B 的目标(隔离)是对的,**层次错了**:`.so` 是在链接期声明「对外导出什么」,只改变符号在动态符号表里的
可见性,COMDAT 去重照常工作;B 是事后改写符号的**绑定语义**,把去重机制的地基抽掉了。
B 的失败方式本身指出了正解必须做在「可见性」这一层。

| | 做法 | 判断 |
| --- | --- | --- |
| **路 1 根治(producer 侧)** | 给 archive 里打包的 libc++ 及依赖加 `-fvisibility=hidden`,C API 标 `visibility("default")` | **对路。** 可见性是编译期符号属性,COMDAT 按名字/组签名去重,**不受影响**;符号进不了 `.dynsym`,通道从根上没有;**Mach-O 里 hidden 变成 `private_extern`,而 private_extern 不参与跨 image 的 weak 定义合并** —— 正好掐断 macOS 那个「逐符号选方向」的机制。零体积代价。成本是重编 + 过一遍 CI,极少数依赖 weak 跨 TU 覆盖的代码要单独放行 |
| **路 2 今天可用(consumer 侧,由 producer 产品化)** | Linux `-Wl,--exclude-libs,ALL`;macOS `-hidden-lchdb` 或 `-load_hidden`(Xcode 15+)。由 chdb-core 以文档 / pkg-config / CMake config 形式随产物发布 | 只改可见性、不改绑定,**不会撞 COMDAT**。缺点是依赖下游正确传参,但由发布方给出可复制的配置远好于每个语言各自摸索 |
| **路 3 退路** | 继续用 `.so`(即 chdb-go 现在的 PR #44) | 今天在所有平台、所有语言都正常。**永远存在的退路**,所以这件事不紧急 |
| ~~方案 B~~ | 预链接 + objcopy 本地化 | **不应作为发布方案。** 与 COMDAT 冲突,且暴露程度随用到的功能增长 |
| ~~方案 C~~ | 关 jemalloc / 去掉 allocator 拦截器 | 在 Linux 静态包上**无对象可关** —— 里面没有 jemalloc |

### 能证明的与不能证明的

- **已证明**:Linux 静态可用;三条件模型(三个条件逐个开关过);B 的 COMDAT 障碍是硬的,
  不是实现问题;COMMON 逃逸有确定修法且 lld 会静默忽略 `-d`;体积零代价。
- **未证明**:路 1 / 路 2 在 macOS 上真的修好了死锁。模型**预测**能修(都作用在条件 ②),
  但本机没有 macOS。

### 最便宜的下一步验证

在一台 mac 上,用现成的 `macos-arm64-libchdb-static.tar.gz` 加一行 `-hidden-lchdb`,
跑 `chdb-go-static-link-deadlock.md` 里那个最小 cgo 程序。**约 30 分钟,不需要重编 chdb-core。**
两种结果都有用:

- 成功 → 路 2 立即可用;路 1 是同一机制的更彻底版本,提给 chdb-core 时有实证。
- 失败 → 说明条件 ② 不是唯一原因,才轮到怀疑两份 libc++ 的 ABI 配置差异。

---

## 五、复现方法

实验室目录 `~/static-lab`,产物清单:

| 文件 | 内容 |
| --- | --- |
| `verify-static.sh <binary> [sealed-object]` | 静态验收检查器。判定两条性质:① 每个「必须唯一」的符号至多一个**全局**定义,额外的 local 定义反而是目标(local 无法被外部绑定 = 真隔离),且可执行文件不得把它们再导出;② 强未定义符号必须是平台基础库能提供的子集 |
| `run-variant.sh <name> <ldflags...>` | 构建 / 验证 / 运行一个链接变体,日志留在 `log-<name>.txt`。600 MiB 的二进制跑完即删 |
| `prog/main.go` | 最小 cgo 测试程序。不只 `SELECT 1`:走 locale 相关格式化、字符串、多线程聚合,并发跑,最后检查 Go runtime 仍健康 |
| `EXPORTS.txt` | 导出契约,来自 `nm -D --defined-only libchdb.so`(而不是手写 —— 已发布的 `.so` 导出什么,就是被生产验证过的集合) |
| `IMPORTS-raw.txt` / `STRONG-UNDEF.txt` / `PLATFORM-PROVIDES.txt` / `UNSATISFIED.txt` | 未定义符号分析的中间产物 |
| `log-twocxx-forced.txt` | 强制两份 runtime 时的检查器输出(12 个 FAIL,通道打开的证据) |
| `log-sealed.txt` | 封装后的检查器输出(全部 LOCAL,零通道) |

四个链接变体及结论:

```bash
# ① baseline:raw archive,无隔离参数 → 通过
./run-variant.sh baseline '${SRCDIR}/libchdb.a -lm -ldl -lpthread -lrt'

# ② 加 -lc++:无效果,--as-needed 直接丢弃依赖 → 通过(条件 ① 未满足)
./run-variant.sh twocxx '${SRCDIR}/libchdb.a -lc++ -lc++abi -lm -ldl -lpthread -lrt'

# ③ 强制两份:通道打开(12 个 FAIL),但方向一致 → 仍通过
./run-variant.sh twocxx-forced '${SRCDIR}/libchdb.a -Wl,--no-as-needed -lc++ -lc++abi -Wl,--as-needed -lm -ldl -lpthread -lrt'
LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libc++.so.1 ./b-twocxx-forced   # 混合绑定方向,仍通过

# ④ 封装后:全部 LOCAL、零通道 → 最小程序通过,chdb-go 测试套件链接失败(COMDAT)
./run-variant.sh sealed '${SRCDIR}/sealed-local.o -Wl,--no-as-needed -lc++ -lc++abi -Wl,--as-needed -lm -ldl -lpthread -lrt'
```

磁盘是这台机器的硬约束(全程 3–5 GB 空闲)。`.a` 1.59 GiB、`sealed.o` 1.31 GiB、
每个二进制 613 MiB,必须逐步清理。若要重跑,`libchdb.a` 从 chdb-core release 重新下载约 3 分钟。

---

## 六、待决策

1. **静态这条路怎么继续**:
   (a) 试路 2 的 Linux 半边(`--exclude-libs,ALL` + chdb-go 全套测试,约 15 分钟,archive 已就位);
   (b) 继续攻 B(需先接受 2.4.3 的 typeinfo 地址唯一性风险,**不建议**);
   (c) 结论已足够,直接向 chdb-core 提路 1;
   (d) 静态继续搁置。
2. **`spike/static-cgo` 分支**:留在本地 / 开 draft PR 存档 / 删除。
3. **`~/static-lab` 里的 1.59 GiB `libchdb.a`**:留着继续实验 / 清掉。
4. **PR #44** 需要一次 `git push --force-with-lease`(权限分类器拦截了 force-push)。
