# 流式处理比对 实现计划

> **面向 AI 代理的工作者：** 必需子技能：使用 superpowers-zh:subagent-driven-development（推荐）或 superpowers-zh:executing-plans 逐任务实现此计划。步骤使用复选框（`- [ ]`）语法来跟踪进度。

**目标：** 为对账框架增加 STREAM 模式：两侧数据以有序流方式逐条消费，内存占用与总条数无关，差异一产生即回调调账。

**架构：** 新增一条与现有 MEMORY 模式平行的流式流水线。`OrderedResourceLoader` 返回按键升序的 `Stream<CheckEntry>`，`StreamingCheckProcessor` 用 merge join 单遍归并两侧流，每条差异立即回调 `CheckSync` 后丢弃，只在 `CheckSummary` 中保留计数与少量样本。`CheckExecutor` 按 `CheckConfig.mode` 分流组装。

**技术栈：** Java 21、Lombok 1.18.34、Hutool 5.5.8、JUnit 5.11.3、Maven（`jdkToolchain` 固定 21）

**规格：** `docs/superpowers/specs/2026-11-05-streaming-reconciliation-design.md`

---

## 全局约束

- 构建必须使用 **JDK 21 toolchain**（`pom.xml` 已为 compiler 与 surefire 固定 `<version>21</version>`）。`~/.m2/toolchains.xml` 须指向 zulu 21。默认 JDK 是 25，直接 `mvn compile` 会因 Lombok `NoSuchFieldException: com.sun.tools.javac.code.TypeTag :: UNKNOWN` 失败。
- 提交信息使用 Conventional Commits（英文），前缀 `feat:` / `test:` / `docs:` / `refactor:`。
- **MEMORY 路径零行为变更**：现有 `ResourceLoader`、`CheckResult`、`DefaultCheckProcessor`、`DefaultAfterCheckProcessor` 的签名与行为一律不动；现有测试文件一个都不改。
- `CheckExecutor.buildExecutor` 现有行为保持不变：它**不读取** `CheckConfig.beforeCheckProcessor` 与 `CheckConfig.afterCheckProcessor`（用户须用 `CheckExecutor.builder()` 直接传入）。STREAM 分支沿用同一约定，不在本次修正该既有缺口。
- 流式路径的差异**不保留全集**：`NO_SYNC` 明细无载体，需要重试的用户在 `CheckSync` 实现内部处理。
- 排序规则两侧必须一致（`String.compareTo` 语义）。框架只校验「每侧内部严格升序」，无法校验「两侧同序」。

## 文件结构

**新增主代码：**

| 文件 | 职责 |
|---|---|
| `src/main/java/io/github/kugin/reconciliation/enums/CheckMode.java` | `MEMORY` / `STREAM` 枚举 |
| `src/main/java/io/github/kugin/reconciliation/domain/CheckSummary.java` | 流式结果摘要（不可变，计数 + 样本） |
| `src/main/java/io/github/kugin/reconciliation/before/OrderedResourceLoader.java` | 有序流式数据源契约 |
| `src/main/java/io/github/kugin/reconciliation/before/OrderedResourceLoaderAdapter.java` | 把 `ResourceLoader` 排序后转成流（过渡用，全量驻载） |
| `src/main/java/io/github/kugin/reconciliation/before/StreamingBeforeCheckProcessor.java` | 流式前置处理：只做 `doPre` 校验，不加载数据 |
| `src/main/java/io/github/kugin/reconciliation/check/StreamingCheckProcessor.java` | merge join 主循环 |
| `src/main/java/io/github/kugin/reconciliation/after/StreamingAfterCheckProcessor.java` | 流式后置处理：`autoSync` 空操作 + `isComplete` 读 summary |

**修改主代码：**

| 文件 | 改动 |
|---|---|
| `src/main/java/io/github/kugin/reconciliation/domain/CheckConfig.java` | 新增 `mode` / `srcOrderedLoader` / `targetOrderedLoader` |
| `src/main/java/io/github/kugin/reconciliation/domain/CheckContext.java` | 新增 `summary` / `preCheckFailed` |
| `src/main/java/io/github/kugin/reconciliation/after/CheckSync.java` | `isComplete` 改为 summary / result 双分支 |
| `src/main/java/io/github/kugin/reconciliation/executor/CheckExecutor.java` | 按 `mode` 分流组装 + 日志分支 |

**新增测试：**

| 文件 | 覆盖 |
|---|---|
| `src/test/java/io/github/kugin/reconciliation/support/RecordingCheckSync.java` | 记录每次 sync 回调收到的 `CheckUnit`，供断言与对拍使用 |
| `src/test/java/io/github/kugin/reconciliation/domain/CheckSummaryTest.java` | 摘要不变量 |
| `src/test/java/io/github/kugin/reconciliation/after/CheckSyncIsCompleteTest.java` | `isComplete` 双分支 |
| `src/test/java/io/github/kugin/reconciliation/before/OrderedResourceLoaderAdapterTest.java` | 适配器排序语义 |
| `src/test/java/io/github/kugin/reconciliation/check/StreamingCheckProcessorTest.java` | merge join 算法 + 护栏 |
| `src/test/java/io/github/kugin/reconciliation/check/StreamingParityTest.java` | MEMORY / STREAM 对拍 |
| `src/test/java/io/github/kugin/reconciliation/executor/StreamingCheckExecutorTest.java` | 端到端状态流转 |

---

## 审查重点（Review Focus）

以下五类是规格隐含、但没有任务覆盖的失败模式。每个都有对应测试钉在指定任务里（测试名见对应任务）。

| # | 输入 / 条件 | 正常人期待的行为 | 钉住的测试 |
|---|---|---|---|
| 1 | 同一个 `CheckExecutor` 实例被两个线程同时处理**不同日期** | 两次运行的计数互不串味；流式处理器必须无实例状态 | 任务 3 ·「并发不同日期不串味」 |
| 2 | 渠道 loader 返回**已被消费过**的同一个 `Stream` 实例 | 立刻失败并给出可读原因，而不是静默比对出空结果 | 任务 3 ·「已消费的流」 |
| 3 | 两侧用了**不同的 key 比较器**（一侧 `String.compareTo`、一侧忽略大小写） | 框架无法自动发现；契约文档必须写明「两侧必须同序」，适配器两侧都用同一比较器 | 任务 2 ·「adapterSortsLexicographicallyOnBothSides」+ README 契约段 |
| 4 | `CheckSync` 回调内部**抛异常** | 比对中断，状态停在 CHECK；**已同步的副作用不回滚** | 任务 5 ·「sync 回调抛异常」 |
| 5 | `CheckEntry.key` 为 `null` | MEMORY 模式抛 NPE（`Collectors.toMap` 不接受 null key），STREAM 模式抛 `CheckException("对账 key 不能为 null")`——异常类型不同是已知差异 | 任务 3 ·「key 为 null」+「MEMORY 模式 null key」 |

---

### 任务 1：领域类型与配置扩展

**文件：**
- 创建：`src/main/java/io/github/kugin/reconciliation/enums/CheckMode.java`
- 创建：`src/main/java/io/github/kugin/reconciliation/domain/CheckSummary.java`
- 修改：`src/main/java/io/github/kugin/reconciliation/domain/CheckContext.java`
- 修改：`src/main/java/io/github/kugin/reconciliation/domain/CheckConfig.java`
- 修改：`src/main/java/io/github/kugin/reconciliation/after/CheckSync.java`
- 测试：`src/test/java/io/github/kugin/reconciliation/domain/CheckSummaryTest.java`
- 测试：`src/test/java/io/github/kugin/reconciliation/after/CheckSyncIsCompleteTest.java`

- [ ] **步骤 1：编写失败的测试 —— `CheckSummary` 不变量**

`src/test/java/io/github/kugin/reconciliation/domain/CheckSummaryTest.java`：

```java
class CheckSummaryTest {

    @Test
    void emptySummaryHasAllCountKeysAndNoSamples() {
        CheckSummary summary = CheckSummary.empty();

        assertEquals(0L, summary.getSourceTotal());
        assertEquals(0L, summary.getTargetTotal());
        assertEquals(0L, summary.getComparedCount());
        assertEquals(3, summary.getStateCounts().size());
        assertEquals(3, summary.getSyncCounts().size());
        assertEquals(0L, summary.getSyncCounts().get(CheckSyncEnum.NO_SYNC));
        assertNotNull(summary.getSamples());
        assertTrue(summary.getSamples().isEmpty());
    }

    @Test
    void builderNormalizesMissingCountKeysToZero() {
        CheckSummary summary = CheckSummary.builder()
                .syncCounts(new HashMap<>(Map.of(CheckSyncEnum.SYNC, 3L)))
                .build();

        assertEquals(3, summary.getSyncCounts().size());
        assertEquals(0L, summary.getSyncCounts().get(CheckSyncEnum.NO_SYNC));
        assertEquals(0L, summary.getSyncCounts().get(CheckSyncEnum.IGNORE));
        assertEquals(3L, summary.getSyncCounts().get(CheckSyncEnum.SYNC));
        assertNotNull(summary.getStateCounts());
        assertNotNull(summary.getSamples());
    }
}
```

- [ ] **步骤 2：运行测试验证失败**

运行：`mvn test -Dtest=CheckSummaryTest`
预期：编译失败 —— `找不到符号: 类 CheckSummary`。

- [ ] **步骤 3：创建 `CheckMode` 枚举**

`src/main/java/io/github/kugin/reconciliation/enums/CheckMode.java`：

```java
package io.github.kugin.reconciliation.enums;

/**
 * 对账执行模式
 *
 * @author Kugin
 */
public enum CheckMode {
    /**
     * 全量驻留: 两侧数据全部载入内存后比对 (默认, 现有行为)
     */
    MEMORY,
    /**
     * 流式比对: 两侧数据按键升序逐条消费, 内存与总条数无关
     */
    STREAM
}
```

- [ ] **步骤 4：创建 `CheckSummary`**

`src/main/java/io/github/kugin/reconciliation/domain/CheckSummary.java`。要点：

- `@Data` + 构造器上 `@Builder`（**不是**类上 `@Builder`）——这样 builder 产出的对象也必经构造器，计数 map 的补全不变量对所有构造路径都成立。
- 字段：`long sourceTotal` / `long targetTotal` / `long comparedCount` / `Map<CheckStateEnum, Long> stateCounts` / `Map<CheckSyncEnum, Long> syncCounts` / `List<CheckUnit> samples`。
- 构造器把传入的 map 复制为 `EnumMap`，并为**每个**枚举常量补 `0L`；`samples` 为 null 时置空列表。
- 静态工厂 `public static CheckSummary empty()`：返回 `CheckSummary.builder().build()`（由构造器补全为全 0）。

- [ ] **步骤 5：扩展 `CheckContext` 与 `CheckConfig`**

`CheckContext` 新增两个字段（`@Data @Builder` 已有，追加即可）：

```java
/**
 * 流式比对结果摘要; MEMORY 模式为 null
 */
private CheckSummary summary;

/**
 * 前置校验未通过标记; 仅 STREAM 模式使用, 为 true 时跳过比对阶段
 */
private boolean preCheckFailed;
```

`CheckConfig` 新增 `mode` 字段：

```java
/**
 * 执行模式: 默认为 MEMORY
 */
@Builder.Default
private CheckMode mode = CheckMode.MEMORY;
```

`CheckConfig` 的两个 ordered loader 字段（`srcOrderedLoader` / `targetOrderedLoader`，类型 `OrderedResourceLoader`）**在任务 2 步骤 1 添加** —— 该接口此刻还不存在，提前引用会导致编译失败。

- [ ] **步骤 6：编写失败的测试 —— `isComplete` 双分支**

`src/test/java/io/github/kugin/reconciliation/after/CheckSyncIsCompleteTest.java`：

```java
class CheckSyncIsCompleteTest {

    /** 不覆写 isComplete 的最小实现, 用于调用默认方法 */
    static class PlainSync implements CheckSync {
        @Override public CheckSyncEnum syncSourceMore(CheckUnit u) { return CheckSyncEnum.SYNC; }
        @Override public CheckSyncEnum syncTargetMore(CheckUnit u) { return CheckSyncEnum.SYNC; }
        @Override public CheckSyncEnum syncDifferent(CheckUnit u) { return CheckSyncEnum.SYNC; }
    }

    @Test
    void memoryModeUsesCheckResult() {
        CheckContext context = CheckContext.builder().build();
        context.setCheckResult(CheckResult.builder()
                .diffDetails(List.of(CheckUnit.builder().key("k").sync(CheckSyncEnum.NO_SYNC).build()))
                .build());

        assertFalse(new PlainSync().isComplete(context));
    }

    @Test
    void streamModeUsesSummary() {
        CheckContext context = CheckContext.builder().build();
        context.setSummary(CheckSummary.builder()
                .syncCounts(Map.of(CheckSyncEnum.NO_SYNC, 1L))
                .build());

        assertFalse(new PlainSync().isComplete(context));
    }

    @Test
    void streamModeWithOnlySyncIsComplete() {
        CheckContext context = CheckContext.builder().build();
        context.setSummary(CheckSummary.builder()
                .syncCounts(Map.of(CheckSyncEnum.SYNC, 4L))
                .build());

        assertTrue(new PlainSync().isComplete(context));
    }

    @Test
    void noSummaryAndNoResultIsNotComplete() {
        assertFalse(new PlainSync().isComplete(CheckContext.builder().build()));
    }
}
```

- [ ] **步骤 7：改造 `CheckSync.isComplete`**

`src/main/java/io/github/kugin/reconciliation/after/CheckSync.java` 的默认方法改为：

```java
default boolean isComplete(CheckContext context) {
    // 流式模式: 读摘要中的 NO_SYNC 计数
    CheckSummary summary = context.getSummary();
    if (summary != null) {
        return summary.getSyncCounts().getOrDefault(CheckSyncEnum.NO_SYNC, 0L) == 0;
    }
    // 全量模式: 原有逻辑, 逐字保留
    CheckResult checkResult = context.getCheckResult();
    if (checkResult == null) {
        return false;
    }
    return checkResult.getDiffDetails().stream()
            .noneMatch(v -> Objects.equals(CheckSyncEnum.NO_SYNC, v.getSync()));
}
```

需新增 `import io.github.kugin.reconciliation.domain.CheckSummary;`。

- [ ] **步骤 8：运行测试验证通过**

运行：`mvn test -Dtest='CheckSummaryTest,CheckSyncIsCompleteTest'`
预期：2 + 4 个测试全部 PASS。

- [ ] **步骤 9：Commit**

```bash
git add src/main/java/io/github/kugin/reconciliation/enums/CheckMode.java \
        src/main/java/io/github/kugin/reconciliation/domain/CheckSummary.java \
        src/main/java/io/github/kugin/reconciliation/domain/CheckContext.java \
        src/main/java/io/github/kugin/reconciliation/domain/CheckConfig.java \
        src/main/java/io/github/kugin/reconciliation/after/CheckSync.java \
        src/test/java/io/github/kugin/reconciliation/domain/CheckSummaryTest.java \
        src/test/java/io/github/kugin/reconciliation/after/CheckSyncIsCompleteTest.java
git commit -m "feat: add CheckMode, CheckSummary and stream-aware isComplete"
```

---

### 任务 2：有序数据源契约与适配器

**文件：**
- 创建：`src/main/java/io/github/kugin/reconciliation/before/OrderedResourceLoader.java`
- 创建：`src/main/java/io/github/kugin/reconciliation/before/OrderedResourceLoaderAdapter.java`
- 修改：`src/main/java/io/github/kugin/reconciliation/domain/CheckConfig.java`（补上任务 1 推迟的两个 loader 字段）
- 测试：`src/test/java/io/github/kugin/reconciliation/before/OrderedResourceLoaderAdapterTest.java`

- [ ] **步骤 1：补齐 `CheckConfig` 的两个 ordered loader 字段**

按任务 1 步骤 5 的说明追加 `srcOrderedLoader` 与 `targetOrderedLoader` 两个字段（类型 `OrderedResourceLoader`，javadoc 见任务 1）。至此任务 1 的 `CheckConfig` 改动完整，项目可编译。

- [ ] **步骤 2：编写失败的测试 —— 适配器排序语义**

`src/test/java/io/github/kugin/reconciliation/before/OrderedResourceLoaderAdapterTest.java`：

```java
class OrderedResourceLoaderAdapterTest {

    private static CheckEntry entry(String key) {
        return new CheckEntry(key, null, Map.of("amount", 1d));
    }

    @Test
    void adapterSortsByKeyAscending() {
        ResourceLoader delegate = date -> List.of(entry("c"), entry("a"), entry("b"));

        List<String> keys = new OrderedResourceLoaderAdapter(delegate)
                .loadOrdered("20210204")
                .map(CheckEntry::getKey)
                .toList();

        assertEquals(List.of("a", "b", "c"), keys);
    }

    @Test
    void adapterSortsLexicographicallyOnBothSides() {
        // 数字序与字典序不同: "10" < "9" 才是字典序, 契约要求两侧都用同一规则
        ResourceLoader delegate = date -> List.of(entry("9"), entry("10"));

        List<String> keys = new OrderedResourceLoaderAdapter(delegate)
                .loadOrdered("20210204")
                .map(CheckEntry::getKey)
                .toList();

        assertEquals(List.of("10", "9"), keys);
    }

    @Test
    void adapterPreservesAllEntries() {
        ResourceLoader delegate = date -> List.of(entry("a"), entry("b"));

        long count = new OrderedResourceLoaderAdapter(delegate).loadOrdered("20210204").count();

        assertEquals(2L, count);
    }
}
```

- [ ] **步骤 3：运行测试验证失败**

运行：`mvn test -Dtest=OrderedResourceLoaderAdapterTest`
预期：编译失败 —— `找不到符号: 类 OrderedResourceLoaderAdapter`。

- [ ] **步骤 4：创建 `OrderedResourceLoader` 接口**

`src/main/java/io/github/kugin/reconciliation/before/OrderedResourceLoader.java`。单方法接口，javadoc 写明四条契约：

1. 严格升序（`String.compareTo`），同侧 key 不得重复；
2. 单遍消费，返回的 Stream 不会被复用；**每次调用必须返回一个新的 Stream**；
3. 稳定快照，遍历期间底层数据不可变；
4. key 不得为 null。

签名：`Stream<CheckEntry> loadOrdered(String date);`

- [ ] **步骤 5：创建 `OrderedResourceLoaderAdapter`**

`src/main/java/io/github/kugin/reconciliation/before/OrderedResourceLoaderAdapter.java`。两个构造器：

- `OrderedResourceLoaderAdapter(ResourceLoader delegate)` —— 比较器取 `String::compareTo`；
- `OrderedResourceLoaderAdapter(ResourceLoader delegate, Comparator<String> keyComparator)`。

`loadOrdered` 实现：`delegate.load(date).stream().sorted(Comparator.comparing(CheckEntry::getKey, keyComparator))`。

类 javadoc 必须包含：**本适配器仍是全量驻载，`delegate.load(date)` 已把数据读进内存，不具备流式的内存特性**，仅用于渠道渐进迁移与对拍测试。

- [ ] **步骤 6：运行测试验证通过**

运行：`mvn test -Dtest=OrderedResourceLoaderAdapterTest`
预期：3 个测试全部 PASS。

- [ ] **步骤 7：Commit**

```bash
git add src/main/java/io/github/kugin/reconciliation/before/OrderedResourceLoader.java \
        src/main/java/io/github/kugin/reconciliation/before/OrderedResourceLoaderAdapter.java \
        src/main/java/io/github/kugin/reconciliation/domain/CheckConfig.java \
        src/test/java/io/github/kugin/reconciliation/before/OrderedResourceLoaderAdapterTest.java
git commit -m "feat: add OrderedResourceLoader contract and list adapter"
```

---

### 任务 3：merge join 核心 —— `StreamingCheckProcessor`

**文件：**
- 创建：`src/main/java/io/github/kugin/reconciliation/check/StreamingCheckProcessor.java`
- 创建：`src/test/java/io/github/kugin/reconciliation/support/RecordingCheckSync.java`
- 测试：`src/test/java/io/github/kugin/reconciliation/check/StreamingCheckProcessorTest.java`

- [ ] **步骤 1：创建 `RecordingCheckSync` 测试辅助类**

`src/test/java/io/github/kugin/reconciliation/support/RecordingCheckSync.java`：`public class RecordingCheckSync implements CheckSync`，持有一个 `List<CheckUnit> recorded`（`CopyOnWriteArrayList`）与一个可配置的返回值 `CheckSyncEnum result`（默认 `SYNC`）；三个 `syncXxx` 方法各自把入参加入 `recorded` 并返回 `result`。另提供 `public List<CheckUnit> getRecorded()`。

- [ ] **步骤 2：编写失败的测试 —— merge join 算法**

`src/test/java/io/github/kugin/reconciliation/check/StreamingCheckProcessorTest.java`。测试骨架：

```java
class StreamingCheckProcessorTest {

    private RecordingCheckSync sync;
    private CheckContext context;
    private ExecutorManager executorManager;

    @BeforeEach
    void setUp() {
        sync = new RecordingCheckSync();
        executorManager = new DefaultExecutorManager("streaming-unit");
        context = CheckContext.builder()
                .id("streaming-unit")
                .date("20210204")
                .executorManager(executorManager)
                .build();
    }

    private void run(List<CheckEntry> source, List<CheckEntry> target) {
        new StreamingCheckProcessor(
                date -> source.stream(),
                date -> target.stream(),
                sync).compare(context);
    }

    private static CheckEntry entry(String key, Object amount) {
        return new CheckEntry(key, null, Map.of("amount", amount));
    }
}
```

各用例断言（`run(...)` 之后断言 `sync.getRecorded()` 与 `context.getSummary()`）：

| 用例 | 输入 | 断言 |
|---|---|---|
| 完全一致 | src=`[a(1),b(1)]` tgt=`[a(1),b(1)]` | `recorded` 为空；`comparedCount==2`；`stateCounts.get(DIFFERENT)==0L` |
| 源侧多 | src=`[a(1),b(1)]` tgt=`[a(1)]` | `recorded` 1 条，`getKey()=="b"`，`getState()==SOURCE_MORE`；`stateCounts.get(SOURCE_MORE)==1L` |
| 目标侧多 | src=`[a(1)]` tgt=`[a(1),b(1)]` | `recorded` 1 条，key `b`，state `TARGET_MORE` |
| 字段不同 | src=`[a(1)]` tgt=`[a(2)]` | `recorded` 1 条，state `DIFFERENT`；`getDifferentFields().get("amount").getSource()==1d`，`.getTarget()==2d` |
| 字段仅源侧有 | src=`[a{amount,x}]` tgt=`[a{amount}]` | state `DIFFERENT`；差异字段含 `x` 且 `getSource()!=null`、`getTarget()==null` |
| 字段仅目标侧有 | src=`[a{amount}]` tgt=`[a{amount,x}]` | state `DIFFERENT`；差异字段含 `x` 且 `getSource()==null`、`getTarget()!=null` |
| 交错 | src=`[a(1),c(1)]` tgt=`[b(1),c(1)]` | `recorded` 2 条：先 `SOURCE_MORE(a)` 后 `TARGET_MORE(b)` |
| 单侧为空 | src=`[]` tgt=`[a(1)]` | 抛 `CheckException`，消息含 `"对账数据未准备好"`；且 `sync.getRecorded()` **为空**（零 sync 副作用） |
| 两侧皆空 | src=`[]` tgt=`[]` | 不抛异常；`summary` 各计数为 0；`recorded` 为空 |

「字段仅源侧有」用例的构造：`new CheckEntry("a", null, Map.of("amount", 1d, "x", 2d))`。

- [ ] **步骤 3：编写失败的测试 —— 护栏与边界**

追加到同一测试类：

| 用例 | 输入 | 断言 |
|---|---|---|
| 数据源乱序 | loader 返回 `[b(1),a(1)]` | 抛 `CheckException`，消息含 `"未按 key 升序"` |
| 同侧 key 重复 | loader 返回 `[a(1),a(2)]` | 抛 `CheckException`，消息含 `"未按 key 升序"`（同一条断言覆盖） |
| key 为 null | loader 返回 `[new CheckEntry(null, null, Map.of("amount",1d))]` | 抛 `CheckException`，消息含 `"对账 key 不能为 null"` |
| 已消费的流 | loader 返回同一个已 `count()` 过的 Stream 实例 | 抛 `IllegalStateException`（JDK 的 "stream has already been operated upon or closed"）——**不吞异常、不静默返回空结果** |
| 并发不同日期不串味 | 同一 `StreamingCheckProcessor` 实例，两个线程分别 `compare` 两个 `CheckContext`（各自不同 date 与不同数据） | 两个 context 的 `summary.getSourceTotal()` 各自等于自己那批数据的条数 |
| MEMORY 模式 null key | MEMORY 路径下 `CheckEntry.key` 为 null | 抛 `NullPointerException`（`Collectors.toMap` 不接受 null key）——**与 STREAM 模式的 `CheckException` 类型不同是已知差异**，此处钉住 MEMORY 侧的实际行为 |
| 计数自洽性 | 任意用例跑完后 | `summary.getSourceTotal() + summary.getTargetTotal() - 2 × summary.getComparedCount() == summary.getStateCounts().get(SOURCE_MORE) + summary.getStateCounts().get(TARGET_MORE)` |
| 样本上限 | 同一 state 产出 8 条差异 | `summary.getSamples()` 中该 state 的条目数为 5（不是 8）；三个 state 各自的样本数均不超过 5 |
| 计数 map 三键齐全 | 任意用例跑完后 | `summary.getSyncCounts().size()==3` 且 `summary.getStateCounts().size()==3`（即使某个 state 计数为 0 也必须有键——规格 §3.2） |

「已消费的流」用例的构造：

```java
Stream<CheckEntry> consumed = Stream.of(entry("a", 1d)).peek(u -> { }).limit(1);
consumed.count();   // 已消费
new StreamingCheckProcessor(date -> consumed, date -> Stream.of(), sync).compare(context);
```

「并发不同日期」用例用 `CountDownLatch` 让两个线程同时进入 `compare`，各自持有独立的 `CheckContext` 与 `ExecutorManager`（id 必须不同，否则 `DefaultExecutorManager` 的 processing key 会互相干扰）。

- [ ] **步骤 4：运行测试验证失败**

运行：`mvn test -Dtest=StreamingCheckProcessorTest`
预期：编译失败 —— `找不到符号: 类 StreamingCheckProcessor`。

- [ ] **步骤 5：实现 `StreamingCheckProcessor`**

`src/main/java/io/github/kugin/reconciliation/check/StreamingCheckProcessor.java`，`extends AbstractCheckProcessor`。

**字段**（全部 final）：`OrderedResourceLoader srcLoader`、`OrderedResourceLoader tgtLoader`、`CheckSync checkSync`（可为 null）。

**构造器**：`public StreamingCheckProcessor(OrderedResourceLoader srcLoader, OrderedResourceLoader tgtLoader, CheckSync checkSync)`。

**`compare(CheckContext)` 覆写**（父类的 `compare` 会调 `compareCheck(context)`，而父类版本读 `context.getSource()`，流式下为 null 会 NPE）：

```
1. 若 context.isPreCheckFailed() 为 true → 直接 return（不设 CHECK 状态, 不开流）
2. context.getExecutorManager().setStatus(ExecutorStatusEnum.CHECK)
3. 调用 doCompare(context)
```

**`compareCheck(CheckContext)` 覆写**：空实现（加注释说明流式模式下空数据检查在 `doCompare` 开流之后进行）。

**`doCompare(CheckContext)` 实现**：

```
1. date = context.getDate()
2. 开两条流: src = srcLoader.loadOrdered(date), tgt = tgtLoader.loadOrdered(date)
   （用 try-with-resources 保证关闭）
3. 取两个 iterator, 先用 hasNext() 判定单侧为空:
   - 恰好一侧为空 → throw new CheckException("对账数据未准备好")
     ★ 必须在开流后、merge 之前判定, 保证异常时零 sync 副作用
   - 两侧都为空 → context.setSummary(CheckSummary.empty()); return
4. merge join 主循环, 算法见规格 §4.1
5. 循环结束 → context.setSummary(累加器转成的 CheckSummary)
```

**merge join 主循环**（规格 §4.1 伪码）：维护源侧、目标侧各一个「当前条目 + 上一个 key」。每次取下一条时调用私有方法校验：

```java
private CheckEntry advance(Iterator<CheckEntry> it, String lastKey)
```

其行为：`it` 无更多元素时返回 null；否则取下一条，key 为 null 时 `throw new CheckException("对账 key 不能为 null")`；`lastKey != null` 且 `key.compareTo(lastKey) <= 0` 时 `throw new CheckException("数据源未按 key 升序或 key 重复: " + lastKey + " -> " + key)`。

**每次产出差异时的处理**（规格 §4.2 的六步，顺序固定）：构造 `CheckUnit`（`key` / `state` / `differentFields`）→ 立即按 state 调用 `checkSync` 的对应方法（`checkSync` 为 null 时一律视为 `CheckSyncEnum.SYNC`）→ `checkUnit.setSync(结果)` → 累加 `stateCounts` 与 `syncCounts` → 若该 state 的样本数不足 5 则存入 `samples` → 丢弃该 `CheckUnit`。

`differentFields` 的构造与 `AbstractCheckProcessor` 保持一致，直接复用父类的 `protected` 方法：`SOURCE_MORE` 用 `createSingle(sourceEntry.getCheckData(), true)`，`TARGET_MORE` 用 `createSingle(targetEntry.getCheckData(), false)`，`DIFFERENT` 用 `compareCheckData(sourceEntry.getCheckData(), targetEntry.getCheckData())`。

**计数语义**：`sourceTotal` = 源侧被消费的条数；`targetTotal` = 目标侧被消费的条数；`comparedCount` = 两侧 key 相等、实际做过字段比较的条数（不论字段是否有差异）。由此恒等式成立：`sourceTotal + targetTotal - 2 × comparedCount == stateCounts[SOURCE_MORE] + stateCounts[TARGET_MORE]`。

**累加器必须是每次调用新建的局部状态**：处理器实例会被多个日期的运行共享（`CheckExecutor` 只构建一次 processor），因此**不得**把计数放进实例字段。用一个 `private static final class` 嵌套累加器持有 `long` 计数、两个 `EnumMap` 与样本列表；`stateCounts` 与 `syncCounts` 初始化时为**每个**枚举常量放入 `0L`。

- [ ] **步骤 6：运行测试验证通过**

运行：`mvn test -Dtest=StreamingCheckProcessorTest`
预期：全部用例 PASS（算法用例 9 条 + 护栏用例 5 条）。

- [ ] **步骤 7：Commit**

```bash
git add src/main/java/io/github/kugin/reconciliation/check/StreamingCheckProcessor.java \
        src/test/java/io/github/kugin/reconciliation/support/RecordingCheckSync.java \
        src/test/java/io/github/kugin/reconciliation/check/StreamingCheckProcessorTest.java
git commit -m "feat: add streaming merge join check processor"
```

---

### 任务 4：流式前置与后置处理器

**文件：**
- 创建：`src/main/java/io/github/kugin/reconciliation/before/StreamingBeforeCheckProcessor.java`
- 创建：`src/main/java/io/github/kugin/reconciliation/after/StreamingAfterCheckProcessor.java`

- [ ] **步骤 1：创建 `StreamingBeforeCheckProcessor`**

`extends AbstractBeforeCheckProcessor`（复用其 `CheckPre` 处理语义）。

- 构造器：`public StreamingBeforeCheckProcessor(CheckPre checkPre)`。
- `doPre(CheckContext)`：`return Objects.isNull(checkPre) || checkPre.doPre(context);`（与 `DefaultBeforeCheckProcessor` 一致）。
- **覆写 `processBeforeCompare(CheckContext)`**：`setStatus(BEFORE)` → 若 `doPre` 返回 false 则 `context.setPreCheckFailed(true)` 并 return → **不加载任何数据**（不调 `getSource` / `getTarget` / `getDifferent`）。
- `getSourceLoader()` / `getTargetLoader()` / `getDifferentLoader()`：父类通过 `ResourceReader` 声明了这三个方法，流式模式不使用它们，一律 `throw new UnsupportedOperationException("STREAM 模式不使用 ResourceReader")`。

- [ ] **步骤 2：创建 `StreamingAfterCheckProcessor`**

`extends AbstractAfterCheckProcessor`（复用其 `processAfterCompare` 的状态流转与 `doAfter` 调度）。

- 构造器：`public StreamingAfterCheckProcessor(CheckSync checkSync, CheckAfter checkAfter)`。
- **覆写 `autoSync(CheckResult)`**：空实现（加注释：sync 已在 CHECK 阶段完成）。父类的 `processAfterCompare` 会把 `context.getCheckResult()`（流式下为 null）传进来，本覆写忽略该参数。
- `doAfter(CheckContext)`：`checkAfter` 非 null 时调用 `checkAfter.doAfter(context)`。
- **覆写 `isComplete(CheckContext)`**（规格 §3.5）：

```java
if (checkSync != null) {
    // 委托给 CheckSync: 其默认实现读 summary, 用户自定义覆写也依然生效
    return checkSync.isComplete(context);
}
// 未配置 CheckSync: 由 summary 判定
CheckSummary summary = context.getSummary();
return summary != null
        && summary.getSyncCounts().getOrDefault(CheckSyncEnum.NO_SYNC, 0L) == 0;
```

- `syncSourceMore` / `syncTargetMore` / `syncDifferent`：`checkSync` 非 null 时委托给它，否则返回 `CheckSyncEnum.SYNC`（与 `DefaultAfterCheckProcessor` 的 null 语义一致）。加注释说明：流式模式下这三个方法不会被调用（`autoSync` 已是空操作），保留实现只为满足 `CheckSync` 契约。

- [ ] **步骤 3：编译验证**

运行：`mvn compile`
预期：`BUILD SUCCESS`，无警告级错误。

- [ ] **步骤 4：Commit**

```bash
git add src/main/java/io/github/kugin/reconciliation/before/StreamingBeforeCheckProcessor.java \
        src/main/java/io/github/kugin/reconciliation/after/StreamingAfterCheckProcessor.java
git commit -m "feat: add streaming before and after check processors"
```

---

### 任务 5：`CheckExecutor` 模式分流与端到端集成

**文件：**
- 修改：`src/main/java/io/github/kugin/reconciliation/executor/CheckExecutor.java`
- 测试：`src/test/java/io/github/kugin/reconciliation/executor/StreamingCheckExecutorTest.java`

- [ ] **步骤 1：编写失败的测试 —— 端到端状态流转**

`src/test/java/io/github/kugin/reconciliation/executor/StreamingCheckExecutorTest.java`。每个用例用**唯一的 executor id**（`DefaultExecutorManager` 用 `day:check:%s:%s` 作 key，且 `ERROR` 状态**不会**移除 processing key——`DefaultExecutorManager.setStatus` 只在 `END` 时移除——所以不同用例必须用不同 id，否则后一个用例会被判定为「正在执行」而直接返回）。

| 用例 | 构造 | 断言 |
|---|---|---|
| 正常执行 | `CheckConfig.mode=STREAM`，两侧 loader 返回有序流，`checkPre` 返回 true | 状态 `END`；`context.getSummary()!=null`；`context.getCheckResult()==null`；`context.getSource()==null`；`context.getTargetMap()==null` |
| sync 在 CHECK 阶段被调用 | `checkSync` 回调里读 `executorManager.getCurrentStatus()` | 回调期间状态为 `"CHECK"`（用 `AtomicReference<CheckExecutor>` 在回调中读取引用方的 manager） |
| 返回 NO_SYNC | `checkSync` 返回 `NO_SYNC` | `isComplete()` 为 false；状态 `ERROR` |
| 返回 IGNORE | `checkSync` 返回 `IGNORE` | 状态 `END`（IGNORE 不算未完成） |
| 返回 SYNC | `checkSync` 返回 `SYNC` | 状态 `END` |
| `doPre` 返回 false | `checkPre` 返回 false | 状态 `ERROR`；两侧 loader **未被调用**（用计数器断言 `AtomicInteger` 为 0） |
| loader 抛异常 | loader 抛 `RuntimeException` | 状态停在 `CHECK`；`executorManager.isProcessing()` 为 false；异常被 `CheckExecutor` 捕获（不向外抛） |
| sync 回调抛异常 | `checkSync.syncDifferent` 抛 `RuntimeException` | 状态停在 `CHECK`；异常被 `CheckExecutor` 捕获；**该条差异之前已完成的 sync 副作用保留、不回滚**（用计数器断言回调被调用的次数） |
| 单侧数据为空 | 一侧返回空流 | 抛出的异常被捕获，状态停在 `CHECK`；`checkSync` 一次都没被调用 |
| 组装期 fail-fast | `mode=STREAM` 但不配 ordered loader | `CheckExecutor.buildExecutor(config)` 抛 `NullPointerException`，消息含 `"STREAM 模式需要"` |
| MEMORY 模式不变 | `mode=MEMORY`（默认） | 状态 `END`；`context.getCheckResult()!=null`；`context.getSummary()==null` |

「loader 抛异常」用例的构造：loader 实现为 `date -> { throw new RuntimeException("boom"); }`。

「单侧数据为空」用例：注意 `CheckExecutor.process` 会 catch 住异常，所以断言的是**状态与回调次数**，不是异常抛出。

- [ ] **步骤 2：运行测试验证失败**

运行：`mvn test -Dtest=StreamingCheckExecutorTest`
预期：全部用例 FAIL —— `CheckConfig` 无 `mode` 字段导致编译失败（先补字段再跑）。

- [ ] **步骤 3：改造 `CheckExecutor.buildExecutor`**

在现有方法里按模式分流（MEMORY 分支的代码**逐字保留**）：

```
mode = checkConfig.getMode() == null ? CheckMode.MEMORY : checkConfig.getMode()

if (mode == CheckMode.STREAM) {
    srcOrdered = Objects.requireNonNull(checkConfig.getSrcOrderedLoader(), "STREAM 模式需要源端 OrderedResourceLoader")
    tgtOrdered = Objects.requireNonNull(checkConfig.getTargetOrderedLoader(), "STREAM 模式需要目标端 OrderedResourceLoader")
    beforeCheckProcessor = new StreamingBeforeCheckProcessor(checkConfig.getCheckPre())
    checkProcessor = Optional.ofNullable(checkConfig.getCheckProcessor())
            .orElseGet(() -> new StreamingCheckProcessor(srcOrdered, tgtOrdered, checkConfig.getCheckSync()))
    afterCheckProcessor = new StreamingAfterCheckProcessor(checkConfig.getCheckSync(), checkConfig.getCheckAfter())
} else {
    ...现有代码原样...
}
```

**注意**：`requireNonNull` 抛的是 `NullPointerException`（消息含 `"STREAM 模式需要"`），与规格 §5.3 一致。

- [ ] **步骤 4：改造 `CheckExecutor.process` 的日志分支**

现有代码取 `checkContext.getSource().size()` 与 `result.getDiffDetailsMap()`，流式下均为 null → NPE。改为三级判定：

```
CheckResult result = checkContext.getCheckResult();
CheckSummary summary = checkContext.getSummary();

if (summary != null) {
    // 流式模式
    log.info("对账结果:", executorManager.getExecutorKey(), MapUtil.of(
        Pair.of("sourceTotal", summary.getSourceTotal()),
        Pair.of("targetTotal", summary.getTargetTotal()),
        Pair.of("compared", summary.getComparedCount()),
        Pair.of("stateCounts", summary.getStateCounts()),
        Pair.of("syncCounts", summary.getSyncCounts())));
} else if (result != null) {
    // 全量模式: 现有日志逻辑原样
    ...
} else {
    // 前置校验未通过, 无结果
    log.info("对账未执行: 前置校验未通过", executorManager.getExecutorKey());
}
```

同时把 `checkContext.getSource().size()` 的取值移入全量分支内（流式下 `getSource()` 为 null）。

- [ ] **步骤 5：运行测试验证通过**

运行：`mvn test -Dtest=StreamingCheckExecutorTest`
预期：全部用例 PASS。

- [ ] **步骤 6：运行全量测试确认无回归**

运行：`mvn test`
预期：全部 PASS，**包括未修改的 `ExecutorTest`、`SimpleCheckExampleTest`、`RedisCheckExecutorIntegrationTest` 等现有测试**。

- [ ] **步骤 7：Commit**

```bash
git add src/main/java/io/github/kugin/reconciliation/executor/CheckExecutor.java \
        src/test/java/io/github/kugin/reconciliation/executor/StreamingCheckExecutorTest.java
git commit -m "feat: wire STREAM mode into CheckExecutor"
```

---

### 任务 6：对拍测试与文档

**文件：**
- 创建：`src/test/java/io/github/kugin/reconciliation/check/StreamingParityTest.java`
- 修改：`README.md`

- [ ] **步骤 1：编写对拍测试**

`src/test/java/io/github/kugin/reconciliation/check/StreamingParityTest.java`。核心思路：同一份数据分别跑 MEMORY 与 STREAM 两条路径，比对两边**记录到的 `CheckUnit` 集合**完全一致。

```
数据生成: Random(42) 固定种子, 生成 100~1000 条
  key 从小整数空间随机取 (保证唯一, 因为 MEMORY 路径的 Collectors.toMap 不接受重复 key)
  每条随机决定: 只在源侧 / 只在目标侧 / 两侧都有(金额相同或不同)

MEMORY 路径: CheckConfig.mode=MEMORY, srcLoader/targetLoader 返回 CheckEntry.wrap(list)
STREAM 路径: CheckConfig.mode=STREAM, srcOrderedLoader/targetOrderedLoader 用
             OrderedResourceLoaderAdapter 包装同样的 list (适配器负责排序)

两侧都用 RecordingCheckSync 收集回调收到的 CheckUnit

断言:
  - 两边记录条数相等
  - 按 key 排序后逐条比较: key 相等、state 相等、
    differentFields 的 key 集合相等、每个 FieldkUnit 的 source/target 值相等
  - 两边 summary 的 stateCounts 逐项相等
```

**排序要求**：MEMORY 路径的 diff 顺序取决于 `HashMap` 迭代顺序，STREAM 路径是 key 升序——**两边都必须按 key 排序后再比较**，不要依赖顺序一致。

**断言差异字段的值相等**时注意类型：`source` / `target` 是 `Object`，用 `assertEquals(expected, actual)` 即可（测试数据统一用 `Double`）。

- [ ] **步骤 2：运行测试验证通过**

运行：`mvn test -Dtest=StreamingParityTest`
预期：PASS。若 FAIL，说明两条路径对同一输入产出了不同差异——**这是真实缺陷**，按失败信息定位 `StreamingCheckProcessor` 的 state 判定或字段比较逻辑。

- [ ] **步骤 3：补充内存特征测试**

在同一测试类追加：用一个惰性 loader 记录被拉取的条数，断言 STREAM 路径两侧拉取计数**始终相差不超过 1**（交错拉取），而 MEMORY 路径两侧一次性拉完。再加一个「无限序列」loader（key 递增），只消费前 N 条即中断，断言测试不会挂住（证明流是惰性的）。

- [ ] **步骤 4：更新 `README.md`**

- 在 `CheckConfig` 参数表中补三行：`mode`（执行模式，默认 `MEMORY`）、`srcOrderedLoader`（STREAM 模式上游有序数据源）、`targetOrderedLoader`（STREAM 模式下游有序数据源）。
- 新增一节「流式比对（STREAM 模式）」，写明：
  - 何时用：日交易量大、两侧数据可按键升序获得；
  - **契约**：两侧必须使用**同一个 key 比较器**（`String.compareTo` 语义），否则框架无法发现错序，结果会静默错误；
  - **快照语义**：比对期间被读取的一侧视为不可变快照，`CheckSync` 若要写回该侧，渠道需在自己的 loader 内先物化；
  - **迁移注意**：现有 `CheckPre` / `CheckAfter` 不能依赖 `context.getSource()`；`CheckSync` 的调用时机从 AFTER 阶段变为 CHECK 阶段；增量对账（差异数据源）流式模式不提供；`doPre` 返回 false 时 STREAM 状态为 `ERROR`（MEMORY 为 `BEFORE`）；
  - `OrderedResourceLoaderAdapter` 是过渡方案，仍是全量驻载。
- 把文末 TODO 的 `- [ ] 相同形式的数据源对比,流式处理比对` 勾选为 `- [x]`。

- [ ] **步骤 5：运行全量测试**

运行：`mvn test`
预期：全部 PASS。

- [ ] **步骤 6：Commit**

```bash
git add src/test/java/io/github/kugin/reconciliation/check/StreamingParityTest.java README.md
git commit -m "test: add streaming parity coverage and document STREAM mode"
```

---

## 规格偏离说明

本计划相对已批准规格有**一处新增**（非规格遗漏，是实现所需）：

- **`CheckContext.preCheckFailed`（布尔，默认 false）**。规格 §5 要求「`doPre` 返回 false → 不进入 CHECK，直接进入 AFTER」，但 `CheckExecutor.process` 是无条件调用 `checkProcessor.compare(context)` 的，现有 `CheckContext` 没有可承载该信号的字段。流式路径用该标记让 `StreamingCheckProcessor.compare` 直接返回，从而真正跳过比对阶段。该字段仅流式路径使用，MEMORY 路径不读它，行为不变。
- **事实性勘误**：「审查重点 #5」表格声称 MEMORY 模式遇 null key 抛 NPE（`Collectors.toMap` 不接受 null key）——实测（JDK 21.0.12.1，zulu）`Collectors.toMap` 允许 null key（底层 `HashMap.merge` 只校验 value），MEMORY 路径**静默接受** null key 并收入 `sourceMap` / `targetMap`；STREAM 路径显式抛 `CheckException("对账 key 不能为 null")`。两者的实际差异是「MEMORY 静默接受 vs STREAM 显式拒绝」，而非「抛 NPE vs 接受」。测试已按实际行为钉住（见 `StreamingCheckProcessorTest`）。

规格其余部分（组件清单、算法、契约、错误语义、测试分层）在本计划中逐条落地，无删减。
