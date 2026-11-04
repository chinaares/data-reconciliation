# 流式处理比对设计规格

- 日期：2026-11-05
- 状态：待审查
- 分类：架构级（Architectural）

## 1. 背景与目标

### 1.1 现状

对账执行器 `CheckExecutor` 走 before → check → after 三段流水线，数据模型是**全量驻留**：

- `AbstractBeforeCheckProcessor.processBeforeCompare`（`src/main/java/io/github/kugin/reconciliation/before/AbstractBeforeCheckProcessor.java:54-61`）把两侧 `ResourceLoader.load(date)` 全部读成 `List<CheckEntry>`，再各建一份 `Map<String, CheckEntry>`。同一批对象在内存中存在两份引用。
- `AbstractCheckProcessor.compareMap`（`src/main/java/io/github/kugin/reconciliation/check/AbstractCheckProcessor.java:51-86`）要求两侧 `Map` 同时在手，才能判出 `SOURCE_MORE` / `TARGET_MORE` / `DIFFERENT`。
- 结果侧是好的：`CheckResult` 只留差异项，量级通常远小于全量。

日交易十万笔量级下，单侧驻留约 50–100MB，两侧 + 两张索引 Map 落在 200–400MB 量级，且随渠道与笔数线性增长。

### 1.2 目标

1. **内存解耦**：流式路径的内存占用与总条数无关。
2. **边比边修**：差异一产生即回调调账，不等全部比对完成。
3. **老路径零影响**：现有用户不改任何代码、不改任何配置，全量路径行为逐字不变。

### 1.3 非目标（YAGNI）

- 不做分桶归并（grace hash join）—— 框架只做 merge join，排序责任上移到渠道。
- 不做落盘外部排序。
- 不做差异全集的持久化（落盘 / Redis 队列 / 重试框架）。
- 不做流式路径下的差异重试机制——`NO_SYNC` 明细由用户的 `CheckSync` 实现自行处理。
- 不并行化比对——merge join 是串行归并，并行无收益且破坏有序契约。
- **不做增量对账（差异数据源）**——见 §5.4。

### 1.4 关键约束

- **渠道异构**：各渠道数据源形态不同，存在两侧均有序与仅一侧有序两种情况。
- **有序性责任在渠道**：框架只做 merge join，要求渠道保证其 loader 返回按键升序的流。
- **快照语义**：框架声明「流式比对期间被读取的一侧视为不可变快照」，写回责任归渠道。

## 2. 总体方案

### 2.1 双模式并存

`CheckConfig` 新增 `CheckMode` 枚举（`MEMORY` / `STREAM`），默认 `MEMORY`。

两条模式是**平行的两套流水线**，共享 `CheckEntry`、`CheckUnit`、`CheckStateEnum`、`CheckSyncEnum` 领域模型，以及 `AbstractCheckProcessor` 中的字段级比较逻辑。

```
┌──────────────── MEMORY 模式（默认，现状不变）────────────────┐
ResourceLoader → List<CheckEntry>                                │
DefaultBeforeCheckProcessor → CheckContext(sourceMap, targetMap) │
DefaultCheckProcessor → compareMap() → CheckResult               │
DefaultAfterCheckProcessor → autoSync → CheckSync                │
└─────────────────────────────────────────────────────────────────┘

┌──────────────── STREAM 模式（新增）────────────────────────────┐
OrderedResourceLoader → Stream<CheckEntry>（按键升序）            │
StreamingBeforeCheckProcessor → 只校验，不加载                    │
StreamingCheckProcessor → merge join → 边比边抛 → CheckSummary    │
StreamingAfterCheckProcessor → 只跑 CheckAfter                   │
└─────────────────────────────────────────────────────────────────┘
```

### 2.2 组件清单

#### 新增

| 文件 | 包 | 职责 |
|---|---|---|
| `OrderedResourceLoader` | `before` | 有序流式数据源契约 |
| `OrderedResourceLoaderAdapter` | `before` | 把 `ResourceLoader`（返回 List）排序后转成 Stream。过渡辅助，非流式方案 |
| `StreamingBeforeCheckProcessor` | `before` | 只执行 `CheckPre.doPre()` 校验，不加载数据 |
| `StreamingCheckProcessor` | `check` | merge join 主循环，产出差异即刻回调 sync。**持有 `CheckSync` 实例**（可为空，为空时默认全部 SYNC），因为流式模式的 sync 发生在 CHECK 阶段而非 AFTER 阶段 |
| `StreamingAfterCheckProcessor` | `after` | 只执行 `CheckAfter.doAfter(context)`，不重复 sync |
| `CheckSummary` | `domain` | 流式模式的结果摘要 |
| `CheckMode` | `enums` | `MEMORY` / `STREAM` |

#### 修改

| 文件 | 改动 | 影响面 |
|---|---|---|
| `CheckConfig` | 新增 `mode`、`srcOrderedLoader`、`targetOrderedLoader` | 纯增量，旧构造方式不受影响 |
| `CheckContext` | 新增 `summary` 字段 | 纯增量；流式下 `getCheckResult()` 为 null |
| `CheckSync.isComplete()` | 默认实现先读 `summary`，无则回退读 `CheckResult` | 全量路径行为完全不变 |
| `CheckExecutor` | 按 `mode` 组装两套 processor；日志按模式分支 | 全量路径日志输出不变 |
| `AbstractCheckProcessor` | **不改**，`StreamingCheckProcessor` 继承它复用 `compareCheckData` / `createSingle` | 无 |

**旧代码零删除、零签名变更。** `ResourceLoader`、`CheckResult`、`DefaultCheckProcessor`、`DefaultAfterCheckProcessor` 全部原样保留。

### 2.3 继承关系

```
AbstractBeforeCheckProcessor（抽象基类，不改）
├── getSource / getTarget / getDifferent
├── processBeforeCompare()  ← 模板方法：doPre → getDifferent → 加载 source/target → 建 Map
└── doPre()                 ← 来自 CheckPre 接口，由 DefaultBeforeCheckProcessor 实现

DefaultBeforeCheckProcessor     extends AbstractBeforeCheckProcessor   ← 现状
StreamingBeforeCheckProcessor  extends AbstractBeforeCheckProcessor   ← 新增：覆盖 processBeforeCompare

AbstractCheckProcessor（抽象基类，不改）
├── compareCheckData(sourceData, targetData)   ← 字段级比较，流式路径复用
├── createSingle(checkData, isSource)          ← 单边数据展开，流式路径复用
└── compare() 模板方法                          ← 校验 source/target 非空后调 doCompare

DefaultCheckProcessor     extends AbstractCheckProcessor   ← 现状：Map 全量比对
StreamingCheckProcessor  extends AbstractCheckProcessor   ← 新增：merge join

AbstractAfterCheckProcessor（抽象基类，不改）
├── processAfterCompare()  ← 模板方法：autoSync → isComplete → doAfter
└── autoSync(CheckResult)  ← 遍历 diffDetails 逐条调 sync 回调

DefaultAfterCheckProcessor     extends AbstractAfterCheckProcessor   ← 现状
StreamingAfterCheckProcessor  extends AbstractAfterCheckProcessor   ← 新增：覆盖 autoSync 为空操作
```

**继承而非组合的理由**：字段级比较逻辑（尤其 `compareCheckData` 对「字段只在一边存在」的处理）是本框架最核心也最易错的部分，必须只有一份实现，否则两条路径迟早行为分叉。

**继承关系的三点说明**：

1. `StreamingBeforeCheckProcessor` 继承 `AbstractBeforeCheckProcessor`，是为了复用 `CheckPre` 的处理语义（`DefaultBeforeCheckProcessor` 中「无 checkPre 即通过」的判定：`Objects.isNull(checkPre) || checkPre.doPre(context)`）。它**覆盖 `processBeforeCompare`**，只做 `doPre` 校验与状态推进，不加载数据。
2. `StreamingCheckProcessor` 继承 `AbstractCheckProcessor`，复用 `compareCheckData` / `createSingle`。它**覆盖 `compare()` 与 `compareCheck()`**——因为父类的 `compareCheck()` 依赖 `context.getSource()` / `getTarget()`（流式下为 null），且其「单侧为空则抛异常」语义需在流式下重新实现（见 §4.3）。
3. `StreamingAfterCheckProcessor` 继承 `AbstractAfterCheckProcessor`，复用 `processAfterCompare` 的状态流转与 `doAfter` 调度。它**覆盖 `autoSync()` 为空操作**，因为 sync 已在 CHECK 阶段完成。

## 3. 核心契约

### 3.1 `OrderedResourceLoader`

```java
public interface OrderedResourceLoader {
    /**
     * 按 key 自然序（String.compareTo）升序、单遍、稳定快照返回数据流。
     *
     * 契约：
     * 1. 严格升序 —— 同侧 key 不得重复
     * 2. 单遍消费 —— 调用方只会遍历一次，返回的 Stream 不得被复用
     * 3. 稳定快照 —— 遍历期间底层数据不可变
     * 4. key 不得为 null
     */
    Stream<CheckEntry> loadOrdered(String date);
}
```

契约说明：

- **契约 1（严格升序）** 是 merge join 成立的前提，同时让「重复 key」成为可检测的契约违反而非静默的数据问题。
- **契约 2（单遍）**：`Stream` 本身就是单遍的，这是选它而非 `List` 的原因之一 —— `List` 可被反复遍历，契约无法强制。
- **契约 3（稳定快照）**：框架声明快照语义，渠道自行保证。若渠道 loader 依赖可变底层存储（如可写表 + 边查边读），必须自己在 loader 内物化，因为 merge join 遍历期间 sync 回调可能写入底层。
- **契约 4（key 非 null）**：`compareTo` 在 null 上会抛 NPE，框架在契约层禁止。

### 3.1.1 `OrderedResourceLoader` 与现有加载器的关系

现有加载器体系：

```
ResourceLoader (函数式接口)     List<CheckEntry> load(String date)
  ├── FileResourceLoader        文件读取
  └── RedisResourceLoader       Redis RList 读取

ResourceReader                  聚合上下游 + 差异数据源
  └── DefaultResourceReader
```

`OrderedResourceLoader` 是**平行的第三种加载器**，不继承 `ResourceLoader`（返回类型不同，无法覆写 `load`）。

**为什么不用 `ResourceLoader` 加默认方法**：默认实现只能是 `load(date).stream()`，那是**假流式**——数据已经全量物化成 `List` 了，内存一点没省。且 `List` 不保证有序，无法表达有序契约。

**两者的分工**：

| | `ResourceLoader` | `OrderedResourceLoader` |
|---|---|---|
| 返回 | `List<CheckEntry>` | `Stream<CheckEntry>` |
| 顺序 | 不保证 | **严格升序（契约保证）** |
| 消费 | 可反复遍历 | 单遍 |
| 内存 | O(n) 全量驻留 | O(1) 流式 |
| 适用模式 | MEMORY | STREAM |

**`OrderedResourceLoaderAdapter`（过渡辅助）**：把一个 `ResourceLoader` 的输出排序后转成 `Stream`：

```java
public class OrderedResourceLoaderAdapter implements OrderedResourceLoader {
    private final ResourceLoader delegate;
    private final Comparator<String> keyComparator;  // 默认 String::compareTo

    @Override
    public Stream<CheckEntry> loadOrdered(String date) {
        return delegate.load(date).stream()
                .sorted(Comparator.comparing(CheckEntry::getKey, keyComparator));
    }
}
```

**必须明确的定位**：这个适配器**不解决内存问题**——`delegate.load(date)` 已经把数据全量读进内存了。它的价值是让渠道能**渐进迁移**（先接入 STREAM 模式验证逻辑正确性，之后再替换为真正的流式 loader），并支撑 §7.3 的对拍测试。

**文档与代码注释中必须标注**：适配器是**全量驻载**，不具备流式的内存特性，不得作为「已实现流式」的依据。

**`ResourceReader` 在 STREAM 模式下的地位**：`ResourceReader` 提供 source/target/different 三个 loader，其中 different 是增量对账专用（§5.4 明确 STREAM 不支持）。因此 **STREAM 模式不使用 `ResourceReader`**，改为在 `CheckConfig` 上直接配置两个 `OrderedResourceLoader`。这也让 `CheckConfig` 的两组 loader 字段语义清晰：MEMORY 用 `srcLoader`/`targetLoader`（或 `resourceReader`），STREAM 用 `srcOrderedLoader`/`targetOrderedLoader`。

### 3.2 `CheckSummary`

```java
public class CheckSummary {
    private long sourceTotal;              // 源侧读到的总条数
    private long targetTotal;              // 目标侧读到的总条数
    private long comparedCount;            // 两边 key 都存在、实际做了字段比较的条数
    private Map<CheckStateEnum, Long> stateCounts;  // SOURCE_MORE / TARGET_MORE / DIFFERENT 计数
    private Map<CheckSyncEnum, Long> syncCounts;     // SYNC / NO_SYNC / IGNORE 计数
    private List<CheckUnit> samples;       // 每状态各取前 N 条样本（N=5），仅供排查
}
```

- `samples` 是**有界的诊断样本**，不是结果全集。流式模式下不存在「差异全集」——这是设计决策 6 的直接后果。保留每状态前 5 条样本供人眼判断差异形态，不至于把内存吃回来。
- `comparedCount` 与 `stateCounts` 共同构成**对账自洽性校验**：`sourceTotal + targetTotal - 2 × comparedCount` 应等于 `stateCounts[SOURCE_MORE] + stateCounts[TARGET_MORE]`，反向证明没有静默漏比。
- `stateCounts` **必须包含 `CheckStateEnum` 的全部三个键**（值可为 0），`syncCounts` **必须包含 `CheckSyncEnum` 的全部三个键**（值可为 0），保证计数 map 语义稳定（调用方无需判空即可读取）。
- `samples` 在 `sourceTotal == 0` 等边界下也必须非 null（空列表），避免调用方到处判空。
- `CheckSummary` 为**不可变对象**，在 CHECK 阶段结束时一次性构建并写入 `CheckContext`。

### 3.3 `CheckSync` 在流式模式下的语义

- 调用时机：在 **CHECK 阶段**，差异一产生即调用，不等全部比对完成。
- 回调被调用时，`CheckUnit` 携带完整信息（key、state、差异字段）。
- 返回值决定该差异计入 `syncCounts` 的哪一项。
- 返回 `NO_SYNC` → 该差异不再有载体；若需重试，在回调实现内部处理。
- 返回 `IGNORE` → 视为已处理（跨日忽略），**不算未完成**，与现有 `isComplete` 语义一致。

### 3.4 `isComplete` 双分支改造

```java
default boolean isComplete(CheckContext context) {
    CheckSummary summary = context.getSummary();
    if (summary != null) {
        // 流式模式：看 NO_SYNC 计数
        return summary.getSyncCounts().getOrDefault(CheckSyncEnum.NO_SYNC, 0L) == 0;
    }
    // 全量模式：原有逻辑，逐字保留
    CheckResult checkResult = context.getCheckResult();
    if (checkResult == null) {
        return false;
    }
    return checkResult.getDiffDetails().stream()
            .noneMatch(v -> Objects.equals(CheckSyncEnum.NO_SYNC, v.getSync()));
}
```

全量路径分支逻辑与改造前完全一致。

**流式模式下 `summary` 为 null 的情形**：

| 情形 | `isComplete` 是否被调用 | 结果 | 执行状态 |
|---|---|---|---|
| `doPre` 返回 false（前置校验未通过） | 是（AFTER 阶段正常执行） | summary 为 null 且 result 为 null → 走全量分支 → `checkResult == null` → 返回 **false** | **ERROR**（由 `setStatus(ERROR)` 设置） |
| CHECK 阶段抛异常 | **否**——异常冒泡至 `CheckExecutor.process` 的 catch 块，AFTER 阶段整个跳过 | 不适用 | **CHECK**（停在最后显式设置值，`pauseByError()` 不改状态，见 §5.2） |

### 3.5 `isComplete` 的分派路径

`isComplete` 有三处实现，调用关系必须精确定义，否则 `doPre` 失败时的 ERROR 语义会被绕过：

```
AbstractAfterCheckProcessor.processAfterCompare()   ← 模板方法，调用 isComplete(context)
  ├── DefaultAfterCheckProcessor.isComplete()       ← 覆写：Objects.isNull(checkSync) || checkSync.isComplete(context)
  └── StreamingAfterCheckProcessor.isComplete()     ← 新增：见下
        └── CheckSync.isComplete() 默认实现          ← 已按 §3.4 改造为双分支
```

**`StreamingAfterCheckProcessor.isComplete` 的实现**：

```java
public boolean isComplete(CheckContext context) {
    if (checkSync != null) {
        // 委托给 CheckSync，其默认实现已支持 summary；用户自定义覆写也依然生效
        return checkSync.isComplete(context);
    }
    // 未配置 CheckSync：流式模式下由 summary 判定
    CheckSummary summary = context.getSummary();
    return summary != null
            && summary.getSyncCounts().getOrDefault(CheckSyncEnum.NO_SYNC, 0L) == 0;
}
```

**两个关键点**：

1. **`checkSync != null` 时必须委托**，不能自己实现判定。否则用户在 `CheckSync` 里自定义的 `isComplete` 覆写会被绕过——同一个人在两种模式下会得到不同结果，违背「同实现跨模式可用」的目标。
2. **`checkSync == null` 时不能用 `Objects.isNull(checkSync) ||` 短路**（`DefaultAfterCheckProcessor.java:55` 的写法）。STREAM 模式下 `checkSync` 是可选的，若照抄该短路逻辑，`doPre` 返回 false 时会得到 `true` → 状态 END 而非 ERROR，与 §5.2 的表格矛盾。流式分支必须回落到 summary 判定：`summary == null` → false → ERROR。

**MEMORY 模式不受影响**：`DefaultAfterCheckProcessor.isComplete` 逐字保留。

**注意 `CheckSync.isComplete` 的默认实现在 §3.4 的改造中，流式分支读 summary、全量分支读 result，两条路径互斥且都不改变原有语义**（全量分支逐字保留原逻辑）。

## 4. merge join 算法

### 4.1 主循环

```java
Iterator<CheckEntry> src = source.iterator();
Iterator<CheckEntry> tgt = target.iterator();
CheckEntry s = src.hasNext() ? src.next() : null;   // 预读
CheckEntry t = tgt.hasNext() ? tgt.next() : null;

while (s != null || t != null) {
    if (s == null) {
        emit(TARGET_MORE, t);
        t = advance(tgt, lastTgtKey);
        continue;
    }
    if (t == null) {
        emit(SOURCE_MORE, s);
        s = advance(src, lastSrcKey);
        continue;
    }
    int cmp = s.getKey().compareTo(t.getKey());
    if (cmp < 0) {
        emit(SOURCE_MORE, s);
        s = advance(src, lastSrcKey);
    } else if (cmp > 0) {
        emit(TARGET_MORE, t);
        t = advance(tgt, lastTgtKey);
    } else {
        Map<String, FieldkUnit> diff = compareCheckData(s.getCheckData(), t.getCheckData());
        if (!diff.isEmpty()) {
            emit(DIFFERENT, s, t, diff);
        }
        s = advance(src, lastSrcKey);
        t = advance(tgt, lastTgtKey);
    }
}
```

**内存特征**：任意时刻只持有 2 个预读的 `CheckEntry` + 1 个差异 Map，即 O(1)，与总条数无关。

### 4.2 `emit()` 的动作序列

顺序固定，不可调换：

1. 构造 `CheckUnit`（key、state、differentFields）
2. **立即**回调 `CheckSync.syncXxx(checkUnit)`，得到 `CheckSyncEnum`
3. `checkUnit.setSync(result)`，累加 `syncCounts`
4. 累加 `stateCounts`
5. 若该状态的样本数 < 5，存入 `samples`
6. **丢弃** `checkUnit`，不进入任何集合

第 6 步是「即产即抛」的落点。

**`CheckSync` 实例的传递路径**：MEMORY 模式下 `CheckSync` 只被 `DefaultAfterCheckProcessor` 持有。STREAM 模式下 sync 发生在 CHECK 阶段，因此 `CheckSync` 必须**同时**被 `StreamingCheckProcessor` 持有。

- `CheckExecutor.buildExecutor()` 组装时，把 `checkConfig.getCheckSync()` 同时传给 `StreamingCheckProcessor` 与 `StreamingAfterCheckProcessor`。
- `StreamingCheckProcessor` 内部按 state 分派到 `syncSourceMore` / `syncTargetMore` / `syncDifferent`，语义与 `AbstractAfterCheckProcessor.autoSync`（`AbstractAfterCheckProcessor.java:36-49`）**逐条对应**。
- **`checkSync` 为 null 时**：与 `DefaultAfterCheckProcessor` 的既有语义一致（`DefaultAfterCheckProcessor.java:31-52`），所有差异视为 `SYNC`。这条语义不变，避免同一个人在两种模式下得到不同结果。
- `StreamingAfterCheckProcessor` 仍持有 `CheckSync`，但**只用于 `isComplete`**——`autoSync` 被覆盖为空操作（sync 已在 CHECK 阶段完成）。`isComplete` 走 `CheckSync` 的默认实现（已按 §3.4 改造为双分支）。

### 4.3 「数据未准备好」语义在流式下的实现

MEMORY 模式有一条重要保护（`AbstractCheckProcessor.java:24-30`）：**单侧为空时抛 `CheckException("对账数据未准备好")`**，防止只加载了一半数据却继续比对。

流式模式下，`context.getSource()` / `getTarget()` 为 null，父类的 `compareCheck()` 不适用，必须在流式路径重新实现同等语义。

**实现方式**：在 merge join 开始**之前**，用 `Iterator.hasNext()` 检查两侧是否为空。`hasNext()` **不消费元素**，因此这个检查是 O(1) 成本、零副作用。

```java
boolean srcEmpty = !src.hasNext();
boolean tgtEmpty = !tgt.hasNext();
// 两侧皆空 → 数据都为空，与 MEMORY 模式一致：继续走，比对结果为空
// 单侧为空 → 数据未准备好，抛异常
if (srcEmpty != tgtEmpty) {
    throw new CheckException("对账数据未准备好");
}
```

**为什么必须在 merge 之前做**：若等到 merge 结束再判断，单侧为空时 merge 已产出大量 `TARGET_MORE` / `SOURCE_MORE` 差异并**触发了全部 sync 回调**——数据已被修改，再抛异常为时已晚。放在 merge 之前可保证**异常时零 sync 副作用**，与 MEMORY 模式的行为对齐。

**两侧皆空时**：MEMORY 模式下 `compareCheck` 直接 return，`doCompare` 仍会执行（`compareMap` 对两个空 Map 产出空差异列表）。流式模式对齐——产出空 summary，各项计数为 0。

### 4.4 护栏

护栏合并为一条断言，同时捕获「乱序」与「重复 key」两种契约违反：

```java
private CheckEntry advance(Iterator<CheckEntry> it, String lastKey) {
    if (!it.hasNext()) {
        return null;
    }
    CheckEntry e = it.next();
    if (e.getKey() == null) {
        throw new CheckException("对账 key 不能为 null");
    }
    if (lastKey != null && e.getKey().compareTo(lastKey) <= 0) {
        throw new CheckException("数据源未按 key 升序或 key 重复: " + lastKey + " -> " + e.getKey());
    }
    return e;
}
```

**为什么必须断言**：merge join 的正确性完全建立在「两侧都升序」之上。若渠道 loader 谎称有序（例如忘记排序），merge join 会**静默产出错误的差异结果** —— 不报错，但结果错。这是最危险的一类 bug。断言把它变成响亮的失败。

**与现有行为的一致性**：内存路径下 `Collectors.toMap` 遇到重复 key 会抛 `IllegalStateException`。流式路径抛 `CheckException` —— 语义等价（都是 fail-fast），但异常类型不同。这是有意的不一致：流式路径是新代码，没有兼容性负担，可以直接用框架自己的异常类型。

## 5. 端到端数据流

```
CheckExecutor.process(date)
  ├─ executorManager.initDate(date) / isProcessing()       ← 不变
  │
  ├─【BEFORE】StreamingBeforeCheckProcessor
  │    ├─ setStatus(BEFORE)
  │    ├─ doPre(context)   ← 只做校验，context 此时无数据
  │    └─ 不加载任何数据
  │         └─ 若 doPre 返回 false → 不进入 CHECK，直接进入 AFTER（summary 为 null）
  │
  ├─【CHECK】StreamingCheckProcessor
  │    ├─ setStatus(CHECK)
  │    ├─ sourceStream = srcOrderedLoader.loadOrdered(date)   ← 建立流（不消费）
  │    ├─ targetStream = tgtOrderedLoader.loadOrdered(date)
  │    ├─ 空检查（hasNext，零副作用）→ 单侧空则抛 CheckException
  │    ├─ merge join 主循环
  │    │    └─ 每条差异：构造 CheckUnit → 立即调 CheckSync → 计数 → 采样 → 丢弃
  │    └─ 收尾：写 CheckSummary 到 context
  │
  ├─【AFTER】StreamingAfterCheckProcessor
  │    ├─ setStatus(AFTER)
  │    ├─ autoSync 为空操作（sync 已在 CHECK 阶段完成）
  │    ├─ isComplete(context)  ← 读 summary 的 NO_SYNC 计数
  │    │    ├─ true  → setStatus(END)
  │    │    └─ false → setStatus(ERROR)
  │    └─ doAfter(context)  ← CheckAfter 回调
  │
  └─ 日志：按 mode 分支，STREAM 模式打印 summary
```

**为什么 before 阶段不加载数据**：一旦 before 阶段调用 `loadOrdered()` 并把流物化成集合，内存就退化了。STREAM 模式下 before 的职责收缩为「前置校验」。`CheckPre.doPre(context)` 调用时 `context` 里没有数据 —— 这与全量模式一致（全量模式下 `doPre` 也在 `getSource()` 之前调用，见 `AbstractBeforeCheckProcessor.java:42-44`）。

### 5.1 `CheckContext` 在两种模式下的内容

| 字段 | MEMORY 模式 | STREAM 模式 |
|---|---|---|
| `source` / `target` | `List<CheckEntry>` | null |
| `sourceMap` / `targetMap` | `Map<String,CheckEntry>` | null |
| `checkResult` | `CheckResult` | null |
| `summary` | null | `CheckSummary`（`doPre` 失败时为 null） |
| `id` / `name` / `date` / `executorManager` / `stopWatch` | 相同 | 相同 |

流式模式下 `CheckAfter.doAfter(context)` 拿不到全量数据 —— 这是必然的，数据已流走。

### 5.2 错误处理

**先厘清执行器的状态语义**：`DefaultExecutorManager.pauseByError()`（`DefaultExecutorManager.java:46-48`）只移除 processing key，**不设置状态为 ERROR**；`RedisExecutorManager.pauseByError()`（`RedisExecutorManager.java:55-57`）同理，只强制解锁。`CheckExecutor.process` 的 catch 块（`CheckExecutor.java:123-126`）调用 `pauseByError()`，**状态停留在异常发生时的最后一个显式设置值**。

状态变为 ERROR 只有一条路径：`AbstractAfterCheckProcessor.processAfterCompare` 中 `isComplete(context)` 返回 false（`AbstractAfterCheckProcessor.java:26`）。

| 场景 | 处理 | 执行状态 | MEMORY 模式对照 |
|---|---|---|---|
| `doPre` 返回 false | 不进入 CHECK；summary 为 null；AFTER 阶段 `isComplete` 返回 false | **ERROR** | MEMORY 模式此处 NPE（`context.getSourceMap()` 为 null）→ catch → `pauseByError()` → 状态停在 **BEFORE**。**两者不对齐** |
| loader 抛异常（IO / 连接） | 向上抛出 → catch → `pauseByError()` | **CHECK**（最后显式设置值） | 同左，一致 |
| 数据源未按 key 升序 | 护栏抛 `CheckException` → catch → `pauseByError()` | **CHECK** | 不适用 |
| 同侧 key 重复 | 同上（同一条断言） | **CHECK** | 不适用 |
| key 为 null | 护栏抛 `CheckException` → catch → `pauseByError()` | **CHECK** | 不适用 |
| 单侧数据为空 | merge 前抛 `CheckException("对账数据未准备好")` → catch → `pauseByError()` | **CHECK** | MEMORY 模式同样抛 `CheckException`，但发生在 `compareCheck` 中，状态停在 **CHECK**，**一致** |
| `CheckSync` 回调抛异常 | 向上抛出 → catch → `pauseByError()` | **CHECK** | MEMORY 模式：`autoSync` 中异常 → 状态停在 **AFTER**，**不一致**（见下） |

**关于 `doPre` 返回 false 的不对齐**：MEMORY 模式此处是 NPE 导致的**意外行为**（状态卡在 BEFORE，看起来像「正在跑」），不是设计意图。STREAM 模式显式走完 AFTER 流程并落到 ERROR，是有意为之的改进。此差异必须写入兼容边界（§6）。

**关于 `CheckSync` 抛异常的状态差异**：MEMORY 模式的 `autoSync` 在 AFTER 阶段执行，异常时状态停在 AFTER；STREAM 模式的 sync 在 CHECK 阶段执行，异常时状态停在 CHECK。两者都是「停在异常发生处」，语义一致，只是具体值不同。

**关于 sync 副作用不回滚**：流式模式下差异已经边比边 sync 了一部分，异常中断后**已 sync 的部分不回滚**。这与 MEMORY 模式相同（MEMORY 的 `autoSync` 也是逐条 forEach，中途异常同样不回滚）。但流式的暴露面更大 —— sync 发生在比对中途，而非全部比对之后。

### 5.3 组装期 fail-fast

在 `CheckExecutor.buildExecutor()` 中校验，构造即失败：

```java
if (mode == CheckMode.STREAM) {
    Objects.requireNonNull(config.getSrcOrderedLoader(), "STREAM 模式需要源端 OrderedResourceLoader");
    Objects.requireNonNull(config.getTgtOrderedLoader(), "STREAM 模式需要目标端 OrderedResourceLoader");
}
```

配置错误必须在启动时就炸，而不是等到跑对账时才发现「比不出任何结果」。

### 5.4 增量对账（差异数据源）在流式下不支持

`AbstractBeforeCheckProcessor.processBeforeCompare`（`AbstractBeforeCheckProcessor.java:45-49`）调用 `getDifferent(date)`，配合 `ResourceReader.getDifferentLoader()`，用于**增量对账**——上次未对完的数据这次继续对。该能力依赖跨次运行的持久化状态。

**STREAM 模式不提供此能力**：`StreamingBeforeCheckProcessor` 覆盖 `processBeforeCompare` 时不调用 `getDifferent`。

理由：增量对账需要「上次哪些没对完」的持久化记录，这与流式的单遍、无状态语义冲突。MEMORY 模式下该功能当前也是未完成的 TODO（源码注释「todo 差异数据处理,重复对比」），流式模式不引入半成品。

### 5.5 日志

`CheckExecutor` 现有日志取 `checkContext.getSource().size()` 与 `result.getDiffDetailsMap()`，流式下均为 null → NPE，必须按 mode 分支：

```
MEMORY: {sourceSize, targetSize, different, details:{state:count}}
STREAM: {sourceTotal, targetTotal, compared, stateCounts:{...}, syncCounts:{...}}
```

## 6. 兼容边界

| 现有元素 | MEMORY 模式 | STREAM 模式 | 用户需要做什么 |
|---|---|---|---|
| `ResourceLoader` | 原样可用 | 不使用 | 无 |
| `OrderedResourceLoader` | 不使用 | 必须实现 | 写新实现 |
| `CheckPre` | 原样可用 | 可用，但**不能依赖 `context.getSource()`** | 注意 |
| `CheckSync` | 原样可用 | 原样可用，但**调用时机从 AFTER 阶段变为 CHECK 阶段** | 注意 |
| `CheckAfter` | 原样可用 | 可用，但**不能依赖 `context.getSource()` / `getTarget()`** | 注意 |
| `CheckResult` | 原样 | 不使用 | 无 |
| `CheckConfig` 其余字段 | 原样 | 原样 | 加 `mode` + 两个 ordered loader |
| `CheckContext` | 原样 | 部分字段为 null | 无 |
| 增量对账（`getDifferent`） | 可用（当前为 TODO 未完成） | **不提供** | 注意 |
| `doPre` 返回 false 时的执行状态 | **BEFORE**（NPE 导致的意外行为） | **ERROR**（显式） | 注意，行为不一致 |

四处「注意」是 STREAM 模式的真实迁移成本，必须在文档首页列出。它们不是 bug，是流式的固有代价 —— 数据流走之后就没有了。

## 7. 测试策略

流式实现的核心风险是「跑通了但结果是错的」—— merge join 在乱序数据上不会报错，只会静默产出错误差异。测试重心不是「能跑通」，而是「结果与全量路径逐条一致」。

### 7.1 L1：merge join 算法单测

用两个有序 `List` 喂给 `StreamingCheckProcessor`，断言产出的 `CheckStateEnum` 序列：

| 用例 | 输入 | 期望 |
|---|---|---|
| 完全一致 | src=[a,b], tgt=[a,b] | 无差异，comparedCount=2 |
| 源侧多 | src=[a,b], tgt=[a] | [SOURCE_MORE(b)] |
| 目标侧多 | src=[a], tgt=[a,b] | [TARGET_MORE(b)] |
| 字段不同 | src=[a{金额=1}], tgt=[a{金额=2}] | [DIFFERENT(a)] |
| 字段仅源侧有 | src=[a{x=1}], tgt=[a{}] | [DIFFERENT(a)]，含 x 字段 |
| 字段仅目标侧有 | src=[a{}], tgt=[a{x=1}] | [DIFFERENT(a)]，含 x 字段 |
| 交错 | src=[a,c], tgt=[b,c] | [SOURCE_MORE(a), TARGET_MORE(b)] |
| 源侧耗尽 | src=[], tgt=[a] | 抛 CheckException（单侧空，§4.3） |
| 目标侧耗尽 | src=[a], tgt=[] | 抛 CheckException（单侧空） |
| 两侧皆空 | src=[], tgt=[] | 无差异，各计数为 0 |

「字段仅一侧有」的两个用例实际验证**继承复用是否真的生效** —— 若 `StreamingCheckProcessor` 误用了别的比较逻辑，这里会红。

### 7.2 L2：契约测试（护栏）

| 用例 | 期望 |
|---|---|
| loader 返回乱序数据 | 抛 `CheckException`，消息含「未按 key 升序」 |
| 同侧 key 重复 | 抛 `CheckException` |
| key 为 null | 抛 `CheckException` |
| 单侧数据为空 | 抛 `CheckException("对账数据未准备好")`，且**零 sync 回调被触发** |

最后一条特别重要：验证 §4.3 的空检查确实发生在 merge 之前，没有产生任何 sync 副作用。

### 7.3 L3：对拍测试（核心）

同一份数据分别喂给 `DefaultCheckProcessor`（全量）与 `StreamingCheckProcessor`（流式），断言两边产出的差异**逐条相同**（key、state、differentFields 逐字段）。

随机数据生成：固定 seed（如 `Random(42)`），生成 100~1000 条，key 从小整数空间随机取。分两种用例 —— 合法有序数据（key 唯一）、含重复的非法数据（后者交由 L2 覆盖，不参与对拍）。

对拍适用前提：全量路径用 `Map` 比对，同 key 重复会抛异常，故对拍数据必须保证 key 唯一。

merge join 与 `compareMap` 是两套独立实现，任何一处对 `DIFFERENT` / `SOURCE_MORE` / `TARGET_MORE` 的判定分歧都会被对拍抓住。这是流式路径正确性的主要保证。

### 7.4 L4：端到端集成测试

走完整 `CheckExecutor`，验证：

| 用例 | 验证点 |
|---|---|
| STREAM 模式正常执行 | 状态流转 BEFORE → CHECK → AFTER → END；`getSummary()` 非 null；`getCheckResult()` 为 null |
| `CheckSync` 回调被调用 | 在 CHECK 阶段被调用（断言回调时 `executorManager.getCurrentStatus() == CHECK`） |
| `CheckSync` 返回 NO_SYNC | `isComplete()` 返回 false → 状态 ERROR |
| `CheckSync` 返回 IGNORE | `isComplete()` 返回 true → 状态 END |
| `CheckSync` 返回 SYNC | 状态 END |
| `doPre` 返回 false | 状态 **ERROR**（与 MEMORY 模式的 BEFORE 不一致，见 §5.2） |
| loader 抛异常 | 状态停在 **CHECK**，`isProcessing()` 为 false |
| 单侧数据为空 | 抛 `CheckException("对账数据未准备好")`，零 sync 回调 |
| MEMORY 模式行为不变 | 现有 `ExecutorTest` 全绿，**不需要改任何现有测试** |
| 组装期 fail-fast | `mode=STREAM` 缺 loader → `buildExecutor()` 抛异常 |

「现有测试一个都不用改」是这次改动的硬指标，直接验证「老路径零影响」的设计承诺。

### 7.5 内存特征的验证

不测「用了多少内存」（脆弱、依赖 GC），而是测**「数据源被消费的时机」**：

用惰性求值的 loader 记录被拉取的条数，断言：

- 内存路径：两条流一次性全部拉完才进入比对
- 流式路径：交错拉取，两侧拉取计数始终相差不超过 1

后者正是 merge join 的真实行为（预读一个，逐个推进），直接证明「没有全量物化」。

更强断言：用「生成无限序列」的 loader（key 递增），只消费前 N 条就中断，断言不会把无限序列读完（否则测试会挂住），直接证明流式是惰性的。

### 7.6 测试文件规划

```
src/test/java/io/github/kugin/reconciliation/
├── check/StreamingCheckProcessorTest.java       L1 + L2
├── check/StreamingParityTest.java               L3 对拍
├── executor/StreamingCheckExecutorTest.java     L4 端到端
└── before/OrderedResourceLoaderAdapterTest.java 适配器
```

全部使用 JUnit 5（仓库已从 JUnit 4.8.2 迁移到 5.11.3，commit `5db2444`）。

## 8. 关键决策记录

| # | 决策 | 理由 | 否决的替代方案 |
|---|---|---|---|
| 1 | 差异结果采用「即产即抛 + 统计摘要」 | 唯一同时满足内存解耦与边比边修的形态 | 保留全量 diff 列表（内存随总条数增长） |
| 2 | 新增流式专属结果类型 `CheckSummary`，不动 `CheckResult` | 避免破坏既有公开 API | 改造 `CheckResult`（回归面大） |
| 3 | 框架只做 merge join，排序责任上移到渠道 | 渠道形态本就各异；框架只保留一条算法路径 | 框架内置分桶归并（十万笔量级收益不足以抵消复杂度） |
| 4 | 「有序」通过 `OrderedResourceLoader` 接口类型层面承诺 | 编译器帮你把关，而非文档约定 | 复用 `ResourceLoader` 加默认方法（无法表达有序承诺） |
| 5 | 声明快照语义，写回责任归渠道 | 绝大多数实现天然满足；框架兜底物化等于自砸脚 | 框架内置物化（引入临时文件管理） |
| 6 | `NO_SYNC` 明细彻底丢弃，只留计数 | 回调时信息最全；保留明细在下游整体故障时会退化成全量驻留 | 保留全部失败明细（故障场景下内存爆炸） |
| 7 | 双模式并存，老路径零改动 | 纯增量组件，无一行改动老代码 | 统一为单一流式内核（需重写 `DefaultCheckProcessor` 与 `CheckResult`） |
| 8 | 字段级比较逻辑用继承复用 | 保证只有一份实现，避免两条路径行为分叉 | 组合封装（逻辑易分叉） |
| 9 | 空数据检查放在 merge 之前，用 `hasNext()` | 零成本零副作用，避免「已 sync 完才抛异常」 | merge 后检查（副作用已发生，语义错误） |
| 10 | 流式模式不提供增量对账（`getDifferent`） | 依赖跨次运行的持久化状态，与流式单遍语义冲突 | 实现增量对账（引入半成品功能） |

## 9. 迁移指引（文档首页需包含）

1. 实现 `OrderedResourceLoader`，保证严格升序、单遍、稳定快照、key 非 null。
2. `CheckConfig` 设 `mode = STREAM`，配置 `srcOrderedLoader` / `targetOrderedLoader`。
3. 检查现有 `CheckPre`：若依赖 `context.getSource()`，需改造。
4. 检查现有 `CheckAfter`：若依赖 `context.getSource()` / `getTarget()`，需改造。
5. 检查现有 `CheckSync`：若依赖「全部比对完成后才被调用」的假设，需改造。
6. 需要差异明细落盘的用户，在自己的 `CheckSync` 实现内部处理。
7. 增量对账（差异数据源）在流式模式下不提供，需另寻方案。
8. `OrderedResourceLoaderAdapter` 可作为过渡方案，但它仍是全量驻载，不是流式方案。
9. 注意 `doPre` 返回 false 时的执行状态与 MEMORY 模式不同（BEFORE vs ERROR）。
