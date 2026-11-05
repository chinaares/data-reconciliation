# data-reconciliation

## 项目简介
通用对账处理工具
- 实现目的：大批量的比较任意两个数据源的任意指定数据内容间的差异，并能自动进行自定义的数据修改同步操作。
> 当前版本：`0.0.1-SNAPSHOT`

## 使用说明
项目执行器`CheckExecutor`是由三个处理器连接起来的：
- `BeforeCheckProcessor`：前置处理器，负责准备对账的双方数据，以及一些前置必要的校验等逻辑。
- `CheckProcessor`：对账处理器，负责双方数据源指定对比内容的数据对比。
- `AfterCheckProcessor`：后置处理器，负责对对账结果的处理，包含自动调账，自定义操作等逻辑。

![CheckExecutor](http://data.kugin.top/other%2FCheckExecutor.png)

对比实体`CheckEntry`的转化方式现实现两种：
1. 基于注解`@CheckIdentity`比较唯一标识,`@CheckField`待比较的字段。
2. 基于适配接口`CheckAdapter`，不同的对比实体实现该接口的`getKey()`与`getCheckData()`方法即可。


`CheckConfig`的配置参数说明：

|         参数         |        参数描述        | 是否必须 |                           备注                           |
| :------------------: | :--------------------: | :------: | :------------------------------------------------------: |
|          id          |      对账唯一标识      |    C     |        name与id不可同时为空,id为空时默认使用name         |
|         name         |        对账名称        |    N     |            名称可能重复，name与id不可同时为空            |
|       checkPre       |        前置校验        |    N     |                支持函数式与自定义接口实现                |
|      srcLoader       |       上游数据源       |    C     |                支持函数式与自定义接口实现                |
|     targetLoader     |       下游数据源       |    C     |                支持函数式与自定义接口实现                |
|    resourceReader    | 全数据源（包含上下游） |    C     | 与`srcLoader`、`targetLoade`r 二选一，支持自定义接口实现 |
|         mode         |        执行模式        |    N     |          默认 `MEMORY`；可选 `STREAM`（流式比对）           |
|   srcOrderedLoader   | STREAM 模式上游有序数据源 |    C     |   仅 STREAM 模式使用，需按 key（`String.compareTo`）严格升序    |
| targetOrderedLoader  | STREAM 模式下游有序数据源 |    C     |   仅 STREAM 模式使用，需按 key（`String.compareTo`）严格升序    |
|      checkSync       |    对账调账同步处理    |    N     |                  默认不做修改，直接通过                  |
|      checkAfter      |    对账后置处理逻辑    |    N     |                     默认不做任何处理                     |
| beforeCheckProcessor |       前置处理器       |    N     |    可以自定义实现或继承`AbstractBeforeCheckProcessor`    |
|    checkProcessor    |       对比处理器       |    N     |       可以自定义实现或继承`AbstractCheckProcessor`       |
| afterCheckProcessor  |       后置处理器       |    N     |    可以自定义实现或继承`AbstractAfterCheckProcessor`     |


使用样例参考 `CheckEntryTest`：对于对比实体的包装方式，`ExecutorTest`：执行器的构建与执行，`SimpleCheckExampleTest`：最简完整示例（注解/适配器两种包装方式 + 差异断言）。

### 流式比对（STREAM 模式）

当单日数据量大、两侧数据都能按 key 升序获得时，可把 `CheckConfig.mode` 设为 `CheckMode.STREAM`，用 merge join 做流式比对：任意时刻两侧各只持有一个当前元素与一个游标，内存占用与总条数无关；差异一产生就回调 `CheckSync` 后丢弃，不产出全量 `CheckResult`，只产出 `CheckSummary`。

- **何时用**：日交易量大、两侧数据可按键升序获得，且只需要「差异 + 摘要」、不需要保留全量差异明细时。数据量小或需要保留全量结果时继续使用默认的 `MEMORY` 模式。
- **契约**：`srcOrderedLoader` / `targetOrderedLoader` 各自必须按 key 严格升序（同侧不得重复），key 不得为 null。**两侧必须使用同一个 key 比较器（`String.compareTo` 语义）**：框架按 `String.compareTo` 合并两侧，但无法校验两侧是否同序——若两侧排序规则不一致，框架无法发现错序，结果会静默错误。
- **快照语义**：比对期间被读取的一侧视为不可变快照。`CheckSync` 若要写回该侧数据，渠道需在自己的 loader 内先物化（例如先落库或先读入内存后再流式返回），不要在回调里直接改动正在被遍历的数据源。
- **迁移注意**：
  - `CheckPre` / `CheckAfter` 不能依赖 `context.getSource()` / `context.getTarget()`：流式模式下这两个字段为 null，全量数据不会加载；
  - `CheckSync` 的调用时机从 AFTER 阶段提前到 CHECK 阶段，异常与副作用语义随之变化；
  - 增量对账（差异数据源）在流式模式下不提供；
  - `doPre` 返回 false 时 STREAM 状态为 `ERROR`，而 MEMORY 模式此时状态停在 `BEFORE`（已知且有意的不对齐）；
  - 是否通过由 `CheckSync.isComplete` / 摘要判定，而不是完整比对结果。
- `OrderedResourceLoaderAdapter` 是过渡方案：它把已有的全量 `ResourceLoader` 排序后转成 `Stream<CheckEntry>`，**仍是全量驻载**，不提供流式内存特性，仅用于渐进迁移与对拍测试，不能作为「已实现流式」的依据。


### TODO:
- [x] 基于redis的对账处理器实现
- [x] 基于redis的执行器管理
- [x] 资源加载器FileResourceLoader实现自动解析
- [x] 相同形式的数据源对比,流式处理比对