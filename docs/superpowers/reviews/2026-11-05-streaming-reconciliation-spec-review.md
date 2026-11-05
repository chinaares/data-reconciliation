# 流式对账设计规格 —— 评审意见与优化清单

- 日期：2026-11-05
- 评审对象：`docs/superpowers/specs/2026-11-05-streaming-reconciliation-design.md`（状态：待审查）
- 关联计划：`docs/superpowers/plans/2026-11-05-streaming-reconciliation.md`
- 实施工作区：`.claude/worktrees/streaming-reconciliation`（分支 `worktree-streaming-reconciliation`）
- 评审方式：规格逐条核对 + 源码对照（`AbstractCheckProcessor` / `AbstractBeforeCheckProcessor` / `AbstractAfterCheckProcessor` / `CheckExecutor` / `DefaultAfterCheckProcessor` / `CheckSync` / `CheckEntry` / `DefaultExecutorManager`）
- 性质：**记录性文档**，不代表已实施；每条给出「建议处置」与「处置状态」，供后续优化取用

---

## 1. 结论摘要

**总体等级：设计评审级（Design Review Ready），距「可放心交由新人按规格独立落地」还差一轮收口。**

一句话评价：**算法与契约想得很透、测试策略是全场最强项，但存在 1 处伪码自相矛盾 + 1 处规格内部不一致 + 若干正确性/运维边角尚未定义。**

本清单中的问题**不阻塞**当前计划推进（任务 1、2 已完成，任务 3 起可继续），但建议在任务 6（对拍与文档）收尾时一并处理，避免规格与实现长期漂移。

| 维度 | 评分 | 简评 |
|---|---|---|
| 目标 / 非目标清晰度 | ★★★★★ | 目标、YAGNI、兼容边界、迁移成本都写死了，「老路径零影响」有硬指标 |
| 算法正确性 | ★★★★☆ | merge join 逻辑正确，但 §4.1 伪码有「首元素绕过护栏」缺陷（D1） |
| 契约完备性 | ★★★☆☆ | 排序契约内部自相矛盾（D2）；null 值语义未覆盖（D4） |
| 错误语义 | ★★★★★ | §5.2 状态机逐场景对齐到 BEFORE/CHECK/ERROR，是全文最扎实的部分 |
| 测试策略 | ★★★★★ | 对拍为主保证 + 惰性消费证明 + 零副作用断言，全场最强项 |
| 可运维性 | ★★☆☆☆ | 「边比边修」却无进度/观测手段，长任务全程黑盒（方向 C） |
| 前瞻性 | ★★★☆☆ | 幂等语义（D6）、比较器单一来源（D2）未显式处理 |

---

## 2. 缺陷清单

### D1 —— §4.1 伪码的「预读」绕过了 §4.4 护栏

**规格位置**：§4.1（`design.md:293-326`，预读在 `:296-297`）、§4.4（`:377-389`）

```java
CheckEntry s = src.hasNext() ? src.next() : null;   // 预读，未经过 advance()
```

`advance()` 是规格中唯一做 null-key 校验与升序校验的地方，但**首元素**是通过预读取得的，绕过了它：

- 首元素 key 为 null → 两侧首元素都 null 时 `s.getKey().compareTo(t.getKey())` 直接 NPE；
- 首元素 null、另一侧为空 → 退化成「对账数据未准备好」，错误信息误导排障；
- 首元素乱序 → 升序护栏完全不生效。

**影响**：规格自称「护栏把它变成响亮的失败」（§4.4），但按伪码直译会留下一个静默缺口——正是规格自己点名的头号风险（「跑通了但结果是错的」）。

**建议处置**：回写 §4.1/§4.4，补一句约束：**每一个被消费的元素（含两侧首元素）都必须经过同一条校验路径**；首次 advance 传 `lastKey = null` 即可复用既有校验。

**状态**：✅ 已由实施计划的预检裁定 **C1** 覆盖（实现侧正确），⚠️ **规格文本仍需回写**。

---

### D2 —— 排序比较器自相矛盾（本次评审最实质的一处）

**规格位置**：§3.1 契约 1（`design.md:130-141`）、§3.1.1 适配器代码（`:180-191`）、§4.1 merge 比较（`:310`）、§4.4 护栏（`:385`）

三处规定彼此冲突：

| 出处 | 规定 |
|---|---|
| §3.1 契约 1 | 严格升序，语义为 `String.compareTo` |
| §3.1.1 适配器 | 暴露**可自定义** `Comparator<String> keyComparator`（默认 `String::compareTo`） |
| §4.1 / §4.4 | merge 与护栏**硬编码** `e.getKey().compareTo(...)` |

**影响**：若给适配器传入 `Comparator.reverseOrder()` 或忽略大小写的比较器，数据按该比较器排序，但 merge 与护栏仍用 `compareTo` 判断 → 要么误报「未按 key 升序」，要么**静默产出错误比对结果**。这正是审查重点 #3 想防的「两侧比较器不同」，但它的根源是**规格内部不一致**，而非单纯文档缺漏。

**建议处置**：见「方向 B」——建议**删除适配器的两参构造器**（契约本就锁死 `String.compareTo`，自定义比较器属投机泛化），从根源消除该冲突；备选方案是把比较器提升为单一共享配置（`CheckConfig.keyComparator`），由护栏、merge、适配器统一取用。

**状态**：⏳ 待决策（实施计划的 Ruling C 仅在 javadoc 上写明「两侧必须使用同一比较器规则」，未解决「护栏硬编码 compareTo」这一层）。

---

### D3 —— `preCheckFailed` 字段缺失于规格，规格单独无法落地

**规格位置**：§5 端到端流程（`design.md:399-426`，`:406`）、§2.2 修改表（`:78-86`）

规格流程图写「若 `doPre` 返回 false → 不进入 CHECK，直接进入 AFTER」，但 `CheckExecutor.process` 是**无条件**调用 `checkProcessor.compare(context)` 的（源码 `CheckExecutor.java:109`）。规格**没有给出承载「跳过 CHECK」信号的字段**，实现者按规格写会得到「doPre 失败后仍然进入 CHECK」的流程。

**建议处置**：把 `CheckContext.preCheckFailed`（boolean，默认 false）升格写入 §2.2「修改」表与 §5 流程图，并在 §3.4 补一行说明其语义（仅 STREAM 路径使用，MEMORY 路径不读它，行为不变）。

**状态**：✅ 已由实施计划的「规格偏离说明」补齐实现，⚠️ **规格文本仍需回写**。

---

### D4 —— null 值语义未覆盖（含共享比较逻辑的一处既有 NPE）

**规格位置**：§3.1 契约（只禁 **key** 为 null）、§4.1（`:318` 直接 `s.getCheckData()`）

规格只禁止 key 为 null，但未定义：

1. `CheckEntry.checkData` 为 null 时的行为；
2. 字段值为 null 时的行为。

**源码佐证**：复用的 `compareCheckData`（`AbstractCheckProcessor.java:98`）写作

```java
if (targetData.containsKey(k) && v.equals(targetData.get(k))) { ... }
```

源侧字段值为 null 时 `v.equals(...)` 直接 **NPE**。这是既有缺陷（MEMORY 路径同样中招），但流式路径「复用一份实现」的设计把它一并继承，且流式下 NPE 发生在**已部分 sync 之后**，暴露面显著更大。

**建议处置**：

- 把 `v.equals(...)` 改为 `Objects.equals(v, targetData.get(k))`（一行硬化，MEMORY/STREAM 双路径同时受益）；
- 在 §3.1 契约中补齐：「key 不得为 null；`checkData` 不得为 null；字段值允许为 null」。

**状态**：⏳ 待处置（属「方向 A」，低成本高收益）。

---

### D5 —— `CheckSync` 回调返回 `null` 未定义

**规格位置**：§4.2 emit 动作序列（`design.md:330-341`）

`emit()` 第 3 步执行 `checkUnit.setSync(result)` 并累加 `syncCounts`。若用户实现未遵守「返回 `CheckSyncEnum`」约定而返回 `null`，则计数 map 会出现 null 值，`getOrDefault(NO_SYNC, 0L)` 的读取语义将出现意外。

**建议处置**：在 §4.2 明确「回调返回 null 一律归一化为 `CheckSyncEnum.SYNC`」，并在 §4.2/§7.2 补一条用例钉住。

**状态**：⏳ 待处置。

---

### D6 —— `CheckSync` 幂等契约缺失（STREAM 特有运维风险）

**规格位置**：§5.2「关于 sync 副作用不回滚」（`design.md:462`）、§9 迁移指引（`:614-624`）

规格只说明「已 sync 的部分不回滚」，**没有要求 `CheckSync` 实现幂等**。但流式模式下副作用与读取交错执行：任务中途崩溃后重跑，会**重复回调已处理过的差异**。这是流式相对 MEMORY 的真实新增风险（MEMORY 的 `autoSync` 在 AFTER 一次性执行，重跑时数据仍完整在内存中）。

**建议处置**：在 §3.3 增加一条契约「STREAM 模式下 `CheckSync` 实现应当幂等：同一 key 的回调可能被重复投递」，并在 §9 迁移指引第 5 条补一句提示。

**状态**：⏳ 待处置（属「方向 D」）。

---

## 3. 优化方向

| # | 方向 | 内容 | 权衡 | 建议 | 优先级 |
|---|---|---|---|---|---|
| A | 共享字段比较逻辑健壮性加固 | `compareCheckData` 用 `Objects.equals`；契约补 null 语义 | 极低风险，纯硬化 | ✅ 做 | 高 |
| B | 消除比较器多重来源 | 删掉适配器两参构造器（推荐）；或比较器提升为单一共享配置 | 方案 1 最简但失去定制能力；方案 2 增 API 面 | ✅ 做（方案 1） | 高 |
| C | 补齐「边比边修」的可观测性 | 廉价版：每 N 条打一条 info 日志；升级版：可选 `ProgressListener` 回调 | 廉价版近零成本；Listener 多一个 API | ✅ 做廉价版，Listener 列为后续需求 | 中高 |
| D | 显式声明 `CheckSync` 幂等契约 | 契约 + 迁移指引各补一条 | 无成本，纯文档 | ✅ 做 | 中 |
| E | `samples` 数据结构对齐 | `List<CheckUnit>` → `Map<CheckStateEnum, List<CheckUnit>>` | 改动小；但计划已按扁平 list 落地，返工成本低 | 可选 | 低 |
| F | 测试策略补三块 | ① null 字段值/null checkData ② `CheckSync` 返回 null ③ 「适配器传 reverseOrder → 护栏报错」钉住 D2 现状；另可考虑 jqwik 属性测试替代固定 seed | 成本低，收益是覆盖新定义 | ✅ 做①②③，属性测试可选 | 中 |
| G | 继承 vs 组合的长期再评估 | 把 `compareCheckData`/`createSingle` 抽成无状态工具类，两路径组合调用 | 与「老代码零改动」硬指标冲突，风险上升 | ⏸ 本轮不做，记入 backlog | 低 |

> **方向 B 补充说明**：删除自定义比较器后，规格 §3.1.1 适配器代码（`design.md:180-191`）需同步简化为单参构造器；同时「审查重点 #3」在规格中的落地方式从「文档提醒」升级为「类型层面不可能配错」，这是本轮收益最大的单点改进。

---

## 4. 与实施计划的关系

实施工作区的进度账本 `.claude/worktrees/streaming-reconciliation/.superpowers/sdd/2026-11-05-streaming-reconciliation/progress.md` 已包含一组预检裁定（C1–C10），与本清单存在重叠：

| 本清单条目 | 计划中的对应裁定 | 关系 |
|---|---|---|
| D1 伪码绕过护栏 | **C1**（每个元素含首元素均须过校验）、**C2**（`lastKey` 消费即更新）、**C3**（`IllegalStateException` 不得包装） | ✅ 实现侧已覆盖，规格文本待回写 |
| D3 `preCheckFailed` | 计划的「规格偏离说明」 | ✅ 实现侧已覆盖，规格文本待回写 |
| D2 比较器冲突 | 任务 2 修复轮 Ruling C（仅在 javadoc 写明「两侧必须同序」） | ⚠️ **仅文档层面缓解，根因未解** |
| 字段名 `getTgtOrderedLoader` 笔误 | **C8**（规格 §5.3 孤例笔误，以 `targetOrderedLoader` 为准） | ✅ 已裁定 |
| D4/D5/D6 | 无对应裁定 | ⏳ 全新 |

**文档漂移风险**：规格在 `master`，计划与实现进行中的分支为 `worktree-streaming-reconciliation`。优化项回写规格时，须同步核对计划中是否已固化了不同做法（尤其 D2 —— 若实现保留了两参构造器而规格删除，需同步改计划与测试）。

---

## 5. 待人类决策的问题

1. **D2 的处置**：删除适配器的自定义比较器构造器（推荐），还是改为单一共享配置？—— 决定规格 §3.1.1、适配器实现与测试三者是否需要改动。
2. **D4 的修复范围**：是否在本次一并修 `AbstractCheckProcessor.compareCheckData` 的 `equals` NPE？该文件属于「老代码零改动」范围，但缺陷同时影响 MEMORY 路径。
3. **D6 的强硬度**：把「幂等」写成契约（推荐）还是仅作为迁移提示？—— 影响渠道方改造 `CheckSync` 的成本评估。

---

## 6. 建议补充的测试清单（对应 §7 测试策略）

| 用例 | 归属 | 目的 |
|---|---|---|
| 源侧字段值为 null 时不抛 NPE，差异字段正确呈现 | L1 | 钉住 D4 修复 |
| `checkData` 为 null 时的行为（抛 `CheckException` 或跳过） | L1/L2 | 钉住 D4 契约 |
| `CheckSync` 回调返回 null 时按 SYNC 计入 | L2 | 钉住 D5 |
| 适配器传入 `reverseOrder` 比较器 → 护栏报「未按 key 升序」 | L2 | 记录 D2 现状（或在方向 B 落地后改为「编译期不可能配错」） |
| 同一差异 key 在重跑场景下被重复回调 | L4 | 支撑 D6 幂等契约 |

---

## 7. 变更记录

| 日期 | 变更 |
|---|---|
| 2026-11-05 | 初版：记录 D1–D6 缺陷、优化方向 A–G、与实施计划的交叉引用、待决策项与补充测试清单 |
