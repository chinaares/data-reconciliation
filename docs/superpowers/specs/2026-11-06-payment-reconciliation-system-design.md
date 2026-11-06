# 支付对账系统（paycoin-reconciliation）设计规格

- 日期：2026-11-06
- 状态：待审查
- 分类：架构级（Architectural）

## 1. 背景与目标

### 1.1 现状

现有支付系统 `paycoin-i18n-ace`（Java 8 / Spring Boot 2.1.18 / 多模块 Maven / JPA + MySQL + Redis List 消息队列）是一个**代收代付平台**，包含渠道接入（`paycoin-channel-globalpay`、`paycoin-channel-dynamic`、`paycoin-channel-callback` 等），但**没有任何对账能力**（全库 grep 对账/reconcil 零命中）。

现有 `data-reconciliation` 是纯 Java 对账引擎库（JDK 21，无 Spring/DB/调度依赖），提供 `CheckExecutor` 三段流水线（before → check → after）、MEMORY/STREAM 双模式、`SOURCE_MORE`/`TARGET_MORE`/`DIFFERENT` 差异分类、防重护栏。引擎**不具备**金额精度语义、容差、单侧为空等能力（详见 §5）。

### 1.2 目标

新建独立项目 **`paycoin-reconciliation`**（与 paycoin 体系命名一致），复用 `data-reconciliation` 引擎作为依赖（不改动引擎本体，见 §5），补齐「与银行/支付渠道对账」能力：

1. 只读拉取 paycoin 代收流水 + 自下载 globalpay CSV 账单，逐笔比对
2. 差异落库、可查询、可导出报告给财务/风控
3. 差错单有状态流转 + 人工处理闭环（复核分离）
4. 硬底线：金额精度（分）、幂等、审计留痕

### 1.3 范围与成功标准

**MVP 范围（1 人 / 1 个月）**：
- 业务线：**代收 PayIn**（平台侧 `CollectForAnotherOrder` + `IncomeRecord`）
- 渠道：**globalpay**，账单为 **CSV 文件**
- 交付形态：后端完整 + 4 页极简运营后台（Thymeleaf）

**成功标准**：1 个月后运营能——① 触发/定时对账；② 差异落库、筛选、查看两侧字段对照；③ 差错单状态流转（认领→处理→复核→关闭，复核人≠处理人）；④ 导出对账日报给财务/风控。

### 1.4 非目标（YAGNI）

- 不做自动调账（自动补单/冲正）——MVP 全部人工工单
- 不做代付（PayOut）、充值、提现对账
- 不做 globalpay 以外渠道
- 不做 SFTP / API 拉账单（MVP 仅本地目录 / HTTP）
- 不做前后端分离（不引入前端构建链）
- 不做多租户（内部平台）
- 不做滚动重对（自动重跑历史未平账日期）——手动补跑即可
- 不做外部 IM 告警（钉钉/企微）——MVP 用页面醒目提示 + Redis 通知

### 1.5 关键约束

- 只读 paycoin，**绝不修改 paycoin 业务数据**（对账是裁判，不是运动员）
- 金额/手续费一律 `BIGINT` 存**分**（long），杜绝浮点
- 凭据零硬编码
- 引擎可升级，但升级作为**独立提交回 `data-reconciliation` 仓库单独改**，版本保持 `0.0.1-SNAPSHOT`（未正式发布，引擎随系统同步优化）

## 2. 总体架构与数据流

### 2.1 项目形态

```
paycoin-reconciliation/                    ← 新仓库，单模块 Spring Boot
├── controller/    ReconcileController、DiffController、TicketController、ReportController
├── service/       ReconcileService、TicketService、ReportService、BillDownloadService
├── recon/         引擎对接：PaycoinSourceLoader、GlobalpayCsvLoader、DiffPersistenceService
├── channel/       GlobalpayCsvParser（表头驱动）
├── entity/        自有库 JPA 实体（见 §3）
├── repository/    Recon 库 JPA + Paycoin 只读 JPA
├── dto/ exception/ config/ schedule/
└── PayReconciliationApplication
```

**技术栈**：Java 21 + Spring Boot + Lombok + Hutool + Redisson + JPA + MySQL 8 + Redis + Thymeleaf + Bootstrap。

**依赖**：`io.github.kugin:data-reconciliation:0.0.1-SNAPSHOT`（本地 `mvn install` 后引用）。

### 2.2 双数据源（`config/DataSourceConfig`）

| 数据源 | 库 | 权限 | 用途 |
|---|---|---|---|
| `recon`（主） | 自有库 | 读写 | 对账批次、差异、差错单、渠道账单、配置、审计 |
| `paycoin`（从） | paycoin 库 | **只读** | `CollectForAnotherOrder`、`IncomeRecord`（只配 Repository 查询，不开事务写） |

### 2.3 一次对账的完整链路（7 步）

1. **触发**：定时任务（每日 T+1）或运营手动触发 → Redisson `RLock`（`recon:globalpay:payin:{date}`），防重入
2. **平台侧加载** `PaycoinSourceLoader.load(date)` → 只读查 `CollectForAnotherOrder`（当日成功）+ `IncomeRecord` → 归一化金额 → `List<CheckEntry>`（key = `thirdpartMerOrderNo`）
3. **渠道侧加载** `GlobalpayCsvLoader.load(date)` → 下载/读取 globalpay CSV → 表头驱动解析 → `List<CheckEntry>`
4. **比对** `CheckExecutor.process(date)` → 引擎产出 `SOURCE_MORE`/`TARGET_MORE`/`DIFFERENT`
5. **差异落地** `DiffPersistenceService` → 读 `checkResult.diffDetails` → 每条差异写 `diff_record` + 生成 `ticket`（OPEN）
6. **批次归档** 写 `reconcile_batch`（笔数、金额、平账率、状态 SUCCESS/FAIL）→ 报告可导出
7. **通知** 差异结果写入 Redis（供报告页/告警消费），大额差异页面置顶

## 3. 数据模型（自有库 `recon`）

全局约定：金额/手续费 `BIGINT` 存**分**；所有表带 `created_at/updated_at`；`oper_log` 只追加不删改。

### 3.1 `reconcile_batch` — 对账批次

| 字段 | 类型 | 说明 |
|---|---|---|
| id | BIGINT PK | |
| batch_no | VARCHAR(64) UNIQUE | `{channel}-{bizType}-{yyyyMMdd}-{seq}` |
| channel / biz_type | VARCHAR(32) | `globalpay` / `payin` |
| account_date | DATE | 账务日 |
| status | VARCHAR(16) | RUNNING / SUCCESS / FAIL |
| source_total / target_total | INT | 平台笔数 / 渠道笔数 |
| matched_count / diff_count | INT | 平账数 / 差异数 |
| source_amount / target_amount | BIGINT | 平台总金额 / 渠道总金额（分） |
| diff_amount | BIGINT | 差异金额（轧差绝对值，分） |
| trigger_type | VARCHAR(16) | SCHEDULE / MANUAL |
| started_at / finished_at | DATETIME | |
| remark | VARCHAR(255) | |

### 3.2 `channel_bill` — 渠道账单文件

| 字段 | 类型 | 说明 |
|---|---|---|
| id | BIGINT PK | |
| channel / account_date | | `globalpay` + 账务日 |
| file_name / file_path | VARCHAR | 原始文件位置 |
| file_hash | VARCHAR(64) | SHA-256，防重复解析/篡改 |
| row_count | INT | 解析明细条数 |
| status | VARCHAR(16) | DOWNLOADED / PARSED / FAILED |
| error_msg | VARCHAR(255) | |

### 3.3 `diff_record` — 差异明细（核心表）

| 字段 | 类型 | 说明 |
|---|---|---|
| id | BIGINT PK | |
| batch_id | BIGINT FK | |
| channel / account_date | | |
| order_no | VARCHAR(64) | 对账 key（订单号） |
| diff_type | VARCHAR(16) | SOURCE_MORE / TARGET_MORE / AMOUNT_DIFF / FEE_DIFF / STATUS_DIFF |
| source_amount / target_amount | BIGINT | 两侧金额（分） |
| source_fee / target_fee | BIGINT | 两侧手续费（分，可空） |
| source_status / target_status | VARCHAR(16) | 两侧状态（可空） |
| diff_fields | JSON | 字段级差异 `{field:{source,target}}` |
| raw_source_json / raw_target_json | JSON | 原始快照（审计回溯） |
| handle_status | VARCHAR(16) | PENDING / IGNORED / RESOLVED |
| ticket_id | BIGINT FK | 关联差错单 |

**唯一约束**：`UNIQUE(channel, account_date, order_no)` —— 同渠道同日同订单只有一条差异（一单一差异），多个字段差异并进 `diff_fields` JSON。

### 3.4 `ticket` — 差错工单

| 字段 | 类型 | 说明 |
|---|---|---|
| id | BIGINT PK | |
| ticket_no | VARCHAR(64) UNIQUE | 工单号 |
| channel / account_date / order_no | | |
| diff_type | VARCHAR(16) | |
| status | VARCHAR(16) | OPEN → PROCESSING → PENDING_REVIEW → CLOSED |
| resolve_type | VARCHAR(16) | MANUAL_ADJUST / IGNORE_CROSS_DAY |
| level | VARCHAR(16) | NORMAL / CRITICAL（按金额阈值） |
| assignee | VARCHAR(64) | 认领人（运营） |
| resolution_remark | VARCHAR(255) | 处理方案 |
| reviewer / review_remark | VARCHAR(64/255) | 复核人 / 复核意见 |
| closed_at | DATETIME | |

### 3.5 `oper_log` — 操作审计（只追加）

| 字段 | 类型 | 说明 |
|---|---|---|
| id | BIGINT PK | |
| operator | VARCHAR(64) | 操作人 |
| action | VARCHAR(32) | 动作（TICKET_PROCESS / TICKET_REVIEW / DIFF_IGNORE 等） |
| target_type / target_id | VARCHAR/BIGINT | 操作对象 |
| before_json / after_json | JSON | 变更前后快照 |
| remark | VARCHAR(255) | |
| created_at | DATETIME | |

### 3.6 `reconcile_config` — 对账配置（运营可调，免发版）

| 字段 | 类型 | 说明 |
|---|---|---|
| id | BIGINT PK | |
| channel / biz_type | VARCHAR(32) | |
| amount_tolerance_cents | BIGINT | 金额容差（分，0 = 精确） |
| fee_tolerance_cents | BIGINT | 手续费容差（分） |
| date_tolerance_days | INT | 日期容差（跨日） |
| success_states | VARCHAR(255) | 平台侧成功状态集合（逗号分隔，对应 paycoin 字典值） |
| critical_amount_cents | BIGINT | 大额差异阈值（分） |
| enabled | BOOLEAN | 是否启用 |

## 4. 引擎对接（不修改引擎本体的前提下）

现有引擎的字段比较是裸 `Object.equals`（`AbstractCheckProcessor.compareCheckData`），无容差、无金额语义、单侧为空抛异常。**MVP 通过「归一化 + 读取结果」在对接层解决精度问题；容差与单侧为空通过 §5 的引擎升级解决。**

- **金额归一化**：在 `ResourceLoader` 产出 `CheckEntry` 之前，金额/手续费统一转为**分（long）**放入 `checkData` —— `Long.equals` 语义正确，引擎照常工作
- **差异落地**：`DiffPersistenceService` **不实现引擎 `CheckSync`**，而是在 `process()` 返回后读 `checkResult.diffDetails` 落地（引擎 `CheckSync` 传 null，默认 SYNC 透传）。引擎只负责「比」，新系统负责「落」

## 5. 引擎升级清单（回 `data-reconciliation` 单独提交）

3 处升级，**全部向后兼容**（默认行为不变），各自独立 commit + 补测试，**版本保持 `0.0.1-SNAPSHOT`**。

### 5.1 升级 1：数值比较语义修正（bugfix）

- **现状**：`compareCheckData` 用 `v.equals(targetData.get(k))`，`BigDecimal("10.0").equals(BigDecimal("10.00"))` 为 false，对金额错判
- **改法**：数值类型（`BigDecimal`/`Number`）用 `compareTo == 0`（scale 无关），非数值仍 `equals`
- **兼容**：非数值类型零影响

### 5.2 升级 2：金额容差（新能力）

- **现状**：无容差，差 1 分即 DIFFERENT
- **改法**：`CheckConfig` 增加 `tolerances: Map<String, BigDecimal>`（字段名 → 容差，单位与字段值一致）；`compareCheckData` 对数值字段先算 `|source − target| <= tolerance` 命中视为相等
- **兼容**：默认 null = 现状精确比较
- **形态约定**：容差只做在引擎层（`tolerances` map），不做业务化的「金额/手续费分开的规则表」；业务上通过给 fee 字段配不同容差值实现
- **与新系统的衔接**：`ReconcileService` 读取 `reconcile_config` 的 `amount_tolerance_cents` / `fee_tolerance_cents`，构造 `CheckConfig.tolerances = {amount: ..., handlingFee: ...}`（容差单位为分，与 checkData 中归一化后的分一致）

### 5.3 升级 3：单侧为空可配（新能力）

- **现状**：`compareCheck` 在「一侧为空、另一侧非空」时抛 `CheckException("对账数据未准备好")`
- **改法**：`CheckConfig` 增加 `allowEmptySide: boolean`（默认 false = 现状抛异常）；true 时一侧空 → 另一侧全部记单边差异（SOURCE_MORE / TARGET_MORE）
- **兼容**：默认 false 保持现有用户行为

## 6. 数据接入与差异分类

### 6.1 平台侧读取 `PaycoinSourceLoader`（实现引擎 `ResourceLoader`）

- 只读查询 `CollectForAnotherOrder`，过滤「当日（`dealTime` 在账务日区间）+ `orderState` ∈ 成功状态集合」
- **源数据精度坑**：paycoin 的 `amount`、`handlingFee` 是 **`Double`**。转换必须走 `String.valueOf(double)` → `BigDecimal` → `movePointRight(2)` 转分，显式 `HALF_UP`；**绝不能用 `(long)(amount*100)`**
- **key**：`thirdpartMerOrderNo`（渠道侧商户订单号，两边都有、最稳的关联键；**需与 globalpay CSV 实际列名核对**，可配置）
- **checkData**：`{amount: 分, handlingFee: 分, status: 归一化状态}`
- **成功状态可配**：`orderState` 值来自 paycoin 字典（`collectForAnotherOrderState`），成功值写入 `reconcile_config.success_states`，默认给常见成功值 + `thirdpartStatus` 成功值 "3"/"5"

### 6.2 渠道侧 `GlobalpayCsvLoader` + `GlobalpayCsvParser`

- `BillDownloadService`：从配置源（MVP：本地目录 / HTTP）取 CSV → SHA-256 → 写 `channel_bill`（DOWNLOADED），同 hash 已存在则跳过重解析
- `GlobalpayCsvParser`（**绝不写死列索引**）：
  1. **表头驱动**：读首行表头 → 按「列名映射」配置（CSV 表头名 → `orderNo/amount/fee/status`）定位列号
  2. **编码**：可配 UTF-8 / GBK，失败降级重试
  3. **金额**：CSV 里是「元」十进制字符串 → `new BigDecimal(String)` → 校验 scale ≤ 2（**超了拒绝并报 FAILED，不悄悄舍入**）→ 转分
  4. **产出** `List<CheckEntry>`（key 与 6.1 同源）
  5. 解析完写 `channel_bill`（PARSED + `row_count`）；失败写 FAILED + `error_msg`

### 6.3 差异分类 `DiffPersistenceService`

| 引擎状态 | 推导 `diff_type` | 逻辑 |
|---|---|---|
| `SOURCE_MORE` | `SOURCE_MORE`（平台有、渠道无） | 直接落 |
| `TARGET_MORE` | `TARGET_MORE`（渠道有、平台无） | 直接落 |
| `DIFFERENT` | `AMOUNT_DIFF` / `FEE_DIFF` / `STATUS_DIFF` | 看 `differentFields` 的 key 命中，优先级：**金额 > 手续费 > 状态**（一单一个主导类型，其余进 `diff_fields` JSON） |

每条差异 → 写 `diff_record`（含原始快照）→ 生成 `ticket`（OPEN）。

## 7. 差错工单状态机

```
OPEN ──认领(运营)──▶ PROCESSING ──提交方案──▶ PENDING_REVIEW
                        ▲                        │
                        │     复核驳回(财务)       │ 复核通过(财务)
                        └────────────────────────┘
                                                   ▼
OPEN / PROCESSING ──忽略(跨日)──▶ CLOSED          CLOSED
```

| 状态 | 操作者 | 动作 |
|---|---|---|
| OPEN | 运营 | 认领 → PROCESSING；或标记跨日忽略 → CLOSED |
| PROCESSING | 运营 | 填 `resolution_remark` + `resolve_type` → PENDING_REVIEW |
| PENDING_REVIEW | 财务 | 通过 → CLOSED；驳回（`review_remark`）→ 退回 PROCESSING |
| CLOSED | — | 终态，落 `closed_at` |

**关键约束**：
- **复核人 ≠ 处理人，强制校验**（`reviewer != assignee`，否则拒绝）—— 三权分离最小落地
- **MVP 不做自动调账**（`automaticPosting: false`）：所有差异进人工工单，绝不直接改 paycoin 数据
- 大额差异（超 `critical_amount_cents`）标 `level=CRITICAL`，列表置顶 + 告警

## 8. 调度、幂等与安全

### 8.1 调度

- 定时：`@Scheduled(cron)` 每日 T+1（如 02:30）触发 globalpay 代收对账
- 手动：`POST /api/reconcile/run?date=...`（补跑历史日期）

### 8.2 幂等（三层）

1. 引擎 `RedisExecutorManager`（Redisson）防同 id+date 重入
2. Redisson `RLock`（`recon:globalpay:payin:{date}`，带超时 + 看门狗）
3. `reconcile_batch.batch_no` 唯一 + `diff_record` 的 `UNIQUE(channel, account_date, order_no)` **upsert**

**重跑语义**：同日期重跑 = 新 `batch_no`（seq 递增），diff 走 upsert 覆盖，报告可追溯每版。

### 8.3 安全

- paycoin **只读账号** + 最小权限，从库不开事务写
- **凭据零硬编码**：渠道下载凭据、DB 密码走环境变量/KMS
- **审计**：`oper_log` 只追加；导出 CSV 做公式注入转义（`= + @ - \t \r` 前缀）
- **权限**：Spring Security + 四角色（运营/财务/风控/系统管理员），风控只读；复核、忽略等敏感操作二次确认

## 9. 报告与运营页面

### 9.1 报告（给财务/风控）

**不建 report 表** —— 从 `reconcile_batch` + `diff_record` 实时聚合导出。

| 指标 | 定义 |
|---|---|
| 平台笔数 / 渠道笔数 | `source_total` / `target_total` |
| 平台金额 / 渠道金额 | `source_amount` / `target_amount`（分） |
| 平账数 / 差异数 | `matched_count` / `diff_count` |
| 平账率 | `matched_count / (matched_count + diff_count) × 100%` |
| 差异金额 | `|source_amount − target_amount|`（轧差） |
| 差异分布 | 按 `diff_type` 分组计数+金额 |

**导出**：CSV（UTF-8 带 BOM + 公式注入转义），差异明细可按日期/类型/状态筛选。

### 9.2 极简页面（4 页，Thymeleaf + Bootstrap）

| 页面 | 内容 |
|---|---|
| 对账批次 | 批次列表（日期/状态/笔数/金额/平账率）、手动触发、查看详情 |
| 差异池 | 差异列表（筛选：日期/类型/状态/金额区间）、详情抽屉（两侧字段对照）、导出 |
| 差错工单 | 工单列表（状态/认领人/级别）、认领/处理/复核/忽略操作 |
| 报告 | 日报 + 导出下载 |

## 10. 错误处理与测试

### 10.1 错误处理

- 统一响应：`Result` 封装 + `@RestControllerAdvice` 全局异常
- **三级失败不静默**：
  1. 账单未就绪：CSV 缺失/下载失败 → `channel_bill.status=FAILED` + `error_msg`，不跑对账，告警
  2. 解析失败：编码/列缺失/金额精度超限 → `FAILED` + 明确 `error_msg`
  3. 对账异常：`reconcile_batch.status=FAIL` + 异常堆栈留痕，告警
- 告警：差异率超阈值 / 大额差异 / 任务失败 → Redis 通知 + 页面醒目提示

### 10.2 测试策略

| 层 | 内容 |
|---|---|
| 引擎仓库（data-reconciliation） | 3 项升级各配单测：BigDecimal scale 无关、容差命中/边界、`allowEmptySide` 两侧空/单侧空 |
| 新系统单测 | CSV 解析器（表头驱动/编码/金额转分/边界 `0.1+0.2`、`99999999999999.99`）、Double→分转换、差异分类优先级、工单状态机（含复核人≠处理人） |
| 集成测试 | Testcontainers（MySQL + Redis）+ 真实引擎跑通一条造数对账链路（5 笔样例：平账/长款/短款/金额差/手续费差） |
| 对拍测试 | 引擎 MEMORY vs STREAM 结果一致 |

## 11. 关键决策记录

| # | 决策 | 选择 |
|---|---|---|
| 1 | 项目定位 | 内部平台，独立新项目，不改 paycoin 本体 |
| 2 | 架构方案 | 单模块 Spring Boot + 引擎作依赖 + 双数据源（方案 1） |
| 3 | 数据接入 | 只读直连 paycoin MySQL + 自下载渠道 CSV（方案 A） |
| 4 | MVP 范围 | 代收 PayIn + globalpay + CSV |
| 5 | 交付形态 | 后端完整 + 极简运营后台（方案 B） |
| 6 | 引擎升级 | 3 项向后兼容升级，回 data-reconciliation 单独提交，版本保持 0.0.1-SNAPSHOT |
| 7 | 关联键 | `thirdpartMerOrderNo`（可配，需与 CSV 核对） |
| 8 | 金额存储 | `BIGINT` 存分（long），Double 源数据经 `String.valueOf → BigDecimal → HALF_UP` 转换 |
| 9 | 差异粒度 | 一单一差异（`UNIQUE(channel, account_date, order_no)`），主导类型 + 字段级 JSON |
| 10 | 差错处理 | 全部人工工单，复核人≠处理人，无自动调账 |
| 11 | 报告 | 实时聚合导出，不建 report 表 |
| 12 | 容差 | 引擎层 `tolerances` map + `reconcile_config` 业务参数 |
