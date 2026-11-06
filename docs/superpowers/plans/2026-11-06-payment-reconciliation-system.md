# paycoin-reconciliation 实现计划

> **面向 AI 代理的工作者：** 必需子技能：使用 subagent-driven-development（推荐）或 executing-plans 逐任务实现此计划。步骤使用复选框（`- [ ]`）语法来跟踪进度。

**目标：** 新建 `paycoin-reconciliation`（单模块 Spring Boot）系统，复用 `data-reconciliation` 引擎依赖，实现「globalpay 代收 CSV 账单 ↔ paycoin 代收流水」逐笔对账 + 差异落库 + 差错工单 + 报告导出。

**架构：** 引擎 3 项向后兼容升级回 `data-reconciliation` 仓库单独提交（版本保持 `0.0.1-SNAPSHOT`）；新系统单模块分层 + 双数据源（`recon` 主库读写、`paycoin` 只读），`ReconcileService` 编排 7 步对账链路，`DiffPersistenceService` 读引擎结果落地，`TicketService` 管工单状态机，4 页 Thymeleaf 后台。

**技术栈：** Java 21、Spring Boot 3.3.x、Lombok、Hutool、Redisson、JPA/Hibernate、MySQL 8、Flyway、Redis、Thymeleaf、JUnit 5 + Mockito + Testcontainers。

**规格：** [2026-11-06-payment-reconciliation-system-design.md](../specs/2026-11-06-payment-reconciliation-system-design.md)

**新项目路径：** `/Users/yangzb/paycoin-projects/paycoin-reconciliation`（与 `paycoin-i18n-ace` 同级）；引擎仓库 = 本仓库 `/Users/yangzb/IdeaProjects/_reconciliation/data-reconciliation`。

## 全局约束

- Java 21（JDK 21 toolchain，与引擎一致）
- 引擎依赖坐标 `io.github.kugin:data-reconciliation:0.0.1-SNAPSHOT`，升级后版本号**不变**（`0.0.1-SNAPSHOT`）
- 引擎升级全部**向后兼容**：默认行为（无容差、无 allowEmptySide）不变
- 金额/手续费一律 `long`（分）/ 数据库 `BIGINT`，绝不用 double 直接参与比较
- 新系统只读 paycoin 库，绝不写 paycoin；paycoin 表名 `collect_for_another_order`、`income_record`
- paycoin 源金额字段是 `Double`（`amount`、`handling_fee`），转换必须走 `String.valueOf → BigDecimal → HALF_UP → 分`
- 新系统基包 `com.simplepay.reconciliation`，表名/列名 snake_case
- 差异一单一记录：`UNIQUE(channel, account_date, order_no)`
- 工单复核人 ≠ 处理人强制校验
- MVP 不做自动调账，全部人工工单
- 凭据零硬编码（DB 密码、渠道下载凭据走环境变量）
- 引擎仓库构建命令 `mvn test -Dtest=<Class>`；新系统 `./mvnw test -Dtest=<Class>`

## 审查重点（Review Focus）

1. **浮点金额陷阱**：`Double` 源金额 `0.29`（double 实为 0.28999…）、`0.1+0.2`（0.30000000000000004）、`99999999999999.99` 转分必须精确，不得差 1 分 → 由任务 6 `AmountUtilTest` 钉住
2. **CSV 金额精度超限**：如 `"100.555"` 必须拒绝并置 `channel_bill.status=FAILED`，不得悄悄舍入 → 由任务 7 `GlobalpayCsvParserTest` 钉住
3. **CSV 表头缺列**：列名映射匹配不到必需列时必须 FAILED + 明确 `error_msg`，不得静默少比对 → 由任务 7 钉住
4. **单侧为空**：渠道当日无流水时 `allowEmptySide=true` 应产出全部单边差异而非抛异常 → 引擎任务 3 钉住
5. **复核越权**：`reviewer == assignee` 必须拒绝（三权分离）→ 任务 12 `TicketServiceTest` 钉住
6. **重跑幂等**：同日期重跑不得产生重复 `diff_record` → 任务 11 集成测试钉住

---

## 阶段 1：引擎升级（data-reconciliation 仓库，独立 commit）

### 任务 1：数值比较语义修正（BigDecimal/Number 用 compareTo）

**文件：**
- 修改：`src/main/java/io/github/kugin/reconciliation/check/AbstractCheckProcessor.java`（`compareCheckData`）
- 测试：`src/test/java/io/github/kugin/reconciliation/check/NumericCompareTest.java`

- [ ] **步骤 1：编写失败的测试**

```java
class NumericCompareTest {
    private final AbstractCheckProcessor processor = new DefaultCheckProcessor();

    @Test
    void bigDecimal_scaleIndependent_equals() {
        Map<String, Object> s = Map.of("amount", new BigDecimal("10.0"));
        Map<String, Object> t = Map.of("amount", new BigDecimal("10.00"));
        assertTrue(processor.compareCheckData(s, t).isEmpty());
    }

    @Test
    void long_values_compare_numerically() {
        Map<String, Object> s = Map.of("amount", 100L);
        Map<String, Object> t = Map.of("amount", 100L);
        assertTrue(processor.compareCheckData(s, t).isEmpty());
    }

    @Test
    void nonNumeric_uses_equals() {
        Map<String, Object> s = Map.of("status", "SUCCESS");
        Map<String, Object> t = Map.of("status", "SUCCESS");
        assertTrue(processor.compareCheckData(s, t).isEmpty());
        Map<String, Object> t2 = Map.of("status", "FAIL");
        assertEquals(1, processor.compareCheckData(s, t2).size());
    }
}
```

- [ ] **步骤 2：运行测试验证失败**

运行：`mvn test -Dtest=NumericCompareTest`
预期：FAIL，`bigDecimal_scaleIndependent_equals` 断言失败（`10.0` vs `10.00` 被误判为差异）

- [ ] **步骤 3：修改 `compareCheckData`，数值类型用 compareTo**

在 `AbstractCheckProcessor.compareCheckData` 的 source 遍历中，把 `v.equals(targetData.get(k))` 替换为私有方法 `equivalent(v, targetData.get(k), null)`；新增 `protected static boolean equivalent(Object a, Object b, BigDecimal tolerance)`：两者皆 null 为 true；一者 null 为 false；`a instanceof Number && b instanceof Number` 时转 `BigDecimal` 用 `compareTo == 0`（`tolerance` 为 null）；否则 `a.equals(b)`。

- [ ] **步骤 4：运行测试验证通过**

运行：`mvn test -Dtest=NumericCompareTest`
预期：PASS

- [ ] **步骤 5：Commit**

```bash
git add src/main/java/io/github/kugin/reconciliation/check/AbstractCheckProcessor.java src/test/java/io/github/kugin/reconciliation/check/NumericCompareTest.java
git commit -m "fix(recon): compare numeric fields with compareTo, not equals"
```

### 任务 2：金额容差（CheckConfig.tolerances）

**文件：**
- 修改：`src/main/java/io/github/kugin/reconciliation/domain/CheckConfig.java`（加 `tolerances`）
- 修改：`src/main/java/io/github/kugin/reconciliation/check/AbstractCheckProcessor.java`（字段 + 重载）
- 修改：`src/main/java/io/github/kugin/reconciliation/check/DefaultCheckProcessor.java`（构造）
- 修改：`src/main/java/io/github/kugin/reconciliation/executor/CheckExecutor.java`（wiring）
- 测试：`src/test/java/io/github/kugin/reconciliation/check/ToleranceTest.java`

- [ ] **步骤 1：编写失败的测试**

```java
class ToleranceTest {
    private final AbstractCheckProcessor p = new DefaultCheckProcessor(Map.of("amount", new BigDecimal("2")), false);

    @Test
    void within_tolerance_is_equal() {
        assertTrue(p.compareCheckData(Map.of("amount", 100L), Map.of("amount", 102L)).isEmpty());
    }

    @Test
    void boundary_equal_to_tolerance_is_equal() {
        assertTrue(p.compareCheckData(Map.of("amount", 100L), Map.of("amount", 102L)).isEmpty());
    }

    @Test
    void beyond_tolerance_is_diff() {
        assertEquals(1, p.compareCheckData(Map.of("amount", 100L), Map.of("amount", 103L)).size());
    }
}
```

- [ ] **步骤 2：运行测试验证失败**

运行：`mvn test -Dtest=ToleranceTest`
预期：FAIL，编译错误（`DefaultCheckProcessor` 无此构造 / `CheckConfig` 无 `tolerances`）

- [ ] **步骤 3：实现容差链路**

- `CheckConfig` 加 `private Map<String, BigDecimal> tolerances;`（builder 字段，默认 null）
- `AbstractCheckProcessor` 加 `protected Map<String, BigDecimal> tolerances;`；把任务 1 的 `equivalent(a, b, tolerance)` 接入：source 遍历时 `tolerances != null ? tolerances.get(k) : null` 作为第三个参数传入
- `DefaultCheckProcessor` 加构造 `public DefaultCheckProcessor(Map<String, BigDecimal> tolerances, boolean allowEmptySide)`（调用 `setTolerances(tolerances)` 并设置 allowEmptySide 字段）与无参构造（null, false）
- `CheckExecutor.buildExecutor` 的 MEMORY 分支把 `new DefaultCheckProcessor()` 换成 `new DefaultCheckProcessor(checkConfig.getTolerances(), checkConfig.isAllowEmptySide())`

- [ ] **步骤 4：运行测试验证通过**

运行：`mvn test -Dtest=ToleranceTest`
预期：PASS

- [ ] **步骤 5：Commit**

```bash
git add src/main/java/io/github/kugin/reconciliation/domain/CheckConfig.java src/main/java/io/github/kugin/reconciliation/check/AbstractCheckProcessor.java src/main/java/io/github/kugin/reconciliation/check/DefaultCheckProcessor.java src/main/java/io/github/kugin/reconciliation/executor/CheckExecutor.java src/test/java/io/github/kugin/reconciliation/check/ToleranceTest.java
git commit -m "feat(recon): add per-field amount tolerance to CheckConfig"
```

### 任务 3：单侧为空可配（CheckConfig.allowEmptySide）

**文件：**
- 修改：`src/main/java/io/github/kugin/reconciliation/domain/CheckConfig.java`（加 `allowEmptySide`）
- 修改：`src/main/java/io/github/kugin/reconciliation/check/AbstractCheckProcessor.java`（字段 + `compareCheck` 门控）
- 测试：`src/test/java/io/github/kugin/reconciliation/check/AllowEmptySideTest.java`

- [ ] **步骤 1：编写失败的测试**

```java
class AllowEmptySideTest {
    private static CheckContext context(boolean srcEmpty, boolean tgtEmpty) {
        List<CheckEntry> src = srcEmpty ? List.of() : List.of(new CheckEntry("a", null, Map.of("amount", 100L)));
        List<CheckEntry> tgt = tgtEmpty ? List.of() : List.of(new CheckEntry("a", null, Map.of("amount", 100L)));
        return CheckContext.builder().source(src).target(tgt)
                .sourceMap(src.stream().collect(Collectors.toMap(CheckEntry::getKey, e -> e)))
                .targetMap(tgt.stream().collect(Collectors.toMap(CheckEntry::getKey, e -> e)))
                .build();
    }

    @Test
    void allowEmptySide_true_produces_diff_not_throw() {
        CheckContext c = context(true, false);
        DefaultCheckProcessor p = new DefaultCheckProcessor(null, true);
        assertDoesNotThrow(() -> p.compare(c));
        assertNotNull(c.getCheckResult());
        assertEquals(1, c.getCheckResult().getDiffDetails().size());
        assertEquals(CheckStateEnum.TARGET_MORE, c.getCheckResult().getDiffDetails().get(0).getState());
    }

    @Test
    void allowEmptySide_false_throws() {
        DefaultCheckProcessor p = new DefaultCheckProcessor(null, false);
        assertThrows(CheckException.class, () -> p.compare(context(true, false)));
    }

    @Test
    void both_empty_returns_early() {
        DefaultCheckProcessor p = new DefaultCheckProcessor(null, false);
        assertDoesNotThrow(() -> p.compare(context(true, true)));
    }
}
```

- [ ] **步骤 2：运行测试验证失败**

运行：`mvn test -Dtest=AllowEmptySideTest`
预期：FAIL，编译错误（`CheckConfig` 无 `allowEmptySide`，`compare` 仍抛异常）

- [ ] **步骤 3：实现 allowEmptySide 门控**

- `CheckConfig` 加 `@Builder.Default private boolean allowEmptySide = false;`
- `AbstractCheckProcessor` 加 `protected boolean allowEmptySide;`（+ setter，供任务 2 的构造调用）；`compareCheck` 改为：两侧皆空 return；`if (!allowEmptySide && (source.isEmpty() || target.isEmpty())) throw new CheckException("对账数据未准备好");`

- [ ] **步骤 4：运行测试验证通过**

运行：`mvn test -Dtest=AllowEmptySideTest`
预期：PASS

- [ ] **步骤 5：Commit**

```bash
git add src/main/java/io/github/kugin/reconciliation/domain/CheckConfig.java src/main/java/io/github/kugin/reconciliation/check/AbstractCheckProcessor.java src/test/java/io/github/kugin/reconciliation/check/AllowEmptySideTest.java
git commit -m "feat(recon): make empty-side behavior configurable"
```

**阶段 1 收尾**：运行 `mvn test`（全量回归，确认 MEMORY/STREAM 既有测试全绿，版本仍 0.0.1-SNAPSHOT），`mvn install -DskipTests` 安装到本地 `.m2` 供新系统引用。

---

## 阶段 2：新系统 paycoin-reconciliation（新仓库）

> 以下任务在新项目 `/Users/yangzb/paycoin-projects/paycoin-reconciliation` 下执行；先 `mvn install` 引擎（阶段 1 收尾）。

### 任务 4：项目骨架 + 双数据源 + Flyway 空迁移

**文件：**
- 创建：`pom.xml`、`src/main/resources/application.yml`、`src/main/java/com/simplepay/reconciliation/PayReconciliationApplication.java`
- 创建：`src/main/java/com/simplepay/reconciliation/config/DataSourceConfig.java`
- 创建：`src/main/resources/db/migration/V1__init.sql`（空，后续任务补表）
- 测试：`src/test/java/com/simplepay/reconciliation/ApplicationContextTest.java`

- [ ] **步骤 1：编写失败的测试**

```java
@SpringBootTest
class ApplicationContextTest {
    @Test
    void contextLoads() { }
}
```

- [ ] **步骤 2：运行测试验证失败**

运行：`./mvnw test -Dtest=ApplicationContextTest`
预期：FAIL（项目不存在/无法启动）

- [ ] **步骤 3：搭建骨架**

- `pom.xml`：parent `spring-boot-starter-parent` 3.3.x；依赖 `data-reconciliation:0.0.1-SNAPSHOT`、`spring-boot-starter-web`、`data-jpa`、`thymeleaf`、`security`、`validation`、`flyway-core` + `flyway-mysql`、`mysql-connector-j`、`redisson`、`hutool-all`、`lombok`、test 依赖 `spring-boot-starter-test`、`testcontainers`(mysql/redis)、`spring-security-test`
- `application.yml`：两个数据源 `recon`（主，Flyway 只跑此库）与 `paycoin`（从，只读，`spring.jpa.hibernate.ddl-auto=none`）；Redis 单机地址；引擎版本注释
- `DataSourceConfig`：`@Primary` recon `DataSource` + `paycoin` 从 `DataSource`，各自 `LocalContainerEntityManagerFactoryBean` + `PlatformTransactionManager`（paycoin 侧事务管理器标记只读，禁用 DDL）
- `PayReconciliationApplication`：`@SpringBootApplication` + `@EnableScheduling`

- [ ] **步骤 4：运行测试验证通过**

运行：`./mvnw test -Dtest=ApplicationContextTest`
预期：PASS（Testcontainers 起 MySQL/Redis，context 正常加载）

- [ ] **步骤 5：Commit**

```bash
git init && git add . && git commit -m "chore: scaffold paycoin-reconciliation with dual datasource"
```

### 任务 5：数据模型（6 张表实体 + Flyway migration + Repository）

**文件：**
- 创建：`src/main/resources/db/migration/V1__init.sql`（6 表 DDL，见规格 §3）
- 创建：`src/main/java/com/simplepay/reconciliation/entity/` 下 6 个实体（`ReconcileBatch`、`ChannelBill`、`DiffRecord`、`Ticket`、`OperLog`、`ReconcileConfig`）
- 创建：`src/main/java/com/simplepay/reconciliation/repository/recon/` 下 6 个 `JpaRepository`
- 测试：`src/test/java/com/simplepay/reconciliation/repository/ReconRepositoryTest.java`

- [ ] **步骤 1：编写失败的测试**

```java
@DataJpaTest
class ReconRepositoryTest {
    @Autowired ReconcileBatchRepository batches;
    @Autowired DiffRecordRepository diffs;

    @Test
    void unique_order_per_channel_date_is_enforced() {
        ReconcileBatch b = batches.save(batch("globalpay", "payin", LocalDate.of(2026,11,1)));
        DiffRecord d1 = diff("globalpay", LocalDate.of(2026,11,1), "O001", DiffType.SOURCE_MORE);
        d1.setBatchId(b.getId());
        diffs.save(d1);
        DiffRecord d2 = diff("globalpay", LocalDate.of(2026,11,1), "O001", DiffType.SOURCE_MORE);
        d2.setBatchId(b.getId());
        assertThrows(DataIntegrityViolationException.class, () -> diffs.saveAndFlush(d2));
    }
}
```

- [ ] **步骤 2：运行测试验证失败**

运行：`./mvnw test -Dtest=ReconRepositoryTest`
预期：FAIL（表不存在）

- [ ] **步骤 3：实现 DDL + 实体 + Repository**

DDL 严格按规格 §3：金额列 `BIGINT`（分）；`diff_record` 唯一约束 `UNIQUE(channel, account_date, order_no)`；`reconcile_batch.batch_no`、`ticket.ticket_no` 唯一；`diff_fields`/`raw_*_json`/`before_json`/`after_json` 用 `JSON` 列（MySQL 8）。实体字段名与列名 snake_case 对齐，金额用 `long`/`Long`，枚举用 `@Enumerated(STRING)`。
另建 `V2__seed_config.sql` 插入一条 `reconcile_config` 默认行（`channel=globalpay, biz_type=payin, amount_tolerance_cents=0, fee_tolerance_cents=0, date_tolerance_days=0, success_states='3,5', critical_amount_cents=1000000, enabled=1`）。

- [ ] **步骤 4：运行测试验证通过**

运行：`./mvnw test -Dtest=ReconRepositoryTest`
预期：PASS

- [ ] **步骤 5：Commit**

```bash
git add . && git commit -m "feat: add recon domain entities and flyway migration"
```

### 任务 6：金额归一化工具 `AmountUtil`

**文件：**
- 创建：`src/main/java/com/simplepay/reconciliation/util/AmountUtil.java`
- 测试：`src/test/java/com/simplepay/reconciliation/util/AmountUtilTest.java`

- [ ] **步骤 1：编写失败的测试**

```java
class AmountUtilTest {
    @Test void double_029_to_cents() { assertEquals(29L, AmountUtil.yuanToCents(0.29)); }
    @Test void double_sum_01_02_to_cents() { assertEquals(30L, AmountUtil.yuanToCents(0.1 + 0.2)); }
    @Test void double_10_to_cents() { assertEquals(1000L, AmountUtil.yuanToCents(10.0)); }
    @Test void double_large_to_cents() { assertEquals(9999999999999999L, AmountUtil.yuanToCents(99999999999999.99)); }
    @Test void string_100_50_to_cents() { assertEquals(10050L, AmountUtil.yuanStringToCents("100.50")); }
    @Test void string_over_scale_rejected() { assertThrows(IllegalArgumentException.class, () -> AmountUtil.yuanStringToCents("100.555")); }
}
```

- [ ] **步骤 2：运行测试验证失败**

运行：`./mvnw test -Dtest=AmountUtilTest`
预期：FAIL（类不存在）

- [ ] **步骤 3：实现两个方法**

`public static long yuanToCents(double yuan)`：`String.valueOf(yuan)` → `new BigDecimal` → `setScale(2, RoundingMode.HALF_UP)` → `movePointRight(2)` → `longValueExact()`。
`public static long yuanStringToCents(String yuanStr)`：`new BigDecimal(yuanStr)` → 校验 `scale() <= 2`（否则抛 `IllegalArgumentException`）→ `movePointRight(2)` → `longValueExact()`。

- [ ] **步骤 4：运行测试验证通过**

运行：`./mvnw test -Dtest=AmountUtilTest`
预期：PASS

- [ ] **步骤 5：Commit**

```bash
git add . && git commit -m "feat: add amount normalization to cents"
```

### 任务 7：GlobalpayCsvParser（表头驱动）

**文件：**
- 创建：`src/main/java/com/simplepay/reconciliation/channel/GlobalpayCsvParser.java`
- 创建：`src/main/java/com/simplepay/reconciliation/channel/CsvColumnMapping.java`（record：`Map<String,String> headerToField` + `String keyField` + `String amountField` + `String feeField` + `String statusField`）
- 测试：`src/test/java/com/simplepay/reconciliation/channel/GlobalpayCsvParserTest.java`

- [ ] **步骤 1：编写失败的测试**

```java
class GlobalpayCsvParserTest {
    private static final CsvColumnMapping MAPPING = new CsvColumnMapping(
        Map.of("商户订单号","orderNo","交易金额","amount","手续费","fee","交易状态","status"),
        "orderNo","amount","fee","status");

    @Test
    void parses_rows_into_check_entries_with_cents() {
        String csv = "商户订单号,交易金额,手续费,交易状态\nO001,100.50,1.20,SUCCESS\n";
        List<CheckEntry> out = new GlobalpayCsvParser().parse(csv, StandardCharsets.UTF_8, MAPPING);
        assertEquals(1, out.size());
        assertEquals("O001", out.get(0).getKey());
        assertEquals(10050L, out.get(0).getCheckData().get("amount"));
        assertEquals(120L, out.get(0).getCheckData().get("fee"));
    }

    @Test
    void missing_required_column_throws() {
        String csv = "商户订单号,交易金额\nO001,100.50\n";
        assertThrows(CsvParseException.class, () -> new GlobalpayCsvParser().parse(csv, StandardCharsets.UTF_8, MAPPING));
    }

    @Test
    void over_scale_amount_rejected() {
        String csv = "商户订单号,交易金额,手续费,交易状态\nO001,100.555,1.20,SUCCESS\n";
        assertThrows(CsvParseException.class, () -> new GlobalpayCsvParser().parse(csv, StandardCharsets.UTF_8, MAPPING));
    }
}
```

- [ ] **步骤 2：运行测试验证失败**

运行：`./mvnw test -Dtest=GlobalpayCsvParserTest`
预期：FAIL（类不存在）

- [ ] **步骤 3：实现表头驱动解析**

`public List<CheckEntry> parse(String csvContent, Charset charset, CsvColumnMapping mapping)`：用 Hutool `CsvReader`（含表头）；读表头行 → 按 `headerToField` 反查必需列（key/amount/fee/status），缺任一必需列抛 `CsvParseException`（明确列名）；逐行取 `key`、`amount`（`AmountUtil.yuanStringToCents`）、`fee`、`status` → `new CheckEntry(key, null, checkData)`；金额 scale 超限或解析异常抛 `CsvParseException`（带行号）。

- [ ] **步骤 4：运行测试验证通过**

运行：`./mvnw test -Dtest=GlobalpayCsvParserTest`
预期：PASS

- [ ] **步骤 5：Commit**

```bash
git add . && git commit -m "feat: add header-driven globalpay csv parser"
```

### 任务 8：PaycoinSourceLoader（只读平台侧）

**文件：**
- 创建：`src/main/java/com/simplepay/reconciliation/repository/paycoin/CollectForAnotherOrderView.java`（只读 @Entity 映射 `collect_for_another_order`）
- 创建：`src/main/java/com/simplepay/reconciliation/repository/paycoin/CollectForAnotherOrderViewRepo.java`
- 创建：`src/main/java/com/simplepay/reconciliation/recon/PaycoinSourceLoader.java`（实现 `ResourceLoader`）
- 测试：`src/test/java/com/simplepay/reconciliation/recon/PaycoinSourceLoaderTest.java`

- [ ] **步骤 1：编写失败的测试**

```java
class PaycoinSourceLoaderTest {
    @Test
    void normalizes_double_amount_to_cents_and_uses_thirdpart_mer_order_no() {
        CollectForAnotherOrderView o = new CollectForAnotherOrderView();
        o.setThirdpartMerOrderNo("GM001"); o.setAmount(100.5); o.setHandlingFee(1.2); o.setOrderState("3");
        PaycoinSourceLoader loader = new PaycoinSourceLoader(repo, List.of("3"));
        when(repo.findByDealTimeBetweenAndOrderStateIn(any(), any(), eq(List.of("3")))).thenReturn(List.of(o));
        List<CheckEntry> out = loader.load("2026-11-01");
        assertEquals("GM001", out.get(0).getKey());
        assertEquals(10050L, out.get(0).getCheckData().get("amount"));
        assertEquals(120L, out.get(0).getCheckData().get("handlingFee"));
    }
}
```

- [ ] **步骤 2：运行测试验证失败**

运行：`./mvnw test -Dtest=PaycoinSourceLoaderTest`
预期：FAIL（类不存在）

- [ ] **步骤 3：实现只读视图 + Loader**

`CollectForAnotherOrderView`：`@Immutable @Entity @Table(name="collect_for_another_order")`，仅映射 `id`、`thirdpartMerOrderNo`（`thirdpart_mer_order_no`）、`amount`（`Double`）、`handlingFee`（`handling_fee`）、`orderState`（`order_state`）、`dealTime`（`deal_time`），字段显式 `@Column` snake_case。
`PaycoinSourceLoader.load(String date)`：`date` 解析为当日区间 → repo 查询「`dealTime` 在区间 + `orderState` ∈ 配置的成功状态集合」→ 每行转 `CheckEntry`（key=`thirdpartMerOrderNo`，checkData=`{amount: yuanToCents(amount), handlingFee: yuanToCents(handlingFee), status: orderState}`）。

- [ ] **步骤 4：运行测试验证通过**

运行：`./mvnw test -Dtest=PaycoinSourceLoaderTest`
预期：PASS

- [ ] **步骤 5：Commit**

```bash
git add . && git commit -m "feat: add read-only paycoin source loader"
```

### 任务 9：GlobalpayCsvLoader + BillDownloadService

**文件：**
- 创建：`src/main/java/com/simplepay/reconciliation/service/BillDownloadService.java`（本地目录/HTTP 取 CSV + SHA-256 + 写 `channel_bill`）
- 创建：`src/main/java/com/simplepay/reconciliation/recon/GlobalpayCsvLoader.java`（实现 `ResourceLoader`）
- 测试：`src/test/java/com/simplepay/reconciliation/recon/GlobalpayCsvLoaderTest.java`

- [ ] **步骤 1：编写失败的测试**

```java
class GlobalpayCsvLoaderTest {
    @Test
    void loads_and_parses_csv_to_check_entries() {
        BillDownloadService dl = mock(BillDownloadService.class);
        when(dl.download("globalpay", "2026-11-01")).thenReturn("商户订单号,交易金额,手续费,交易状态\nGM001,100.50,1.20,SUCCESS");
        GlobalpayCsvLoader loader = new GlobalpayCsvLoader(dl, mapping, utf8);
        List<CheckEntry> out = loader.load("2026-11-01");
        assertEquals(1, out.size());
        assertEquals("GM001", out.get(0).getKey());
    }
}
```

- [ ] **步骤 2：运行测试验证失败**

运行：`./mvnw test -Dtest=GlobalpayCsvLoaderTest`
预期：FAIL（类不存在）

- [ ] **步骤 3：实现下载 + 解析**

`BillDownloadService.download(String channel, String accountDate)`：从配置源（MVP 本地目录优先，其次 HTTP）读文件 → SHA-256 → `channel_bill` 判重（同 hash 已 PARSED 则跳过）→ 返回文件内容；文件缺失抛 `BillNotReadyException`。
`GlobalpayCsvLoader.load(String date)`：`download` → 编码探测（UTF-8 优先，失败降级 GBK）→ `GlobalpayCsvParser.parse` → `List<CheckEntry>`；解析成功写 `channel_bill.status=PARSED + row_count`，失败写 `FAILED + error_msg` 并抛 `CsvParseException`。

- [ ] **步骤 4：运行测试验证通过**

运行：`./mvnw test -Dtest=GlobalpayCsvLoaderTest`
预期：PASS

- [ ] **步骤 5：Commit**

```bash
git add . && git commit -m "feat: add globalpay csv loader with download service"
```

### 任务 10：ReconcileService（7 步编排）

**文件：**
- 创建：`src/main/java/com/simplepay/reconciliation/service/ReconcileService.java`
- 测试：`src/test/java/com/simplepay/reconciliation/service/ReconcileServiceTest.java`

- [ ] **步骤 1：编写失败的测试**

```java
class ReconcileServiceTest {
    @Test
    void runs_and_archives_batch_with_counts() {
        // mock 两个 loader：平台 [O001, O002]，渠道 [O001(金额一致), O003]
        when(paycoinLoader.load(any())).thenReturn(List.of(
            entry("O001", Map.of("amount", 100L, "handlingFee", 0L, "status", "3")),
            entry("O002", Map.of("amount", 200L, "handlingFee", 0L, "status", "3"))));
        when(csvLoader.load(any())).thenReturn(List.of(
            entry("O001", Map.of("amount", 100L, "handlingFee", 0L, "status", "3")),
            entry("O003", Map.of("amount", 300L, "handlingFee", 0L, "status", "3"))));
        ReconcileBatch batch = service.run("2026-11-01");
        assertEquals(1, batch.getMatchedCount());
        assertEquals(2, batch.getDiffCount());
        assertEquals("SUCCESS", batch.getStatus());
        // diff 类型：O002 → SOURCE_MORE，O003 → TARGET_MORE
    }
}
```

- [ ] **步骤 2：运行测试验证失败**

运行：`./mvnw test -Dtest=ReconcileServiceTest`
预期：FAIL（类不存在）

- [ ] **步骤 3：实现 `public ReconcileBatch run(String date)`**

按规格 §2.3 的 7 步：① 构造 `CheckConfig`（`id="globalpay:payin"`、`srcLoader=PaycoinSourceLoader`、`targetLoader=GlobalpayCsvLoader`、`allowEmptySide=true`、`tolerances` 从 `reconcile_config` 读入）；② `CheckExecutor.buildExecutor(config).process(date)`；③ 读 `context.getCheckResult()` → `DiffPersistenceService.persist(...)`；④ 写 `reconcile_batch`（matched/diff 计数、金额、平账率、状态 SUCCESS），异常时状态 FAIL + remark 留堆栈。

- [ ] **步骤 4：运行测试验证通过**

运行：`./mvnw test -Dtest=ReconcileServiceTest`
预期：PASS

- [ ] **步骤 5：Commit**

```bash
git add . && git commit -m "feat: add reconciliation orchestration service"
```

### 任务 11：DiffPersistenceService（差异分类 + 落库 + 生成工单）

**文件：**
- 创建：`src/main/java/com/simplepay/reconciliation/recon/DiffPersistenceService.java`
- 创建：`src/main/java/com/simplepay/reconciliation/entity/DiffType.java`（枚举）
- 测试：`src/test/java/com/simplepay/reconciliation/recon/DiffPersistenceServiceTest.java`

- [ ] **步骤 1：编写失败的测试**

```java
class DiffPersistenceServiceTest {
    @Test void different_amount_classified_amount_diff() {
        CheckUnit u = unit("O004", CheckStateEnum.DIFFERENT, Map.of("amount", fu(100L, 110L)));
        service.persist(singleResult(u), 1L, "globalpay", LocalDate.of(2026,11,1));
        assertEquals(DiffType.AMOUNT_DIFF, diffRepo.findAll().get(0).getDiffType());
    }
    @Test void different_fee_classified_fee_diff() {
        CheckUnit u = unit("O005", CheckStateEnum.DIFFERENT, Map.of("handlingFee", fu(10L, 12L)));
        service.persist(singleResult(u), 1L, "globalpay", LocalDate.of(2026,11,1));
        assertEquals(DiffType.FEE_DIFF, diffRepo.findAll().get(0).getDiffType());
    }
    @Test void different_status_classified_status_diff() {
        CheckUnit u = unit("O006", CheckStateEnum.DIFFERENT, Map.of("status", fu("3", "5")));
        service.persist(singleResult(u), 1L, "globalpay", LocalDate.of(2026,11,1));
        assertEquals(DiffType.STATUS_DIFF, diffRepo.findAll().get(0).getDiffType());
    }
    @Test void upsert_idempotent_on_rerun() {
        CheckUnit u = unit("O002", CheckStateEnum.SOURCE_MORE, Map.of());
        service.persist(singleResult(u), 1L, "globalpay", LocalDate.of(2026,11,1));
        service.persist(singleResult(u), 2L, "globalpay", LocalDate.of(2026,11,1));
        assertEquals(1, diffRepo.count());
    }
}
```

- [ ] **步骤 2：运行测试验证失败**

运行：`./mvnw test -Dtest=DiffPersistenceServiceTest`
预期：FAIL（类不存在）

- [ ] **步骤 3：实现 `public void persist(CheckResult result, Long batchId, String channel, LocalDate accountDate)`**

遍历 `result.getDiffDetails()`：`SOURCE_MORE`/`TARGET_MORE` 直接映射；`DIFFERENT` 看 `differentFields` key 命中优先级 **amount > handlingFee > status** 推导 `DiffType`。每条差异 upsert `diff_record`（按 `UNIQUE(channel, account_date, order_no)`），含 `raw_source_json`/`raw_target_json` 原始快照；`handle_status=PENDING`；生成 `ticket`（OPEN，金额超阈值标 CRITICAL）。

- [ ] **步骤 4：运行测试验证通过**

运行：`./mvnw test -Dtest=DiffPersistenceServiceTest`
预期：PASS

- [ ] **步骤 5：Commit**

```bash
git add . && git commit -m "feat: add diff classification and persistence"
```

### 任务 12：TicketService（状态机 + 复核分离）

**文件：**
- 创建：`src/main/java/com/simplepay/reconciliation/service/TicketService.java`
- 测试：`src/test/java/com/simplepay/reconciliation/service/TicketServiceTest.java`

- [ ] **步骤 1：编写失败的测试**

```java
class TicketServiceTest {
    @Test void assign_open_to_processing() { }
    @Test void submit_processing_to_pending_review() { }
    @Test void review_by_same_assignee_rejected() {
        // assignee=op1，reviewer=op1 → assertThrows(IllegalStateException)
    }
    @Test void review_by_other_approves_to_closed() { }
    @Test void review_reject_back_to_processing() { }
    @Test void ignore_open_to_closed() { }
}
```

- [ ] **步骤 2：运行测试验证失败**

运行：`./mvnw test -Dtest=TicketServiceTest`
预期：FAIL（类不存在）

- [ ] **步骤 3：实现状态机**

`assign(ticketId, operator)`：OPEN → PROCESSING（设 `assignee`）；`submit(ticketId, operator, resolveType, remark)`：PROCESSING → PENDING_REVIEW；`review(ticketId, reviewer, approve, remark)`：PENDING_REVIEW 且 **`reviewer != assignee` 否则抛 `IllegalStateException`**，approve → CLOSED，驳回 → PROCESSING；`ignore(ticketId, operator)`：OPEN/PROCESSING → CLOSED（`resolve_type=IGNORE_CROSS_DAY`）。每次迁移写 `oper_log`（before/after JSON）。

- [ ] **步骤 4：运行测试验证通过**

运行：`./mvnw test -Dtest=TicketServiceTest`
预期：PASS

- [ ] **步骤 5：Commit**

```bash
git add . && git commit -m "feat: add ticket state machine with reviewer separation"
```

### 任务 13：调度与幂等（@Scheduled + Redisson 锁）

**文件：**
- 创建：`src/main/java/com/simplepay/reconciliation/schedule/ReconcileJob.java`
- 修改：`src/main/java/com/simplepay/reconciliation/config/ScheduleConfig.java`
- 测试：`src/test/java/com/simplepay/reconciliation/schedule/ReconcileJobTest.java`

- [ ] **步骤 1：编写失败的测试**

```java
class ReconcileJobTest {
    @Test void lock_prevents_concurrent_run() {
        // RLock 被占时，第二次 run 不触发 ReconcileService
    }
    @Test void rerun_same_date_creates_new_batch_and_no_duplicate_diff() { }
}
```

- [ ] **步骤 2：运行测试验证失败**

运行：`./mvnw test -Dtest=ReconcileJobTest`
预期：FAIL（类不存在）

- [ ] **步骤 3：实现定时 + 锁**

`ReconcileJob`：`@Scheduled(cron = "${recon.cron:0 30 2 * * ?}")` 触发「昨日」对账；方法内 `RLock`（`recon:globalpay:payin:{date}`，`tryLock` 带超时 + 看门狗），拿不到锁则跳过并记日志；调用 `ReconcileService.run(date)`。重跑幂等由任务 11 的 upsert + `reconcile_batch.batch_no`（seq 递增）保证。

- [ ] **步骤 4：运行测试验证通过**

运行：`./mvnw test -Dtest=ReconcileJobTest`
预期：PASS

- [ ] **步骤 5：Commit**

```bash
git add . && git commit -m "feat: add scheduled reconciliation with redisson lock"
```

### 任务 14：Controller + Result 封装 + 异常处理

**文件：**
- 创建：`src/main/java/com/simplepay/reconciliation/common/Result.java`
- 创建：`src/main/java/com/simplepay/reconciliation/exception/GlobalExceptionHandler.java`
- 创建：`src/main/java/com/simplepay/reconciliation/controller/ReconcileController.java`、`DiffController.java`、`TicketController.java`、`ReportController.java`
- 测试：`src/test/java/com/simplepay/reconciliation/controller/TicketControllerTest.java`

- [ ] **步骤 1：编写失败的测试**

```java
@WebMvcTest(TicketController.class)
class TicketControllerTest {
    @Test void review_by_same_assignee_returns_error() {
        // POST /api/tickets/{id}/review，reviewer==assignee → 4xx + 明确错误码
    }
}
```

- [ ] **步骤 2：运行测试验证失败**

运行：`./mvnw test -Dtest=TicketControllerTest`
预期：FAIL（类不存在）

- [ ] **步骤 3：实现 REST API + 统一响应**

- `Result`：`success(data)` / `fail(code, msg)` 封装；`GlobalExceptionHandler`：`@RestControllerAdvice` 映射 `BillNotReadyException`/`CsvParseException`/`IllegalStateException` 等为稳定 HTTP 语义 + 结构化错误体
- `ReconcileController`：`POST /api/reconcile/run?date=...`、`GET /api/reconcile/batches`（分页）
- `DiffController`：`GET /api/diffs`（筛选：日期/类型/状态/金额区间 + 分页）、`GET /api/diffs/{id}`
- `TicketController`：`POST /api/tickets/{id}/assign`、`/submit`、`/review`、`/ignore`、`GET /api/tickets`（筛选）
- `ReportController`：`GET /api/reports/daily?date=...`、`GET /api/reports/export?date=...`

- [ ] **步骤 4：运行测试验证通过**

运行：`./mvnw test -Dtest=TicketControllerTest`
预期：PASS

- [ ] **步骤 5：Commit**

```bash
git add . && git commit -m "feat: add rest api with unified result and error handling"
```

### 任务 15：ReportService（聚合 + CSV 导出）

**文件：**
- 创建：`src/main/java/com/simplepay/reconciliation/service/ReportService.java`
- 测试：`src/test/java/com/simplepay/reconciliation/service/ReportServiceTest.java`

- [ ] **步骤 1：编写失败的测试**

```java
class ReportServiceTest {
    @Test void daily_report_computes_match_rate() {
        // matched=1, diff=4 → 平账率 20.0%
    }
    @Test void csv_export_escapes_formula_injection() {
        // orderNo 以 "=" 开头 → 导出时前缀 ' 转义
    }
}
```

- [ ] **步骤 2：运行测试验证失败**

运行：`./mvnw test -Dtest=ReportServiceTest`
预期：FAIL（类不存在）

- [ ] **步骤 3：实现聚合 + 导出**

`dailyReport(date)`：从 `reconcile_batch` + `diff_record` 聚合（平台/渠道笔数金额、matched/diff、平账率 `matched/(matched+diff)×100%`、差异金额轧差、按 `diff_type` 分布）。
`exportCsv(date)`：UTF-8 带 BOM；所有文本字段对 `= + @ - \t \r` 前缀做公式注入转义；流式写 `HttpServletResponse`。

- [ ] **步骤 4：运行测试验证通过**

运行：`./mvnw test -Dtest=ReportServiceTest`
预期：PASS

- [ ] **步骤 5：Commit**

```bash
git add . && git commit -m "feat: add report aggregation and csv export"
```

### 任务 16：Security（Spring Security 四角色）

**文件：**
- 创建：`src/main/java/com/simplepay/reconciliation/config/SecurityConfig.java`
- 测试：`src/test/java/com/simplepay/reconciliation/config/SecurityConfigTest.java`

- [ ] **步骤 1：编写失败的测试**

```java
class SecurityConfigTest {
    @Test void risk_role_readonly() { /* 风控角色 POST /api/tickets → 403 */ }
    @Test void reviewer_role_required_for_review() { /* 非财务角色 /review → 403 */ }
}
```

- [ ] **步骤 2：运行测试验证失败**

运行：`./mvnw test -Dtest=SecurityConfigTest`
预期：FAIL（类不存在）

- [ ] **步骤 3：实现四角色权限**

`SecurityConfig`：四角色 `OPERATOR`（运营，全功能）、`FINANCE`（财务，复核 + 只读）、`RISK`（风控，只读）、`ADMIN`（系统管理员，用户管理 + 审计查看）。`/api/tickets/{id}/review` 仅 FINANCE；写接口仅 OPERATOR/ADMIN；GET 接口四角色可读。

- [ ] **步骤 4：运行测试验证通过**

运行：`./mvnw test -Dtest=SecurityConfigTest`
预期：PASS

- [ ] **步骤 5：Commit**

```bash
git add . && git commit -m "feat: add role-based access control"
```

### 任务 17：运营页面（4 页 Thymeleaf）

**文件：**
- 创建：`src/main/java/com/simplepay/reconciliation/controller/PageController.java`
- 创建：`src/main/resources/templates/` 下 `batch.html`、`diff.html`、`ticket.html`、`report.html`（+ 公共 `layout.html`）
- 测试：`src/test/java/com/simplepay/reconciliation/controller/PageControllerTest.java`

- [ ] **步骤 1：编写失败的测试**

```java
@WebMvcTest(PageController.class)
class PageControllerTest {
    @Test void batch_page_renders() { /* GET /pages/batches → 200 且含 "对账批次" */ }
    @Test void diff_page_renders() { }
    @Test void ticket_page_renders() { }
    @Test void report_page_renders() { }
}
```

- [ ] **步骤 2：运行测试验证失败**

运行：`./mvnw test -Dtest=PageControllerTest`
预期：FAIL（类不存在）

- [ ] **步骤 3：实现 4 页**

`PageController` 提供 4 个路由转发到模板；页面按规格 §9.2：批次列表 + 手动触发、差异池（筛选 + 详情抽屉两侧字段对照 + 导出）、工单（状态流转操作）、报告（日报 + 导出）。用 Bootstrap + 少量 jQuery，服务端渲染，不引入前端构建链。

- [ ] **步骤 4：运行测试验证通过**

运行：`./mvnw test -Dtest=PageControllerTest`
预期：PASS

- [ ] **步骤 5：Commit**

```bash
git add . && git commit -m "feat: add thymeleaf operations pages"
```

### 任务 18：集成测试（全链路 5 笔样例）

**文件：**
- 创建：`src/test/java/com/simplepay/reconciliation/ReconciliationIntegrationTest.java`

- [ ] **步骤 1：编写失败的测试**

```java
@Testcontainers
@SpringBootTest
class ReconciliationIntegrationTest {
    @Test
    void full_pipeline_produces_expected_diffs() {
        // 平台造 5 笔：O001 平账、O002 仅平台(SOURCE_MORE)、O003 仅渠道(TARGET_MORE)、
        //   O004 金额差(AMOUNT_DIFF)、O005 手续费差(FEE_DIFF)
        // 渠道 CSV 对应造 4 行（O001 一致、O003 渠道多、O004 金额不同、O005 手续费不同）
        // 断言：batch.matched_count==1、diff_count==4，diff_type 分布正确，ticket 各生成 OPEN
    }

    @Test
    void rerun_same_date_no_duplicate_diff() {
        // run(date) 两次 → diff_record 行数不变
    }
}
```

- [ ] **步骤 2：运行测试验证失败**

运行：`./mvnw test -Dtest=ReconciliationIntegrationTest`
预期：FAIL（类不存在）

- [ ] **步骤 3：实现集成测试**

用 Testcontainers 起 MySQL + Redis；`@DynamicPropertySource` 注入连接；`@Sql` 造平台侧数据 + 临时 CSV 文件；调 `ReconcileService.run(date)`；断言批次计数、差异类型、工单状态、重跑幂等（diff 行数不变）。

- [ ] **步骤 4：运行测试验证通过**

运行：`./mvnw test -Dtest=ReconciliationIntegrationTest`
预期：PASS

- [ ] **步骤 5：Commit**

```bash
git add . && git commit -m "test: add full pipeline integration test"
```

---

## 自检记录

- **规格覆盖度**：规格 §2（架构/数据流）→ 任务 4/10；§3（数据模型）→ 任务 5；§4（归一化 + 读结果落地）→ 任务 6/10/11；§5（引擎 3 升级）→ 任务 1/2/3；§6（平台读取/CSV 解析/差异分类）→ 任务 7/8/9/11；§7（状态机）→ 任务 12；§8（调度/幂等/安全）→ 任务 13/16；§9（报告/页面）→ 任务 15/17；§10（错误处理/测试）→ 任务 14/18。
- **步骤扫描**：每个步骤均有可检查结果；测试给名字+断言，代码步骤给签名+规格取值，函数体仅算法无法由签名/测试决定处给出。
- **类型一致性**：`DiffType`（任务 11 定义）在任务 5 实体、任务 11/15/18 使用，命名一致；`CheckEntry`/`CheckConfig`/`CheckResult` 均来自引擎，签名以引擎为准；`tolerances` 键名 `amount`/`handlingFee` 与任务 8 的 checkData 键一致。
- **审查重点**：6 条均已有对应任务测试（任务 6/7/3/12/11/18），见各任务步骤 1。
