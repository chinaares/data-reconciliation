# Changelog

本文件记录对账引擎（data-reconciliation）的行为变更。
格式参考 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，版本号遵循 [语义化版本](https://semver.org/lang/zh-CN/)。

## [Unreleased]

### Changed

- **数值字段比较语义变更**：`AbstractCheckProcessor.compareCheckData` 对数值字段（`BigDecimal` / 其它 `Number`）
  改为**按数值比较**（转 `BigDecimal` 后 `compareTo == 0`），不再使用 `Object.equals`。
  - `BigDecimal` scale 不同但值相同（如 `10.0` 与 `10.00`）不再视为差异；
  - 包装类型不同但数值相同（如 `Long 100` 与 `Integer 100`）不再视为差异；
  - `Float` / `Double` 通过 `toString()` 转换后比较，不受二进制浮点误差影响。
  - 非数值字段（`String`、`Boolean`、枚举等）语义**不变**，仍使用 `equals` 比较。
  - ⚠️ 下游若依赖「类型或 scale 不同即视为差异」的旧行为，请检查并调整。

- **null 值处理变更**：source 与 target 同为 `null` 现在判为相等（旧实现抛 `NullPointerException`）；
  一侧为 `null`、另一侧非 `null` 判为差异。

### Fixed

- **NaN / Infinity 显式暴露**：source 与 target 的数值字段为 `NaN` 或 `Infinity`（含 `Double`/`Float` 两种类型）时，
  现在**判定为差异**并记录 warn 日志，不再静默判为相等。
  旧实现走 `equals` 退化路径，而 `Double.NaN.equals(Double.NaN)` 为 `true`，
  会把「两侧金额都非法」的数据质量问题静默吞掉。

### Added

- **按字段配置金额容差**：`CheckConfig.tolerances`（`Map<String, BigDecimal>`，字段名 → 容差）。
  数值字段的差值绝对值 ≤ 容差时判定为相等。
  - 未配置（`null`）时保持精确比较，**默认行为不变**（向后兼容）；
  - 键必须是 **checkData 字段名 / 实体字段名**，**不是数据库列名**；键名对不上会静默不生效；
  - 容差单位与被比较字段值的单位一致（如字段单位为「元」，容差即以「元」计）；
  - 对默认的 `DefaultCheckProcessor`（MEMORY 模式）与 `StreamingCheckProcessor`（STREAM 模式）**均生效**，
    配置自定义 `checkProcessor` 时不生效。
  - 新增公开构造器 `DefaultCheckProcessor(Map<String, BigDecimal>)`，供直接组装对账处理器时携带容差。
  - 新增公开构造器 `StreamingCheckProcessor(OrderedResourceLoader, OrderedResourceLoader, CheckSync, Map<String, BigDecimal>)`，
    原三参构造保留并委托新构造器（容差为 `null`，等价于精确比较）。
  - ⚠️ 修复：STREAM 模式下 `CheckConfig.tolerances` 此前被**静默忽略**（流式处理器的容差恒为 `null`），
    「配置了却不生效」比「不配置」更危险，现已打通配置到处理器的透传链路。

- **单侧为空可配**：`CheckConfig.allowEmptySide`（`boolean`，默认 `false`）。
  恰好一侧数据为空时的行为由「抛 `CheckException`（对账数据未准备好）」改为可配置：
  - `false`（默认）：保持原行为，抛异常，**默认行为不变**（向后兼容）；
  - `true`：不抛异常，改为把非空一侧的全部数据记为单边差异
    （源侧为空 → `TARGET_MORE`，目标侧为空 → `SOURCE_MORE`）；
  - 两侧都为空时始终不抛异常，直接产出空结果；
  - MEMORY 与 STREAM 两种模式均生效；配置自定义 `checkProcessor` 时不生效。
  - 新增公开构造器 `DefaultCheckProcessor(Map<String, BigDecimal>, boolean)`，
    原单参构造保留并委托新构造器（`allowEmptySide = false`）。

- `AbstractCheckProcessor.equivalent(Object, Object, BigDecimal)`：
  统一的字段等价性判断入口，支持可选的数值容差（`tolerance`），为后续容差比较能力预留。

[Unreleased]: https://github.com/chinaares/data-reconciliation/compare/HEAD
