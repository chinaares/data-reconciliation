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

- `AbstractCheckProcessor.equivalent(Object, Object, BigDecimal)`：
  统一的字段等价性判断入口，支持可选的数值容差（`tolerance`），为后续容差比较能力预留。

[Unreleased]: https://github.com/chinaares/data-reconciliation/compare/HEAD
