# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Prerequisites

- JDK 25. The `pom.xml` pins `<jdkToolchain><version>25</version></jdkToolchain>` for both
  `maven-compiler-plugin` and `maven-surefire-plugin`, so **a JDK 25 toolchain is mandatory** —
  Maven aborts when it cannot find one.
- Install one, e.g. `brew install openjdk` (Temurin 25) or use an existing JDK 25
  (e.g. `/Library/Java/JavaVirtualMachines/temurin-25.jdk/Contents/Home`).
- Lombok must be **≥ 1.18.40** — older versions cannot run on JDK 25 and fail with Lombok's
  `NoSuchFieldException: com.sun.tools.javac.code.TypeTag :: UNKNOWN`. The `pom.xml` pins 1.18.48.
- `~/.m2/toolchains.xml` must point at that JDK 25 home — `<vendor>` must match the actual JDK
  vendor (`adoptium` for Temurin), otherwise Maven silently falls back to the default JDK:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<toolchains>
  <toolchain>
    <type>jdk</type>
    <provides>
      <version>25</version>
      <vendor>adoptium</vendor>
    </provides>
    <configuration>
      <jdkHome>/Library/Java/JavaVirtualMachines/temurin-25.jdk/Contents/Home</jdkHome>
    </configuration>
  </toolchain>
</toolchains>
```

Verify the toolchain is picked up — the build log must print the JDK path, not just succeed:

```bash
mvn clean compile -X 2>&1 | grep "Toolchain in maven-compiler-plugin"
# [INFO] Toolchain in maven-compiler-plugin: JDK[/Library/Java/JavaVirtualMachines/temurin-25.jdk/Contents/Home]
```

- **The published bytecode still targets Java 21.** `main.java.version` stays at 21, so the jar
  keeps running on Java 21 runtimes; only the compiler and the forked test JVM are JDK 25. Raise
  `main.java.version` separately if Java 25 language features are ever needed.
- The upgrade rationale, the measured risk matrix and the exact version matrix behind these pins
  live in `docs/superpowers/plans/2026-11-16-jdk-25-upgrade.md`.

## Build / Test

```bash
# Build the project
mvn compile

# Run all tests
mvn test

# Run a single test class
mvn test -Dtest=ExecutorTest

# Run a single test method
mvn test -Dtest=ExecutorTest#testExecutor
```

## Architecture

This is a generic data reconciliation (对账) framework for comparing data between two arbitrary data sources, identifying differences, and optionally auto-syncing corrections. Java 21 bytecode built on a JDK 25 toolchain, Maven, with Lombok and Hutool.

**Pipeline**: The `CheckExecutor` chains three processors in sequence:
1. **`BeforeCheckProcessor`** — Loads source/target data (`ResourceLoader.load(date)` returns `List<CheckEntry>`), runs pre-checks (`CheckPre`). The default impl (`DefaultBeforeCheckProcessor`) wraps a `ResourceReader` which provides separate `ResourceLoader` instances for source and target data sources.
2. **`CheckProcessor`** — Compares source and target `Map<String, CheckEntry>` by key. Produces `CheckResult` with diffs categorized as `SOURCE_MORE`, `TARGET_MORE`, or `DIFFERENT`. The default impl (`DefaultCheckProcessor`) delegates to the template method `compareMap()` in `AbstractCheckProcessor`.
3. **`AfterCheckProcessor`** — Auto-syncs diffs via `CheckSync` (per-diff-type callbacks: `syncSourceMore`, `syncTargetMore`, `syncDifferent`), then runs `CheckAfter.doAfter()`. Default sync behavior (when `CheckSync` is null): all diffs pass through as `SYNC`.

**Key domain types**:
- `CheckEntry` — Wraps an entity for comparison with a `key` (String) and `checkData` (Map of field → value). Created via `CheckEntry.wrap(entity)` which auto-detects `@CheckIdentity`/`@CheckField` annotations or delegates to `CheckAdapter` interface if the entity implements it. Adapter takes priority over annotations.
- `CheckConfig` — Builder-style config for an executor. At minimum needs `id` or `name`, and a way to load data (either `resourceReader` or `srcLoader`+`targetLoader`). All processor overrides are optional.
- `CheckResult` — Contains `diffDetails` (flat `List<CheckUnit>`), `diffDetailsMap` (grouped by `CheckStateEnum`), and separate `sourceDiffMap`/`targetDiffMap` for entries only on one side.
- `CheckUnit` — Per-entity diff: `key`, `state` (enum), `sync` (enum), `differentFields` (Map of field name → `FieldkUnit` with source/target values).
- `CheckContext` — Carries everything through the pipeline: source/target lists and maps, result, executor manager, stopwatch.

**Executor lifecycle**: `ExecutorManager` prevents re-entrant execution of the same id+date. Two implementations:
- `DefaultExecutorManager` — in-memory via `FIFOCache` singleton (`SimpleCache`)
- `RedisExecutorManager` — distributed via Redisson `RLock`/`RBucket`, for multi-node deployments

Status progresses: `BEFORE` → `CHECK` → `AFTER` → `END` (or `ERROR` on exception).

**Two ways to define what gets compared**:
1. Annotations — `@CheckIdentity` on the identity field, `@CheckField(order=…)` on fields to compare. `CheckEntry.wrap()` uses reflection.
2. `CheckAdapter` interface — entity implements `getKey()` and `getCheckData()`. This takes priority over annotations.

**Data loading**:
- `FileResourceLoader` — Reads data from files (path format string + date), parses lines via a `FileEntityParser` function. Supports auto-detect of wrapper strategy when `identityField` and `checkFields` are empty.
- `RedisResourceLoader` — Reads data from Redis `RList` via Redisson. Same pattern as `FileResourceLoader`: key pattern + parser + auto-detect.

**Redis integration** (`org.redisson:redisson:3.52.0`): `RedisExecutorManager` + `RedisResourceLoader`. Tests use `com.github.codemonstur:embedded-redis` (no external Redis needed).
