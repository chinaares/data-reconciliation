package io.github.kugin.reconciliation.executor;

import io.github.kugin.reconciliation.after.CheckSync;
import io.github.kugin.reconciliation.domain.CheckConfig;
import io.github.kugin.reconciliation.domain.CheckContext;
import io.github.kugin.reconciliation.domain.CheckEntry;
import io.github.kugin.reconciliation.domain.CheckSummary;
import io.github.kugin.reconciliation.domain.CheckUnit;
import io.github.kugin.reconciliation.enums.CheckMode;
import io.github.kugin.reconciliation.enums.CheckStateEnum;
import io.github.kugin.reconciliation.enums.CheckSyncEnum;
import io.github.kugin.reconciliation.enums.ExecutorStatusEnum;
import io.github.kugin.reconciliation.support.RecordingCheckSync;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link CheckExecutor} 的 STREAM 模式端到端测试
 * <p>
 * 每个用例使用唯一的 executor id: {@code DefaultExecutorManager} 的 processing key 是
 * {@code day:check:%s:%s} 且只有 END 状态才会被移除, 复用 id 会让后一个用例被判定为「正在执行」
 *
 * @author Kugin
 */
class StreamingCheckExecutorTest {

    /**
     * 对账日期
     */
    private static final String DATE = "20210204";

    private static CheckEntry entry(String key, Object amount) {
        return new CheckEntry(key, null, Map.of("amount", amount));
    }

    /**
     * 捕获回调细节的调账替身
     * <p>
     * 记录: 回调次数、成功次数、回调被调用那一刻单元上的调账结果(必须为 null)
     */
    private static final class CapturingCheckSync implements CheckSync {

        /**
         * 回调返回值
         */
        private final CheckSyncEnum result;

        /**
         * 回调次数
         */
        private final AtomicInteger calls = new AtomicInteger();

        /**
         * 成功返回的次数
         */
        private final AtomicInteger completed = new AtomicInteger();

        /**
         * 回调被调用那一刻, 单元上的调账结果
         */
        private CheckSyncEnum syncSeenInCallback;

        /**
         * 第几次回调抛异常, 0 表示不抛
         */
        private int failOnCall;

        /**
         * 每次回调都会执行的钩子
         */
        private Runnable onCallback;

        private CapturingCheckSync(CheckSyncEnum result) {
            this.result = result;
        }

        @Override
        public CheckSyncEnum syncSourceMore(CheckUnit checkUnit) {
            return record(checkUnit);
        }

        @Override
        public CheckSyncEnum syncTargetMore(CheckUnit checkUnit) {
            return record(checkUnit);
        }

        @Override
        public CheckSyncEnum syncDifferent(CheckUnit checkUnit) {
            return record(checkUnit);
        }

        private CheckSyncEnum record(CheckUnit checkUnit) {
            int call = calls.incrementAndGet();
            //回调被调用的这一刻, 单元上的调账结果尚未写入
            syncSeenInCallback = checkUnit.getSync();
            if (onCallback != null) {
                onCallback.run();
            }
            if (call == failOnCall) {
                throw new RuntimeException("boom");
            }
            completed.incrementAndGet();
            return result;
        }
    }

    /**
     * STREAM 模式正常执行: 只有摘要, 没有全量结果与全量数据
     */
    @Test
    void streamModeProducesSummaryOnly() {
        CheckConfig config = CheckConfig.builder()
                .id("stream-normal")
                .mode(CheckMode.STREAM)
                .srcOrderedLoader(date -> Stream.of(entry("a", 1d)))
                .targetOrderedLoader(date -> Stream.of(entry("a", 1d)))
                .checkPre(context -> true)
                .checkSync(new RecordingCheckSync())
                .build();

        CheckExecutor executor = CheckExecutor.buildExecutor(config);
        CheckContext context = executor.process(DATE);

        assertEquals(ExecutorStatusEnum.END.toString(), executor.getExecutorManager().getCurrentStatus());
        assertNotNull(context.getSummary());
        assertNull(context.getCheckResult());
        assertNull(context.getSource());
        assertNull(context.getTargetMap());
    }

    /**
     * 调账回调在 CHECK 阶段即被触发, 而不是 AFTER 阶段
     */
    @Test
    void syncCallbackRunsDuringCheckPhase() {
        AtomicReference<CheckExecutor> holder = new AtomicReference<>();
        AtomicReference<String> statusDuringSync = new AtomicReference<>();
        CapturingCheckSync sync = new CapturingCheckSync(CheckSyncEnum.SYNC);
        sync.onCallback = () -> statusDuringSync.set(holder.get().getExecutorManager().getCurrentStatus());

        CheckConfig config = CheckConfig.builder()
                .id("stream-sync-in-check")
                .mode(CheckMode.STREAM)
                .srcOrderedLoader(date -> Stream.of(entry("a", 1d)))
                .targetOrderedLoader(date -> Stream.of(entry("a", 2d)))
                .checkSync(sync)
                .build();

        CheckExecutor executor = CheckExecutor.buildExecutor(config);
        holder.set(executor);
        executor.process(DATE);

        assertEquals(1, sync.calls.get());
        assertEquals(ExecutorStatusEnum.CHECK.toString(), statusDuringSync.get());
        assertEquals(ExecutorStatusEnum.END.toString(), executor.getExecutorManager().getCurrentStatus());
    }

    /**
     * 返回 NO_SYNC: 未调账, 判定为未通过, 状态 ERROR
     */
    @Test
    void noSyncResultEndsInError() {
        RecordingCheckSync sync = new RecordingCheckSync();
        sync.setResult(CheckSyncEnum.NO_SYNC);

        CheckConfig config = CheckConfig.builder()
                .id("stream-no-sync")
                .mode(CheckMode.STREAM)
                .srcOrderedLoader(date -> Stream.of(entry("a", 1d), entry("b", 1d)))
                .targetOrderedLoader(date -> Stream.of(entry("a", 2d), entry("b", 2d)))
                .checkSync(sync)
                .build();

        CheckExecutor executor = CheckExecutor.buildExecutor(config);
        CheckContext context = executor.process(DATE);

        assertEquals(ExecutorStatusEnum.ERROR.toString(), executor.getExecutorManager().getCurrentStatus());
        assertFalse(executor.getExecutorManager().isComplete());
        //checkSync 非 null 时 isComplete 委托给 checkSync
        assertFalse(sync.isComplete(context));
        //未调账计数等于差异条数, 其余两类为 0
        CheckSummary summary = context.getSummary();
        assertEquals(2L, summary.getSyncCounts().get(CheckSyncEnum.NO_SYNC));
        assertEquals(0L, summary.getSyncCounts().get(CheckSyncEnum.SYNC));
        assertEquals(0L, summary.getSyncCounts().get(CheckSyncEnum.IGNORE));
    }

    /**
     * 返回 IGNORE: 不算未调账, 判定为通过
     */
    @Test
    void ignoreResultEndsSuccessfully() {
        RecordingCheckSync sync = new RecordingCheckSync();
        sync.setResult(CheckSyncEnum.IGNORE);

        CheckConfig config = CheckConfig.builder()
                .id("stream-ignore")
                .mode(CheckMode.STREAM)
                .srcOrderedLoader(date -> Stream.of(entry("a", 1d)))
                .targetOrderedLoader(date -> Stream.of(entry("a", 2d)))
                .checkSync(sync)
                .build();

        CheckExecutor executor = CheckExecutor.buildExecutor(config);
        CheckContext context = executor.process(DATE);

        assertEquals(ExecutorStatusEnum.END.toString(), executor.getExecutorManager().getCurrentStatus());
        assertTrue(sync.isComplete(context));
        assertEquals(1L, context.getSummary().getSyncCounts().get(CheckSyncEnum.IGNORE));
    }

    /**
     * 返回 SYNC: 判定为通过
     */
    @Test
    void syncResultEndsSuccessfully() {
        RecordingCheckSync sync = new RecordingCheckSync();
        sync.setResult(CheckSyncEnum.SYNC);

        CheckConfig config = CheckConfig.builder()
                .id("stream-sync")
                .mode(CheckMode.STREAM)
                .srcOrderedLoader(date -> Stream.of(entry("a", 1d)))
                .targetOrderedLoader(date -> Stream.of(entry("a", 2d)))
                .checkSync(sync)
                .build();

        CheckExecutor executor = CheckExecutor.buildExecutor(config);
        CheckContext context = executor.process(DATE);

        assertEquals(ExecutorStatusEnum.END.toString(), executor.getExecutorManager().getCurrentStatus());
        assertTrue(sync.isComplete(context));
        assertEquals(1L, context.getSummary().getSyncCounts().get(CheckSyncEnum.SYNC));
    }

    /**
     * 前置校验不通过: 两侧数据源一次都不会被调用, 状态 ERROR
     */
    @Test
    void failedPreCheckSkipsLoadersAndEndsInError() {
        AtomicInteger srcCalls = new AtomicInteger();
        AtomicInteger targetCalls = new AtomicInteger();
        RecordingCheckSync sync = new RecordingCheckSync();

        CheckConfig config = CheckConfig.builder()
                .id("stream-pre-check-fail")
                .mode(CheckMode.STREAM)
                .srcOrderedLoader(date -> {
                    srcCalls.incrementAndGet();
                    return Stream.of(entry("a", 1d));
                })
                .targetOrderedLoader(date -> {
                    targetCalls.incrementAndGet();
                    return Stream.of(entry("a", 1d));
                })
                .checkPre(context -> false)
                .checkSync(sync)
                .build();

        CheckExecutor executor = CheckExecutor.buildExecutor(config);
        CheckContext context = executor.process(DATE);

        assertEquals(ExecutorStatusEnum.ERROR.toString(), executor.getExecutorManager().getCurrentStatus());
        assertEquals(0, srcCalls.get());
        assertEquals(0, targetCalls.get());
        assertTrue(sync.getRecorded().isEmpty());
        assertNull(context.getSummary());
    }

    /**
     * 数据源抛异常: 异常被捕获, 状态停在 CHECK, processing 标记被清除
     */
    @Test
    void loaderExceptionStopsAtCheckPhase() {
        CheckConfig config = CheckConfig.builder()
                .id("stream-loader-error")
                .mode(CheckMode.STREAM)
                .srcOrderedLoader(date -> {
                    throw new RuntimeException("boom");
                })
                .targetOrderedLoader(date -> Stream.of(entry("a", 1d)))
                .build();

        CheckExecutor executor = CheckExecutor.buildExecutor(config);
        assertDoesNotThrow(() -> executor.process(DATE));

        assertEquals(ExecutorStatusEnum.CHECK.toString(), executor.getExecutorManager().getCurrentStatus());
        assertFalse(executor.getExecutorManager().isProcessing());
    }

    /**
     * 调账回调抛异常: 异常被捕获, 状态停在 CHECK, 已完成的调账副作用保留且不回滚
     */
    @Test
    void syncCallbackExceptionKeepsEarlierSideEffects() {
        CapturingCheckSync sync = new CapturingCheckSync(CheckSyncEnum.SYNC);
        //第二条差异的回调抛异常
        sync.failOnCall = 2;

        CheckConfig config = CheckConfig.builder()
                .id("stream-sync-error")
                .mode(CheckMode.STREAM)
                .srcOrderedLoader(date -> Stream.of(entry("a", 1d), entry("b", 1d)))
                .targetOrderedLoader(date -> Stream.of(entry("a", 2d), entry("b", 2d)))
                .checkSync(sync)
                .build();

        CheckExecutor executor = CheckExecutor.buildExecutor(config);
        assertDoesNotThrow(() -> executor.process(DATE));

        assertEquals(ExecutorStatusEnum.CHECK.toString(), executor.getExecutorManager().getCurrentStatus());
        assertFalse(executor.getExecutorManager().isProcessing());
        //两条差异都被回调过, 但只有第一条的调账副作用完成
        assertEquals(2, sync.calls.get());
        assertEquals(1, sync.completed.get());
    }

    /**
     * 单侧数据为空: 数据没准备好, 零调账副作用
     */
    @Test
    void oneSideEmptyProducesNoSyncSideEffect() {
        RecordingCheckSync sync = new RecordingCheckSync();

        CheckConfig config = CheckConfig.builder()
                .id("stream-one-side-empty")
                .mode(CheckMode.STREAM)
                .srcOrderedLoader(date -> Stream.of())
                .targetOrderedLoader(date -> Stream.of(entry("a", 1d)))
                .checkSync(sync)
                .build();

        CheckExecutor executor = CheckExecutor.buildExecutor(config);
        assertDoesNotThrow(() -> executor.process(DATE));

        assertEquals(ExecutorStatusEnum.CHECK.toString(), executor.getExecutorManager().getCurrentStatus());
        assertTrue(sync.getRecorded().isEmpty());
    }

    /**
     * 组装期快速失败: STREAM 模式缺少有序数据源时直接抛异常
     */
    @Test
    void streamModeWithoutOrderedLoadersFailsFast() {
        CheckConfig config = CheckConfig.builder()
                .id("stream-missing-loader")
                .mode(CheckMode.STREAM)
                .build();

        NullPointerException exception = assertThrows(NullPointerException.class,
                () -> CheckExecutor.buildExecutor(config));

        assertTrue(exception.getMessage().contains("STREAM 模式需要"), exception.getMessage());
    }

    /**
     * MEMORY 模式不受影响: 仍然产出全量结果, 没有摘要
     */
    @Test
    void memoryModeRemainsUnchanged() {
        CheckConfig config = CheckConfig.builder()
                .id("memory-unchanged")
                .mode(CheckMode.MEMORY)
                .srcLoader(date -> List.of(entry("a", 1d)))
                .targetLoader(date -> List.of(entry("a", 1d)))
                .build();

        CheckExecutor executor = CheckExecutor.buildExecutor(config);
        CheckContext context = executor.process(DATE);

        assertEquals(ExecutorStatusEnum.END.toString(), executor.getExecutorManager().getCurrentStatus());
        assertNotNull(context.getCheckResult());
        assertNull(context.getSummary());
    }

    /**
     * mode 为 null 时回落到 MEMORY: {@code @Builder.Default} 只在未显式赋值时生效, 显式置 null 时 mode 就是 null
     */
    @Test
    void nullModeFallsBackToMemoryMode() {
        CheckConfig config = CheckConfig.builder()
                .id("null-mode-memory")
                .mode(null)
                .srcLoader(date -> List.of(entry("a", 1d)))
                .targetLoader(date -> List.of(entry("a", 1d)))
                .build();
        assertNull(config.getMode());

        CheckExecutor executor = CheckExecutor.buildExecutor(config);
        CheckContext context = executor.process(DATE);

        assertEquals(ExecutorStatusEnum.END.toString(), executor.getExecutorManager().getCurrentStatus());
        assertNotNull(context.getCheckResult());
        assertNull(context.getSummary());
    }

    /**
     * 未配置 checkSync 时, 是否通过由摘要回落判定: 摘要里没有未调账即为通过
     */
    @Test
    void withoutCheckSyncCompletenessFallsBackToSummary() {
        CheckConfig config = CheckConfig.builder()
                .id("stream-no-check-sync")
                .mode(CheckMode.STREAM)
                .srcOrderedLoader(date -> Stream.of(entry("a", 1d)))
                .targetOrderedLoader(date -> Stream.of(entry("a", 2d)))
                .build();

        CheckExecutor executor = CheckExecutor.buildExecutor(config);
        CheckContext context = executor.process(DATE);

        assertEquals(ExecutorStatusEnum.END.toString(), executor.getExecutorManager().getCurrentStatus());
        CheckSummary summary = context.getSummary();
        assertNotNull(summary);
        //没有 checkSync 时差异一律视为已调账
        assertEquals(1L, summary.getSyncCounts().get(CheckSyncEnum.SYNC));
        assertEquals(0L, summary.getSyncCounts().get(CheckSyncEnum.NO_SYNC));
    }

    /**
     * 未配置 checkSync 且前置校验不通过: 摘要为 null, 回落分支判定为未通过
     */
    @Test
    void withoutCheckSyncAndFailedPreCheckEndsInError() {
        CheckConfig config = CheckConfig.builder()
                .id("stream-no-check-sync-pre-check-fail")
                .mode(CheckMode.STREAM)
                .srcOrderedLoader(date -> Stream.of(entry("a", 1d)))
                .targetOrderedLoader(date -> Stream.of(entry("a", 1d)))
                .checkPre(context -> false)
                .build();

        CheckExecutor executor = CheckExecutor.buildExecutor(config);
        executor.process(DATE);

        assertEquals(ExecutorStatusEnum.ERROR.toString(), executor.getExecutorManager().getCurrentStatus());
    }

    /**
     * 产出顺序: 回调先于调账结果写入, 样本上的调账结果则是回调的返回值
     */
    @Test
    void syncResultIsAssignedAfterCallbackReturns() {
        CapturingCheckSync sync = new CapturingCheckSync(CheckSyncEnum.IGNORE);

        CheckConfig config = CheckConfig.builder()
                .id("stream-emit-order")
                .mode(CheckMode.STREAM)
                .srcOrderedLoader(date -> Stream.of(entry("a", 1d)))
                .targetOrderedLoader(date -> Stream.of(entry("a", 2d)))
                .checkSync(sync)
                .build();

        CheckExecutor executor = CheckExecutor.buildExecutor(config);
        CheckContext context = executor.process(DATE);

        assertEquals(1, sync.calls.get());
        //回调被调用的那一刻, 单元上的调账结果必须还是 null
        assertNull(sync.syncSeenInCallback);
        //样本上的调账结果已被写成回调的返回值
        List<CheckUnit> samples = context.getSummary().getSamples();
        assertEquals(1, samples.size());
        assertEquals(CheckSyncEnum.IGNORE, samples.get(0).getSync());
        assertEquals(CheckStateEnum.DIFFERENT, samples.get(0).getState());
    }

    /**
     * 流式结果日志必须真正带出摘要数据
     * <p>
     * Hutool 的 {@code Log.info(format, args)} 走 {@code StrUtil.format}, 格式串没有 {@code {}} 占位符时参数会被丢弃,
     * 因此流式分支的日志必须带占位符
     */
    @Test
    void streamResultLogCarriesSummaryData() throws Exception {
        CheckConfig config = CheckConfig.builder()
                .id("stream-result-log")
                .mode(CheckMode.STREAM)
                .srcOrderedLoader(date -> Stream.of(entry("a", 1d)))
                .targetOrderedLoader(date -> Stream.of(entry("a", 2d)))
                .checkSync(new RecordingCheckSync())
                .build();

        CheckExecutor executor = CheckExecutor.buildExecutor(config);
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        PrintStream original = System.out;
        try {
            System.setOut(new PrintStream(buffer, true, StandardCharsets.UTF_8));
            executor.process(DATE);
        } finally {
            System.setOut(original);
        }

        String output = buffer.toString(StandardCharsets.UTF_8);
        assertTrue(output.contains("对账结果"), output);
        assertTrue(output.contains("sourceTotal"), output);
        assertTrue(output.contains("targetTotal"), output);
        assertTrue(output.contains("compared"), output);
        assertTrue(output.contains("stateCounts"), output);
        assertTrue(output.contains("syncCounts"), output);
    }
}
