package io.github.kugin.reconciliation.check;

import io.github.kugin.reconciliation.before.DefaultBeforeCheckProcessor;
import io.github.kugin.reconciliation.before.DefaultResourceReader;
import io.github.kugin.reconciliation.before.ResourceLoader;
import io.github.kugin.reconciliation.domain.CheckContext;
import io.github.kugin.reconciliation.domain.CheckEntry;
import io.github.kugin.reconciliation.domain.CheckException;
import io.github.kugin.reconciliation.domain.CheckSummary;
import io.github.kugin.reconciliation.domain.CheckUnit;
import io.github.kugin.reconciliation.enums.CheckStateEnum;
import io.github.kugin.reconciliation.enums.CheckSyncEnum;
import io.github.kugin.reconciliation.executor.DefaultExecutorManager;
import io.github.kugin.reconciliation.executor.ExecutorManager;
import io.github.kugin.reconciliation.support.RecordingCheckSync;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

    /**
     * 计数自洽: 两侧被消费的条数与实际比对条数, 必须与两侧单边多出的条数吻合
     *
     * @param summary 摘要
     */
    private static void assertCountsConsistent(CheckSummary summary) {
        long sourceMore = summary.getStateCounts().get(CheckStateEnum.SOURCE_MORE);
        long targetMore = summary.getStateCounts().get(CheckStateEnum.TARGET_MORE);
        assertEquals(sourceMore + targetMore,
                summary.getSourceTotal() + summary.getTargetTotal() - 2 * summary.getComparedCount());
    }

    @Test
    void identicalEntriesProduceNoDiff() {
        run(List.of(entry("a", 1d), entry("b", 1d)), List.of(entry("a", 1d), entry("b", 1d)));

        assertTrue(sync.getRecorded().isEmpty());
        CheckSummary summary = context.getSummary();
        assertEquals(2L, summary.getComparedCount());
        assertEquals(0L, summary.getStateCounts().get(CheckStateEnum.DIFFERENT));
        assertEquals(2L, summary.getSourceTotal());
        assertEquals(2L, summary.getTargetTotal());
        assertCountsConsistent(summary);
    }

    @Test
    void sourceSideHasMoreEntry() {
        run(List.of(entry("a", 1d), entry("b", 1d)), List.of(entry("a", 1d)));

        assertEquals(1, sync.getRecorded().size());
        CheckUnit unit = sync.getRecorded().get(0);
        assertEquals("b", unit.getKey());
        assertEquals(CheckStateEnum.SOURCE_MORE, unit.getState());
        assertEquals(1L, context.getSummary().getStateCounts().get(CheckStateEnum.SOURCE_MORE));
        assertCountsConsistent(context.getSummary());
    }

    @Test
    void targetSideHasMoreEntry() {
        run(List.of(entry("a", 1d)), List.of(entry("a", 1d), entry("b", 1d)));

        assertEquals(1, sync.getRecorded().size());
        CheckUnit unit = sync.getRecorded().get(0);
        assertEquals("b", unit.getKey());
        assertEquals(CheckStateEnum.TARGET_MORE, unit.getState());
        assertEquals(1L, context.getSummary().getStateCounts().get(CheckStateEnum.TARGET_MORE));
        assertCountsConsistent(context.getSummary());
    }

    @Test
    void fieldValueDiffers() {
        run(List.of(entry("a", 1d)), List.of(entry("a", 2d)));

        assertEquals(1, sync.getRecorded().size());
        CheckUnit unit = sync.getRecorded().get(0);
        assertEquals(CheckStateEnum.DIFFERENT, unit.getState());
        assertEquals(1d, unit.getDifferentFields().get("amount").getSource());
        assertEquals(2d, unit.getDifferentFields().get("amount").getTarget());
        assertCountsConsistent(context.getSummary());
    }

    /**
     * 字段仅源侧有: 继承父类的字段比较逻辑才能识别出「只有源侧存在」的字段
     */
    @Test
    void fieldOnlyExistsOnSourceSide() {
        run(List.of(new CheckEntry("a", null, Map.of("amount", 1d, "x", 2d))),
                List.of(entry("a", 1d)));

        assertEquals(1, sync.getRecorded().size());
        CheckUnit unit = sync.getRecorded().get(0);
        assertEquals(CheckStateEnum.DIFFERENT, unit.getState());
        assertNotNull(unit.getDifferentFields().get("x"));
        assertEquals(2d, unit.getDifferentFields().get("x").getSource());
        assertNull(unit.getDifferentFields().get("x").getTarget());
    }

    /**
     * 字段仅目标侧有
     */
    @Test
    void fieldOnlyExistsOnTargetSide() {
        run(List.of(entry("a", 1d)),
                List.of(new CheckEntry("a", null, Map.of("amount", 1d, "x", 2d))));

        assertEquals(1, sync.getRecorded().size());
        CheckUnit unit = sync.getRecorded().get(0);
        assertEquals(CheckStateEnum.DIFFERENT, unit.getState());
        assertNotNull(unit.getDifferentFields().get("x"));
        assertNull(unit.getDifferentFields().get("x").getSource());
        assertEquals(2d, unit.getDifferentFields().get("x").getTarget());
    }

    @Test
    void interleavedEntriesAreReportedInOrder() {
        run(List.of(entry("a", 1d), entry("c", 1d)), List.of(entry("b", 1d), entry("c", 1d)));

        assertEquals(2, sync.getRecorded().size());
        assertEquals("a", sync.getRecorded().get(0).getKey());
        assertEquals(CheckStateEnum.SOURCE_MORE, sync.getRecorded().get(0).getState());
        assertEquals("b", sync.getRecorded().get(1).getKey());
        assertEquals(CheckStateEnum.TARGET_MORE, sync.getRecorded().get(1).getState());
        assertCountsConsistent(context.getSummary());
    }

    /**
     * 源侧为空: 数据没准备好, 且必须在 merge 之前抛出, 保证零 sync 副作用
     */
    @Test
    void emptySourceSideIsRejectedWithoutSyncSideEffect() {
        CheckException exception = assertThrows(CheckException.class,
                () -> run(List.of(), List.of(entry("a", 1d))));

        assertTrue(exception.getMessage().contains("对账数据未准备好"));
        assertTrue(sync.getRecorded().isEmpty());
    }

    /**
     * 目标侧为空: 与源侧为空对称, 两个方向都要覆盖
     */
    @Test
    void emptyTargetSideIsRejectedWithoutSyncSideEffect() {
        CheckException exception = assertThrows(CheckException.class,
                () -> run(List.of(entry("a", 1d)), List.of()));

        assertTrue(exception.getMessage().contains("对账数据未准备好"));
        assertTrue(sync.getRecorded().isEmpty());
    }

    @Test
    void bothSidesEmptyProducesEmptySummary() {
        run(List.of(), List.of());

        assertTrue(sync.getRecorded().isEmpty());
        CheckSummary summary = context.getSummary();
        assertNotNull(summary);
        assertEquals(0L, summary.getSourceTotal());
        assertEquals(0L, summary.getTargetTotal());
        assertEquals(0L, summary.getComparedCount());
        assertTrue(summary.getSamples().isEmpty());
        assertCountsConsistent(summary);
    }

    @Test
    void unorderedSourceIsRejected() {
        CheckException exception = assertThrows(CheckException.class,
                () -> run(List.of(entry("b", 1d), entry("a", 1d)), List.of(entry("b", 1d), entry("a", 1d))));

        assertTrue(exception.getMessage().contains("未按 key 升序"));
    }

    @Test
    void duplicatedKeyOnSameSideIsRejected() {
        CheckException exception = assertThrows(CheckException.class,
                () -> run(List.of(entry("a", 1d), entry("a", 2d)), List.of(entry("a", 1d), entry("a", 2d))));

        assertTrue(exception.getMessage().contains("未按 key 升序"));
    }

    /**
     * null key 必须在首元素处就被拒绝: 首元素同样走护栏, 不能退化成 NPE
     */
    @Test
    void nullKeyIsRejected() {
        CheckException exception = assertThrows(CheckException.class,
                () -> run(List.of(new CheckEntry(null, null, Map.of("amount", 1d))), List.of(entry("a", 1d))));

        assertTrue(exception.getMessage().contains("对账 key 不能为 null"));
    }

    /**
     * 已消费的流必须原样抛出 JDK 的 IllegalStateException, 不得吞掉或包装
     */
    @Test
    void alreadyConsumedStreamIsNotSwallowed() {
        Stream<CheckEntry> consumed = Stream.of(entry("a", 1d)).peek(u -> {
        }).limit(1);
        consumed.count();

        assertThrows(IllegalStateException.class,
                () -> new StreamingCheckProcessor(date -> consumed, date -> Stream.of(), sync).compare(context));
    }

    /**
     * 同一处理器实例被不同日期并发使用时, 计数不得串味
     */
    @Test
    void concurrentRunsOnDifferentDatesDoNotMix() throws Exception {
        Map<String, List<CheckEntry>> sourceData = Map.of(
                "20210204", List.of(entry("a", 1d), entry("b", 1d)),
                "20210205", List.of(entry("x", 1d)));
        Map<String, List<CheckEntry>> targetData = Map.of(
                "20210204", List.of(entry("a", 1d), entry("b", 1d)),
                "20210205", List.of(entry("x", 1d)));
        StreamingCheckProcessor processor = new StreamingCheckProcessor(
                date -> sourceData.get(date).stream(),
                date -> targetData.get(date).stream(),
                new RecordingCheckSync());

        CheckContext first = contextOf("streaming-concurrent-a", "20210204");
        CheckContext second = contextOf("streaming-concurrent-b", "20210205");

        CountDownLatch start = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread threadA = new Thread(() -> {
            await(start);
            try {
                processor.compare(first);
            } catch (Throwable e) {
                failure.compareAndSet(null, e);
            }
        });
        Thread threadB = new Thread(() -> {
            await(start);
            try {
                processor.compare(second);
            } catch (Throwable e) {
                failure.compareAndSet(null, e);
            }
        });
        threadA.start();
        threadB.start();
        start.countDown();
        threadA.join();
        threadB.join();

        assertNull(failure.get());
        assertEquals(2L, first.getSummary().getSourceTotal());
        assertEquals(1L, second.getSummary().getSourceTotal());
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private static CheckContext contextOf(String id, String date) {
        return CheckContext.builder()
                .id(id)
                .date(date)
                .executorManager(new DefaultExecutorManager(id))
                .build();
    }

    /**
     * MEMORY 路径的 null key 行为与 STREAM 路径不同
     * <p>
     * {@code Collectors.toMap} 底层是 {@link java.util.HashMap}, 而 JDK 21 的 {@code HashMap.merge}
     * 只对 value 与 mergeFunction 做非空校验, key 允许为 null —— 所以 MEMORY 路径不会抛异常,
     * 而是静默接受 null key。(只有 {@link java.util.concurrent.ConcurrentHashMap#merge} 才拒绝 null key。)
     * <p>
     * STREAM 路径则由 merge join 的护栏显式拒绝, 抛 {@link CheckException}("对账 key 不能为 null")。
     * 此处钉住 MEMORY 侧的实际行为, 两者的 null key 行为不同是已知差异
     */
    @Test
    void memoryModeSilentlyAcceptsNullKeyWhileStreamModeRejectsIt() {
        ResourceLoader sourceLoader = date -> List.of(new CheckEntry(null, null, Map.of("amount", 1d)));
        ResourceLoader targetLoader = date -> List.of(entry("a", 1d));
        DefaultBeforeCheckProcessor before = new DefaultBeforeCheckProcessor(
                new DefaultResourceReader(sourceLoader, targetLoader), null);

        // MEMORY 路径: 不抛异常, null key 被静默收进 sourceMap
        assertDoesNotThrow(() -> before.processBeforeCompare(context));
        assertTrue(context.getSourceMap().containsKey(null));

        // STREAM 路径: 同一份数据被护栏显式拒绝
        CheckException exception = assertThrows(CheckException.class,
                () -> run(List.of(new CheckEntry(null, null, Map.of("amount", 1d))), List.of(entry("a", 1d))));
        assertTrue(exception.getMessage().contains("对账 key 不能为 null"));
    }

    @Test
    void countsAreConsistent() {
        run(List.of(entry("a", 1d), entry("b", 1d), entry("d", 1d)),
                List.of(entry("a", 1d), entry("c", 1d), entry("d", 1d)));

        CheckSummary summary = context.getSummary();
        assertEquals(3L, summary.getSourceTotal());
        assertEquals(3L, summary.getTargetTotal());
        assertEquals(2L, summary.getComparedCount());
        assertEquals(1L, summary.getStateCounts().get(CheckStateEnum.SOURCE_MORE));
        assertEquals(1L, summary.getStateCounts().get(CheckStateEnum.TARGET_MORE));
        assertCountsConsistent(summary);
    }

    @Test
    void samplesAreCappedPerState() {
        List<CheckEntry> source = new ArrayList<>();
        List<CheckEntry> target = new ArrayList<>();
        for (int i = 1; i <= 8; i++) {
            source.add(entry("a" + i, 1d));
            target.add(entry("a" + i, 2d));
        }

        run(source, target);

        CheckSummary summary = context.getSummary();
        assertEquals(8L, summary.getStateCounts().get(CheckStateEnum.DIFFERENT));
        long differentSamples = summary.getSamples().stream()
                .filter(unit -> CheckStateEnum.DIFFERENT.equals(unit.getState()))
                .count();
        assertEquals(5L, differentSamples);
        for (CheckStateEnum state : CheckStateEnum.values()) {
            long count = summary.getSamples().stream()
                    .filter(unit -> state.equals(unit.getState()))
                    .count();
            assertTrue(count <= 5L, "状态 " + state + " 的样本数不应超过 5");
        }
    }

    @Test
    void countMapsAlwaysContainEveryEnumConstant() {
        run(List.of(entry("a", 1d)), List.of(entry("a", 2d)));

        CheckSummary summary = context.getSummary();
        assertEquals(3, summary.getStateCounts().size());
        assertEquals(3, summary.getSyncCounts().size());
        for (CheckStateEnum state : CheckStateEnum.values()) {
            assertNotNull(summary.getStateCounts().get(state));
        }
        for (CheckSyncEnum syncEnum : CheckSyncEnum.values()) {
            assertNotNull(summary.getSyncCounts().get(syncEnum));
        }
    }
}
