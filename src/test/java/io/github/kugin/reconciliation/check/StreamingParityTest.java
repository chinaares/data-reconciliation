package io.github.kugin.reconciliation.check;

import io.github.kugin.reconciliation.after.CheckSync;
import io.github.kugin.reconciliation.before.OrderedResourceLoaderAdapter;
import io.github.kugin.reconciliation.before.ResourceLoader;
import io.github.kugin.reconciliation.domain.CheckConfig;
import io.github.kugin.reconciliation.domain.CheckContext;
import io.github.kugin.reconciliation.domain.CheckEntry;
import io.github.kugin.reconciliation.domain.CheckSummary;
import io.github.kugin.reconciliation.domain.CheckUnit;
import io.github.kugin.reconciliation.domain.FieldkUnit;
import io.github.kugin.reconciliation.enums.CheckMode;
import io.github.kugin.reconciliation.enums.CheckStateEnum;
import io.github.kugin.reconciliation.enums.CheckSyncEnum;
import io.github.kugin.reconciliation.enums.ExecutorStatusEnum;
import io.github.kugin.reconciliation.executor.CheckExecutor;
import io.github.kugin.reconciliation.support.RecordingCheckSync;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MEMORY 与 STREAM 两条路径的对拍测试
 * <p>
 * 对拍只使用合法数据: key 唯一且非 null、两侧各自按 key 严格升序。
 * 重复 key / null key 不属于对拍范畴, 由 {@link StreamingCheckProcessorTest} 的护栏用例覆盖。
 * <p>
 * 两类用例:
 * <ol>
 *     <li>逻辑一致性: 同一份数据分别跑两条路径, 逐条比较记录到的 {@link CheckUnit};</li>
 *     <li>内存特征: 用惰性 loader 记录拉取时刻, 证明 STREAM 是交错拉取且不在首次产出差异前全量物化。</li>
 * </ol>
 *
 * @author Kugin
 */
class StreamingParityTest {

    /**
     * 对账日期
     */
    private static final String DATE = "20210204";

    /**
     * 惰性/无限序列用例的条数
     */
    private static final int LAZY_LIMIT = 200;

    /**
     * 状态字段的候选值
     */
    private static final List<String> STATUSES = List.of("NEW", "PAID", "WAIT");

    // ------------------------------------------------------------------
    // 一、逻辑一致性对拍
    // ------------------------------------------------------------------

    /**
     * 同一份随机数据(100~1000 条, 固定种子), 两条路径记录的差异必须逐条一致
     * <p>
     * 覆盖三种 diff 状态与多字段差异; 两侧必须按 key 排序后再比较, 因为 MEMORY 的产出顺序来自
     * {@link java.util.HashMap} 迭代顺序, 而 STREAM 是 key 升序
     */
    @Test
    void randomDatasetProducesIdenticalDiffsInBothModes() {
        Dataset dataset = randomDataset();

        RecordingCheckSync memorySync = new RecordingCheckSync();
        CheckContext memoryContext = runMemory("parity-random-memory", dataset.source(), dataset.target(), memorySync);

        RecordingCheckSync streamSync = new RecordingCheckSync();
        CheckContext streamContext = runStream("parity-random-stream", dataset.source(), dataset.target(), streamSync);

        List<CheckUnit> memoryUnits = memorySync.getRecorded();
        List<CheckUnit> streamUnits = streamSync.getRecorded();
        //数据集必须真正覆盖全部三种差异状态, 否则对拍没有意义
        assertEquals(EnumSet.allOf(CheckStateEnum.class),
                memoryUnits.stream().map(CheckUnit::getState).collect(Collectors.toSet()));

        assertUnitsIdentical(memoryUnits, streamUnits);

        CheckSummary summary = streamContext.getSummary();
        assertNotNull(summary, "STREAM 路径应产出摘要");
        assertNull(streamContext.getCheckResult(), "STREAM 路径不应产出全量结果");
        assertNotNull(memoryContext.getCheckResult(), "MEMORY 路径应产出全量结果");
        assertNull(memoryContext.getSummary(), "MEMORY 路径不应产出摘要");

        assertEquals(countStates(memoryUnits), summary.getStateCounts(), "两侧各状态的差异计数必须一致");
        assertEquals(dataset.source().size(), summary.getSourceTotal());
        assertEquals(dataset.target().size(), summary.getTargetTotal());
        assertEquals(comparedCount(dataset), summary.getComparedCount());
        assertEquals(memoryUnits.size(), summary.getStateCounts().values().stream()
                .mapToLong(Long::longValue).sum(), "摘要中各状态计数之和必须等于差异条数");
    }

    // ------------------------------------------------------------------
    // 二、内存特征: 交错拉取
    // ------------------------------------------------------------------

    /**
     * STREAM 路径两侧交错拉取: 任意一次产出元素的时刻, 两侧已拉取条数相差不超过 1
     * <p>
     * 两侧使用完全相同的 key 集合与长度, 因此不存在单侧耗尽后的排空阶段
     * (排空阶段一侧连续单独拉取、计数差必然大于 1, 那是 merge join 的固有性质, 不是缺陷)。
     */
    @Test
    void streamPullsBothSidesInterleaved() {
        PullProbe probe = new PullProbe();
        List<CheckEntry> sourceData = entries(LAZY_LIMIT, 1d);
        List<CheckEntry> targetData = entries(LAZY_LIMIT, 1d);
        RecordingCheckSync sync = new RecordingCheckSync();

        CheckConfig config = CheckConfig.builder()
                .id("parity-interleaved")
                .mode(CheckMode.STREAM)
                .srcOrderedLoader(date -> sourceData.stream().peek(entry -> probe.sourceProduced()))
                .targetOrderedLoader(date -> targetData.stream().peek(entry -> probe.targetProduced()))
                .checkSync(sync)
                .build();

        CheckContext context = run("parity-interleaved", config);

        assertTrue(sync.getRecorded().isEmpty(), "两侧数据完全一致时不应有差异回调");
        assertEquals(LAZY_LIMIT, probe.sourceTotal());
        assertEquals(LAZY_LIMIT, probe.targetTotal());
        assertEquals(LAZY_LIMIT, context.getSummary().getComparedCount());
        assertFalse(probe.snapshots().isEmpty(), "必须记录到每一次元素产出");

        for (int[] snapshot : probe.snapshots()) {
            assertTrue(Math.abs(snapshot[0] - snapshot[1]) <= 1,
                    "交错拉取期间两侧已拉取条数之差不应超过 1, 实际源侧 " + snapshot[0] + " / 目标侧 " + snapshot[1]);
        }
    }

    // ------------------------------------------------------------------
    // 三、内存特征: 惰性 / 非物化
    // ------------------------------------------------------------------

    /**
     * 源侧是无限递增序列, 只用前 N 条: 证明流是惰性的, 且首次产出差异时源侧远未全量物化
     * <p>
     * 若 STREAM 路径在比对前全量收集源侧, 本用例既会拉取无限序列无法结束, 也会在首次产出差异时
     * 已消费 N 条。两条断言缺一不可: 前者证明不会挂住, 后者证明没有全量驻载。
     */
    @Test
    @Timeout(10)
    void infiniteSourceIsConsumedLazilyAndStopsAtLimit() {
        PullProbe probe = new PullProbe();
        FirstEmitProbe sync = new FirstEmitProbe(probe);

        CheckConfig config = CheckConfig.builder()
                .id("parity-infinite-stream")
                .mode(CheckMode.STREAM)
                .srcOrderedLoader(date -> Stream.iterate(0, i -> i + 1)
                        .map(StreamingParityTest::key)
                        .map(key -> amountEntry(key, 1d))
                        .peek(entry -> probe.sourceProduced())
                        .limit(LAZY_LIMIT))
                .targetOrderedLoader(date -> entries(LAZY_LIMIT, 2d).stream()
                        .peek(entry -> probe.targetProduced()))
                .checkSync(sync)
                .build();

        CheckContext context = run("parity-infinite-stream", config);

        CheckSummary summary = context.getSummary();
        assertNotNull(summary, "STREAM 路径应产出摘要");
        assertEquals(LAZY_LIMIT, summary.getComparedCount());
        assertEquals(LAZY_LIMIT, summary.getStateCounts().get(CheckStateEnum.DIFFERENT));
        assertEquals(LAZY_LIMIT, probe.sourceTotal(), "无限序列只应被消费前 N 条");

        int[] firstEmit = sync.firstEmitPulled();
        assertNotNull(firstEmit, "每个 key 的金额都不同, 首次比对就应产出差异");
        assertTrue(firstEmit[0] <= 2,
                "首次产出差异时源侧不应已全量物化, 实际已拉取 " + firstEmit[0] + " 条");
    }

    /**
     * 双向对照: 同一份有限数据, 首次产出差异时 MEMORY 已全量拉完源侧, STREAM 只拉了常数条
     * <p>
     * MEMORY 的 {@link ResourceLoader#load(String)} 按接口形状一次性返回整个 List,
     * 因此首次产出差异时源侧计数必然等于 N; STREAM 侧则 <= 2。
     */
    @Test
    void firstEmitPullCountSeparatesMemoryMaterializationFromStreaming() {
        //MEMORY: loader 一次性返回整个列表
        PullProbe memoryProbe = new PullProbe();
        FirstEmitProbe memorySync = new FirstEmitProbe(memoryProbe);
        ResourceLoader memorySourceLoader = date -> {
            List<CheckEntry> data = entries(LAZY_LIMIT, 1d);
            memoryProbe.sourceProduced(data.size());
            return data;
        };
        ResourceLoader memoryTargetLoader = date -> {
            List<CheckEntry> data = entries(LAZY_LIMIT, 2d);
            memoryProbe.targetProduced(data.size());
            return data;
        };
        CheckConfig memoryConfig = CheckConfig.builder()
                .id("parity-first-emit-memory")
                .mode(CheckMode.MEMORY)
                .srcLoader(memorySourceLoader)
                .targetLoader(memoryTargetLoader)
                .checkSync(memorySync)
                .build();
        run("parity-first-emit-memory", memoryConfig);

        int[] memoryFirstEmit = memorySync.firstEmitPulled();
        assertNotNull(memoryFirstEmit, "每个 key 的金额都不同, 首次比对就应产出差异");
        assertEquals(LAZY_LIMIT, memoryFirstEmit[0], "MEMORY 在首次产出差异前必须已全量加载源侧");

        //STREAM: 同样的数据改为惰性流
        PullProbe streamProbe = new PullProbe();
        FirstEmitProbe streamSync = new FirstEmitProbe(streamProbe);
        List<CheckEntry> sourceData = entries(LAZY_LIMIT, 1d);
        List<CheckEntry> targetData = entries(LAZY_LIMIT, 2d);
        CheckConfig streamConfig = CheckConfig.builder()
                .id("parity-first-emit-stream")
                .mode(CheckMode.STREAM)
                .srcOrderedLoader(date -> sourceData.stream().peek(entry -> streamProbe.sourceProduced()))
                .targetOrderedLoader(date -> targetData.stream().peek(entry -> streamProbe.targetProduced()))
                .checkSync(streamSync)
                .build();
        run("parity-first-emit-stream", streamConfig);

        int[] streamFirstEmit = streamSync.firstEmitPulled();
        assertNotNull(streamFirstEmit, "每个 key 的金额都不同, 首次比对就应产出差异");
        assertTrue(streamFirstEmit[0] <= 2,
                "STREAM 首次产出差异时源侧不应已全量物化, 实际已拉取 " + streamFirstEmit[0] + " 条");
    }

    // ------------------------------------------------------------------
    // 执行与断言辅助
    // ------------------------------------------------------------------

    /**
     * MEMORY 路径执行: 全量 loader + 全量比对
     *
     * @param id     执行器唯一标识
     * @param source 源侧数据
     * @param target 目标侧数据
     * @param sync   调账回调
     * @return 对账上下文
     */
    private static CheckContext runMemory(String id, List<CheckEntry> source, List<CheckEntry> target, CheckSync sync) {
        CheckConfig config = CheckConfig.builder()
                .id(id)
                .mode(CheckMode.MEMORY)
                .srcLoader(date -> source)
                .targetLoader(date -> target)
                .checkSync(sync)
                .build();
        return run(id, config);
    }

    /**
     * STREAM 路径执行: 用 {@link OrderedResourceLoaderAdapter} 把同一份全量数据排序后转成流
     * <p>
     * 适配器本身仍是全量驻载, 但对拍验证的是逻辑一致性而非内存特征, 使用它是合理的
     *
     * @param id     执行器唯一标识
     * @param source 源侧数据
     * @param target 目标侧数据
     * @param sync   调账回调
     * @return 对账上下文
     */
    private static CheckContext runStream(String id, List<CheckEntry> source, List<CheckEntry> target, CheckSync sync) {
        CheckConfig config = CheckConfig.builder()
                .id(id)
                .mode(CheckMode.STREAM)
                .srcOrderedLoader(new OrderedResourceLoaderAdapter(date -> source))
                .targetOrderedLoader(new OrderedResourceLoaderAdapter(date -> target))
                .checkSync(sync)
                .build();
        return run(id, config);
    }

    /**
     * 执行并断言正常结束
     * <p>
     * {@link CheckExecutor#process(String)} 会吞掉异常并把状态停在出错阶段, 因此必须断言 END,
     * 否则「两条路径都抛异常、都没记录到差异」会被误判为对拍一致
     *
     * @param id     执行器唯一标识
     * @param config 配置
     * @return 对账上下文
     */
    private static CheckContext run(String id, CheckConfig config) {
        CheckExecutor executor = CheckExecutor.buildExecutor(config);
        CheckContext context = executor.process(DATE);
        assertEquals(ExecutorStatusEnum.END.toString(), executor.getExecutorManager().getCurrentStatus(),
                "执行器应正常结束: " + id);
        return context;
    }

    /**
     * 逐条比较两侧记录的差异单元
     *
     * @param memoryUnits MEMORY 路径记录的差异
     * @param streamUnits STREAM 路径记录的差异
     */
    private static void assertUnitsIdentical(List<CheckUnit> memoryUnits, List<CheckUnit> streamUnits) {
        List<CheckUnit> expected = sortedByKey(memoryUnits);
        List<CheckUnit> actual = sortedByKey(streamUnits);
        assertEquals(expected.size(), actual.size(), "两条路径记录的差异条数必须一致");
        for (int i = 0; i < expected.size(); i++) {
            assertUnitEquals(expected.get(i), actual.get(i));
        }
    }

    /**
     * 比较两个差异单元: key、state、差异字段集合与每个字段的 source/target 值
     *
     * @param expected MEMORY 路径的差异单元
     * @param actual   STREAM 路径的差异单元
     */
    private static void assertUnitEquals(CheckUnit expected, CheckUnit actual) {
        assertEquals(expected.getKey(), actual.getKey(), "差异单元的 key 必须一致");
        assertEquals(expected.getState(), actual.getState(), "差异单元的状态必须一致: " + expected.getKey());
        Map<String, FieldkUnit> expectedFields = expected.getDifferentFields();
        Map<String, FieldkUnit> actualFields = actual.getDifferentFields();
        assertNotNull(expectedFields);
        assertNotNull(actualFields);
        assertEquals(expectedFields.keySet(), actualFields.keySet(), "差异字段集合必须一致: " + expected.getKey());
        for (Map.Entry<String, FieldkUnit> entry : expectedFields.entrySet()) {
            FieldkUnit expectedField = entry.getValue();
            FieldkUnit actualField = actualFields.get(entry.getKey());
            assertNotNull(actualField);
            assertEquals(expectedField.getSource(), actualField.getSource(),
                    "字段 " + entry.getKey() + " 的源值必须一致: " + expected.getKey());
            assertEquals(expectedField.getTarget(), actualField.getTarget(),
                    "字段 " + entry.getKey() + " 的目标值必须一致: " + expected.getKey());
        }
    }

    private static List<CheckUnit> sortedByKey(List<CheckUnit> units) {
        return units.stream().sorted(Comparator.comparing(CheckUnit::getKey)).toList();
    }

    private static Map<CheckStateEnum, Long> countStates(List<CheckUnit> units) {
        Map<CheckStateEnum, Long> counts = new EnumMap<>(CheckStateEnum.class);
        for (CheckStateEnum state : CheckStateEnum.values()) {
            counts.put(state, 0L);
        }
        units.forEach(unit -> counts.merge(unit.getState(), 1L, Long::sum));
        return counts;
    }

    /**
     * 两侧 key 相同的条数, 即实际做过字段比较的条数
     *
     * @param dataset 数据集
     * @return 两侧 key 交集大小
     */
    private static long comparedCount(Dataset dataset) {
        Set<String> sourceKeys = dataset.source().stream().map(CheckEntry::getKey).collect(Collectors.toSet());
        Set<String> targetKeys = dataset.target().stream().map(CheckEntry::getKey).collect(Collectors.toSet());
        return sourceKeys.stream().filter(targetKeys::contains).count();
    }

    /**
     * 生成对拍数据
     * <p>
     * 固定种子 42, 100~1000 条。key 从小整数空间随机取且显式保证唯一: MEMORY 路径用
     * {@code Collectors.toMap} 构建 Map, 重复 key 会抛 {@link IllegalStateException}。
     * 每条随机决定只在源侧、只在目标侧或两侧都有; 两侧都有的再随机决定金额/状态是否不同。
     *
     * @return 对拍数据
     */
    private static Dataset randomDataset() {
        Random random = new Random(42);
        int total = 100 + random.nextInt(901);
        Set<Integer> uniqueKeys = new LinkedHashSet<>();
        while (uniqueKeys.size() < total) {
            //key 空间是条数的 4 倍, 保证能在有限次内取满且全部唯一
            uniqueKeys.add(random.nextInt(total * 4) + 1);
        }
        List<Integer> keys = new ArrayList<>(uniqueKeys);
        Collections.shuffle(keys, random);

        List<CheckEntry> source = new ArrayList<>();
        List<CheckEntry> target = new ArrayList<>();
        for (Integer key : keys) {
            String keyText = String.valueOf(key);
            int side = random.nextInt(100);
            if (side < 15) {
                source.add(entry(keyText, amount(random), status(random)));
                continue;
            }
            if (side < 30) {
                target.add(entry(keyText, amount(random), status(random)));
                continue;
            }
            double sourceAmount = amount(random);
            String sourceStatus = status(random);
            double targetAmount = random.nextBoolean() ? sourceAmount : amount(random);
            String targetStatus = random.nextInt(4) == 0 ? status(random) : sourceStatus;
            source.add(entry(keyText, sourceAmount, sourceStatus));
            target.add(entry(keyText, targetAmount, targetStatus));
        }
        return new Dataset(source, target);
    }

    private static double amount(Random random) {
        return random.nextInt(1000) / 10.0;
    }

    private static String status(Random random) {
        return STATUSES.get(random.nextInt(STATUSES.size()));
    }

    private static CheckEntry entry(String key, double amount, String status) {
        Map<String, Object> checkData = new LinkedHashMap<>();
        checkData.put("amount", amount);
        checkData.put("status", status);
        return new CheckEntry(key, null, checkData);
    }

    private static CheckEntry amountEntry(String key, double amount) {
        return new CheckEntry(key, null, Map.of("amount", amount));
    }

    /**
     * 生成 key 严格升序的条目: 零填充保证 {@link String#compareTo} 顺序与数值顺序一致
     *
     * @param count  条数
     * @param amount 金额
     * @return 条目列表
     */
    private static List<CheckEntry> entries(int count, double amount) {
        return IntStream.range(0, count)
                .mapToObj(i -> amountEntry(key(i), amount))
                .toList();
    }

    /**
     * 零填充的递增 key: 保证 {@link String#compareTo} 的字典序与数值序一致
     *
     * @param value 数值
     * @return 定宽 key
     */
    private static String key(int value) {
        return String.format("%06d", value);
    }

    /**
     * 对拍数据
     *
     * @param source 源侧数据
     * @param target 目标侧数据
     */
    private record Dataset(List<CheckEntry> source, List<CheckEntry> target) {
    }

    /**
     * 拉取探针: 记录两侧每次产出元素时各自的累计条数快照
     */
    private static final class PullProbe {

        private final AtomicInteger sourcePulled = new AtomicInteger();
        private final AtomicInteger targetPulled = new AtomicInteger();
        private final List<int[]> snapshots = new CopyOnWriteArrayList<>();

        /**
         * 源侧产出 count 条(一次性加载的场景可一次传入全量条数)
         *
         * @param count 条数
         */
        private void sourceProduced(int count) {
            sourcePulled.addAndGet(count);
            snapshots.add(currentSnapshot());
        }

        private void sourceProduced() {
            sourceProduced(1);
        }

        private void targetProduced(int count) {
            targetPulled.addAndGet(count);
            snapshots.add(currentSnapshot());
        }

        private void targetProduced() {
            targetProduced(1);
        }

        private int[] currentSnapshot() {
            return new int[]{sourcePulled.get(), targetPulled.get()};
        }

        private int sourceTotal() {
            return sourcePulled.get();
        }

        private int targetTotal() {
            return targetPulled.get();
        }

        private List<int[]> snapshots() {
            return snapshots;
        }
    }

    /**
     * 首次产出差异探针: 在第一次回调 {@link CheckSync} 的时刻抓取两侧已拉取条数
     */
    private static final class FirstEmitProbe implements CheckSync {

        private final PullProbe probe;

        private final AtomicReference<int[]> firstEmitPulled = new AtomicReference<>();

        private FirstEmitProbe(PullProbe probe) {
            this.probe = probe;
        }

        @Override
        public CheckSyncEnum syncSourceMore(CheckUnit checkUnit) {
            return capture();
        }

        @Override
        public CheckSyncEnum syncTargetMore(CheckUnit checkUnit) {
            return capture();
        }

        @Override
        public CheckSyncEnum syncDifferent(CheckUnit checkUnit) {
            return capture();
        }

        private CheckSyncEnum capture() {
            firstEmitPulled.compareAndSet(null, probe.currentSnapshot());
            return CheckSyncEnum.SYNC;
        }

        private int[] firstEmitPulled() {
            return firstEmitPulled.get();
        }
    }
}
