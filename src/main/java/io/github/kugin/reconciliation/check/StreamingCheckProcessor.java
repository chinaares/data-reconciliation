package io.github.kugin.reconciliation.check;

import io.github.kugin.reconciliation.after.CheckSync;
import io.github.kugin.reconciliation.before.OrderedResourceLoader;
import io.github.kugin.reconciliation.domain.CheckContext;
import io.github.kugin.reconciliation.domain.CheckEntry;
import io.github.kugin.reconciliation.domain.CheckException;
import io.github.kugin.reconciliation.domain.CheckSummary;
import io.github.kugin.reconciliation.domain.CheckUnit;
import io.github.kugin.reconciliation.domain.FieldkUnit;
import io.github.kugin.reconciliation.enums.CheckStateEnum;
import io.github.kugin.reconciliation.enums.CheckSyncEnum;
import io.github.kugin.reconciliation.enums.ExecutorStatusEnum;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * 流式对账处理器: 两侧有序流做 merge join, 差异一产生即回调 {@link CheckSync} 后丢弃
 * <p>
 * 与全量模式({@link DefaultCheckProcessor})的差别:
 * <ol>
 *     <li>不持有全量数据, 任意时刻只持有两侧各一个当前元素, 内存占用与总条数无关;</li>
 *     <li>不产出 {@link io.github.kugin.reconciliation.domain.CheckResult}, 只产出 {@link CheckSummary} 摘要;</li>
 *     <li>调账回调在 CHECK 阶段即触发, 而不是 AFTER 阶段。</li>
 * </ol>
 * <p>
 * 有序性由渠道保证, 本类以护栏强制: key 不得为 null, 同侧必须按 key 严格升序且不重复
 *
 * @author Kugin
 */
public class StreamingCheckProcessor extends AbstractCheckProcessor {

    /**
     * 每种对账状态保留的样本数上限
     */
    private static final int SAMPLE_LIMIT = 5;

    /**
     * 源侧有序数据源
     */
    private final OrderedResourceLoader srcLoader;

    /**
     * 目标侧有序数据源
     */
    private final OrderedResourceLoader tgtLoader;

    /**
     * 调账回调, 可为 null
     */
    private final CheckSync checkSync;

    public StreamingCheckProcessor(OrderedResourceLoader srcLoader, OrderedResourceLoader tgtLoader, CheckSync checkSync) {
        this.srcLoader = srcLoader;
        this.tgtLoader = tgtLoader;
        this.checkSync = checkSync;
    }

    /**
     * 对账入口
     * <p>
     * 父类的实现会读取 {@code context.getSource()}, 流式模式下该字段为 null, 故此处整体覆写
     *
     * @param context 上下文
     */
    @Override
    public void compare(CheckContext context) {
        //前置校验未通过, 不进入比对阶段, 也不开流
        if (context.isPreCheckFailed()) {
            return;
        }
        context.getExecutorManager().setStatus(ExecutorStatusEnum.CHECK);
        doCompare(context);
    }

    /**
     * 空实现: 流式模式下上下文没有 source/target, 空数据检查在 {@link #doCompare(CheckContext)} 开流之后进行
     *
     * @param context 上下文
     */
    @Override
    public void compareCheck(CheckContext context) {
        //do-nothing
    }

    @Override
    public void doCompare(CheckContext context) {
        String date = context.getDate();
        try (Stream<CheckEntry> srcStream = srcLoader.loadOrdered(date);
             Stream<CheckEntry> tgtStream = tgtLoader.loadOrdered(date)) {
            Iterator<CheckEntry> src = srcStream.iterator();
            Iterator<CheckEntry> tgt = tgtStream.iterator();
            //hasNext 不消费元素, 此处判定是零副作用的
            boolean srcEmpty = !src.hasNext();
            boolean tgtEmpty = !tgt.hasNext();
            //恰好一侧为空: 数据没准备好
            //必须在 merge 之前判定, 否则差异已产出且调账回调已被触发, 再抛异常为时已晚
            if (srcEmpty != tgtEmpty) {
                throw new CheckException("对账数据未准备好");
            }
            //两侧都为空: 与全量模式一致, 产出空摘要
            if (srcEmpty) {
                context.setSummary(CheckSummary.empty());
                return;
            }
            Accumulator accumulator = new Accumulator();
            mergeJoin(src, tgt, accumulator);
            context.setSummary(accumulator.toSummary());
        }
    }

    /**
     * merge join 主循环
     * <p>
     * 任意时刻只持有两侧各一个当前元素, 内存占用与总条数无关
     *
     * @param src        源侧迭代器
     * @param tgt        目标侧迭代器
     * @param accumulator 累加器
     */
    private void mergeJoin(Iterator<CheckEntry> src, Iterator<CheckEntry> tgt, Accumulator accumulator) {
        Cursor srcCursor = new Cursor(src);
        Cursor tgtCursor = new Cursor(tgt);
        //首次拉取同样走护栏, 保证首元素的 null key 与乱序也被拒绝
        CheckEntry s = srcCursor.next();
        CheckEntry t = tgtCursor.next();
        while (s != null || t != null) {
            if (s == null) {
                emit(CheckStateEnum.TARGET_MORE, t.getKey(), createSingle(t.getCheckData(), false), accumulator);
                t = tgtCursor.next();
                continue;
            }
            if (t == null) {
                emit(CheckStateEnum.SOURCE_MORE, s.getKey(), createSingle(s.getCheckData(), true), accumulator);
                s = srcCursor.next();
                continue;
            }
            int cmp = s.getKey().compareTo(t.getKey());
            if (cmp < 0) {
                emit(CheckStateEnum.SOURCE_MORE, s.getKey(), createSingle(s.getCheckData(), true), accumulator);
                s = srcCursor.next();
            } else if (cmp > 0) {
                emit(CheckStateEnum.TARGET_MORE, t.getKey(), createSingle(t.getCheckData(), false), accumulator);
                t = tgtCursor.next();
            } else {
                Map<String, FieldkUnit> different = compareCheckData(s.getCheckData(), t.getCheckData());
                //key 相等即视为做过一次字段比较, 无论字段是否有差异
                accumulator.countCompared();
                if (!different.isEmpty()) {
                    emit(CheckStateEnum.DIFFERENT, s.getKey(), different, accumulator);
                }
                s = srcCursor.next();
                t = tgtCursor.next();
            }
        }
        accumulator.sourceTotal = srcCursor.count();
        accumulator.targetTotal = tgtCursor.count();
    }

    /**
     * 产出差异
     * <p>
     * 动作序列固定, 不可调换: 构造单元 -> 立即回调调账 -> 记录调账结果 -> 累加计数 -> 留存样本 -> 丢弃单元
     *
     * @param state           对账状态
     * @param key             对账标识
     * @param differentFields 差异字段
     * @param accumulator     累加器
     */
    private void emit(CheckStateEnum state, String key, Map<String, FieldkUnit> differentFields, Accumulator accumulator) {
        CheckUnit checkUnit = CheckUnit.builder()
                .key(key)
                .state(state)
                .differentFields(differentFields)
                .build();
        //立即回调调账, 差异不等结果产生
        CheckSyncEnum syncResult = dispatchSync(checkUnit, state);
        checkUnit.setSync(syncResult);
        accumulator.countState(state);
        accumulator.countSync(syncResult);
        accumulator.addSample(checkUnit);
        //丢弃该单元, 不进入任何集合
    }

    /**
     * 调账回调分发
     *
     * @param checkUnit 对账单元
     * @param state     对账状态
     * @return 调账结果
     */
    private CheckSyncEnum dispatchSync(CheckUnit checkUnit, CheckStateEnum state) {
        if (checkSync == null) {
            //与 DefaultAfterCheckProcessor 既有语义一致: 没有调账实现时一律视为已调账
            return CheckSyncEnum.SYNC;
        }
        CheckSyncEnum result = switch (state) {
            case SOURCE_MORE -> checkSync.syncSourceMore(checkUnit);
            case TARGET_MORE -> checkSync.syncTargetMore(checkUnit);
            case DIFFERENT -> checkSync.syncDifferent(checkUnit);
        };
        //回调未给出结果时按已调账处理, 与全量模式对 null 的判定保持一致
        return result == null ? CheckSyncEnum.SYNC : result;
    }

    /**
     * 护栏: 拉取下一个元素, 同时校验 key 非空与严格升序
     *
     * @param it      迭代器
     * @param lastKey 上一个被消费的元素的 key, 为 null 表示首次拉取
     * @return 下一个元素, 侧已耗尽时返回 null
     */
    private CheckEntry advance(Iterator<CheckEntry> it, String lastKey) {
        if (!it.hasNext()) {
            return null;
        }
        CheckEntry entry = it.next();
        if (entry.getKey() == null) {
            throw new CheckException("对账 key 不能为 null");
        }
        if (lastKey != null && entry.getKey().compareTo(lastKey) <= 0) {
            throw new CheckException("数据源未按 key 升序或 key 重复: " + lastKey + " -> " + entry.getKey());
        }
        return entry;
    }

    /**
     * 单侧游标: 负责拉取元素、护栏校验与消费计数
     */
    private final class Cursor {

        private final Iterator<CheckEntry> iterator;

        /**
         * 上一个被消费的元素的 key, 用于升序护栏
         */
        private String lastKey;

        /**
         * 已消费的条数
         */
        private long consumed;

        private Cursor(Iterator<CheckEntry> iterator) {
            this.iterator = iterator;
        }

        /**
         * 拉取下一个元素
         *
         * @return 下一个元素, 侧已耗尽时返回 null
         */
        private CheckEntry next() {
            CheckEntry entry = advance(iterator, lastKey);
            if (entry == null) {
                return null;
            }
            //消费即更新 lastKey, 升序护栏才能覆盖每一对相邻元素
            lastKey = entry.getKey();
            consumed++;
            return entry;
        }

        /**
         * 已消费的条数
         *
         * @return 条数
         */
        private long count() {
            return consumed;
        }
    }

    /**
     * 比对累加器
     * <p>
     * 处理器实例会被多个日期的运行共享, 因此计数必须是每次调用新建的局部状态
     */
    private static final class Accumulator {

        /**
         * 源侧被消费的条数
         */
        private long sourceTotal;

        /**
         * 目标侧被消费的条数
         */
        private long targetTotal;

        /**
         * 两侧 key 相等、实际做过字段比较的条数
         */
        private long comparedCount;

        /**
         * 各对账状态的计数
         */
        private final EnumMap<CheckStateEnum, Long> stateCounts = new EnumMap<>(CheckStateEnum.class);

        /**
         * 各调账状态的计数
         */
        private final EnumMap<CheckSyncEnum, Long> syncCounts = new EnumMap<>(CheckSyncEnum.class);

        /**
         * 各对账状态的差异样本
         */
        private final EnumMap<CheckStateEnum, List<CheckUnit>> samples = new EnumMap<>(CheckStateEnum.class);

        private Accumulator() {
            for (CheckStateEnum state : CheckStateEnum.values()) {
                stateCounts.put(state, 0L);
                samples.put(state, new ArrayList<>());
            }
            for (CheckSyncEnum sync : CheckSyncEnum.values()) {
                syncCounts.put(sync, 0L);
            }
        }

        /**
         * 累加一次字段比较
         */
        private void countCompared() {
            comparedCount++;
        }

        /**
         * 累加对账状态计数
         *
         * @param state 对账状态
         */
        private void countState(CheckStateEnum state) {
            stateCounts.merge(state, 1L, Long::sum);
        }

        /**
         * 累加调账状态计数
         *
         * @param sync 调账状态
         */
        private void countSync(CheckSyncEnum sync) {
            syncCounts.merge(sync, 1L, Long::sum);
        }

        /**
         * 留存差异样本, 每种状态最多 {@link StreamingCheckProcessor#SAMPLE_LIMIT} 条
         *
         * @param checkUnit 对账单元
         */
        private void addSample(CheckUnit checkUnit) {
            List<CheckUnit> stateSamples = samples.get(checkUnit.getState());
            if (stateSamples.size() < SAMPLE_LIMIT) {
                stateSamples.add(checkUnit);
            }
        }

        /**
         * 转为摘要
         *
         * @return 对账摘要
         */
        private CheckSummary toSummary() {
            List<CheckUnit> allSamples = new ArrayList<>();
            for (List<CheckUnit> stateSamples : samples.values()) {
                allSamples.addAll(stateSamples);
            }
            return CheckSummary.builder()
                    .sourceTotal(sourceTotal)
                    .targetTotal(targetTotal)
                    .comparedCount(comparedCount)
                    .stateCounts(stateCounts)
                    .syncCounts(syncCounts)
                    .samples(allSamples)
                    .build();
        }
    }
}
