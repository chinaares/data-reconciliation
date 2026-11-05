package io.github.kugin.reconciliation.domain;

import io.github.kugin.reconciliation.enums.CheckStateEnum;
import io.github.kugin.reconciliation.enums.CheckSyncEnum;
import lombok.Builder;
import lombok.Data;

import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/**
 * 对账结果摘要
 * <p>
 * 用于承载流式比对过程中的累计统计, 不可变对象
 *
 * @author Kugin
 */
@Data
public class CheckSummary {

    /**
     * 源数据总条数
     */
    private final long sourceTotal;

    /**
     * 目标数据总条数
     */
    private final long targetTotal;

    /**
     * 实际比对条数
     */
    private final long comparedCount;

    /**
     * 各对账状态的计数: 状态枚举的每个常量都有对应的计数
     */
    private final Map<CheckStateEnum, Long> stateCounts;

    /**
     * 各调账状态的计数: 状态枚举的每个常量都有对应的计数
     */
    private final Map<CheckSyncEnum, Long> syncCounts;

    /**
     * 差异样本: 便于排查问题时查看明细
     */
    private final List<CheckUnit> samples;

    /**
     * 构造时补全计数的默认值, 缺失的枚举常量补 0
     * <p>
     * {@link Builder} 标注在构造器上, 保证所有构造路径都经过此处的补全逻辑
     */
    @Builder
    public CheckSummary(long sourceTotal, long targetTotal, long comparedCount,
                        Map<CheckStateEnum, Long> stateCounts,
                        Map<CheckSyncEnum, Long> syncCounts,
                        List<CheckUnit> samples) {
        this.sourceTotal = sourceTotal;
        this.targetTotal = targetTotal;
        this.comparedCount = comparedCount;
        this.stateCounts = Collections.unmodifiableMap(normalize(stateCounts, CheckStateEnum.class));
        this.syncCounts = Collections.unmodifiableMap(normalize(syncCounts, CheckSyncEnum.class));
        this.samples = samples == null ? List.of() : List.copyOf(samples);
    }

    /**
     * 空摘要: 各项计数均为 0
     *
     * @return 空摘要
     */
    public static CheckSummary empty() {
        return CheckSummary.builder().build();
    }

    /**
     * 复制传入的计数并补全缺失的枚举常量
     *
     * @param counts 原始计数
     * @param type   枚举类型
     * @param <E>    枚举类型
     * @return 补全后的计数
     */
    private static <E extends Enum<E>> Map<E, Long> normalize(Map<E, Long> counts, Class<E> type) {
        EnumMap<E, Long> normalized = new EnumMap<>(type);
        for (E constant : type.getEnumConstants()) {
            normalized.put(constant, 0L);
        }
        if (counts != null) {
            counts.forEach((key, value) -> {
                if (key != null && value != null) {
                    normalized.put(key, value);
                }
            });
        }
        return normalized;
    }
}
