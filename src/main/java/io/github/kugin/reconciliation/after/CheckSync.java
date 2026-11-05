package io.github.kugin.reconciliation.after;

import io.github.kugin.reconciliation.domain.CheckContext;
import io.github.kugin.reconciliation.domain.CheckResult;
import io.github.kugin.reconciliation.domain.CheckSummary;
import io.github.kugin.reconciliation.domain.CheckUnit;
import io.github.kugin.reconciliation.enums.CheckSyncEnum;

import java.util.Objects;

/**
 * 差错处理
 *
 * @author Kugin
 */
public interface CheckSync {

    /**
     * 同步处理源数据多的情况
     *
     * @param checkUnit 对账单元
     */
    CheckSyncEnum syncSourceMore(CheckUnit checkUnit);

    /**
     * 同步处理目标数据多的情况
     *
     * @param checkUnit 对账单元
     */
    CheckSyncEnum syncTargetMore(CheckUnit checkUnit);

    /**
     * 同步处理源数据与目标数据有差异的情况
     *
     * @param checkUnit 对账单元
     */
    CheckSyncEnum syncDifferent(CheckUnit checkUnit);

    /**
     * 对账是否通过
     * <p>
     * 此处入参不是checkResult,是为了解决,对比过程中,没有不同,但实际状态又不能认为成功的情况:如支付中 两边都是pay_wait的情况,对比不出来,但又不能视为通过
     * <p>
     * 流式模式下上下文携带摘要, 以摘要中的未调账计数判定; 全量模式下仍以比对结果判定
     *
     * @param context 对账上下文
     * @return 对账是否通过
     */
    default boolean isComplete(CheckContext context) {
        // 流式模式: 读摘要中的 NO_SYNC 计数
        CheckSummary summary = context.getSummary();
        if (summary != null) {
            return summary.getSyncCounts().getOrDefault(CheckSyncEnum.NO_SYNC, 0L) == 0;
        }
        // 全量模式: 原有逻辑, 逐字保留
        CheckResult checkResult = context.getCheckResult();
        if (checkResult == null) {
            return false;
        }
        return checkResult.getDiffDetails().stream().noneMatch(v -> Objects.equals(CheckSyncEnum.NO_SYNC, v.getSync()));
    }
}
