package io.github.kugin.reconciliation.after;

import io.github.kugin.reconciliation.domain.CheckContext;
import io.github.kugin.reconciliation.domain.CheckResult;
import io.github.kugin.reconciliation.domain.CheckSummary;
import io.github.kugin.reconciliation.domain.CheckUnit;
import io.github.kugin.reconciliation.enums.CheckSyncEnum;

import java.util.Objects;

/**
 * 流式后置处理器: 只做状态流转与后置回调, 不再触发调账
 * <p>
 * 与全量模式({@link DefaultAfterCheckProcessor})的差别:
 * <ol>
 *     <li>调账已在比对阶段完成, 这里的自动调账为空实现;</li>
 *     <li>是否通过由摘要(summary)中的未调账计数判定, 而不是比对结果。</li>
 * </ol>
 * <p>
 * 状态语义: 前置校验未通过时摘要为 null, 判定为未通过, 状态停在 ERROR
 * (MEMORY 模式此时状态停在 BEFORE, 两者不对齐是已知且有意的)
 *
 * @author Kugin
 */
public class StreamingAfterCheckProcessor extends AbstractAfterCheckProcessor {

    /**
     * 差错处理, 可为 null
     */
    private final CheckSync checkSync;

    /**
     * 后置处理, 可为 null
     */
    private final CheckAfter checkAfter;

    public StreamingAfterCheckProcessor(CheckSync checkSync, CheckAfter checkAfter) {
        this.checkSync = checkSync;
        this.checkAfter = checkAfter;
    }

    /**
     * 自动调账: 空实现
     * <p>
     * 流式模式下 sync 已在 CHECK 阶段完成, 此处不再重复调账
     *
     * @param checkResult 对账结果, 流式模式下为 null, 本实现不使用该参数
     */
    @Override
    public void autoSync(CheckResult checkResult) {
        //do-nothing: sync 已在 CHECK 阶段完成
    }

    @Override
    public void doAfter(CheckContext context) {
        if (Objects.nonNull(checkAfter)) {
            checkAfter.doAfter(context);
        }
    }

    /**
     * 对账是否通过
     * <p>
     * 配置了 {@link CheckSync} 时必须委托给它: 其默认实现读摘要, 用户自定义覆写也依然生效
     *
     * @param context 上下文
     * @return 对账是否通过
     */
    @Override
    public boolean isComplete(CheckContext context) {
        if (Objects.nonNull(checkSync)) {
            return checkSync.isComplete(context);
        }
        //未配置 CheckSync: 由摘要判定, 摘要为 null(前置校验未通过)时视为未通过
        CheckSummary summary = context.getSummary();
        return summary != null
                && summary.getSyncCounts().getOrDefault(CheckSyncEnum.NO_SYNC, 0L) == 0;
    }

    /**
     * 流式模式下这三个方法不会被调用(autoSync 已是空操作), 保留实现只为满足 {@link CheckSync} 契约
     *
     * @param checkUnit 对账单元
     * @return 调账结果
     */
    @Override
    public CheckSyncEnum syncSourceMore(CheckUnit checkUnit) {
        if (Objects.isNull(checkSync)) {
            return CheckSyncEnum.SYNC;
        }
        return checkSync.syncSourceMore(checkUnit);
    }

    /**
     * 流式模式下这三个方法不会被调用(autoSync 已是空操作), 保留实现只为满足 {@link CheckSync} 契约
     *
     * @param checkUnit 对账单元
     * @return 调账结果
     */
    @Override
    public CheckSyncEnum syncTargetMore(CheckUnit checkUnit) {
        if (Objects.isNull(checkSync)) {
            return CheckSyncEnum.SYNC;
        }
        return checkSync.syncTargetMore(checkUnit);
    }

    /**
     * 流式模式下这三个方法不会被调用(autoSync 已是空操作), 保留实现只为满足 {@link CheckSync} 契约
     *
     * @param checkUnit 对账单元
     * @return 调账结果
     */
    @Override
    public CheckSyncEnum syncDifferent(CheckUnit checkUnit) {
        if (Objects.isNull(checkSync)) {
            return CheckSyncEnum.SYNC;
        }
        return checkSync.syncDifferent(checkUnit);
    }
}
