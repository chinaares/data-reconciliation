package io.github.kugin.reconciliation.support;

import io.github.kugin.reconciliation.after.CheckSync;
import io.github.kugin.reconciliation.domain.CheckUnit;
import io.github.kugin.reconciliation.enums.CheckSyncEnum;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 记录调账回调的测试辅助实现
 * <p>
 * 三个回调各自把入参记录下来, 并返回可配置的调账结果
 *
 * @author Kugin
 */
public class RecordingCheckSync implements CheckSync {

    /**
     * 被记录的调账单元, 顺序即回调顺序
     */
    private final List<CheckUnit> recorded = new CopyOnWriteArrayList<>();

    /**
     * 回调返回值
     */
    private CheckSyncEnum result = CheckSyncEnum.SYNC;

    @Override
    public CheckSyncEnum syncSourceMore(CheckUnit checkUnit) {
        recorded.add(checkUnit);
        return result;
    }

    @Override
    public CheckSyncEnum syncTargetMore(CheckUnit checkUnit) {
        recorded.add(checkUnit);
        return result;
    }

    @Override
    public CheckSyncEnum syncDifferent(CheckUnit checkUnit) {
        recorded.add(checkUnit);
        return result;
    }

    /**
     * 被记录的调账单元
     *
     * @return 调账单元列表
     */
    public List<CheckUnit> getRecorded() {
        return recorded;
    }

    /**
     * 设置回调返回值
     *
     * @param result 调账结果
     */
    public void setResult(CheckSyncEnum result) {
        this.result = result;
    }
}
