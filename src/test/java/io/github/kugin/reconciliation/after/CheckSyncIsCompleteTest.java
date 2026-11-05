package io.github.kugin.reconciliation.after;

import io.github.kugin.reconciliation.domain.CheckContext;
import io.github.kugin.reconciliation.domain.CheckResult;
import io.github.kugin.reconciliation.domain.CheckSummary;
import io.github.kugin.reconciliation.domain.CheckUnit;
import io.github.kugin.reconciliation.enums.CheckSyncEnum;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CheckSyncIsCompleteTest {

    /** 不覆写 isComplete 的最小实现, 用于调用默认方法 */
    static class PlainSync implements CheckSync {
        @Override public CheckSyncEnum syncSourceMore(CheckUnit u) { return CheckSyncEnum.SYNC; }
        @Override public CheckSyncEnum syncTargetMore(CheckUnit u) { return CheckSyncEnum.SYNC; }
        @Override public CheckSyncEnum syncDifferent(CheckUnit u) { return CheckSyncEnum.SYNC; }
    }

    @Test
    void memoryModeUsesCheckResult() {
        CheckContext context = CheckContext.builder().build();
        context.setCheckResult(CheckResult.builder()
                .diffDetails(List.of(CheckUnit.builder().key("k").sync(CheckSyncEnum.NO_SYNC).build()))
                .build());

        assertFalse(new PlainSync().isComplete(context));
    }

    @Test
    void streamModeUsesSummary() {
        CheckContext context = CheckContext.builder().build();
        context.setSummary(CheckSummary.builder()
                .syncCounts(Map.of(CheckSyncEnum.NO_SYNC, 1L))
                .build());

        assertFalse(new PlainSync().isComplete(context));
    }

    @Test
    void streamModeWithOnlySyncIsComplete() {
        CheckContext context = CheckContext.builder().build();
        context.setSummary(CheckSummary.builder()
                .syncCounts(Map.of(CheckSyncEnum.SYNC, 4L))
                .build());

        assertTrue(new PlainSync().isComplete(context));
    }

    @Test
    void noSummaryAndNoResultIsNotComplete() {
        assertFalse(new PlainSync().isComplete(CheckContext.builder().build()));
    }
}
