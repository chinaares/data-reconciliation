package io.github.kugin.reconciliation.domain;

import io.github.kugin.reconciliation.enums.CheckSyncEnum;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CheckSummaryTest {

    @Test
    void emptySummaryHasAllCountKeysAndNoSamples() {
        CheckSummary summary = CheckSummary.empty();

        assertEquals(0L, summary.getSourceTotal());
        assertEquals(0L, summary.getTargetTotal());
        assertEquals(0L, summary.getComparedCount());
        assertEquals(3, summary.getStateCounts().size());
        assertEquals(3, summary.getSyncCounts().size());
        assertEquals(0L, summary.getSyncCounts().get(CheckSyncEnum.NO_SYNC));
        assertNotNull(summary.getSamples());
        assertTrue(summary.getSamples().isEmpty());
    }

    @Test
    void builderNormalizesMissingCountKeysToZero() {
        CheckSummary summary = CheckSummary.builder()
                .syncCounts(new HashMap<>(Map.of(CheckSyncEnum.SYNC, 3L)))
                .build();

        assertEquals(3, summary.getSyncCounts().size());
        assertEquals(0L, summary.getSyncCounts().get(CheckSyncEnum.NO_SYNC));
        assertEquals(0L, summary.getSyncCounts().get(CheckSyncEnum.IGNORE));
        assertEquals(3L, summary.getSyncCounts().get(CheckSyncEnum.SYNC));
        assertNotNull(summary.getStateCounts());
        assertNotNull(summary.getSamples());
    }

    @Test
    void countsAreCopiedAndNotModifiableByCaller() {
        Map<CheckSyncEnum, Long> source = new HashMap<>();
        source.put(CheckSyncEnum.SYNC, 1L);

        CheckSummary summary = CheckSummary.builder().syncCounts(source).build();
        source.put(CheckSyncEnum.SYNC, 99L);

        assertEquals(1L, summary.getSyncCounts().get(CheckSyncEnum.SYNC));
        assertThrows(UnsupportedOperationException.class,
                () -> summary.getSyncCounts().put(CheckSyncEnum.SYNC, 99L));
        assertThrows(UnsupportedOperationException.class,
                () -> summary.getSamples().add(CheckUnit.builder().key("k").build()));
    }
}
