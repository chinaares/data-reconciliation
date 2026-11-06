package io.github.kugin.reconciliation.check;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NumericCompareTest {
    private final AbstractCheckProcessor processor = new DefaultCheckProcessor();

    @Test
    void bigDecimal_scaleIndependent_equals() {
        Map<String, Object> s = Map.of("amount", new BigDecimal("10.0"));
        Map<String, Object> t = Map.of("amount", new BigDecimal("10.00"));
        assertTrue(processor.compareCheckData(s, t).isEmpty());
    }

    @Test
    void long_values_compare_numerically() {
        Map<String, Object> s = Map.of("amount", 100L);
        Map<String, Object> t = Map.of("amount", 100L);
        assertTrue(processor.compareCheckData(s, t).isEmpty());
    }

    @Test
    void nonNumeric_uses_equals() {
        Map<String, Object> s = Map.of("status", "SUCCESS");
        Map<String, Object> t = Map.of("status", "SUCCESS");
        assertTrue(processor.compareCheckData(s, t).isEmpty());
        Map<String, Object> t2 = Map.of("status", "FAIL");
        assertEquals(1, processor.compareCheckData(s, t2).size());
    }
}
