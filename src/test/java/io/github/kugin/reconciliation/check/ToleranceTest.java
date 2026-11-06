package io.github.kugin.reconciliation.check;

import io.github.kugin.reconciliation.domain.CheckConfig;
import io.github.kugin.reconciliation.domain.FieldkUnit;
import io.github.kugin.reconciliation.executor.CheckExecutor;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ToleranceTest {
    private final AbstractCheckProcessor p = new DefaultCheckProcessor(Map.of("amount", new BigDecimal("2")));

    @Test
    void within_tolerance_is_equal() {
        assertTrue(p.compareCheckData(Map.of("amount", 100L), Map.of("amount", 101L)).isEmpty());
    }

    @Test
    void boundary_equal_to_tolerance_is_equal() {
        assertTrue(p.compareCheckData(Map.of("amount", 100L), Map.of("amount", 102L)).isEmpty());
    }

    @Test
    void beyond_tolerance_is_diff() {
        assertEquals(1, p.compareCheckData(Map.of("amount", 100L), Map.of("amount", 103L)).size());
    }

    /**
     * 未配置容差时保持精确比较(向后兼容)
     */
    @Test
    void no_tolerance_keeps_exact_compare() {
        AbstractCheckProcessor exact = new DefaultCheckProcessor();
        assertEquals(1, exact.compareCheckData(Map.of("amount", 100L), Map.of("amount", 102L)).size());
    }

    /**
     * 容差只作用于配置过的字段, 其余字段仍精确比较
     */
    @Test
    void tolerance_only_applies_to_configured_field() {
        Map<String, Object> source = Map.of("amount", 100L, "status", "SUCCESS");
        Map<String, Object> target = Map.of("amount", 102L, "status", "FAILED");
        Map<String, FieldkUnit> diff = p.compareCheckData(source, target);
        assertEquals(1, diff.size());
        assertTrue(diff.containsKey("status"));
    }

    /**
     * CheckConfig 承载容差配置, 并透传到执行器组装出的对账处理器
     */
    @Test
    void check_config_carries_tolerances_into_executor() {
        CheckConfig config = CheckConfig.builder()
                .id("tolerance-test")
                .tolerances(Map.of("amount", new BigDecimal("2")))
                .build();
        assertEquals(new BigDecimal("2"), config.getTolerances().get("amount"));

        CheckExecutor executor = CheckExecutor.buildExecutor(config);
        assertTrue(executor.getCheckProcessor() instanceof DefaultCheckProcessor);
        AbstractCheckProcessor processor = (AbstractCheckProcessor) executor.getCheckProcessor();
        assertTrue(processor.compareCheckData(Map.of("amount", 100L), Map.of("amount", 102L)).isEmpty());
    }
}
