package io.github.kugin.reconciliation.check;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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

    /**
     * 跨类型数值相等: 包装类型不同但数值相同, 应判为无差异
     */
    @Test
    void crossTypeInteger_compare_numerically() {
        Map<String, Object> s = Map.of("amount", 100L);
        Map<String, Object> t = Map.of("amount", 100);
        assertTrue(processor.compareCheckData(s, t).isEmpty());
        // 数值不同则仍应报差异
        Map<String, Object> t2 = Map.of("amount", 101);
        assertEquals(1, processor.compareCheckData(s, t2).size());
    }

    /**
     * BigDecimal scale不同但值相等, 应判为无差异
     */
    @Test
    void bigDecimal_differentScale_sameValue() {
        Map<String, Object> s = Map.of("amount", new BigDecimal("1.50"));
        Map<String, Object> t = Map.of("amount", new BigDecimal("1.5"));
        assertTrue(processor.compareCheckData(s, t).isEmpty());
        // 值不同则仍应报差异
        Map<String, Object> t2 = Map.of("amount", new BigDecimal("1.51"));
        assertEquals(1, processor.compareCheckData(s, t2).size());
    }

    /**
     * Double 与 BigDecimal 混合比较: 走 toString 转换路径, 应判为无差异
     */
    @Test
    void double_and_bigDecimal_compare_numerically() {
        Map<String, Object> s = Map.of("amount", 0.1d);
        Map<String, Object> t = Map.of("amount", new BigDecimal("0.1"));
        assertTrue(processor.compareCheckData(s, t).isEmpty());
        Map<String, Object> t2 = Map.of("amount", new BigDecimal("0.2"));
        assertEquals(1, processor.compareCheckData(s, t2).size());
    }

    /**
     * Float 与 BigDecimal 混合比较: 走 toString 转换路径, 应判为无差异
     */
    @Test
    void float_and_bigDecimal_compare_numerically() {
        Map<String, Object> s = Map.of("amount", 1.5f);
        Map<String, Object> t = Map.of("amount", new BigDecimal("1.5"));
        assertTrue(processor.compareCheckData(s, t).isEmpty());
    }

    /**
     * NaN 无法比较, 必须显式判为差异, 不能静默判等
     */
    @Test
    void nan_is_reported_as_difference() {
        Map<String, Object> s = Map.of("amount", Double.NaN);
        Map<String, Object> t = Map.of("amount", Double.NaN);
        assertEquals(1, processor.compareCheckData(s, t).size());
        // NaN 与 Float.NaN 跨类型同样判为差异
        Map<String, Object> t2 = Map.of("amount", Float.NaN);
        assertEquals(1, processor.compareCheckData(s, t2).size());
    }

    /**
     * Infinity 无法比较, 必须显式判为差异
     */
    @Test
    void infinity_is_reported_as_difference() {
        Map<String, Object> s = Map.of("amount", Double.POSITIVE_INFINITY);
        Map<String, Object> t = Map.of("amount", Double.POSITIVE_INFINITY);
        assertEquals(1, processor.compareCheckData(s, t).size());
        Map<String, Object> t2 = Map.of("amount", Float.NEGATIVE_INFINITY);
        assertEquals(1, processor.compareCheckData(s, t2).size());
    }

    /**
     * null 值处理: 双 null 相等, 单 null 为差异
     */
    @Test
    void null_values_handling() {
        Map<String, Object> s = new HashMap<>();
        s.put("amount", null);
        Map<String, Object> t = new HashMap<>();
        t.put("amount", null);
        assertTrue(processor.compareCheckData(s, t).isEmpty());
        Map<String, Object> t2 = Map.of("amount", new BigDecimal("1"));
        assertEquals(1, processor.compareCheckData(s, t2).size());
        Map<String, Object> s2 = Map.of("amount", new BigDecimal("1"));
        assertEquals(1, processor.compareCheckData(s2, t).size());
    }

    /**
     * 容差分支: 差值在容差内视为等价, 超出容差判为差异
     */
    @Test
    void tolerance_branch() {
        BigDecimal tolerance = new BigDecimal("0.02");
        // 差值 0.01 <= 0.02, 等价
        assertTrue(AbstractCheckProcessor.equivalent(new BigDecimal("100.00"), new BigDecimal("100.01"), tolerance));
        // 差值 0.05 > 0.02, 不等价
        assertFalse(AbstractCheckProcessor.equivalent(new BigDecimal("100.00"), new BigDecimal("100.05"), tolerance));
        // 容差为 0 时等价于精确比较
        assertTrue(AbstractCheckProcessor.equivalent(new BigDecimal("10.0"), new BigDecimal("10.00"), BigDecimal.ZERO));
        assertFalse(AbstractCheckProcessor.equivalent(new BigDecimal("10.0"), new BigDecimal("10.01"), BigDecimal.ZERO));
        // 容差分支同样适用于跨类型数值
        assertTrue(AbstractCheckProcessor.equivalent(100L, 100.01d, tolerance));
    }

    /**
     * equivalent 的基础契约: null 与非数值类型
     */
    @Test
    void equivalent_contract() {
        assertTrue(AbstractCheckProcessor.equivalent(null, null, null));
        assertFalse(AbstractCheckProcessor.equivalent(null, 1, null));
        assertFalse(AbstractCheckProcessor.equivalent(1, null, null));
        assertTrue(AbstractCheckProcessor.equivalent("SUCCESS", "SUCCESS", null));
        assertFalse(AbstractCheckProcessor.equivalent("SUCCESS", "FAIL", null));
        // 数值与非数值不做隐式转换
        assertFalse(AbstractCheckProcessor.equivalent(new BigDecimal("1"), "1", null));
    }
}
