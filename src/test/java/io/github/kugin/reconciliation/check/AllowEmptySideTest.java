package io.github.kugin.reconciliation.check;

import io.github.kugin.reconciliation.domain.CheckConfig;
import io.github.kugin.reconciliation.domain.CheckContext;
import io.github.kugin.reconciliation.domain.CheckEntry;
import io.github.kugin.reconciliation.domain.CheckException;
import io.github.kugin.reconciliation.enums.CheckStateEnum;
import io.github.kugin.reconciliation.executor.CheckExecutor;
import io.github.kugin.reconciliation.executor.DefaultExecutorManager;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AllowEmptySideTest {
    private static CheckContext context(boolean srcEmpty, boolean tgtEmpty) {
        List<CheckEntry> src = srcEmpty ? List.of() : List.of(new CheckEntry("a", null, Map.of("amount", 100L)));
        List<CheckEntry> tgt = tgtEmpty ? List.of() : List.of(new CheckEntry("a", null, Map.of("amount", 100L)));
        return CheckContext.builder().source(src).target(tgt)
                .sourceMap(src.stream().collect(Collectors.toMap(CheckEntry::getKey, e -> e)))
                .targetMap(tgt.stream().collect(Collectors.toMap(CheckEntry::getKey, e -> e)))
                .executorManager(new DefaultExecutorManager("allow-empty-side"))
                .build();
    }

    @Test
    void allowEmptySide_true_produces_diff_not_throw() {
        CheckContext c = context(true, false);
        DefaultCheckProcessor p = new DefaultCheckProcessor(null, true);
        assertDoesNotThrow(() -> p.compare(c));
        assertNotNull(c.getCheckResult());
        assertEquals(1, c.getCheckResult().getDiffDetails().size());
        assertEquals(CheckStateEnum.TARGET_MORE, c.getCheckResult().getDiffDetails().get(0).getState());
    }

    @Test
    void allowEmptySide_false_throws() {
        DefaultCheckProcessor p = new DefaultCheckProcessor(null, false);
        assertThrows(CheckException.class, () -> p.compare(context(true, false)));
    }

    @Test
    void both_empty_returns_early() {
        DefaultCheckProcessor p = new DefaultCheckProcessor(null, false);
        assertDoesNotThrow(() -> p.compare(context(true, true)));
    }

    /**
     * 源侧为空与目标侧为空是对称的, 两个方向都要覆盖
     */
    @Test
    void allowEmptySide_true_produces_source_more_when_source_has_data() {
        CheckContext c = context(false, true);
        DefaultCheckProcessor p = new DefaultCheckProcessor(null, true);
        assertDoesNotThrow(() -> p.compare(c));
        assertNotNull(c.getCheckResult());
        assertEquals(1, c.getCheckResult().getDiffDetails().size());
        assertEquals(CheckStateEnum.SOURCE_MORE, c.getCheckResult().getDiffDetails().get(0).getState());
    }

    /**
     * 单参构造保留向后兼容: 未显式开启时行为与改造前一致(抛异常)
     */
    @Test
    void single_argument_constructor_keeps_throwing() {
        DefaultCheckProcessor p = new DefaultCheckProcessor(null);
        assertThrows(CheckException.class, () -> p.compare(context(true, false)));
    }

    /**
     * 默认值为 false, 保持向后兼容
     */
    @Test
    void check_config_defaults_to_false() {
        assertFalse(CheckConfig.builder().id("allow-empty-side-default").build().isAllowEmptySide());
    }

    /**
     * CheckConfig 承载该开关, 并透传到执行器组装出的对账处理器
     */
    @Test
    void check_config_carries_allow_empty_side_into_executor() {
        CheckConfig config = CheckConfig.builder()
                .id("allow-empty-side-config")
                .allowEmptySide(true)
                .build();
        assertTrue(config.isAllowEmptySide());

        CheckExecutor executor = CheckExecutor.buildExecutor(config);
        assertTrue(executor.getCheckProcessor() instanceof DefaultCheckProcessor);

        CheckContext c = context(true, false);
        assertDoesNotThrow(() -> executor.getCheckProcessor().compare(c));
        assertNotNull(c.getCheckResult());
        assertEquals(1, c.getCheckResult().getDiffDetails().size());
        assertEquals(CheckStateEnum.TARGET_MORE, c.getCheckResult().getDiffDetails().get(0).getState());
    }

    /**
     * 未开启时, 执行器组装出的处理器保持抛异常行为
     */
    @Test
    void executor_keeps_throwing_when_not_enabled() {
        CheckExecutor executor = CheckExecutor.buildExecutor(CheckConfig.builder().id("allow-empty-side-off").build());
        assertThrows(CheckException.class, () -> executor.getCheckProcessor().compare(context(true, false)));
    }
}
