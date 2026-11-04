package io.github.kugin.reconciliation.example;

import cn.hutool.core.map.MapUtil;
import io.github.kugin.reconciliation.annotation.CheckField;
import io.github.kugin.reconciliation.annotation.CheckIdentity;
import io.github.kugin.reconciliation.domain.CheckAdapter;
import io.github.kugin.reconciliation.domain.CheckConfig;
import io.github.kugin.reconciliation.domain.CheckContext;
import io.github.kugin.reconciliation.domain.CheckEntry;
import io.github.kugin.reconciliation.domain.CheckResult;
import io.github.kugin.reconciliation.domain.CheckUnit;
import io.github.kugin.reconciliation.domain.FieldkUnit;
import io.github.kugin.reconciliation.enums.CheckStateEnum;
import io.github.kugin.reconciliation.enums.CheckSyncEnum;
import io.github.kugin.reconciliation.enums.ExecutorStatusEnum;
import io.github.kugin.reconciliation.executor.CheckExecutor;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 对账框架最简使用示例
 * <p>
 * 步骤: 声明比对实体 -> 组装 {@link CheckConfig} -> 执行 {@link CheckExecutor} -> 断言差异结果
 *
 * @author Kugin
 */
public class SimpleCheckExampleTest {

    private static final String DATE = "20210204";

    /**
     * 注解方式: {@code @CheckIdentity} 声明唯一标识, {@code @CheckField} 声明待比较字段
     */
    @Data
    @AllArgsConstructor
    static class Order {
        @CheckIdentity
        String orderNo;
        @CheckField(order = 1)
        Double amount;
        @CheckField(order = 2)
        String status;
    }

    /**
     * 适配器方式: 实现 {@link CheckAdapter}, 优先级高于注解
     */
    @Data
    @AllArgsConstructor
    static class OrderAdapter implements CheckAdapter {
        String orderNo;
        Double amount;

        @Override
        public String getKey() {
            return orderNo;
        }

        @Override
        public Map<String, Object> getCheckData() {
            return MapUtil.of("amount", amount);
        }
    }

    /**
     * 调账处理: 示例中全部放行, 视为已调账
     */
    static class SimpleCheckSync implements io.github.kugin.reconciliation.after.CheckSync {
        @Override
        public CheckSyncEnum syncSourceMore(CheckUnit checkUnit) {
            return CheckSyncEnum.SYNC;
        }

        @Override
        public CheckSyncEnum syncTargetMore(CheckUnit checkUnit) {
            return CheckSyncEnum.SYNC;
        }

        @Override
        public CheckSyncEnum syncDifferent(CheckUnit checkUnit) {
            return CheckSyncEnum.SYNC;
        }
    }

    @Test
    public void testAnnotationCheck() {
        //上游: A001 一致, A002 金额不同, A003 仅上游存在
        List<Order> source = List.of(
                new Order("A001", 100d, "PAID"),
                new Order("A002", 200d, "PAID"),
                new Order("A003", 300d, "PAID"));
        //下游: A001 一致, A002 金额不同, A004 仅下游存在
        List<Order> target = List.of(
                new Order("A001", 100d, "PAID"),
                new Order("A002", 250d, "PAID"),
                new Order("A004", 400d, "PAID"));

        CheckConfig config = CheckConfig.builder()
                .id("simple-annotation")
                .name("订单对账示例")
                .srcLoader(date -> CheckEntry.wrap(source))
                .targetLoader(date -> CheckEntry.wrap(target))
                .checkPre(context -> true)
                .checkSync(new SimpleCheckSync())
                .checkAfter(context -> System.out.println("对账结果: " + context.getCheckResult().getDiffDetails()))
                .build();

        CheckExecutor executor = CheckExecutor.buildExecutor(config);
        CheckContext context = executor.process(DATE);

        //执行完成
        assertEquals(ExecutorStatusEnum.END.toString(), executor.getExecutorManager().getCurrentStatus());

        CheckResult result = context.getCheckResult();
        //共 3 条差异: 上游多账 + 存在差异 + 下游多账
        assertEquals(3, result.getDiffDetails().size());
        assertEquals(1, result.getDiffDetailsMap().get(CheckStateEnum.SOURCE_MORE).size());
        assertEquals(1, result.getDiffDetailsMap().get(CheckStateEnum.DIFFERENT).size());
        assertEquals(1, result.getDiffDetailsMap().get(CheckStateEnum.TARGET_MORE).size());

        //单边存在的数据
        assertTrue(result.getSourceDiffMap().containsKey("A003"));
        assertTrue(result.getTargetDiffMap().containsKey("A004"));

        //字段级差异明细
        CheckUnit diff = result.getDiffDetailsMap().get(CheckStateEnum.DIFFERENT).get(0);
        assertEquals("A002", diff.getKey());
        FieldkUnit amount = diff.getDifferentFields().get("amount");
        assertEquals(200d, amount.getSource());
        assertEquals(250d, amount.getTarget());
        //status 两侧一致, 不会出现在差异明细中
        assertNull(diff.getDifferentFields().get("status"));

        //调账后全部同步
        assertTrue(result.getDiffDetails().stream().allMatch(unit -> CheckSyncEnum.SYNC == unit.getSync()));
    }

    @Test
    public void testAdapterCheck() {
        List<OrderAdapter> source = List.of(
                new OrderAdapter("A001", 100d),
                new OrderAdapter("A002", 200d));
        List<OrderAdapter> target = List.of(
                new OrderAdapter("A001", 100d),
                new OrderAdapter("A002", 250d));

        //最小配置: 只需要 id 与上下游数据源
        CheckConfig config = CheckConfig.builder()
                .id("simple-adapter")
                .srcLoader(date -> CheckEntry.wrap(source))
                .targetLoader(date -> CheckEntry.wrap(target))
                .build();

        CheckContext context = CheckExecutor.buildExecutor(config).process(DATE);

        CheckResult result = context.getCheckResult();
        assertEquals(1, result.getDiffDetails().size());

        CheckUnit diff = result.getDiffDetails().get(0);
        assertEquals("A002", diff.getKey());
        assertEquals(CheckStateEnum.DIFFERENT, diff.getState());
        //未配置 checkSync 时, 差异默认标记为已调账
        assertEquals(CheckSyncEnum.SYNC, diff.getSync());
    }
}
