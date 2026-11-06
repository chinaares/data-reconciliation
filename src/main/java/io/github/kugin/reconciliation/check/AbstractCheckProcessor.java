package io.github.kugin.reconciliation.check;


import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import io.github.kugin.reconciliation.domain.*;
import io.github.kugin.reconciliation.enums.CheckStateEnum;
import io.github.kugin.reconciliation.enums.CheckSyncEnum;
import io.github.kugin.reconciliation.enums.ExecutorStatusEnum;
import io.github.kugin.reconciliation.executor.ExecutorManager;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Kugin
 */
public abstract class AbstractCheckProcessor implements CheckProcessor {

    private static final Log log = LogFactory.get();

    public void compareCheck(CheckContext context) {
        List<CheckEntry> source = context.getSource();
        List<CheckEntry> target = context.getTarget();
        //都为空时,继续走
        if (source.isEmpty() && target.isEmpty()){
            return;
        }
        //单个为空时,认为数据没准备好
        if (source.isEmpty() || target.isEmpty()) {
            throw new CheckException("对账数据未准备好");
        }
    }

    public abstract void doCompare(CheckContext context);

    @Override
    public void compare(CheckContext context) {
        ExecutorManager executorManager = context.getExecutorManager();
        executorManager.setStatus(ExecutorStatusEnum.CHECK);

        compareCheck(context);
        doCompare(context);
    }

    /**
     * 比较两个Map数据的不同
     *
     * @param sourceMap 源数据
     * @param targetMap 目标数据
     * @return
     */
    protected List<CheckUnit> compareMap(Map<String, CheckEntry> sourceMap, Map<String, CheckEntry> targetMap) {
        List<CheckUnit> unitDetails = new ArrayList<>();
        sourceMap.forEach((k, v) -> {
            if (!targetMap.containsKey(k)) {
                unitDetails.add(CheckUnit.builder()
                        .key(k)
                        .state(CheckStateEnum.SOURCE_MORE)
                        .sync(CheckSyncEnum.NO_SYNC)
                        .differentFields(createSingle(v.getCheckData(), true))
                        .build());
                return;
            }
            Map<String, FieldkUnit> fieldkUnitMap = compareCheckData(v.getCheckData(), targetMap.get(k).getCheckData());
            if (fieldkUnitMap == null || fieldkUnitMap.isEmpty()) {
                return;
            }
            unitDetails.add(CheckUnit.builder()
                    .key(k)
                    .state(CheckStateEnum.DIFFERENT)
                    .sync(CheckSyncEnum.NO_SYNC)
                    .differentFields(fieldkUnitMap)
                    .build());
        });
        targetMap.forEach((k, v) -> {
            if (!sourceMap.containsKey(k)) {
                unitDetails.add(CheckUnit.builder()
                        .key(k)
                        .state(CheckStateEnum.TARGET_MORE)
                        .sync(CheckSyncEnum.NO_SYNC)
                        .differentFields(createSingle(v.getCheckData(), false))
                        .build());
            }

        });
        return unitDetails;
    }

    /**
     * 比较两个 checkData 的详细属性不同
     *
     * @param sourceData 来源 checkData
     * @param targetData 目标 checkData
     * @return
     */
    public Map<String, FieldkUnit> compareCheckData(Map<String, Object> sourceData, Map<String, Object> targetData) {
        Map<String, FieldkUnit> differnt = new HashMap<>();
        sourceData.forEach((k, v) -> {
            if (targetData.containsKey(k) && equivalent(v, targetData.get(k), null)) {
                return;
            }
            differnt.put(k, FieldkUnit.builder().fieldName(k).source(v).target(targetData.get(k)).build());
        });
        targetData.forEach((k, v) -> {
            if (sourceData.containsKey(k)) {
                return;
            }
            differnt.put(k, FieldkUnit.builder().fieldName(k).target(v).build());
        });
        return differnt;
    }

    /**
     * 判断两个字段值是否等价
     * <p>
     * 数值类型(Number)按数值比较, 与scale无关, 例如 BigDecimal("10.0") 与 BigDecimal("10.00") 视为相等;
     * NaN/Infinity 等无法比较的数值一律判为差异, 避免把非法金额静默当成一致;
     * 其余类型使用 {@link Object#equals(Object)} 比较.
     *
     * @param a        源值
     * @param b        目标值
     * @param tolerance 数值容差, 为null时要求数值完全相等
     * @return 是否等价
     */
    protected static boolean equivalent(Object a, Object b, BigDecimal tolerance) {
        if (a == null && b == null) {
            return true;
        }
        if (a == null || b == null) {
            return false;
        }
        if (a instanceof Number && b instanceof Number) {
            if (isNotFinite((Number) a) || isNotFinite((Number) b)) {
                //NaN/Infinity 无法比较, 显式判为差异, 不静默判等
                log.warn("数值无法比较(NaN/Infinity), 判定为差异: source={}, target={}", a, b);
                return false;
            }
            BigDecimal left = toBigDecimal((Number) a);
            BigDecimal right = toBigDecimal((Number) b);
            if (left == null || right == null) {
                log.warn("数值无法转换为BigDecimal, 判定为差异: source={}, target={}", a, b);
                return false;
            }
            if (tolerance == null) {
                return left.compareTo(right) == 0;
            }
            return left.subtract(right).abs().compareTo(tolerance.abs()) <= 0;
        }
        return a.equals(b);
    }

    /**
     * 判断数值是否为NaN或Infinity
     *
     * @param number 数值
     * @return 是否为NaN或Infinity
     */
    private static boolean isNotFinite(Number number) {
        if (number instanceof Double d) {
            return Double.isNaN(d) || Double.isInfinite(d);
        }
        if (number instanceof Float f) {
            return Float.isNaN(f) || Float.isInfinite(f);
        }
        return false;
    }

    /**
     * 将Number转换为BigDecimal, 无法转换时返回null
     *
     * @param number 数值
     * @return BigDecimal, 不可转换时为null
     */
    private static BigDecimal toBigDecimal(Number number) {
        if (number instanceof BigDecimal bigDecimal) {
            return bigDecimal;
        }
        if (number instanceof BigInteger bigInteger) {
            return new BigDecimal(bigInteger);
        }
        if (number instanceof Byte || number instanceof Short || number instanceof Integer
                || number instanceof Long || number instanceof AtomicInteger || number instanceof AtomicLong) {
            return BigDecimal.valueOf(number.longValue());
        }
        try {
            return new BigDecimal(number.toString());
        } catch (NumberFormatException e) {
            //意料之外的兜底: NaN/Infinity 已在 isNotFinite 中提前拦截
            log.warn("数值无法转换为BigDecimal: {}", number);
            log.warn(e);
            return null;
        }
    }

    /**
     * source_more 与 target_more 的情况下,将checkData映射为fieldkUnitMap 放入checkUnit
     * 方便使用拿到原始值, 方判断是否需要处理
     *
     * @param checkData 原始值
     * @param isSource  数据来源: true:源, false:目标
     * @return
     */
    public Map<String, FieldkUnit> createSingle(Map<String, Object> checkData, boolean isSource) {
        Map<String, FieldkUnit> differnt = new HashMap<>();
        checkData.forEach((k, v) -> {
            if (isSource) {
                differnt.put(k, FieldkUnit.builder().fieldName(k).source(v).build());
                return;
            }
            differnt.put(k, FieldkUnit.builder().fieldName(k).target(v).build());
        });
        return differnt;
    }
}
