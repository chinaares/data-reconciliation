package io.github.kugin.reconciliation.check;

import io.github.kugin.reconciliation.domain.CheckContext;
import io.github.kugin.reconciliation.domain.CheckEntry;
import io.github.kugin.reconciliation.domain.CheckResult;
import io.github.kugin.reconciliation.domain.CheckUnit;
import io.github.kugin.reconciliation.enums.CheckStateEnum;

import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Kugin
 */
public class DefaultCheckProcessor extends AbstractCheckProcessor {

    /**
     * 精确比较, 不设置任何容差
     */
    public DefaultCheckProcessor() {
        this(null);
    }

    /**
     * 指定数值字段容差
     *
     * @param tolerances 字段名 -> 容差, 可为null(等价于精确比较)
     */
    public DefaultCheckProcessor(Map<String, BigDecimal> tolerances) {
        this(tolerances, false);
    }

    /**
     * 指定数值字段容差与单侧为空时的处理方式
     *
     * @param tolerances    字段名 -> 容差, 可为null(等价于精确比较)
     * @param allowEmptySide true: 单侧为空时记为单边差异; false: 抛异常(默认)
     */
    public DefaultCheckProcessor(Map<String, BigDecimal> tolerances, boolean allowEmptySide) {
        setTolerances(tolerances);
        setAllowEmptySide(allowEmptySide);
    }

    @Override
    public void doCompare(CheckContext context) {
        Map<String, CheckEntry> sourceMap = context.getSourceMap();
        Map<String, CheckEntry> targetMap = context.getTargetMap();
        List<CheckUnit> unitDetails = compareMap(sourceMap, targetMap);
        Map<CheckStateEnum, List<CheckUnit>> unitdetailsMap = unitDetails.stream().collect(Collectors.groupingBy(CheckUnit::getState));
        Map<String, CheckEntry> sourceDiffMap = Optional.ofNullable(unitdetailsMap.get(CheckStateEnum.SOURCE_MORE)).orElse(Collections.emptyList())
                .stream().collect(Collectors.toMap(CheckUnit::getKey, unit -> sourceMap.get(unit.getKey())));
        Map<String, CheckEntry> targetDiffMap = Optional.ofNullable(unitdetailsMap.get(CheckStateEnum.TARGET_MORE)).orElse(Collections.emptyList()).stream()
                .collect(Collectors.toMap(CheckUnit::getKey, unit -> targetMap.get(unit.getKey())));
        CheckResult result = CheckResult.builder()
                .sourceDiffMap(sourceDiffMap)
                .targetDiffMap(targetDiffMap)
                .diffDetails(unitDetails)
                .diffDetailsMap(unitdetailsMap)
                .build();
        context.setCheckResult(result);
    }
}
