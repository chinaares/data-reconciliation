package io.github.kugin.reconciliation.executor;

import cn.hutool.core.date.StopWatch;
import cn.hutool.core.lang.Pair;
import cn.hutool.core.map.MapUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import io.github.kugin.reconciliation.after.AfterCheckProcessor;
import io.github.kugin.reconciliation.after.DefaultAfterCheckProcessor;
import io.github.kugin.reconciliation.after.StreamingAfterCheckProcessor;
import io.github.kugin.reconciliation.before.BeforeCheckProcessor;
import io.github.kugin.reconciliation.before.DefaultBeforeCheckProcessor;
import io.github.kugin.reconciliation.before.DefaultResourceReader;
import io.github.kugin.reconciliation.before.OrderedResourceLoader;
import io.github.kugin.reconciliation.before.ResourceReader;
import io.github.kugin.reconciliation.before.StreamingBeforeCheckProcessor;
import io.github.kugin.reconciliation.check.CheckProcessor;
import io.github.kugin.reconciliation.check.DefaultCheckProcessor;
import io.github.kugin.reconciliation.check.StreamingCheckProcessor;
import io.github.kugin.reconciliation.domain.CheckConfig;
import io.github.kugin.reconciliation.domain.CheckContext;
import io.github.kugin.reconciliation.domain.CheckResult;
import io.github.kugin.reconciliation.domain.CheckSummary;
import io.github.kugin.reconciliation.enums.CheckMode;
import io.github.kugin.reconciliation.enums.CheckStateEnum;
import lombok.Builder;
import lombok.Getter;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * 对账执行器
 *
 * @author Kugin
 */
@Builder
@Getter
public class CheckExecutor {

    private static final Log log = LogFactory.get();

    private final String id;

    private final BeforeCheckProcessor beforeCheckProcessor;

    private final CheckProcessor checkProcessor;

    private final AfterCheckProcessor afterCheckProcessor;

    private final CheckConfig checkConfig;

    private final ExecutorManager executorManager;

    /**
     * 创建执行器
     * <p>
     * 按 {@link CheckConfig#getMode()} 分流:
     * <ul>
     *     <li>MEMORY(默认): 全量加载后比对, 产出比对结果;</li>
     *     <li>STREAM: 两侧有序数据流做 merge join, 只产出摘要, 调账在比对阶段即触发。</li>
     * </ul>
     *
     * @param checkConfig 配置参数
     * @return 执行器
     */
    public static CheckExecutor buildExecutor(CheckConfig checkConfig) {
        //mode 为 null 时回落到 MEMORY: @Builder.Default 只在 builder 未显式赋值时生效
        CheckMode mode = checkConfig.getMode() == null ? CheckMode.MEMORY : checkConfig.getMode();
        BeforeCheckProcessor beforeCheckProcessor;
        CheckProcessor checkProcessor;
        AfterCheckProcessor afterCheckProcessor;
        if (mode == CheckMode.STREAM) {
            //流式模式: 两侧有序数据源必须配置, 缺失时在组装期快速失败
            OrderedResourceLoader srcOrderedLoader = Objects.requireNonNull(checkConfig.getSrcOrderedLoader(),
                    "STREAM 模式需要源端 OrderedResourceLoader");
            OrderedResourceLoader targetOrderedLoader = Objects.requireNonNull(checkConfig.getTargetOrderedLoader(),
                    "STREAM 模式需要目标端 OrderedResourceLoader");

            //前置处理器组装
            beforeCheckProcessor = new StreamingBeforeCheckProcessor(checkConfig.getCheckPre());

            //对账处理器组装
            checkProcessor = Optional.ofNullable(checkConfig.getCheckProcessor())
                    .orElseGet(() -> new StreamingCheckProcessor(srcOrderedLoader, targetOrderedLoader, checkConfig.getCheckSync()));

            //后置处理器组装
            afterCheckProcessor = new StreamingAfterCheckProcessor(checkConfig.getCheckSync(), checkConfig.getCheckAfter());
        } else {
            //前置处理器组装
            ResourceReader resourceReader = Optional.ofNullable(checkConfig.getResourceReader())
                    .orElse(new DefaultResourceReader(checkConfig.getSrcLoader(), checkConfig.getTargetLoader()));
            beforeCheckProcessor = new DefaultBeforeCheckProcessor(resourceReader, checkConfig.getCheckPre());

            //对账处理器组装
            checkProcessor = Optional.ofNullable(checkConfig.getCheckProcessor()).orElse(new DefaultCheckProcessor());

            //后置处理器组装
            afterCheckProcessor = new DefaultAfterCheckProcessor(checkConfig.getCheckSync(), checkConfig.getCheckAfter());
        }

        //对账管理器组装
        String id = Optional.ofNullable(checkConfig.getId()).orElse(checkConfig.getName());
        ExecutorManager executorManager = new DefaultExecutorManager(id);
        return CheckExecutor.builder()
                .id(id)
                .beforeCheckProcessor(beforeCheckProcessor)
                .checkProcessor(checkProcessor)
                .afterCheckProcessor(afterCheckProcessor)
                .checkConfig(checkConfig)
                .executorManager(executorManager)
                .build();
    }

    /**
     * 执行器业务逻辑
     */
    public CheckContext process(String date) {
        executorManager.initDate(date);
        CheckContext checkContext = CheckContext.builder()
                .id(this.id)
                .name(checkConfig.getName())
                .date(date)
                .executorManager(executorManager)
                .build();
        log.info("对账开始:", executorManager.getExecutorKey());
        StopWatch stopWatch = new StopWatch(id);
        checkContext.setStopWatch(stopWatch);
        try {
            if (executorManager.isProcessing()) {
                //任务正在执行中
                log.info("当前任务正在执行,本次执行忽略", executorManager.getExecutorKey());
                return checkContext;
            }
            stopWatch.start(beforeCheckProcessor.getClass().getName());

            //todo 细节日志补充.
            beforeCheckProcessor.processBeforeCompare(checkContext);

            stopWatch.stop();
            stopWatch.start(checkProcessor.getClass().getName());

            checkProcessor.compare(checkContext);

            CheckResult result = checkContext.getCheckResult();
            CheckSummary summary = checkContext.getSummary();
            if (summary != null) {
                //流式模式: 只有摘要, 不触碰全量模式的结果与数据
                //Hutool 的 Log.info(format, args) 走 StrUtil.format, 格式串没有 {} 占位符时参数会被丢弃, 故此处必须带占位符
                log.info("对账结果:{} {}", executorManager.getExecutorKey(), MapUtil.of(
                        Pair.of("sourceTotal", summary.getSourceTotal()),
                        Pair.of("targetTotal", summary.getTargetTotal()),
                        Pair.of("compared", summary.getComparedCount()),
                        Pair.of("stateCounts", summary.getStateCounts()),
                        Pair.of("syncCounts", summary.getSyncCounts())));
            } else if (result != null) {
                //全量模式: 现有日志逻辑原样
                Map<CheckStateEnum, Integer> map = result.getDiffDetailsMap().entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().size()));
                log.info("对账结果:", executorManager.getExecutorKey(), MapUtil.of(Pair.of("sourceSize", checkContext.getSource().size()),
                        Pair.of("targetSize", checkContext.getTarget().size()),
                        Pair.of("different", result.getDiffDetails().size()),
                        Pair.of("details", map)));
            } else {
                //前置校验未通过, 无结果
                log.info("对账未执行: 前置校验未通过:{}", executorManager.getExecutorKey());
            }
            stopWatch.stop();
            stopWatch.start(afterCheckProcessor.getClass().getName());

            afterCheckProcessor.processAfterCompare(checkContext);
            stopWatch.stop();
        } catch (Exception e) {
            log.warn("对账异常结束", executorManager.getExecutorKey() + ":" + executorManager.getCurrentStatus(), e);
            executorManager.pauseByError();
        }
        log.info(stopWatch.prettyPrint());
        log.info("对账结束:", executorManager.getExecutorKey());
        return checkContext;
    }
}
