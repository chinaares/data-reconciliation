package io.github.kugin.reconciliation.domain;

import io.github.kugin.reconciliation.after.AfterCheckProcessor;
import io.github.kugin.reconciliation.after.CheckAfter;
import io.github.kugin.reconciliation.after.CheckSync;
import io.github.kugin.reconciliation.before.BeforeCheckProcessor;
import io.github.kugin.reconciliation.before.CheckPre;
import io.github.kugin.reconciliation.before.OrderedResourceLoader;
import io.github.kugin.reconciliation.before.ResourceLoader;
import io.github.kugin.reconciliation.before.ResourceReader;
import io.github.kugin.reconciliation.check.CheckProcessor;
import io.github.kugin.reconciliation.enums.CheckMode;
import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;
import java.util.Map;

/**
 * 对账配置
 *
 * @author Kugin
 */
@Data
@Builder
public class CheckConfig {
    /**
     * 对账唯一标识:进度管理以id为主
     * name与id不可同时为空
     */
    private String id;

    /**
     * 对账名称: 名称可能重复
     */
    private String name;

    //前置处理器配置-----------------------------------

    /**
     * 自定义前置处理器:可不依赖于ResourceReader
     */
    private BeforeCheckProcessor beforeCheckProcessor;

    /**
     * 前置校验: 可为空
     */
    private CheckPre checkPre;

    /**
     * 上游数据源
     */
    private ResourceLoader srcLoader;

    /**
     * 下游数据源
     */
    private ResourceLoader targetLoader;

    /**
     * STREAM 模式的上游有序数据源: 仅 STREAM 模式使用
     */
    private OrderedResourceLoader srcOrderedLoader;

    /**
     * STREAM 模式的下游有序数据源: 仅 STREAM 模式使用
     */
    private OrderedResourceLoader targetOrderedLoader;

    /**
     * 数据源读取 (包含上下游)
     */
    private ResourceReader resourceReader;

    //对账处理器配置-----------------------------------

    /**
     * 自定义对账处理器
     */
    private CheckProcessor checkProcessor;

    /**
     * 数值字段容差: 字段名 -> 容差
     * <p>
     * 数值字段的差值绝对值不超过容差时判定为相等; 未配置(为null)或该字段未配容差时按精确比较。
     * <p>
     * <b>键名口径</b>: 必须是 checkData 字段名 / 实体字段名(即参与比较的字段键),
     * <b>不是数据库列名</b>; 键名对不上属于"配了却不生效"的静默失败, 请核对后再上线。
     * <p>
     * <b>生效范围</b>: 对默认的 {@link io.github.kugin.reconciliation.check.DefaultCheckProcessor}(MEMORY 模式)
     * 与 {@link io.github.kugin.reconciliation.check.StreamingCheckProcessor}(STREAM 模式)均生效;
     * 配置了自定义 {@link CheckProcessor} 时本项不生效。
     * <p>
     * <b>单位</b>: 容差单位与被比较字段值的单位一致(如字段单位为"元", 容差即以"元"计)。
     */
    private Map<String, BigDecimal> tolerances;

    /**
     * 单侧数据为空时是否记为单边差异
     * <p>
     * 默认 false: 恰好一侧为空时抛 {@link io.github.kugin.reconciliation.domain.CheckException}("对账数据未准备好"),
     * 认为数据尚未准备好。设为 true 时不抛异常, 改为把非空一侧的全部数据记为单边差异
     * (源侧为空 -> SOURCE_MORE, 目标侧为空 -> TARGET_MORE)。
     * <p>
     * <b>两侧都为空</b>时始终不抛异常, 直接产出空结果。
     * <p>
     * <b>生效范围</b>: 对默认的 {@link io.github.kugin.reconciliation.check.DefaultCheckProcessor}(MEMORY 模式)
     * 与 {@link io.github.kugin.reconciliation.check.StreamingCheckProcessor}(STREAM 模式)均生效;
     * 配置了自定义 {@link CheckProcessor} 时本项不生效。
     */
    @Builder.Default
    private boolean allowEmptySide = false;

    //后置处理器配置-----------------------------------

    /**
     * 自定义后置处理器: 可不依赖于CheckSync
     */
    private AfterCheckProcessor afterCheckProcessor;

    /**
     * 对账结果同步处理器: 可为空
     */
    private CheckSync checkSync;

    /**
     * 对账后置处理逻辑: 可为空
     */
    private CheckAfter checkAfter;

    /**
     * 执行模式: 默认为 MEMORY
     */
    @Builder.Default
    private CheckMode mode = CheckMode.MEMORY;
}
