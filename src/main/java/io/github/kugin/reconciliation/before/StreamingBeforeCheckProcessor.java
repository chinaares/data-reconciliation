package io.github.kugin.reconciliation.before;

import io.github.kugin.reconciliation.domain.CheckContext;
import io.github.kugin.reconciliation.enums.ExecutorStatusEnum;

import java.util.Objects;

/**
 * 流式前置处理器: 只做前置校验, 不加载任何数据
 * <p>
 * 与全量模式({@link DefaultBeforeCheckProcessor})的差别:
 * <ol>
 *     <li>不读取数据源, 两侧数据由 {@link io.github.kugin.reconciliation.check.StreamingCheckProcessor} 在比对阶段按需开流;</li>
 *     <li>前置校验不通过时置 {@code preCheckFailed} 标记并立即返回, 比对阶段据此跳过, 两侧数据源一次都不会被调用;</li>
 *     <li>不使用 {@link ResourceReader}, 增量对账(差异数据源)在流式模式下不提供。</li>
 * </ol>
 * <p>
 * 状态语义: 前置校验不通过时置 {@code preCheckFailed} 标记, 比对阶段据此跳过,
 * 最终状态为 ERROR(MEMORY 模式此时状态停在 BEFORE, 两者不对齐是已知且有意的)
 *
 * @author Kugin
 */
public class StreamingBeforeCheckProcessor extends AbstractBeforeCheckProcessor {

    /**
     * 前置校验
     */
    private final CheckPre checkPre;

    public StreamingBeforeCheckProcessor(CheckPre checkPre) {
        this.checkPre = checkPre;
    }

    @Override
    public boolean doPre(CheckContext context) {
        return Objects.isNull(checkPre) || checkPre.doPre(context);
    }

    /**
     * 前置处理
     * <p>
     * 父类的实现会加载 source/target 并构建 Map, 流式模式下不需要, 故此处整体覆写:
     * 前置校验不通过时直接返回, 不触碰任何数据源
     *
     * @param context 上下文
     */
    @Override
    public void processBeforeCompare(CheckContext context) {
        context.getExecutorManager().setStatus(ExecutorStatusEnum.BEFORE);
        //前置校验不通过, 不加载任何数据, 由比对阶段根据 preCheckFailed 标记跳过
        if (!doPre(context)) {
            context.setPreCheckFailed(true);
            return;
        }
    }

    @Override
    public ResourceLoader getSourceLoader() {
        throw new UnsupportedOperationException("STREAM 模式不使用 ResourceReader");
    }

    @Override
    public ResourceLoader getTargetLoader() {
        throw new UnsupportedOperationException("STREAM 模式不使用 ResourceReader");
    }

    @Override
    public ResourceLoader getDifferentLoader() {
        throw new UnsupportedOperationException("STREAM 模式不使用 ResourceReader");
    }
}
