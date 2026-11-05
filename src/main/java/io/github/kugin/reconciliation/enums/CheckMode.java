package io.github.kugin.reconciliation.enums;

/**
 * 对账执行模式
 *
 * @author Kugin
 */
public enum CheckMode {
    /**
     * 全量驻留: 两侧数据全部载入内存后比对 (默认, 现有行为)
     */
    MEMORY,
    /**
     * 流式比对: 两侧数据按键升序逐条消费, 内存与总条数无关
     */
    STREAM
}
