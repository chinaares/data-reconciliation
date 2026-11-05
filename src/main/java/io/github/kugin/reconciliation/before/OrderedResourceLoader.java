package io.github.kugin.reconciliation.before;

import io.github.kugin.reconciliation.domain.CheckEntry;

import java.util.stream.Stream;

/**
 * 有序数据源: STREAM 模式的数据加载契约
 * <p>
 * 契约:
 * 1. 严格升序: 按 key 升序排列(语义为 {@link String#compareTo}), 同侧 key 不得重复;
 * 2. 单遍消费: 返回的 Stream 只能被消费一次, 不会被复用; 每次调用必须返回一个新的 Stream;
 * 3. 稳定快照: 遍历期间底层数据不可变;
 * 4. key 不得为 null。
 *
 * @author Kugin
 */
@FunctionalInterface
public interface OrderedResourceLoader {

    /**
     * 加载指定日期的有序数据
     *
     * @param date 数据日期
     * @return 按 key 严格升序排列的实体流
     */
    Stream<CheckEntry> loadOrdered(String date);
}
