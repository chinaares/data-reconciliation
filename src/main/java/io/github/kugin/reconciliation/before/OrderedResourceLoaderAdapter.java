package io.github.kugin.reconciliation.before;

import io.github.kugin.reconciliation.domain.CheckEntry;

import java.util.Comparator;
import java.util.stream.Stream;

/**
 * {@link OrderedResourceLoader} 的全量列表适配器
 * <p>
 * 注意: 本适配器仍是全量驻载, {@code delegate.load(date)} 已把数据读进内存,
 * 不具备流式的内存特性, 仅用于渠道渐进迁移与对拍测试,
 * 不得作为「已实现流式」的依据。
 *
 * @author Kugin
 */
public class OrderedResourceLoaderAdapter implements OrderedResourceLoader {

    /**
     * 被适配的全量数据源
     */
    private final ResourceLoader delegate;

    /**
     * key 排序规则: 两侧必须使用同一比较器规则, 框架无法校验两侧同序
     */
    private final Comparator<String> keyComparator;

    public OrderedResourceLoaderAdapter(ResourceLoader delegate) {
        this(delegate, String::compareTo);
    }

    /**
     * 两侧必须使用同一比较器规则, 框架无法校验两侧同序
     *
     * @param delegate      被适配的全量数据源
     * @param keyComparator key 排序规则
     */
    public OrderedResourceLoaderAdapter(ResourceLoader delegate, Comparator<String> keyComparator) {
        this.delegate = delegate;
        this.keyComparator = keyComparator;
    }

    /**
     * 适配器保持纯透传: null key 由框架的 merge join 迭代器守卫抛
     * CheckException("对账 key 不能为 null"), 适配器不抛 NPE
     */
    @Override
    public Stream<CheckEntry> loadOrdered(String date) {
        return delegate.load(date).stream()
                .sorted(Comparator.comparing(CheckEntry::getKey, Comparator.nullsFirst(keyComparator)));
    }
}
