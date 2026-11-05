package io.github.kugin.reconciliation.before;

import io.github.kugin.reconciliation.domain.CheckEntry;

import java.util.Comparator;
import java.util.stream.Stream;

/**
 * {@link OrderedResourceLoader} 的全量列表适配器
 * <p>
 * 注意: 本适配器仍是全量驻载, {@code delegate.load(date)} 已把数据读进内存,
 * 不具备流式的内存特性, 仅用于渠道渐进迁移与对拍测试。
 *
 * @author Kugin
 */
public class OrderedResourceLoaderAdapter implements OrderedResourceLoader {

    /**
     * 被适配的全量数据源
     */
    private final ResourceLoader delegate;

    /**
     * key 排序规则: 两侧必须一致
     */
    private final Comparator<String> keyComparator;

    public OrderedResourceLoaderAdapter(ResourceLoader delegate) {
        this(delegate, String::compareTo);
    }

    public OrderedResourceLoaderAdapter(ResourceLoader delegate, Comparator<String> keyComparator) {
        this.delegate = delegate;
        this.keyComparator = keyComparator;
    }

    @Override
    public Stream<CheckEntry> loadOrdered(String date) {
        return delegate.load(date).stream().sorted(Comparator.comparing(CheckEntry::getKey, keyComparator));
    }
}
