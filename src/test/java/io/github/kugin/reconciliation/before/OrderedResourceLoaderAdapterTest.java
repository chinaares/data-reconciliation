package io.github.kugin.reconciliation.before;

import io.github.kugin.reconciliation.domain.CheckEntry;
import org.junit.jupiter.api.Test;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;

class OrderedResourceLoaderAdapterTest {

    private static CheckEntry entry(String key) {
        return new CheckEntry(key, null, Map.of("amount", 1d));
    }

    @Test
    void adapterSortsByKeyAscending() {
        ResourceLoader delegate = date -> List.of(entry("c"), entry("a"), entry("b"));

        List<String> keys = new OrderedResourceLoaderAdapter(delegate)
                .loadOrdered("20210204")
                .map(CheckEntry::getKey)
                .toList();

        assertEquals(List.of("a", "b", "c"), keys);
    }

    @Test
    void adapterSortsLexicographicallyOnBothSides() {
        // 数字序与字典序不同: "10" < "9" 才是字典序, 契约要求两侧都用同一规则
        ResourceLoader delegate = date -> List.of(entry("9"), entry("10"));

        List<String> keys = new OrderedResourceLoaderAdapter(delegate)
                .loadOrdered("20210204")
                .map(CheckEntry::getKey)
                .toList();

        assertEquals(List.of("10", "9"), keys);
    }

    @Test
    void adapterPreservesAllEntries() {
        ResourceLoader delegate = date -> List.of(entry("a"), entry("b"));

        long count = new OrderedResourceLoaderAdapter(delegate).loadOrdered("20210204").count();

        assertEquals(2L, count);
    }

    /**
     * null key 不得在适配器处抛 NPE: 适配器保持纯透传,
     * 由框架的 merge join 迭代器守卫抛 CheckException("对账 key 不能为 null")
     */
    @Test
    void adapterDoesNotThrowOnNullKey() {
        ResourceLoader delegate = date -> List.of(entry("a"), entry(null));

        List<CheckEntry> entries = new OrderedResourceLoaderAdapter(delegate)
                .loadOrdered("20210204")
                .toList();

        assertEquals(2, entries.size());
        assertNull(entries.get(0).getKey());
        assertEquals("a", entries.get(1).getKey());
    }

    @Test
    void adapterUsesProvidedComparator() {
        ResourceLoader delegate = date -> List.of(entry("a"), entry("b"), entry("c"));

        List<String> keys = new OrderedResourceLoaderAdapter(delegate, Comparator.reverseOrder())
                .loadOrdered("20210204")
                .map(CheckEntry::getKey)
                .toList();

        assertEquals(List.of("c", "b", "a"), keys);
    }

    /**
     * 契约 ②: 每次调用必须返回一个新的 Stream, 且各自可完整消费
     */
    @Test
    void adapterReturnsNewStreamPerCall() {
        ResourceLoader delegate = date -> List.of(entry("a"), entry("b"));
        OrderedResourceLoaderAdapter adapter = new OrderedResourceLoaderAdapter(delegate);

        Stream<CheckEntry> first = adapter.loadOrdered("20210204");
        Stream<CheckEntry> second = adapter.loadOrdered("20210204");

        assertNotSame(first, second);
        assertEquals(List.of("a", "b"), first.map(CheckEntry::getKey).toList());
        assertEquals(List.of("a", "b"), second.map(CheckEntry::getKey).toList());
    }
}
