package io.github.kugin.reconciliation.before;

import io.github.kugin.reconciliation.domain.CheckEntry;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
}
