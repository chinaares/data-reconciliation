package io.github.kugin.reconciliation.before;

import io.github.kugin.reconciliation.annotation.CheckField;
import io.github.kugin.reconciliation.annotation.CheckIdentity;
import io.github.kugin.reconciliation.domain.CheckAdapter;
import io.github.kugin.reconciliation.domain.CheckEntry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class FileResourceLoaderTest {

    @TempDir
    Path tempDir;

    @Test
    public void testAutoDetectUsesCheckAdapterWhenFieldsEmpty() throws IOException {
        writeFile("adapter.txt", "a1,10.5", "a2,20.5");

        FileResourceLoader<AdapterItem> loader = new FileResourceLoader<>(
                tempDir.resolve("%s.txt").toString(), null, null,
                line -> {
                    String[] parts = line.split(",");
                    return new AdapterItem(parts[0], Double.parseDouble(parts[1]));
                });

        List<CheckEntry> entries = loader.load("adapter");

        assertEquals(2, entries.size());
        assertEquals("adapter-a1", entries.get(0).getKey());
        assertEquals(10.5, entries.get(0).getCheckData().get("amount"));
        assertEquals("adapter-a2", entries.get(1).getKey());
        assertEquals(20.5, entries.get(1).getCheckData().get("amount"));
    }

    @Test
    public void testAutoDetectUsesAnnotationsWhenFieldsEmpty() throws IOException {
        writeFile("annotated.txt", "b1,100.0");

        FileResourceLoader<AnnotatedItem> loader = new FileResourceLoader<>(
                tempDir.resolve("%s.txt").toString(), "", Collections.emptyList(),
                line -> {
                    String[] parts = line.split(",");
                    return new AnnotatedItem(parts[0], Double.parseDouble(parts[1]));
                });

        List<CheckEntry> entries = loader.load("annotated");

        assertEquals(1, entries.size());
        assertEquals("b1", entries.get(0).getKey());
        assertEquals(100.0, entries.get(0).getCheckData().get("amount"));
    }

    @Test
    public void testExplicitFieldsAreUsedWhenProvided() throws IOException {
        writeFile("explicit.txt", "c1,300.0");

        FileResourceLoader<PlainItem> loader = new FileResourceLoader<>(
                tempDir.resolve("%s.txt").toString(), "id", Arrays.asList("amount"),
                line -> {
                    String[] parts = line.split(",");
                    return new PlainItem(parts[0], Double.parseDouble(parts[1]));
                });

        List<CheckEntry> entries = loader.load("explicit");

        assertEquals(1, entries.size());
        assertEquals("c1", entries.get(0).getKey());
        assertEquals(300.0, entries.get(0).getCheckData().get("amount"));
    }

    private void writeFile(String name, String... lines) throws IOException {
        Files.write(tempDir.resolve(name), Arrays.asList(lines));
    }

    static class AdapterItem implements CheckAdapter {
        private final String id;
        private final Double amount;

        AdapterItem(String id, Double amount) {
            this.id = id;
            this.amount = amount;
        }

        @Override
        public String getKey() {
            return "adapter-" + id;
        }

        @Override
        public Map<String, Object> getCheckData() {
            return Map.of("amount", amount);
        }
    }

    static class AnnotatedItem {
        @CheckIdentity
        private final String id;
        @CheckField
        private final Double amount;

        AnnotatedItem(String id, Double amount) {
            this.id = id;
            this.amount = amount;
        }
    }

    static class PlainItem {
        private final String id;
        private final Double amount;

        PlainItem(String id, Double amount) {
            this.id = id;
            this.amount = amount;
        }
    }
}
