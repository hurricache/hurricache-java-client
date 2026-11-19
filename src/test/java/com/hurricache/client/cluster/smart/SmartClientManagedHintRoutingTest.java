package com.hurricache.client.cluster.smart;

import com.hurricache.TestBaseCluster;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Mode;
import com.hurricache.client.intf.Payload;
import com.hurricache.client.intf.Variant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Проверка, что сервер ПОЛНОСТЬЮ доверяет клиентскому KeyHint (client-managed hashes):
 *  - strong hash = идентичность ключа: не совпал — NOT_FOUND, сервер не перехеширует;
 *  - weak hash = размещение (weak % partitions): чужая партиция — NOT_FOUND,
 *    никакого re-route и re-hash со стороны сервера.
 */
public class SmartClientManagedHintRoutingTest extends TestBaseCluster {

    // Тестовый кластер: partitions = 1024, значит weak=1 и weak=2 — разные партиции
    private static final int STRONG = 1001;
    private static final int WEAK_OWNED = 1;
    private static final int WEAK_FOREIGN = 2;

    // =========================================================================
    // ПОЗИТИВ: полный цикл create/get/replace/remove с клиентским хинтом
    // =========================================================================

    @Test
    @DisplayName("Create with managed hint, read value on MASTER and BACKUP")
    void testCreateAndReadWithManagedHint() throws ExecutionException, InterruptedException {
        String key = "mh_create_" + UUID.randomUUID();
        KeyHintData hint = KeyHintData.of(STRONG, WEAK_OWNED);

        client.create(key, hint, v("managed_value")).get();
        waitForCondition(() -> client.setMode(Mode.BACKUP).containsKey(key, hint).get()
                && client.setMode(Mode.MASTER).containsKey(key, hint).get());

        Variant master = client.setMode(Mode.MASTER).get(key, hint).get();
        assertEquals("managed_value", str(master));

        Variant backup = client.setMode(Mode.BACKUP).get(key, hint).get();
        assertEquals("managed_value", str(backup));
    }

    @Test
    @DisplayName("Replace with managed hint updates value on MASTER and BACKUP")
    void testReplaceWithManagedHint() throws ExecutionException, InterruptedException {
        String key = "mh_replace_" + UUID.randomUUID();
        KeyHintData hint = KeyHintData.of(STRONG, WEAK_OWNED);

        client.create(key, hint, v("old_value")).get();
        waitForCondition(() -> client.setMode(Mode.BACKUP).containsKey(key, hint).get()
                && client.setMode(Mode.MASTER).containsKey(key, hint).get());

        Variant old = client.replace(key, hint, v("new_value")).get();
        assertEquals("old_value", str(old));

        assertEquals("new_value", str(client.setMode(Mode.MASTER).get(key, hint).get()));
        waitForCondition(() -> "new_value".equals(str(client.setMode(Mode.BACKUP).get(key, hint).get())));
    }

    @Test
    @DisplayName("Remove with managed hint makes key disappear everywhere")
    void testRemoveWithManagedHint() throws ExecutionException, InterruptedException {
        String key = "mh_remove_" + UUID.randomUUID();
        KeyHintData hint = KeyHintData.of(STRONG, WEAK_OWNED);

        client.create(key, hint, v("to_delete")).get();
        waitForCondition(() -> client.setMode(Mode.BACKUP).containsKey(key, hint).get()
                && client.setMode(Mode.MASTER).containsKey(key, hint).get());

        assertTrue(client.remove(key, hint).get());

        waitForCondition(() -> !client.setMode(Mode.BACKUP).containsKey(key, hint).get()
                && !client.setMode(Mode.MASTER).containsKey(key, hint).get());
        assertNotFound(client.get(key, hint));
    }

    // =========================================================================
    // TRUST: сервер не перехеширует идентичность (strong)
    // =========================================================================

    @Test
    @DisplayName("Wrong strong hash reads as NOT_FOUND (server trusts hint, no rehash)")
    void testWrongStrongHashIsNotFound() throws ExecutionException, InterruptedException {
        String key = "mh_strong_" + UUID.randomUUID();
        KeyHintData hint = KeyHintData.of(STRONG, WEAK_OWNED);

        client.create(key, hint, v("value")).get();
        waitForCondition(() -> client.setMode(Mode.BACKUP).containsKey(key, hint).get()
                && client.setMode(Mode.MASTER).containsKey(key, hint).get());

        // Та же строка ключа, та же партиция, но другая идентичность —
        // если сервер доверяет хинту, он ищет по strong=9999 и не находит
        KeyHintData foreignStrong = KeyHintData.of(9999, WEAK_OWNED);
        assertNotFound(client.get(key, foreignStrong));

        // А с правильным strong — находит
        assertEquals("value", str(client.get(key, hint).get()));
    }

    // =========================================================================
    // TRUST: сервер не перехеширует размещение (weak)
    // =========================================================================

    @Test
    @DisplayName("Wrong weak hash reads as NOT_FOUND (server trusts placement)")
    void testWrongWeakHashIsNotFound() throws ExecutionException, InterruptedException {
        String key = "mh_weak_" + UUID.randomUUID();
        KeyHintData hint = KeyHintData.of(STRONG, WEAK_OWNED);

        client.create(key, hint, v("value")).get();
        waitForCondition(() -> client.setMode(Mode.BACKUP).containsKey(key, hint).get()
                && client.setMode(Mode.MASTER).containsKey(key, hint).get());

        // Тот же strong, но другая партиция (weak=2 % 1024 != weak=1 % 1024):
        // сервер смотрит в партицию 2 и не должен находить ключ из партиции 1
        KeyHintData foreignWeak = KeyHintData.of(STRONG, WEAK_FOREIGN);
        assertNotFound(client.get(key, foreignWeak));

        // С правильным weak — находит
        assertEquals("value", str(client.get(key, hint).get()));
    }

    // =========================================================================
    // CO-LOCATION: пачка ключей с одним weak hash — все операции успешны
    // =========================================================================

    @Test
    @DisplayName("Many keys with the same weak hash are all reachable with that hint")
    void testCoLocatedKeysWithSharedWeakHash() throws ExecutionException, InterruptedException {
        String prefix = "mh_coloc_" + UUID.randomUUID() + "_";
        int count = 100;

        for (int i = 0; i < count; i++) {
            client.create(prefix + i, KeyHintData.of(STRONG + i, WEAK_OWNED), v("coloc_" + i)).get();
        }
        waitForCondition(() -> client.setMode(Mode.BACKUP)
                .containsKey(prefix + (count - 1), KeyHintData.of(STRONG + count - 1, WEAK_OWNED)).get()
                && client.setMode(Mode.MASTER)
                .containsKey(prefix + (count - 1), KeyHintData.of(STRONG + count - 1, WEAK_OWNED)).get());

        for (int i = 0; i < count; i++) {
            KeyHintData hint = KeyHintData.of(STRONG + i, WEAK_OWNED);
            assertEquals("coloc_" + i, str(client.setMode(Mode.MASTER).get(prefix + i, hint).get()),
                    "master read failed for key " + i);
        }
        for (int i = 0; i < count; i++) {
            KeyHintData hint = KeyHintData.of(STRONG + i, WEAK_OWNED);
            assertEquals("coloc_" + i, str(client.setMode(Mode.BACKUP).get(prefix + i, hint).get()),
                    "backup read failed for key " + i);
        }
    }

    // =========================================================================
    // Контейнер с клиентским хинтом
    // =========================================================================

    @Test
    @DisplayName("HashMap with managed hint: putAll/getInContainer on MASTER and BACKUP")
    void testHashMapWithManagedHint() throws ExecutionException, InterruptedException {
        String key = "mh_map_" + UUID.randomUUID();
        KeyHintData hint = KeyHintData.of(STRONG, WEAK_OWNED);

        Map<Payload, Variant> initial = new HashMap<>();
        initial.put(p("f1"), v("v1"));
        client.createMap(key, hint, initial).get();
        waitForCondition(() -> client.setMode(Mode.BACKUP).containsKey(key, hint).get()
                && client.setMode(Mode.MASTER).containsKey(key, hint).get());

        client.putAll(key, hint, List.of(p("f2")), List.of(v("v2"))).get();
        waitForCondition(() -> "v2".equals(
                str(client.setMode(Mode.BACKUP).getInContainer(key, hint, "f2".getBytes()).get())));

        assertEquals("v1", str(client.setMode(Mode.MASTER).getInContainer(key, hint, "f1".getBytes()).get()));
        assertEquals("v2", str(client.setMode(Mode.BACKUP).getInContainer(key, hint, "f2".getBytes()).get()));
    }
}
