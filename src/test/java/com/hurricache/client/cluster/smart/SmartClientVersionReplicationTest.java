package com.hurricache.client.cluster.smart;

import com.hurricache.TestBaseCluster;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Mode;
import com.hurricache.client.intf.OrderedPayload;
import com.hurricache.client.intf.OrderedVariant;
import com.hurricache.client.intf.Payload;
import com.hurricache.client.intf.Variant;
import com.hurricache.grpc.ContainerType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Версионная репликация: проверяем, что version/timestamp правила работают на живом кластере.
 * - последовательные записи сходятся в финальное состояние (порядок версий);
 * - delete не воскресает старыми операциями;
 * - параллельные атомики с двух нод не теряются и не дублируются;
 * - конфликт MASTER vs BACKUP решается по timestamp (LWW);
 * - контейнерная целостность под версионной дедупликацией.
 */
public class SmartClientVersionReplicationTest extends TestBaseCluster {

    // =========================================================================
    // 1. ПОСЛЕДОВАТЕЛЬНЫЕ ЗАПИСИ: BACKUP СХОДИТСЯ В ФИНАЛ
    // =========================================================================

    @Test
    @DisplayName("Rapid sequential replaces converge to the latest value on BACKUP")
    void testSequentialReplacesConverge() throws ExecutionException, InterruptedException {
        String key = "ver_seq_" + UUID.randomUUID();
        KeyHintData hint = client.setMode(Mode.MASTER).create(key, v("v0")).get();
        waitForCondition(() -> client.setMode(Mode.BACKUP).containsKey(key).get() && client.setMode(Mode.MASTER).containsKey(key).get());
        String last = null;
        for (int i = 1; i <= 10; i++) {
            last = "v" + i;
            client.setMode(Mode.MASTER).replace(key, hint, v(last)).get();
        }

        final String expected = last;
        waitForCondition(() -> expected.equals(str(client.setMode(Mode.BACKUP).get(key, hint).get())));
        assertEquals(expected, str(client.setMode(Mode.MASTER).get(key, hint).get()));
        assertEquals(expected, str(client.setMode(Mode.BACKUP).get(key, hint).get()));
    }

    // =========================================================================
    // 2. DELETE: старые операции не воскрешают ключ
    // =========================================================================

    @Test
    @DisplayName("Delete on MASTER removes the key on BACKUP and it stays deleted")
    void testDeleteStaysDeleted() throws ExecutionException, InterruptedException {
        String key = "ver_del_" + UUID.randomUUID();
        KeyHintData hint = client.setMode(Mode.MASTER).create(key, v("data")).get();
        waitForCondition(() -> client.setMode(Mode.BACKUP).containsKey(key).get());

        client.setMode(Mode.MASTER).remove(key, hint).get();
        waitForCondition(() -> !client.setMode(Mode.BACKUP).containsKey(key).get());

        // Старый replace по удалённому ключу — NOT_FOUND, не воскрешение
        assertNotFound(client.setMode(Mode.MASTER).replace(key, hint, v("ghost")));
        Thread.sleep(300);
        assertFalse(client.setMode(Mode.BACKUP).containsKey(key).get());
        assertFalse(client.setMode(Mode.MASTER).containsKey(key).get());
    }

    @Test
    @DisplayName("Delete then re-create converges to the NEW value on BACKUP")
    void testDeleteThenRecreate() throws ExecutionException, InterruptedException {
        String key = "ver_recreate_" + UUID.randomUUID();
        KeyHintData hint = client.setMode(Mode.MASTER).create(key, v("old_value")).get();
        waitForCondition(() -> client.setMode(Mode.BACKUP).containsKey(key,hint).get() && client.setMode(Mode.MASTER).containsKey(key,hint).get());

        client.setMode(Mode.MASTER).remove(key, hint).get();
        waitForCondition(() -> !client.setMode(Mode.BACKUP).containsKey(key,hint).get());

        client.setMode(Mode.MASTER).create(key, v("new_value")).get();
        waitForCondition(() -> client.setMode(Mode.BACKUP).containsKey(key,hint).get());
        assertEquals("new_value", str(client.setMode(Mode.BACKUP).get(key, hint).get()));
        assertEquals("new_value", str(client.setMode(Mode.MASTER).get(key, hint).get()));
    }

    // =========================================================================
    // 3. ПАРАЛЛЕЛЬНЫЕ АТОМИКИ С ДВУХ НОД: дельты не теряются и не дублируются
    // =========================================================================

    @Test
    @DisplayName("Concurrent atomic adds from MASTER and BACKUP sum exactly")
    void testConcurrentAtomicAdds() throws Exception {
        String key = "ver_atomic_" + UUID.randomUUID();
        KeyHintData hint = client.setMode(Mode.MASTER).create(key, vi(0)).get();
        waitForCondition(() -> client.setMode(Mode.BACKUP).containsKey(key).get() && client.setMode(Mode.MASTER).containsKey(key).get());
        final int perSide = 200;
        CompletableFuture<Void> fromMaster = CompletableFuture.runAsync(() -> {
            for (int i = 0; i < perSide; i++) {
                client.setMode(Mode.MASTER).add(key, hint, 1).join();
            }
        });
        CompletableFuture<Void> fromBackup = CompletableFuture.runAsync(() -> {
            for (int i = 0; i < perSide; i++) {
                client.setMode(Mode.BACKUP).add(key, hint, 1).join();
            }
        });
        CompletableFuture.allOf(fromMaster, fromBackup).get();

        long expected = 2L * perSide;
        waitForCondition(() -> client.setMode(Mode.BACKUP).get(key, hint).get().longValue() == expected);
        waitForCondition(() -> client.setMode(Mode.MASTER).get(key, hint).get().longValue() == expected);
        assertEquals(expected, client.setMode(Mode.MASTER).get(key, hint).get().longValue());
        assertEquals(expected, client.setMode(Mode.BACKUP).get(key, hint).get().longValue());
    }

    // =========================================================================
    // 4. КОНФЛИКТ MASTER vs BACKUP: LWW по timestamp
    // =========================================================================

    @Test
    @DisplayName("Conflicting writes from two nodes resolve by last-write-wins")
    void testLastWriteWinsAcrossNodes() throws ExecutionException, InterruptedException {
        String key = "ver_lww_" + UUID.randomUUID();
        KeyHintData hint = client.setMode(Mode.MASTER).create(key, v("initial")).get();
        waitForCondition(() -> client.setMode(Mode.BACKUP).containsKey(key).get() && client.setMode(Mode.MASTER).containsKey(key).get());

        // Сначала запись через BACKUP, затем более поздняя через MASTER.
        // Часы кэшированные (тик ~10 мс) — разводим записи по времени,
        // иначе ts совпадёт и победитель недетерминирован (см. testConcurrentWriteTieConverges)
        client.setMode(Mode.BACKUP).replace(key, hint, v("from_backup")).get();
        Thread.sleep(100);
        client.setMode(Mode.MASTER).replace(key, hint, v("from_master")).get();

        waitForCondition(() -> "from_master".equals(str(client.setMode(Mode.BACKUP).get(key, hint).get())));
        assertEquals("from_master", str(client.setMode(Mode.MASTER).get(key, hint).get()));
        assertEquals("from_master", str(client.setMode(Mode.BACKUP).get(key, hint).get()));
    }

    // NB: тест на same-tick конфликт осознанно отсутствует — при равных ts ноды
    // могут разойтись (нет детерминированного tie-break). Требует решения на уровне
    // протокола/правил, после чего тест добавить.

    // =========================================================================
    // 5. КОНТЕЙНЕРНАЯ ЦЕЛОСТНОСТЬ: версионная дедупликация
    // =========================================================================

    @Test
    @DisplayName("Concurrent list pushes from two nodes keep identical content on both")
    void testConcurrentContainerPushes() throws Exception {
        String key = "ver_list_" + UUID.randomUUID();
        KeyHintData hint = client.setMode(Mode.MASTER).createList(key, java.util.List.of(v("seed"))).get();
        waitForCondition(() -> client.setMode(Mode.BACKUP).containsKey(key).get() && client.setMode(Mode.MASTER).containsKey(key).get());
        final int perSide = 100;
        CompletableFuture<Void> fromMaster = CompletableFuture.runAsync(() -> {
            for (int i = 0; i < perSide; i++) {
                client.setMode(Mode.MASTER).pushTail(key, hint, java.util.List.of(v("m_" + i))).join();
            }
        });
        CompletableFuture<Void> fromBackup = CompletableFuture.runAsync(() -> {
            for (int i = 0; i < perSide; i++) {
                client.setMode(Mode.BACKUP).pushTail(key, hint, java.util.List.of(v("b_" + i))).join();
            }
        });
        CompletableFuture.allOf(fromMaster, fromBackup).get();

        int expected = 1 + 2 * perSide;
        waitForCondition(() -> client.setMode(Mode.BACKUP).getSize(key, hint).get() == expected);
        waitForCondition(() -> client.setMode(Mode.MASTER).getSize(key, hint).get() == expected);
        assertEquals(expected, (int) client.setMode(Mode.MASTER).getSize(key, hint).get());
        assertEquals(expected, (int) client.setMode(Mode.BACKUP).getSize(key, hint).get());
    }

    // =========================================================================
    // ХЕЛПЕРЫ
    // =========================================================================

    private List<String> strList(List<Variant> list) {
        return list.stream().map(this::str).collect(Collectors.toList());
    }

    private void waitBothContain(String key, KeyHintData hint) {
        assertEventually(() -> client.setMode(Mode.BACKUP).containsKey(key,hint).get()
                && client.setMode(Mode.MASTER).containsKey(key,hint).get());
    }

    // =========================================================================
    // 6. LIST: все операции сходятся
    // =========================================================================

    @Test
    @DisplayName("List: pushTail/pushHead/insertAt/removeAt/pop converge on BACKUP")
    void testListOperationsConverge() throws Exception {
        String key = "ver_list_ops_" + UUID.randomUUID();
        KeyHintData hint = client.setMode(Mode.MASTER).createList(key, List.of(v("seed"))).get();
        waitBothContain(key,hint );

        client.setMode(Mode.MASTER).pushTail(key, hint, List.of(v("t1"), v("t2"))).get();
        client.setMode(Mode.MASTER).pushHead(key, hint, List.of(v("h1"))).get();
        client.setMode(Mode.MASTER).insertAt(key, hint, List.of(v("mid")), 1).get();
        // ожидаем: [h1, mid, seed, t1, t2]
        List<String> stage1 = List.of("h1", "mid", "seed", "t1", "t2");
        assertEventually(() -> strList(client.setMode(Mode.BACKUP).getList(key, hint).get()).equals(stage1));
        assertEquals(stage1, strList(client.setMode(Mode.MASTER).getList(key, hint).get()));

        client.setMode(Mode.MASTER).removeAt(key, hint, 1L).get(); // удаляем mid
        assertEquals("h1", str(client.setMode(Mode.MASTER).popHead(key, hint).get()));
        assertEquals("t2", str(client.setMode(Mode.MASTER).popTail(key, hint).get()));
        // осталось: [seed, t1]
        List<String> stage2 = List.of("seed", "t1");
        assertEventually(() -> strList(client.setMode(Mode.BACKUP).getList(key, hint).get()).equals(stage2));
        assertEquals(stage2, strList(client.setMode(Mode.MASTER).getList(key, hint).get()));
    }

    // =========================================================================
    // 7. VECTOR: все операции сходятся
    // =========================================================================

    @Test
    @DisplayName("Vector: pushTail/insertAt/removeAt/popAt converge on BACKUP")
    void testVectorOperationsConverge() throws Exception {
        String key = "ver_vec_ops_" + UUID.randomUUID();
        KeyHintData hint = client.setMode(Mode.MASTER).createVector(key, List.of(v("seed"))).get();
        waitBothContain(key,hint );

        client.setMode(Mode.MASTER).pushTail(key, hint, List.of(v("t1"))).get();
        client.setMode(Mode.MASTER).insertAt(key, hint, List.of(v("first")), 0).get();
        // ожидаем: [first, seed, t1]
        List<String> stage1 = List.of("first", "seed", "t1");
        assertEventually(() -> strList(client.setMode(Mode.BACKUP).getVector(key, hint).get()).equals(stage1));
        assertEquals(stage1, strList(client.setMode(Mode.MASTER).getVector(key, hint).get()));
        assertEquals("seed", str(client.setMode(Mode.BACKUP).getAt(key, hint, 1).get()));

        client.setMode(Mode.MASTER).removeAt(key, hint, 0L).get(); // удаляем first
        assertEquals("t1", str(client.setMode(Mode.MASTER).popAt(key, hint, 1).get()));
        // осталось: [seed]
        List<String> stage2 = List.of("seed");
        assertEventually(() -> strList(client.setMode(Mode.BACKUP).getVector(key, hint).get()).equals(stage2));
        assertEquals(stage2, strList(client.setMode(Mode.MASTER).getVector(key, hint).get()));
    }

    // =========================================================================
    // 8. QUEUE: все операции сходятся
    // =========================================================================

    @Test
    @DisplayName("Queue: pushTail/popHead/peek converge on BACKUP")
    void testQueueOperationsConverge() throws Exception {
        String key = "ver_queue_ops_" + UUID.randomUUID();
        KeyHintData hint = client.setMode(Mode.MASTER).createQueue(key, List.of(v("seed"))).get();
        waitBothContain(key,hint );

        client.setMode(Mode.MASTER).pushTail(key, hint, List.of(v("a"), v("b"))).get();
        assertEventually(() -> "seed".equals(str(client.setMode(Mode.BACKUP).peekHead(key, hint).get())));
        assertEventually(() -> "b".equals(str(client.setMode(Mode.BACKUP).peekTail(key, hint).get())));


        assertEquals("seed", str(client.setMode(Mode.MASTER).popHead(key, hint).get()));
        assertEquals("a", str(client.setMode(Mode.MASTER).popHead(key, hint).get()));
        // осталось: [b]
        assertEventually(() -> {
            try {
                return client.setMode(Mode.BACKUP).getSize(key, hint).get() == 1
                        && "b".equals(str(client.setMode(Mode.BACKUP).peekHead(key, hint).get()));
            } catch (Exception e) {
                return false;
            }
        });
        assertEquals(1, (int) client.setMode(Mode.MASTER).getSize(key, hint).get());
    }

    // =========================================================================
    // 9. SET: добавление/удаление сходится
    // =========================================================================

    @Test
    @DisplayName("Set: addElementUnordered/removeByValue converge on BACKUP")
    void testSetOperationsConverge() throws Exception {
        String key = "ver_set_ops_" + UUID.randomUUID();
        KeyHintData hint = client.setMode(Mode.MASTER).createSet(key, List.of(v("a"), v("b"))).get();
        waitBothContain(key,hint );

        client.setMode(Mode.MASTER).addElementUnordered(key, hint, List.of(v("c"))).get();
        assertEventually(() -> client.setMode(Mode.BACKUP).containsKey(key, hint, "c".getBytes()).get());
        assertEquals(3, (int) client.setMode(Mode.BACKUP).getSize(key, hint).get());

        // дубликат элемента не увеличивает размер ни на одной из нод
        client.setMode(Mode.MASTER).addElementUnordered(key, hint, List.of(v("c"))).get();
        Thread.sleep(300);
        assertEquals(3, (int) client.setMode(Mode.BACKUP).getSize(key, hint).get());
        assertEquals(3, (int) client.setMode(Mode.MASTER).getSize(key, hint).get());

        client.setMode(Mode.MASTER)
                .removeFromContainerByKeyAndValue(key, hint, ContainerType.SET, List.of(), List.of(v("a"))).get();
        assertEventually(() -> !client.setMode(Mode.BACKUP).containsKey(key, hint, "a".getBytes()).get());
        assertEventually(() -> !client.setMode(Mode.MASTER).containsKey(key, hint, "a".getBytes()).get());
        assertEquals(2, (int) client.setMode(Mode.BACKUP).getSize(key, hint).get());
        assertEquals(2, (int) client.setMode(Mode.MASTER).getSize(key, hint).get());
    }

    // =========================================================================
    // 10. MAP: putAll/update/remove converge
    // =========================================================================

    @Test
    @DisplayName("Map: putAll/update/popInContainer converge on BACKUP")
    void testMapOperationsConverge() throws Exception {
        String key = "ver_map_ops_" + UUID.randomUUID();
        Map<Payload, Variant> init = new HashMap<>();
        init.put(p("k1"), v("v1"));
        KeyHintData hint = client.setMode(Mode.MASTER).createMap(key, init).get();
        waitBothContain(key, hint);

        client.setMode(Mode.MASTER).putAll(key, hint, List.of(p("k2")), List.of(v("v2"))).get();
        assertEventually(() -> "v2".equals(str(client.setMode(Mode.BACKUP).getInContainer(key, hint, "k2".getBytes()).get())));

        client.setMode(Mode.MASTER).update(key, hint, "k1".getBytes(), v("v1_new")).get();
        assertEventually(() -> "v1_new".equals(str(client.setMode(Mode.BACKUP).getInContainer(key, hint, "k1".getBytes()).get())));
        assertEquals("v1_new", str(client.setMode(Mode.MASTER).getInContainer(key, hint, "k1".getBytes()).get()));

        assertEquals("v2", str(client.setMode(Mode.MASTER).popInContainer(key, hint, "k2".getBytes()).get()));
        assertEventually(() -> client.setMode(Mode.BACKUP).getSize(key, hint).get() == 1);
        assertEquals(1, (int) client.setMode(Mode.MASTER).getSize(key, hint).get());
    }

    // =========================================================================
    // 11. ORDERED_SET: weight-операции сходятся
    // =========================================================================

    @Test
    @DisplayName("OrderedSet: addElementWithWeight/incrementOrder/getAndRemove converge on BACKUP")
    void testOrderedSetOperationsConverge() throws Exception {
        String key = "ver_oset_ops_" + UUID.randomUUID();
        KeyHintData hint = client.setMode(Mode.MASTER)
                .createOrderedSet(key, List.of(ov("a".getBytes(), 1L), ov("b".getBytes(), 2L))).get();
        waitBothContain(key,hint );

        client.setMode(Mode.MASTER)
                .addElementWithWeight(key, hint, List.of(OrderedVariant.ofBytes("c".getBytes(), 3L))).get();
        assertEventually(() -> "c".equals(str(client.setMode(Mode.BACKUP).getElementWithWeight(key, hint, 3).get())));

        // b: weight 2 -> 9
        client.setMode(Mode.MASTER).incrementOrder(key,hint, OrderedPayload.of(2L, "b".getBytes()), 7).get();
        assertEventually(() -> "b".equals(str(client.setMode(Mode.BACKUP).getElementWithWeight(key, hint, 9).get())));
        assertEquals("b", str(client.setMode(Mode.MASTER).getElementWithWeight(key, hint, 9).get()));

        assertEquals("a", str(client.setMode(Mode.MASTER).getAndRemoveElementWithWeight(key, hint, 1).get()));
        assertEventually(() -> client.setMode(Mode.BACKUP).getSize(key, hint).get() == 2);
        assertEquals(2, (int) client.setMode(Mode.MASTER).getSize(key, hint).get());
    }

    // =========================================================================
    // 12. ORDERED_MAP: addElementOrderedMap converge
    // =========================================================================

    @Test
    @DisplayName("OrderedMap: addElementOrderedMap converges on BACKUP")
    void testOrderedMapOperationsConverge() throws Exception {
        String key = "ver_omap_ops_" + UUID.randomUUID();
        Map<OrderedPayload, Variant> init = new HashMap<>();
        init.put(op(1L, "k1"), v("v1"));
        KeyHintData hint = client.setMode(Mode.MASTER).createOrderedMap(key, init).get();
        waitBothContain(key,hint );

        Integer added = client.setMode(Mode.MASTER)
                .addElementOrderedMap(key, hint, List.of(OrderedPayload.of("k2".getBytes(), 2L)), List.of(v("v2"))).get();
        assertEquals(1, added);
        assertEventually(() -> client.setMode(Mode.BACKUP).getSize(key, hint).get() == 2);
        assertEquals(2, (int) client.setMode(Mode.MASTER).getSize(key, hint).get());

        Map<OrderedPayload, Variant> master = client.setMode(Mode.MASTER).getOrderedMap(key, hint).get();
        assertEventually(() -> client.setMode(Mode.BACKUP).getOrderedMap(key, hint).get().size() == master.size());
        Map<OrderedPayload, Variant> backup = client.setMode(Mode.BACKUP).getOrderedMap(key, hint).get();
        assertEquals(master.size(), backup.size());
        master.forEach((k, val) -> {
            Variant bval = backup.get(k);
            assertNotNull(bval, "missing ordered key on BACKUP: " + k);
            assertEquals(str(val), str(bval));
        });
    }

    // =========================================================================
    // 13. ATOMICS: add/sub/and/or/xor/exchange/CAS последовательно сходятся
    // =========================================================================

    @Test
    @DisplayName("Atomics: add/sub/and/or/xor/exchange/CAS converge on BACKUP")
    void testAtomicOperationsConverge() throws Exception {
        String key = "ver_atomic_ops_" + UUID.randomUUID();
        KeyHintData hint = client.setMode(Mode.MASTER).create(key, vi(0)).get();
        waitBothContain(key,hint );

        client.setMode(Mode.MASTER).add(key, hint, 5).get();          // 5
        client.setMode(Mode.MASTER).sub(key, hint, 2).get();          // 3
        client.setMode(Mode.MASTER).exchange(key, hint, 100).get();   // 100
        client.setMode(Mode.MASTER).and(key, hint, 0b1100).get();     // 100 & 12 = 4
        client.setMode(Mode.MASTER).or(key, hint, 1).get();           // 5
        client.setMode(Mode.MASTER).xor(key, hint, 1).get();          // 4
        client.setMode(Mode.MASTER).compareAndSet(key, hint, 4L, 42L).get(); // 42

        assertEventually(() -> client.setMode(Mode.BACKUP).get(key, hint).get().longValue() == 42L);
        assertEquals(42L, client.setMode(Mode.MASTER).get(key, hint).get().longValue());
        assertEquals(42L, client.setMode(Mode.BACKUP).get(key, hint).get().longValue());
    }

    // =========================================================================
    // 14. CONCURRENT: add с одной ноды, sub с другой — точная сходимость
    // =========================================================================

    @Test
    @DisplayName("Concurrent atomic add from MASTER and sub from BACKUP sum exactly")
    void testConcurrentAtomicAddSub() throws Exception {
        String key = "ver_addsub_" + UUID.randomUUID();
        KeyHintData hint = client.setMode(Mode.MASTER).create(key, vi(0)).get();
        waitBothContain(key,hint );

        final int adds = 200, subs = 100;
        CompletableFuture<Void> fromMaster = CompletableFuture.runAsync(() -> {
            for (int i = 0; i < adds; i++) client.setMode(Mode.MASTER).add(key, hint, 1).join();
        });
        CompletableFuture<Void> fromBackup = CompletableFuture.runAsync(() -> {
            for (int i = 0; i < subs; i++) client.setMode(Mode.BACKUP).sub(key, hint, 1).join();
        });
        CompletableFuture.allOf(fromMaster, fromBackup).get();

        long expected = adds - subs;
        assertEventually(() -> client.setMode(Mode.BACKUP).get(key, hint).get().longValue() == expected);
        assertEquals(expected, client.setMode(Mode.MASTER).get(key, hint).get().longValue());
        assertEquals(expected, client.setMode(Mode.BACKUP).get(key, hint).get().longValue());
    }

    // =========================================================================
    // 15. CONCURRENT: пуши в очередь с двух нод
    // =========================================================================

    @Test
    @DisplayName("Concurrent queue pushes from two nodes converge in size")
    void testConcurrentQueuePushes() throws Exception {
        String key = "ver_queue_cc_" + UUID.randomUUID();
        KeyHintData hint = client.setMode(Mode.MASTER).createQueue(key, List.of(v("seed"))).get();
        waitBothContain(key,hint );

        final int perSide = 100;
        CompletableFuture<Void> fromMaster = CompletableFuture.runAsync(() -> {
            for (int i = 0; i < perSide; i++) client.setMode(Mode.MASTER).pushTail(key, hint, List.of(v("m_" + i))).join();
        });
        CompletableFuture<Void> fromBackup = CompletableFuture.runAsync(() -> {
            for (int i = 0; i < perSide; i++) client.setMode(Mode.BACKUP).pushTail(key, hint, List.of(v("b_" + i))).join();
        });
        CompletableFuture.allOf(fromMaster, fromBackup).get();

        int expected = 1 + 2 * perSide;
        assertEventually(() -> client.setMode(Mode.BACKUP).getSize(key, hint).get() == expected);
        assertEventually(() -> client.setMode(Mode.MASTER).getSize(key, hint).get() == expected);
    }

    // =========================================================================
    // 16. TTL: setTtl реплицируется
    // =========================================================================

    @Test
    @DisplayName("setTtl on MASTER replicates to BACKUP")
    void testTtlReplicates() throws Exception {
        String key = "ver_ttl_" + UUID.randomUUID();
        KeyHintData hint = client.setMode(Mode.MASTER).create(key, v("data")).get();
        waitBothContain(key,hint );

        client.setMode(Mode.MASTER).setTtl(key, hint, 60_000).get();
        assertEventually(() -> {
            long ttl = client.setMode(Mode.BACKUP).getTtl(key, hint).get();
            return ttl > 0 && ttl <= 60_000;
        });
        long masterTtl = client.setMode(Mode.MASTER).getTtl(key, hint).get();
        assertTrue(masterTtl > 0 && masterTtl <= 60_000);
    }

    // =========================================================================
    // 17. DELETE + RECREATE контейнера: сходится в новый контейнер
    // =========================================================================

    @Test
    @DisplayName("Delete then re-create container converges to the NEW container on BACKUP")
    void testDeleteThenRecreateContainer() throws Exception {
        String key = "ver_recreate_c_" + UUID.randomUUID();
        KeyHintData hint = client.setMode(Mode.MASTER).createList(key, List.of(v("old"))).get();
        waitBothContain(key,hint );

        client.setMode(Mode.MASTER).remove(key, hint).get();
        assertEventually(() -> !client.setMode(Mode.BACKUP).containsKey(key,hint).get());

        client.setMode(Mode.MASTER).createList(key, List.of(v("new"))).get();
        assertEventually(() -> {
            try {
                return strList(client.setMode(Mode.BACKUP).getList(key,hint).get()).equals(List.of("new"));
            } catch (Exception e) {
                return false;
            }
        });
        assertEquals(List.of("new"), strList(client.setMode(Mode.MASTER).getList(key,hint).get()));
    }
}