package com.hurricache.client.cluster.stress;

import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Mode;
import com.hurricache.client.intf.Variant;
import com.hurricache.grpc.ContainerType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class SmartClientHashMapStressTest extends BaseClusterStressTest {

    // Удаление с немедленным возвратом элемента: ключ в контейнере есть всегда,
    // NOT_FOUND не возникает (кроме редких честных гонок в MT — они видны в error breakdown)
    private CompletableFuture<Variant> popAndRestore(String key, KeyHintData hint, int i) {
        byte[] ek = ("key_" + (i % CONTAINER_SIZE)).getBytes();
        return client.popInContainer(key, hint, ek)
                .handle((old, ex) -> old)
                .thenCompose(old -> client.putAll(key, hint, List.of(p("key_" + (i % CONTAINER_SIZE))),
                                                  List.of(v("value_" + i)))
                                        .thenApply(x -> old));
    }

    private CompletableFuture<Integer> removeAndRestore(String key, KeyHintData hint, int i) {
        byte[] ek = ("key_" + (i % CONTAINER_SIZE)).getBytes();
        return client.remove(key, hint, ek)
                .handle((removed, ex) -> removed == null ? 0 : removed)
                .thenCompose(removed -> {
                    if (removed != null && removed > 0) {
                        return client.putAll(key, hint, List.of(p("key_" + (i % CONTAINER_SIZE))),
                                             List.of(v("value_" + i)))
                                   .thenApply(x -> removed);
                    }
                    return CompletableFuture.completedFuture(removed);
                });
    }

    private CompletableFuture<Integer> removeByKeyAndRestore(String key, KeyHintData hint, int i) {
        return client.removeFromContainerByKey(key, hint, p("key_" + (i % CONTAINER_SIZE)))
                .handle((removed, ex) -> removed == null ? 0 : removed)
                .thenCompose(removed -> {
                    if (removed != null && removed > 0) {
                        return client.putAll(key, hint, List.of(p("key_" + (i % CONTAINER_SIZE))),
                                             List.of(v("value_" + i)))
                                   .thenApply(x -> removed);
                    }
                    return CompletableFuture.completedFuture(removed);
                });
    }

    private CompletableFuture<Integer> removeByKeyValueAndRestore(String key, KeyHintData hint, int i) {
        return client.removeFromContainerByKeyAndValue(key, hint, ContainerType.MAP,
                                                       List.of(p("key_" + (i % CONTAINER_SIZE))),
                                                       List.of(v("value_" + (i % 100))))
                .handle((removed, ex) -> removed == null ? 0 : removed)
                .thenCompose(removed -> {
                    if (removed != null && removed > 0) {
                        return client.putAll(key, hint, List.of(p("key_" + (i % CONTAINER_SIZE))),
                                             List.of(v("value_" + (i % 100))))
                                   .thenApply(x -> removed);
                    }
                    return CompletableFuture.completedFuture(removed);
                });
    }

    @Test
    @DisplayName("Stress test: putAll")
    void stressTestPutAll() throws Exception {
        String key = "stress_m_put_" + UUID.randomUUID();
        KeyHintData hint = client.createMap(key, generateMapData(CONTAINER_SIZE / 10)).get();
        waitForCondition(() -> client.setMode(Mode.BACKUP).containsKey(key).get()
                && client.setMode(Mode.MASTER).containsKey(key).get());

        runWarmup(WARMUP_ITERATIONS, i -> client.putAll(key, hint, List.of(p("new_key_" + i)), List.of(v("new_value_" + i))));
        runMetricsBenchmark("Map.putAll", CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> client.putAll(key, hint, List.of(p("new_key_" + i)), List.of(v("new_value_" + i))));
    }

    @Test
    @DisplayName("Stress test: getInContainer")
    void stressTestGetInContainer() throws Exception {
        String key = "stress_m_get_in_" + UUID.randomUUID();
        KeyHintData hint = client.createMap(key, generateMapData(CONTAINER_SIZE)).get();
        waitForCondition(() -> client.setMode(Mode.BACKUP).containsKey(key).get()
                && client.setMode(Mode.MASTER).containsKey(key).get());

        runWarmup(WARMUP_ITERATIONS, i -> client.getInContainer(key, hint, ("key_" + (i % CONTAINER_SIZE)).getBytes()));
        runMetricsBenchmark("Map.getInContainer", CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> client.getInContainer(key, hint, ("key_" + (i % CONTAINER_SIZE)).getBytes()));
    }

    @Test
    @DisplayName("Stress test: getInContainer (no hint)")
    void stressTestGetInContainerNoHint() throws Exception {
        String key = "stress_m_get_in_nh_" + UUID.randomUUID();
        client.createMap(key, generateMapData(CONTAINER_SIZE)).get();
        waitForCondition(() -> client.setMode(Mode.BACKUP).containsKey(key).get()
                && client.setMode(Mode.MASTER).containsKey(key).get());

        runWarmup(WARMUP_ITERATIONS, i -> client.getInContainer(key, ("key_" + (i % CONTAINER_SIZE)).getBytes()));
        runMetricsBenchmark("Map.getInContainer (no hint)", CONTAINER_SIZE, BENCHMARK_ITERATIONS,
                            i -> client.getInContainer(key, ("key_" + (i % CONTAINER_SIZE)).getBytes()));
    }

    @Test
    @DisplayName("Stress test: update")
    void stressTestUpdate() throws Exception {
        String key = "stress_m_update_" + UUID.randomUUID();
        KeyHintData hint = client.createMap(key, generateMapData(CONTAINER_SIZE)).get();
        waitForCondition(() -> client.setMode(Mode.BACKUP).containsKey(key).get()
                && client.setMode(Mode.MASTER).containsKey(key).get());

        runWarmup(WARMUP_ITERATIONS, i -> client.update(key, hint, ("key_" + (i % CONTAINER_SIZE)).getBytes(), v("new_value_" + i)));
        runMetricsBenchmark("Map.update", CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> client.update(key, hint, ("key_" + (i % CONTAINER_SIZE)).getBytes(), v("new_value_" + i)));
    }

    @Test
    @DisplayName("Stress test: popInContainer")
    void stressTestPopInContainer() throws Exception {
        String key = "stress_m_pop_" + UUID.randomUUID();
        KeyHintData hint = client.createMap(key, generateMapData(CONTAINER_SIZE)).get();
        System.out.println("Created map: " + key + " with hint: " + hint);
        waitForCondition(() -> client.setMode(Mode.BACKUP).containsKey(key).get()
                && client.setMode(Mode.MASTER).containsKey(key).get());
        waitForCondition(() -> {
            return client.setMode(Mode.MASTER).getSize(key, hint).get() == CONTAINER_SIZE && client.setMode(Mode.BACKUP).getSize(key, hint).get() == CONTAINER_SIZE;
        });

        runWarmup(WARMUP_ITERATIONS, i -> {
            if (i % 10 == 0) client.putAll(key, hint, List.of(p("key_" + i)), List.of(v("value_" + i))).get();
            return null;
        });

        runMetricsBenchmark("Map.popInContainer", CONTAINER_SIZE, BENCHMARK_ITERATIONS,
                            i -> popAndRestore(key, hint, i));
    }

    @Test
    @DisplayName("Stress test: remove (elementKey)")
    void stressTestRemove() throws Exception {
        String key = "stress_m_remove_" + UUID.randomUUID();
        KeyHintData hint = client.createMap(key, generateMapData(CONTAINER_SIZE)).get();
        waitForCondition(() -> {
            return client.setMode(Mode.MASTER).getSize(key, hint).get() == CONTAINER_SIZE && client.setMode(Mode.BACKUP).getSize(key, hint).get() == CONTAINER_SIZE;
        });

        runWarmup(WARMUP_ITERATIONS, i -> removeAndRestore(key, hint, i));

        runMetricsBenchmark("Map.remove", CONTAINER_SIZE, BENCHMARK_ITERATIONS,
                            i -> removeAndRestore(key, hint, i));
    }

    @Test
    @DisplayName("Stress test: removeFromContainerByKeyAndValue")
    void stressTestRemoveFromContainerByKeyAndValue() throws Exception {
        String key = "stress_m_remove_val_" + UUID.randomUUID();
        Map<com.hurricache.client.intf.Payload, com.hurricache.client.intf.Variant> initialData = new HashMap<>();
        for (int i = 0; i < CONTAINER_SIZE; i++) {
            initialData.put(p("key_" + i), v("value_" + (i % 100)));
        }
        KeyHintData hint = client.createMap(key, initialData).get();
        waitForCondition(() -> client.setMode(Mode.BACKUP).containsKey(key).get()
                && client.setMode(Mode.MASTER).containsKey(key).get());

        runWarmup(WARMUP_ITERATIONS, i -> removeByKeyValueAndRestore(key, hint, i));

        runMetricsBenchmark("Map.removeFromContainerByKeyAndValue", CONTAINER_SIZE, BENCHMARK_ITERATIONS,
                            i -> removeByKeyValueAndRestore(key, hint, i));
    }

    @Test
    @DisplayName("Stress test: removeFromContainerByKey")
    void stressTestRemoveFromContainerByKey() throws Exception {
        String key = "stress_m_remove_key_" + UUID.randomUUID();
        KeyHintData hint = client.createMap(key, generateMapData(CONTAINER_SIZE)).get();
        System.out.println("Created map: " + key + " with hint: " + hint);
        waitForCondition(() -> {
            return client.setMode(Mode.MASTER).getSize(key, hint).get() == CONTAINER_SIZE && client.setMode(Mode.BACKUP).getSize(key, hint).get() == CONTAINER_SIZE;
        });

        runWarmup(WARMUP_ITERATIONS, i -> removeByKeyAndRestore(key, hint, i));

        runMetricsBenchmark("Map.removeFromContainerByKey", CONTAINER_SIZE, BENCHMARK_ITERATIONS,
                            i -> removeByKeyAndRestore(key, hint, i));
    }

    @Test
    @DisplayName("Stress test: getSize")
    void stressTestMapGetSize() throws Exception {
        String key = "stress_m_size_" + UUID.randomUUID();
        KeyHintData hint = client.createMap(key, generateMapData(CONTAINER_SIZE)).get();
        waitForCondition(() -> client.setMode(Mode.BACKUP).containsKey(key).get()
                && client.setMode(Mode.MASTER).containsKey(key).get());

        runWarmup(WARMUP_ITERATIONS, i -> client.getSize(key, hint));
        runMetricsBenchmark("Map.getSize", CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> client.getSize(key, hint));
    }

    // MULTI-THREADED TESTS

    @Test
    @DisplayName("Stress test: getInContainer (multi-threaded)")
    void stressTestGetInContainerMultiThreaded() throws Exception {
        String key = "stress_m_mt_" + UUID.randomUUID();
        KeyHintData hint = client.createMap(key, generateMapData(CONTAINER_SIZE)).get();
        waitForCondition(() -> client.setMode(Mode.BACKUP).containsKey(key).get()
                && client.setMode(Mode.MASTER).containsKey(key).get());

        runMetricsBenchmarkMT("Map.getInContainer", CONTAINER_SIZE, BENCHMARK_ITERATIONS, THREAD_COUNT,
                                  (threadId, i) -> client.getInContainer(key, hint, ("key_" + (i % CONTAINER_SIZE)).getBytes()));
    }

    @Test
    @DisplayName("Stress test: putAll (multi-threaded)")
    void stressTestPutAllMultiThreaded() throws Exception {
        String key = "stress_m_mt_put_" + UUID.randomUUID();
        KeyHintData hint = client.createMap(key, generateMapData(CONTAINER_SIZE / 10)).get();
        waitForCondition(() -> client.setMode(Mode.BACKUP).containsKey(key).get()
                && client.setMode(Mode.MASTER).containsKey(key).get());

        runMetricsBenchmarkMT("Map.putAll", CONTAINER_SIZE, BENCHMARK_ITERATIONS, THREAD_COUNT,
                                  (threadId, i) -> client.putAll(key, hint, List.of(p("new_key_" + i)), List.of(v("new_value_" + i))));
    }

    @Test
    @DisplayName("Stress test: update (multi-threaded)")
    void stressTestUpdateMultiThreaded() throws Exception {
        String key = "stress_m_mt_update_" + UUID.randomUUID();
        KeyHintData hint = client.createMap(key, generateMapData(CONTAINER_SIZE)).get();
        waitForCondition(() -> client.setMode(Mode.BACKUP).containsKey(key).get()
                && client.setMode(Mode.MASTER).containsKey(key).get());

        runMetricsBenchmarkMT("Map.update", CONTAINER_SIZE, BENCHMARK_ITERATIONS, THREAD_COUNT,
                                  (threadId, i) -> client.update(key, hint, ("key_" + (i % CONTAINER_SIZE)).getBytes(), v("new_value_" + i)));
    }

    @Test
    @DisplayName("Stress test: popInContainer (multi-threaded)")
    void stressTestPopInContainerMultiThreaded() throws Exception {
        String key = "stress_m_mt_pop_" + UUID.randomUUID();
        KeyHintData hint = client.createMap(key, generateMapData(CONTAINER_SIZE)).get();
        waitForCondition(() -> {
            return client.setMode(Mode.MASTER).getSize(key, hint).get() == CONTAINER_SIZE && client.setMode(Mode.BACKUP).getSize(key, hint).get() == CONTAINER_SIZE;
        });

        runMetricsBenchmarkMT("Map.popInContainer", CONTAINER_SIZE, BENCHMARK_ITERATIONS, THREAD_COUNT,
                              (threadId, i) -> popAndRestore(key, hint, i));
    }

    @Test
    @DisplayName("Stress test: remove (multi-threaded)")
    void stressTestRemoveMultiThreaded() throws Exception {
        String key = "stress_m_mt_remove_" + UUID.randomUUID();
        KeyHintData hint = client.createMap(key, generateMapData(CONTAINER_SIZE)).get();

        waitForCondition(() -> {
            return client.setMode(Mode.MASTER).getSize(key, hint).get() == CONTAINER_SIZE && client.setMode(Mode.BACKUP).getSize(key, hint).get() == CONTAINER_SIZE;
        });

        runMetricsBenchmarkMT("Map.remove", CONTAINER_SIZE, BENCHMARK_ITERATIONS, THREAD_COUNT,
                              (threadId, i) -> removeAndRestore(key, hint, i));
    }

    @Test
    @DisplayName("Stress test: removeFromContainerByKeyAndValue (multi-threaded)")
    void stressTestRemoveFromContainerByKeyAndValueMultiThreaded() throws Exception {
        String key = "stress_m_mt_remove_val_" + UUID.randomUUID();
        Map<com.hurricache.client.intf.Payload, com.hurricache.client.intf.Variant> initialData = new HashMap<>();
        for (int i = 0; i < CONTAINER_SIZE; i++) {
            initialData.put(p("key_" + i), v("value_" + (i % 100)));
        }
        KeyHintData hint = client.createMap(key, initialData).get();
        System.out.println("Created map: " + key + " with hint: " + hint);
        waitForCondition(() -> client.setMode(Mode.BACKUP).containsKey(key).get()
                && client.setMode(Mode.MASTER).containsKey(key).get());

        runMetricsBenchmarkMT("Map.removeFromContainerByKeyAndValue", CONTAINER_SIZE, BENCHMARK_ITERATIONS, THREAD_COUNT,
                              (threadId, i) -> removeByKeyValueAndRestore(key, hint, i));
    }

    @Test
    @DisplayName("Stress test: removeFromContainerByKey (multi-threaded)")
    void stressTestRemoveFromContainerByKeyMultiThreaded() throws Exception {
        String key = "stress_m_mt_remove_key_" + UUID.randomUUID();
        KeyHintData hint = client.createMap(key, generateMapData(CONTAINER_SIZE)).get();
        waitForCondition(() -> {
            return client.setMode(Mode.MASTER).getSize(key, hint).get() == CONTAINER_SIZE && client.setMode(Mode.BACKUP).getSize(key, hint).get() == CONTAINER_SIZE;
        });

        runMetricsBenchmarkMT("Map.removeFromContainerByKey", CONTAINER_SIZE, BENCHMARK_ITERATIONS, THREAD_COUNT,
                              (threadId, i) -> removeByKeyAndRestore(key, hint, i));
    }
}