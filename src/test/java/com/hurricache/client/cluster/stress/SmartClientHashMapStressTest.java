package com.hurricache.client.cluster.stress;

import com.hurricache.client.intf.KeyHintData;
import com.hurricache.grpc.ContainerType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class SmartClientHashMapStressTest extends BaseClusterStressTest {

    @Test
    @DisplayName("Stress test: getMap")
    void stressTestGetMap() throws Exception {
        String key = "stress_m_get_" + UUID.randomUUID();
        KeyHintData hint = client.createMap(key, generateMapData(CONTAINER_SIZE)).get();
        Thread.sleep(1000);

        runWarmup(WARMUP_ITERATIONS, i -> client.getMap(key, hint));
        runBenchmark("Map.getMap", CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> client.getMap(key, hint));
    }

    @Test
    @DisplayName("Stress test: putAll")
    void stressTestPutAll() throws Exception {
        String key = "stress_m_put_" + UUID.randomUUID();
        KeyHintData hint = client.createMap(key, generateMapData(CONTAINER_SIZE / 10)).get();
        Thread.sleep(1000);

        runWarmup(WARMUP_ITERATIONS, i -> client.putAll(key, hint, List.of(p("new_key_" + i)), List.of(v("new_value_" + i))));
        runBenchmark("Map.putAll", CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> client.putAll(key, hint, List.of(p("new_key_" + i)), List.of(v("new_value_" + i))));
    }

    @Test
    @DisplayName("Stress test: getInContainer")
    void stressTestGetInContainer() throws Exception {
        String key = "stress_m_get_in_" + UUID.randomUUID();
        KeyHintData hint = client.createMap(key, generateMapData(CONTAINER_SIZE)).get();
        Thread.sleep(1000);

        runWarmup(WARMUP_ITERATIONS, i -> client.getInContainer(key, hint, ("key_" + (i % CONTAINER_SIZE)).getBytes()));
        runBenchmark("Map.getInContainer", CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> client.getInContainer(key, hint, ("key_" + (i % CONTAINER_SIZE)).getBytes()));
    }

    @Test
    @DisplayName("Stress test: update")
    void stressTestUpdate() throws Exception {
        String key = "stress_m_update_" + UUID.randomUUID();
        KeyHintData hint = client.createMap(key, generateMapData(CONTAINER_SIZE)).get();
        Thread.sleep(1000);

        runWarmup(WARMUP_ITERATIONS, i -> client.update(key, hint, ("key_" + (i % CONTAINER_SIZE)).getBytes(), v("new_value_" + i)));
        runBenchmark("Map.update", CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> client.update(key, hint, ("key_" + (i % CONTAINER_SIZE)).getBytes(), v("new_value_" + i)));
    }

    @Test
    @DisplayName("Stress test: popInContainer")
    void stressTestPopInContainer() throws Exception {
        String key = "stress_m_pop_" + UUID.randomUUID();
        KeyHintData hint = client.createMap(key, generateMapData(CONTAINER_SIZE)).get();
        System.out.println("Created map: " + key + " with hint: " + hint);
        Thread.sleep(5000);

        runWarmup(WARMUP_ITERATIONS, i -> {
            if (i % 10 == 0) client.putAll(key, hint, List.of(p("key_" + i)), List.of(v("value_" + i))).get();
            return null;
        });

        runBenchmark("Map.popInContainer", CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> {
            var future = client.popInContainer(key, hint, ("key_" + (i % CONTAINER_SIZE)).getBytes());
            if (i % 10 == 0) {
                client.putAll(key, hint, List.of(p("key_" + i)), List.of(v("value_" + i)));
            }
            return future;
        });
    }

    @Test
    @DisplayName("Stress test: remove (elementKey)")
    void stressTestRemove() throws Exception {
        String key = "stress_m_remove_" + UUID.randomUUID();
        KeyHintData hint = client.createMap(key, generateMapData(CONTAINER_SIZE)).get();
        Thread.sleep(1000);

        runWarmup(WARMUP_ITERATIONS, i -> {
            client.remove(key, hint, ("key_" + (i % CONTAINER_SIZE)).getBytes()).get();
            if (i % 10 == 0) client.putAll(key, hint, List.of(p("key_" + i)), List.of(v("value_" + i))).get();
            return null;
        });

        runBenchmark("Map.remove", CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> {
            var future = client.remove(key, hint, ("key_" + (i % CONTAINER_SIZE)).getBytes());
            if (i % 10 == 0) {
                client.putAll(key, hint, List.of(p("key_" + i)), List.of(v("value_" + i)));
            }
            return future;
        });
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
        Thread.sleep(1000);

        runWarmup(WARMUP_ITERATIONS, i -> {
            client.removeFromContainerByKeyAndValue(key, hint, ContainerType.MAP, List.of(p("key_" + i)), List.of(v("value_" + (i % 100)))).get();
            if (i % 10 == 0) client.putAll(key, hint, List.of(p("key_" + i)), List.of(v("value_" + i))).get();
            return null;
        });

        runBenchmark("Map.removeFromContainerByKeyAndValue", CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> {
            var future = client.removeFromContainerByKeyAndValue(key, hint, ContainerType.MAP, List.of(p("key_" + i)), List.of(v("value_" + (i % 100))));
            if (i % 10 == 0) {
                client.putAll(key, hint, List.of(p("key_" + i)), List.of(v("value_" + i)));
            }
            return future;
        });
    }

    @Test
    @DisplayName("Stress test: removeFromContainerByKey")
    void stressTestRemoveFromContainerByKey() throws Exception {
        String key = "stress_m_remove_key_" + UUID.randomUUID();
        KeyHintData hint = client.createMap(key, generateMapData(CONTAINER_SIZE)).get();
        System.out.println("Created map: " + key + " with hint: " + hint);
        Thread.sleep(1000);

        runWarmup(WARMUP_ITERATIONS, i -> {
            client.removeFromContainerByKey(key, hint, p("key_" + (i % CONTAINER_SIZE))).get();
            if (i % 10 == 0) client.putAll(key, hint, List.of(p("key_" + i)), List.of(v("value_" + i))).get();
            return null;
        });

        runBenchmark("Map.removeFromContainerByKey", CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> {
            var future = client.removeFromContainerByKey(key, hint, p("key_" + (i % CONTAINER_SIZE)));
            if (i % 10 == 0) {
                client.putAll(key, hint, List.of(p("key_" + i)), List.of(v("value_" + i)));
            }
            return future;
        });
    }

    @Test
    @DisplayName("Stress test: getSize")
    void stressTestMapGetSize() throws Exception {
        String key = "stress_m_size_" + UUID.randomUUID();
        KeyHintData hint = client.createMap(key, generateMapData(CONTAINER_SIZE)).get();
        Thread.sleep(1000);

        runWarmup(WARMUP_ITERATIONS, i -> client.getSize(key, hint));
        runBenchmark("Map.getSize", CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> client.getSize(key, hint));
    }

    // MULTI-THREADED TESTS

    @Test
    @DisplayName("Stress test: getInContainer (multi-threaded)")
    void stressTestGetInContainerMultiThreaded() throws Exception {
        String key = "stress_m_mt_" + UUID.randomUUID();
        KeyHintData hint = client.createMap(key, generateMapData(CONTAINER_SIZE)).get();
        Thread.sleep(1000);

        runMultiThreadedBenchmark("Map.getInContainer", CONTAINER_SIZE, BENCHMARK_ITERATIONS, THREAD_COUNT,
                                  (threadId, i) -> client.getInContainer(key, hint, ("key_" + (i % CONTAINER_SIZE)).getBytes()));
    }

    @Test
    @DisplayName("Stress test: putAll (multi-threaded)")
    void stressTestPutAllMultiThreaded() throws Exception {
        String key = "stress_m_mt_put_" + UUID.randomUUID();
        KeyHintData hint = client.createMap(key, generateMapData(CONTAINER_SIZE / 10)).get();
        Thread.sleep(1000);

        runMultiThreadedBenchmark("Map.putAll", CONTAINER_SIZE, BENCHMARK_ITERATIONS, THREAD_COUNT,
                                  (threadId, i) -> client.putAll(key, hint, List.of(p("new_key_" + i)), List.of(v("new_value_" + i))));
    }

    @Test
    @DisplayName("Stress test: update (multi-threaded)")
    void stressTestUpdateMultiThreaded() throws Exception {
        String key = "stress_m_mt_update_" + UUID.randomUUID();
        KeyHintData hint = client.createMap(key, generateMapData(CONTAINER_SIZE)).get();
        Thread.sleep(1000);

        runMultiThreadedBenchmark("Map.update", CONTAINER_SIZE, BENCHMARK_ITERATIONS, THREAD_COUNT,
                                  (threadId, i) -> client.update(key, hint, ("key_" + (i % CONTAINER_SIZE)).getBytes(), v("new_value_" + i)));
    }

    @Test
    @DisplayName("Stress test: popInContainer (multi-threaded)")
    void stressTestPopInContainerMultiThreaded() throws Exception {
        String key = "stress_m_mt_pop_" + UUID.randomUUID();
        KeyHintData hint = client.createMap(key, generateMapData(CONTAINER_SIZE)).get();
        Thread.sleep(1000);

        runMultiThreadedBenchmark("Map.popInContainer", CONTAINER_SIZE, BENCHMARK_ITERATIONS, THREAD_COUNT, (threadId, i) -> {
            var future = client.popInContainer(key, hint, ("key_" + (i % CONTAINER_SIZE)).getBytes());
            if (i % 10 == 0) {
                client.putAll(key, hint, List.of(p("key_" + i)), List.of(v("value_" + i)));
            }
            return future;
        });
    }

    @Test
    @DisplayName("Stress test: remove (multi-threaded)")
    void stressTestRemoveMultiThreaded() throws Exception {
        String key = "stress_m_mt_remove_" + UUID.randomUUID();
        KeyHintData hint = client.createMap(key, generateMapData(CONTAINER_SIZE)).get();
        Thread.sleep(1000);

        runMultiThreadedBenchmark("Map.remove", CONTAINER_SIZE, BENCHMARK_ITERATIONS, THREAD_COUNT, (threadId, i) -> {
            var future = client.remove(key, hint, ("key_" + (i % CONTAINER_SIZE)).getBytes());
            if (i % 10 == 0) {
                client.putAll(key, hint, List.of(p("key_" + i)), List.of(v("value_" + i)));
            }
            return future;
        });
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
        Thread.sleep(1000);

        runMultiThreadedBenchmark("Map.removeFromContainerByKeyAndValue", CONTAINER_SIZE, BENCHMARK_ITERATIONS, THREAD_COUNT, (threadId, i) -> {
            var future = client.removeFromContainerByKeyAndValue(key, hint, ContainerType.MAP, List.of(p("key_" + i)), List.of(v("value_" + (i % 100))));
            if (i % 10 == 0) {
                client.putAll(key, hint, List.of(p("key_" + i)), List.of(v("value_" + i)));
            }
            return future;
        });
    }

    @Test
    @DisplayName("Stress test: removeFromContainerByKey (multi-threaded)")
    void stressTestRemoveFromContainerByKeyMultiThreaded() throws Exception {
        String key = "stress_m_mt_remove_key_" + UUID.randomUUID();
        KeyHintData hint = client.createMap(key, generateMapData(CONTAINER_SIZE)).get();
        Thread.sleep(1000);

        runMultiThreadedBenchmark("Map.removeFromContainerByKey", CONTAINER_SIZE, BENCHMARK_ITERATIONS, THREAD_COUNT, (threadId, i) -> {
            var future = client.removeFromContainerByKey(key, hint, p("key_" + (i % CONTAINER_SIZE)));
            if (i % 10 == 0) {
                client.putAll(key, hint, List.of(p("key_" + i)), List.of(v("value_" + i)));
            }
            return future;
        });
    }
}