package com.hurricache.client.standalone.stress;

import com.hurricache.client.intf.KeyHintData;
import com.hurricache.grpc.ContainerType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

/**
 * Standalone HashMap stress test with latency/error metrics.
 * Whole-container getMap intentionally omitted (no stress value);
 * getInContainer exists in hint and no-hint variants.
 */
public class ClientHashMapStressTest extends BaseStandaloneStressTest {

    @Test
    @DisplayName("Stress test: putAll")
    void stressTestPutAll() throws Exception {
        String key = "s_m_put_" + UUID.randomUUID();
        KeyHintData hint = client.createMap(key, generateMapData(CONTAINER_SIZE / 10)).get();
        runMetricsBenchmark("Map.putAll", CONTAINER_SIZE, BENCHMARK_ITERATIONS,
                            i -> client.putAll(key, hint, List.of(p("new_key_" + i)), List.of(v("new_value_" + i))));
    }

    @Test
    @DisplayName("Stress test: getInContainer (KeyHint)")
    void stressTestGetInContainer() throws Exception {
        String key = "s_m_get_in_" + UUID.randomUUID();
        KeyHintData hint = client.createMap(key, generateMapData(CONTAINER_SIZE)).get();
        runMetricsBenchmark("Map.getInContainer (KeyHint)", CONTAINER_SIZE, BENCHMARK_ITERATIONS,
                            i -> client.getInContainer(key, hint, ("key_" + (i % CONTAINER_SIZE)).getBytes()));
    }

    @Test
    @DisplayName("Stress test: getInContainer (no hint)")
    void stressTestGetInContainerNoHint() throws Exception {
        String key = "s_m_get_in_nh_" + UUID.randomUUID();
        client.createMap(key, generateMapData(CONTAINER_SIZE)).get();
        runMetricsBenchmark("Map.getInContainer (no hint)", CONTAINER_SIZE, BENCHMARK_ITERATIONS,
                            i -> client.getInContainer(key, ("key_" + (i % CONTAINER_SIZE)).getBytes()));
    }

    @Test
    @DisplayName("Stress test: update")
    void stressTestUpdate() throws Exception {
        String key = "s_m_update_" + UUID.randomUUID();
        KeyHintData hint = client.createMap(key, generateMapData(CONTAINER_SIZE)).get();
        runMetricsBenchmark("Map.update", CONTAINER_SIZE, BENCHMARK_ITERATIONS,
                            i -> client.update(key, hint, ("key_" + (i % CONTAINER_SIZE)).getBytes(), v("new_value_" + i)));
    }

    @Test
    @DisplayName("Stress test: popInContainer")
    void stressTestPopInContainer() throws Exception {
        String key = "s_m_pop_" + UUID.randomUUID();
        KeyHintData hint = client.createMap(key, generateMapData(CONTAINER_SIZE)).get();
        runMetricsBenchmark("Map.popInContainer", CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> {
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
        String key = "s_m_remove_" + UUID.randomUUID();
        KeyHintData hint = client.createMap(key, generateMapData(CONTAINER_SIZE)).get();
        runMetricsBenchmark("Map.remove", CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> {
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
        String key = "s_m_remove_val_" + UUID.randomUUID();
        KeyHintData hint = client.createMap(key, generateMapData(CONTAINER_SIZE)).get();
        runMetricsBenchmark("Map.removeFromContainerByKeyAndValue", CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> {
            var future = client.removeFromContainerByKeyAndValue(key, hint, ContainerType.MAP,
                                                                 List.of(p("key_" + (i % CONTAINER_SIZE))), List.of(v("value_" + (i % 100))));
            if (i % 10 == 0) {
                client.putAll(key, hint, List.of(p("key_" + (i % CONTAINER_SIZE))), List.of(v("value_" + (i % 100))));
            }
            return future;
        });
    }

    @Test
    @DisplayName("Stress test: removeFromContainerByKey")
    void stressTestRemoveFromContainerByKey() throws Exception {
        String key = "s_m_remove_key_" + UUID.randomUUID();
        KeyHintData hint = client.createMap(key, generateMapData(CONTAINER_SIZE)).get();
        runMetricsBenchmark("Map.removeFromContainerByKey", CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> {
            var future = client.removeFromContainerByKey(key, hint, p("key_" + (i % CONTAINER_SIZE)));
            if (i % 10 == 0) {
                client.putAll(key, hint, List.of(p("key_" + (i % CONTAINER_SIZE))), List.of(v("value_" + (i % 100))));
            }
            return future;
        });
    }

    @Test
    @DisplayName("Stress test: getSize")
    void stressTestGetSize() throws Exception {
        String key = "s_m_size_" + UUID.randomUUID();
        KeyHintData hint = client.createMap(key, generateMapData(CONTAINER_SIZE)).get();
        runMetricsBenchmark("Map.getSize", CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> client.getSize(key, hint));
    }

    @Test
    @DisplayName("Stress test: getInContainer (multi-threaded)")
    void stressTestGetInContainerMT() throws Exception {
        String key = "s_m_mt_" + UUID.randomUUID();
        KeyHintData hint = client.createMap(key, generateMapData(CONTAINER_SIZE)).get();
        runMetricsBenchmarkMT("Map.getInContainer", CONTAINER_SIZE, BENCHMARK_ITERATIONS, THREAD_COUNT,
                              (threadId, i) -> client.getInContainer(key, hint, ("key_" + (i % CONTAINER_SIZE)).getBytes()));
    }

    @Test
    @DisplayName("Stress test: putAll (multi-threaded)")
    void stressTestPutAllMT() throws Exception {
        String key = "s_m_mt_put_" + UUID.randomUUID();
        KeyHintData hint = client.createMap(key, generateMapData(CONTAINER_SIZE / 10)).get();
        runMetricsBenchmarkMT("Map.putAll", CONTAINER_SIZE, BENCHMARK_ITERATIONS, THREAD_COUNT,
                              (threadId, i) -> client.putAll(key, hint, List.of(p("new_key_" + i)), List.of(v("new_value_" + i))));
    }
}
