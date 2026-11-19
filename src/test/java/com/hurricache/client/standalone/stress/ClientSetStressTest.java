package com.hurricache.client.standalone.stress;

import com.hurricache.client.intf.KeyHintData;
import com.hurricache.grpc.ContainerType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

/**
 * Standalone Set stress test with latency/error metrics.
 * Whole-container getSet intentionally omitted; containsKey hint/no-hint.
 */
public class ClientSetStressTest extends BaseStandaloneStressTest {

    @Test
    @DisplayName("Stress test: addElementUnordered")
    void stressTestAddElementUnordered() throws Exception {
        String key = "s_s_add_" + UUID.randomUUID();
        KeyHintData hint = client.createSet(key, generateListData(CONTAINER_SIZE / 10)).get();
        runMetricsBenchmark("Set.addElementUnordered", CONTAINER_SIZE, BENCHMARK_ITERATIONS,
                            i -> client.addElementUnordered(key, hint, List.of(v("item_" + i))));
    }

    @Test
    @DisplayName("Stress test: containsKey (KeyHint)")
    void stressTestContainsKey() throws Exception {
        String key = "s_s_contains_" + UUID.randomUUID();
        KeyHintData hint = client.createSet(key, generateListData(CONTAINER_SIZE)).get();
        runMetricsBenchmark("Set.containsKey (KeyHint)", CONTAINER_SIZE, BENCHMARK_ITERATIONS,
                            i -> client.containsKey(key, hint, ("item_" + (i % 100)).getBytes()));
    }

    @Test
    @DisplayName("Stress test: containsKey (no hint)")
    void stressTestContainsKeyNoHint() throws Exception {
        String key = "s_s_contains_nh_" + UUID.randomUUID();
        client.createSet(key, generateListData(CONTAINER_SIZE)).get();
        runMetricsBenchmark("Set.containsKey (no hint)", CONTAINER_SIZE, BENCHMARK_ITERATIONS,
                            i -> client.containsKey(key, ("item_" + (i % 100)).getBytes()));
    }

    @Test
    @DisplayName("Stress test: removeFromContainerByKeyAndValue")
    void stressTestRemoveFromContainerByKeyAndValue() throws Exception {
        String key = "s_s_remove_val_" + UUID.randomUUID();
        KeyHintData hint = client.createSet(key, generateListData(CONTAINER_SIZE)).get();
        runMetricsBenchmark("Set.removeFromContainerByKeyAndValue", CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> {
            var future = client.removeFromContainerByKeyAndValue(key, hint, ContainerType.SET,
                                                                 List.of(p("item_" + (i % 100))), List.of(v("item_" + (i % 100))));
            if (i % 10 == 0) client.addElementUnordered(key, hint, List.of(v("item_" + i)));
            return future;
        });
    }

    @Test
    @DisplayName("Stress test: removeFromContainerByKey")
    void stressTestRemoveFromContainerByKey() throws Exception {
        String key = "s_s_remove_key_" + UUID.randomUUID();
        KeyHintData hint = client.createSet(key, generateListData(CONTAINER_SIZE)).get();
        runMetricsBenchmark("Set.removeFromContainerByKey", CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> {
            var future = client.removeFromContainerByKey(key, hint, p("item_" + (i % 100)));
            if (i % 10 == 0) client.addElementUnordered(key, hint, List.of(v("item_" + i)));
            return future;
        });
    }

    @Test
    @DisplayName("Stress test: getSize")
    void stressTestGetSize() throws Exception {
        String key = "s_s_size_" + UUID.randomUUID();
        KeyHintData hint = client.createSet(key, generateListData(CONTAINER_SIZE)).get();
        runMetricsBenchmark("Set.getSize", CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> client.getSize(key, hint));
    }

    @Test
    @DisplayName("Stress test: containsKey (multi-threaded)")
    void stressTestContainsKeyMT() throws Exception {
        String key = "s_s_mt_contains_" + UUID.randomUUID();
        KeyHintData hint = client.createSet(key, generateListData(CONTAINER_SIZE)).get();
        runMetricsBenchmarkMT("Set.containsKey", CONTAINER_SIZE, BENCHMARK_ITERATIONS, THREAD_COUNT,
                              (threadId, i) -> client.containsKey(key, hint, ("item_" + (i % 100)).getBytes()));
    }

    @Test
    @DisplayName("Stress test: addElementUnordered (multi-threaded)")
    void stressTestAddElementUnorderedMT() throws Exception {
        String key = "s_s_mt_add_" + UUID.randomUUID();
        KeyHintData hint = client.createSet(key, generateListData(CONTAINER_SIZE / 10)).get();
        runMetricsBenchmarkMT("Set.addElementUnordered", CONTAINER_SIZE, BENCHMARK_ITERATIONS, THREAD_COUNT,
                              (threadId, i) -> client.addElementUnordered(key, hint, List.of(v("item_" + i))));
    }
}
