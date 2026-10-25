package com.hurricache.client.cluster.stress;

import com.hurricache.client.intf.KeyHintData;
import com.hurricache.grpc.ContainerType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

public class SmartClientSetStressTest extends BaseClusterStressTest {

    @Test
    @DisplayName("Stress test: getSet")
    void stressTestGetSet() throws Exception {
        String key = "stress_s_get_" + UUID.randomUUID();
        KeyHintData hint = client.createSet(key, generateListData(CONTAINER_SIZE)).get();
        Thread.sleep(500);

        runWarmup(WARMUP_ITERATIONS, i -> client.getSet(key, hint));
        runBenchmark("Set.getSet", CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> client.getSet(key, hint));
    }

    @Test
    @DisplayName("Stress test: addElementUnordered")
    void stressTestAddElementUnordered() throws Exception {
        String key = "stress_s_add_" + UUID.randomUUID();
        KeyHintData hint = client.createSet(key, generateListData(CONTAINER_SIZE / 10)).get();
        Thread.sleep(500);

        runWarmup(WARMUP_ITERATIONS, i -> client.addElementUnordered(key, hint, List.of(v("new_" + i))));
        runBenchmark("Set.addElementUnordered", CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> client.addElementUnordered(key, hint, List.of(v("new_" + i))));
    }

    @Test
    @DisplayName("Stress test: containsKey")
    void stressTestContainsKey() throws Exception {
        String key = "stress_s_contains_" + UUID.randomUUID();
        KeyHintData hint = client.createSet(key, generateListData(CONTAINER_SIZE)).get();
        Thread.sleep(500);

        runWarmup(WARMUP_ITERATIONS, i -> client.containsKey(key, hint, ("item_" + (i % CONTAINER_SIZE)).getBytes()));
        runBenchmark("Set.containsKey", CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> client.containsKey(key, hint, ("item_" + (i % CONTAINER_SIZE)).getBytes()));
    }

    @Test
    @DisplayName("Stress test: removeFromContainerByKeyAndValue")
    void stressTestRemoveFromContainerByKeyAndValue() throws Exception {
        String key = "stress_s_remove_val_" + UUID.randomUUID();
        KeyHintData hint = client.createSet(key, generateListData(CONTAINER_SIZE)).get();
        Thread.sleep(500);

        runWarmup(WARMUP_ITERATIONS, i -> {
            client.removeFromContainerByKeyAndValue(key, hint, ContainerType.SET, List.of(), List.of(v("item_" + (i % CONTAINER_SIZE)))).get();
            if (i % 10 == 0) client.addElementUnordered(key, hint, List.of(v("item_" + i))).get();
            return null;
        });

        runBenchmark("Set.removeFromContainerByKeyAndValue", CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> {
            var future = client.removeFromContainerByKeyAndValue(key, hint, ContainerType.SET, List.of(), List.of(v("item_" + (i % CONTAINER_SIZE))));
            if (i % 10 == 0) client.addElementUnordered(key, hint, List.of(v("item_" + i)));
            return future;
        });
    }

    @Test
    @DisplayName("Stress test: removeFromContainerByKey")
    void stressTestRemoveFromContainerByKey() throws Exception {
        String key = "stress_s_remove_key_" + UUID.randomUUID();
        KeyHintData hint = client.createSet(key, generateListData(CONTAINER_SIZE)).get();
        Thread.sleep(500);

        runWarmup(WARMUP_ITERATIONS, i -> {
            client.removeFromContainerByKey(key, hint, v("item_" + (i % CONTAINER_SIZE))).get();
            if (i % 10 == 0) client.addElementUnordered(key, hint, List.of(v("item_" + i))).get();
            return null;
        });

        runBenchmark("Set.removeFromContainerByKey", CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> {
            var future = client.removeFromContainerByKey(key, hint, v("item_" + (i % CONTAINER_SIZE)));
            if (i % 10 == 0) client.addElementUnordered(key, hint, List.of(v("item_" + i)));
            return future;
        });
    }

    @Test
    @DisplayName("Stress test: getSize")
    void stressTestSetGetSize() throws Exception {
        String key = "stress_s_size_" + UUID.randomUUID();
        KeyHintData hint = client.createSet(key, generateListData(CONTAINER_SIZE)).get();
        Thread.sleep(500);

        runWarmup(WARMUP_ITERATIONS, i -> client.getSize(key, hint));
        runBenchmark("Set.getSize", CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> client.getSize(key, hint));
    }

    // MULTI-THREADED TESTS

    @Test
    @DisplayName("Stress test: containsKey (multi-threaded)")
    void stressTestContainsKeyMultiThreaded() throws Exception {
        String key = "stress_s_mt_" + UUID.randomUUID();
        KeyHintData hint = client.createSet(key, generateListData(CONTAINER_SIZE)).get();
        Thread.sleep(500);

        runMultiThreadedBenchmark("Set.containsKey", CONTAINER_SIZE, BENCHMARK_ITERATIONS, THREAD_COUNT,
                                  (threadId, i) -> client.containsKey(key, hint, ("item_" + (i % CONTAINER_SIZE)).getBytes()));
    }

    @Test
    @DisplayName("Stress test: addElementUnordered (multi-threaded)")
    void stressTestAddElementUnorderedMultiThreaded() throws Exception {
        String key = "stress_s_mt_add_" + UUID.randomUUID();
        KeyHintData hint = client.createSet(key, generateListData(CONTAINER_SIZE / 10)).get();
        Thread.sleep(500);

        runMultiThreadedBenchmark("Set.addElementUnordered", CONTAINER_SIZE, BENCHMARK_ITERATIONS, THREAD_COUNT,
                                  (threadId, i) -> client.addElementUnordered(key, hint, List.of(v("new_" + i))));
    }

    @Test
    @DisplayName("Stress test: removeFromContainerByKeyAndValue (multi-threaded)")
    void stressTestRemoveFromContainerByKeyAndValueMultiThreaded() throws Exception {
        String key = "stress_s_mt_remove_val_" + UUID.randomUUID();
        KeyHintData hint = client.createSet(key, generateListData(CONTAINER_SIZE)).get();
        Thread.sleep(500);

        runMultiThreadedBenchmark("Set.removeFromContainerByKeyAndValue", CONTAINER_SIZE, BENCHMARK_ITERATIONS, THREAD_COUNT, (threadId, i) -> {
            var future = client.removeFromContainerByKeyAndValue(key, hint, ContainerType.SET, List.of(), List.of(v("item_" + (i % CONTAINER_SIZE))));
            if (i % 10 == 0) client.addElementUnordered(key, hint, List.of(v("item_" + i)));
            return future;
        });
    }

    @Test
    @DisplayName("Stress test: removeFromContainerByKey (multi-threaded)")
    void stressTestRemoveFromContainerByKeyMultiThreaded() throws Exception {
        String key = "stress_s_mt_remove_key_" + UUID.randomUUID();
        KeyHintData hint = client.createSet(key, generateListData(CONTAINER_SIZE)).get();
        Thread.sleep(500);

        runMultiThreadedBenchmark("Set.removeFromContainerByKey", CONTAINER_SIZE, BENCHMARK_ITERATIONS, THREAD_COUNT, (threadId, i) -> {
            var future = client.removeFromContainerByKey(key, hint, v("item_" + (i % CONTAINER_SIZE)));
            if (i % 10 == 0) client.addElementUnordered(key, hint, List.of(v("item_" + i)));
            return future;
        });
    }
}