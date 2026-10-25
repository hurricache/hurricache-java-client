package com.hurricache.client.cluster.stress;

import com.hurricache.client.intf.KeyHintData;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

public class SmartClientVectorStressTest extends BaseClusterStressTest {

    private static final int VECTOR_CONTAINER_SIZE = 10000;

    @Test
    @DisplayName("Stress test: getVector")
    void stressTestGetVector() throws Exception {
        String key = "stress_v_get_" + UUID.randomUUID();
        KeyHintData hint = client.createVector(key, generateListData(VECTOR_CONTAINER_SIZE)).get();
        Thread.sleep(500);

        runWarmup(WARMUP_ITERATIONS, i -> client.getVector(key, hint));
        runBenchmark("Vector.getVector", VECTOR_CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> client.getVector(key, hint));
    }

    @Test
    @DisplayName("Stress test: getAt")
    void stressTestGetAt() throws Exception {
        String key = "stress_v_at_" + UUID.randomUUID();
        KeyHintData hint = client.createVector(key, generateListData(VECTOR_CONTAINER_SIZE)).get();
        Thread.sleep(500);

        runWarmup(WARMUP_ITERATIONS, i -> client.getAt(key, hint, i % VECTOR_CONTAINER_SIZE));
        runBenchmark("Vector.getAt", VECTOR_CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> client.getAt(key, hint, i % VECTOR_CONTAINER_SIZE));
    }

    @Test
    @DisplayName("Stress test: popAt")
    void stressTestPopAt() throws Exception {
        String key = "stress_v_pop_" + UUID.randomUUID();
        KeyHintData hint = client.createVector(key, generateListData(VECTOR_CONTAINER_SIZE)).get();
        Thread.sleep(500);

        runWarmup(WARMUP_ITERATIONS, i -> client.popAt(key, hint, i % VECTOR_CONTAINER_SIZE));
        runBenchmark("Vector.popAt", VECTOR_CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> client.popAt(key, hint, i % VECTOR_CONTAINER_SIZE));
    }

    @Test
    @DisplayName("Stress test: insertAt")
    void stressTestInsertAt() throws Exception {
        String key = "stress_v_insert_" + UUID.randomUUID();
        KeyHintData hint = client.createVector(key, generateListData(VECTOR_CONTAINER_SIZE)).get();
        Thread.sleep(500);

        runWarmup(WARMUP_ITERATIONS, i -> client.insertAt(key, hint, List.of(v("new_" + i)), i % VECTOR_CONTAINER_SIZE));
        runBenchmark("Vector.insertAt", VECTOR_CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> client.insertAt(key, hint, List.of(v("new_" + i)), i % VECTOR_CONTAINER_SIZE));
    }

    @Test
    @DisplayName("Stress test: removeAt")
    void stressTestRemoveAt() throws Exception {
        String key = "stress_v_remove_" + UUID.randomUUID();
        KeyHintData hint = client.createVector(key, generateListData(VECTOR_CONTAINER_SIZE)).get();
        Thread.sleep(500);

        runWarmup(WARMUP_ITERATIONS, i -> client.removeAt(key, hint, i % VECTOR_CONTAINER_SIZE));
        runBenchmark("Vector.removeAt", VECTOR_CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> client.removeAt(key, hint, i % VECTOR_CONTAINER_SIZE));
    }

    @Test
    @DisplayName("Stress test: getSize")
    void stressTestVectorGetSize() throws Exception {
        String key = "stress_v_size_" + UUID.randomUUID();
        KeyHintData hint = client.createVector(key, generateListData(VECTOR_CONTAINER_SIZE)).get();
        Thread.sleep(500);

        runWarmup(WARMUP_ITERATIONS, i -> client.getSize(key, hint));
        runBenchmark("Vector.getSize", VECTOR_CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> client.getSize(key, hint));
    }

    // MULTI-THREADED TESTS

    @Test
    @DisplayName("Stress test: getAt (multi-threaded)")
    void stressTestGetAtMultiThreaded() throws Exception {
        String key = "stress_v_mt_" + UUID.randomUUID();
        KeyHintData hint = client.createVector(key, generateListData(VECTOR_CONTAINER_SIZE)).get();
        Thread.sleep(500);

        runMultiThreadedBenchmark("Vector.getAt", VECTOR_CONTAINER_SIZE, BENCHMARK_ITERATIONS, THREAD_COUNT,
                                  (threadId, i) -> client.getAt(key, hint, i % VECTOR_CONTAINER_SIZE));
    }

    @Test
    @DisplayName("Stress test: popAt (multi-threaded)")
    void stressTestPopAtMultiThreaded() throws Exception {
        String key = "stress_v_mt_pop_" + UUID.randomUUID();
        KeyHintData hint = client.createVector(key, generateListData(VECTOR_CONTAINER_SIZE)).get();
        Thread.sleep(500);

        runMultiThreadedBenchmark("Vector.popAt", VECTOR_CONTAINER_SIZE, BENCHMARK_ITERATIONS, THREAD_COUNT, (threadId, i) -> {
            var future = client.popAt(key, hint, i % VECTOR_CONTAINER_SIZE);
            if (i % 10 == 0) client.pushTail(key, hint, List.of(v("item_mt_" + i)));
            return future;
        });
    }

    @Test
    @DisplayName("Stress test: insertAt (multi-threaded)")
    void stressTestVectorInsertAtMultiThreaded() throws Exception {
        String key = "stress_v_mt_insert_" + UUID.randomUUID();
        KeyHintData hint = client.createVector(key, generateListData(VECTOR_CONTAINER_SIZE)).get();
        Thread.sleep(500);

        runMultiThreadedBenchmark("Vector.insertAt", VECTOR_CONTAINER_SIZE, BENCHMARK_ITERATIONS, THREAD_COUNT,
                                  (threadId, i) -> client.insertAt(key, hint, List.of(v("new_" + i)), i % VECTOR_CONTAINER_SIZE));
    }

    @Test
    @DisplayName("Stress test: removeAt (multi-threaded)")
    void stressTestVectorRemoveAtMultiThreaded() throws Exception {
        String key = "stress_v_mt_remove_" + UUID.randomUUID();
        KeyHintData hint = client.createVector(key, generateListData(VECTOR_CONTAINER_SIZE)).get();
        Thread.sleep(500);

        runMultiThreadedBenchmark("Vector.removeAt", VECTOR_CONTAINER_SIZE, BENCHMARK_ITERATIONS, THREAD_COUNT, (threadId, i) -> {
            var future = client.removeAt(key, hint, i % VECTOR_CONTAINER_SIZE);
            if (i % 10 == 0) client.pushTail(key, hint, List.of(v("item_mt_" + i)));
            return future;
        });
    }
}