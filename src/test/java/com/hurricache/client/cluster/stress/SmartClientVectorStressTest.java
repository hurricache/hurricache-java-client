package com.hurricache.client.cluster.stress;

import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Mode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

public class SmartClientVectorStressTest extends BaseClusterStressTest {
    // pop/remove с немедленным возвратом в хвост: размер вектора стабилен, OUT_OF_RANGE не возникает
    private java.util.concurrent.CompletableFuture<com.hurricache.client.intf.Variant> popAtAndRestore(
            String key, KeyHintData hint, int i) {
        // до CONCURRENCY операций может быть в полёте одновременно — позицию ограничиваем,
        // чтобы даже при максимальной просадке размера не выйти за границы
        int pos = i % (VECTOR_CONTAINER_SIZE - CONCURRENCY);
        return client.popAt(key, hint, pos)
                .thenCompose(old -> client.pushTail(key, hint, java.util.List.of(v("item_" + i)))
                                        .thenApply(x -> old));
    }

    private java.util.concurrent.CompletableFuture<Integer> removeAtAndRestore(
            String key, KeyHintData hint, int i) {
        // до CONCURRENCY операций может быть в полёте одновременно — позицию ограничиваем,
        // чтобы даже при максимальной просадке размера не выйти за границы
        int pos = i % (VECTOR_CONTAINER_SIZE - CONCURRENCY);
        return client.removeAt(key, hint, pos)
                .thenCompose(removed -> removed != null && removed > 0
                        ? client.pushTail(key, hint, java.util.List.of(v("item_" + i)))
                                  .thenApply(x -> removed)
                        : java.util.concurrent.CompletableFuture.completedFuture(removed));
    }


    private static final int VECTOR_CONTAINER_SIZE = 10000;

    @Test
    @DisplayName("Stress test: getAt")
    void stressTestGetAt() throws Exception {
        String key = "stress_v_at_" + UUID.randomUUID();
        KeyHintData hint = client.createVector(key, generateListData(VECTOR_CONTAINER_SIZE)).get();
        waitForCondition(() -> client.setMode(Mode.BACKUP).containsKey(key).get()
                && client.setMode(Mode.MASTER).containsKey(key).get());

        runWarmup(WARMUP_ITERATIONS, i -> client.getAt(key, hint, i % VECTOR_CONTAINER_SIZE));
        runMetricsBenchmark("Vector.getAt", VECTOR_CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> client.getAt(key, hint, i % VECTOR_CONTAINER_SIZE));
    }

    @Test
    @DisplayName("Stress test: getAt (no hint)")
    void stressTestVectorGetAtNoHint() throws Exception {
        String key = "stress_v_get_at_nh_" + UUID.randomUUID();
        client.createVector(key, generateListData(CONTAINER_SIZE)).get();
        waitForCondition(() -> client.setMode(Mode.BACKUP).containsKey(key).get()
                && client.setMode(Mode.MASTER).containsKey(key).get());

        runWarmup(WARMUP_ITERATIONS, i -> client.getAt(key, i % CONTAINER_SIZE));
        runMetricsBenchmark("Vector.getAt (no hint)", CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> client.getAt(key, i % CONTAINER_SIZE));
    }

    @Test
    @DisplayName("Stress test: popAt")
    void stressTestPopAt() throws Exception {
        String key = "stress_v_pop_" + UUID.randomUUID();
        KeyHintData hint = client.createVector(key, generateListData(VECTOR_CONTAINER_SIZE)).get();
        waitForCondition(() -> {
            return client.setMode(Mode.MASTER).getSize(key, hint).get() == VECTOR_CONTAINER_SIZE && client.setMode(Mode.BACKUP).getSize(key, hint).get() == VECTOR_CONTAINER_SIZE;
        });


        runWarmup(WARMUP_ITERATIONS, i -> popAtAndRestore(key, hint, i));
        runMetricsBenchmark("Vector.popAt", VECTOR_CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> popAtAndRestore(key, hint, i));
    }

    @Test
    @DisplayName("Stress test: insertAt")
    void stressTestInsertAt() throws Exception {
        String key = "stress_v_insert_" + UUID.randomUUID();
        KeyHintData hint = client.createVector(key, generateListData(VECTOR_CONTAINER_SIZE)).get();
        waitForCondition(() -> client.setMode(Mode.BACKUP).containsKey(key).get()
                && client.setMode(Mode.MASTER).containsKey(key).get());

        runWarmup(WARMUP_ITERATIONS, i -> client.insertAt(key, hint, List.of(v("new_" + i)), i % VECTOR_CONTAINER_SIZE));
        runMetricsBenchmark("Vector.insertAt", VECTOR_CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> client.insertAt(key, hint, List.of(v("new_" + i)), i % VECTOR_CONTAINER_SIZE));
    }

    @Test
    @DisplayName("Stress test: removeAt")
    void stressTestRemoveAt() throws Exception {
        String key = "stress_v_remove_" + UUID.randomUUID();
        KeyHintData hint = client.createVector(key, generateListData(VECTOR_CONTAINER_SIZE)).get();
        waitForCondition(() -> client.setMode(Mode.BACKUP).containsKey(key).get()
                && client.setMode(Mode.MASTER).containsKey(key).get());

        runWarmup(WARMUP_ITERATIONS, i -> removeAtAndRestore(key, hint, i));
        runMetricsBenchmark("Vector.removeAt", VECTOR_CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> removeAtAndRestore(key, hint, i));
    }

    @Test
    @DisplayName("Stress test: getSize")
    void stressTestVectorGetSize() throws Exception {
        String key = "stress_v_size_" + UUID.randomUUID();
        KeyHintData hint = client.createVector(key, generateListData(VECTOR_CONTAINER_SIZE)).get();
        waitForCondition(() -> client.setMode(Mode.BACKUP).containsKey(key).get()
                && client.setMode(Mode.MASTER).containsKey(key).get());

        runWarmup(WARMUP_ITERATIONS, i -> client.getSize(key, hint));
        runMetricsBenchmark("Vector.getSize", VECTOR_CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> client.getSize(key, hint));
    }

    // MULTI-THREADED TESTS

    @Test
    @DisplayName("Stress test: getAt (multi-threaded)")
    void stressTestGetAtMultiThreaded() throws Exception {
        String key = "stress_v_mt_" + UUID.randomUUID();
        KeyHintData hint = client.createVector(key, generateListData(VECTOR_CONTAINER_SIZE)).get();
        waitForCondition(() -> client.setMode(Mode.BACKUP).containsKey(key).get()
                && client.setMode(Mode.MASTER).containsKey(key).get());

        runMetricsBenchmarkMT("Vector.getAt", VECTOR_CONTAINER_SIZE, BENCHMARK_ITERATIONS, THREAD_COUNT,
                                  (threadId, i) -> client.getAt(key, hint, i % VECTOR_CONTAINER_SIZE));
    }

    @Test
    @DisplayName("Stress test: popAt (multi-threaded)")
    void stressTestPopAtMultiThreaded() throws Exception {
        String key = "stress_v_mt_pop_" + UUID.randomUUID();
        KeyHintData hint = client.createVector(key, generateListData(VECTOR_CONTAINER_SIZE)).get();
        waitForCondition(() -> {
            return client.setMode(Mode.MASTER).getSize(key, hint).get() == VECTOR_CONTAINER_SIZE && client.setMode(Mode.BACKUP).getSize(key, hint).get() == VECTOR_CONTAINER_SIZE;
        });


        runMetricsBenchmarkMT("Vector.popAt", VECTOR_CONTAINER_SIZE, BENCHMARK_ITERATIONS, THREAD_COUNT,
                              (threadId, i) -> popAtAndRestore(key, hint, i));
    }

    @Test
    @DisplayName("Stress test: insertAt (multi-threaded)")
    void stressTestVectorInsertAtMultiThreaded() throws Exception {
        String key = "stress_v_mt_insert_" + UUID.randomUUID();
        KeyHintData hint = client.createVector(key, generateListData(VECTOR_CONTAINER_SIZE)).get();
        waitForCondition(() -> client.setMode(Mode.BACKUP).containsKey(key).get()
                && client.setMode(Mode.MASTER).containsKey(key).get());

        runMetricsBenchmarkMT("Vector.insertAt", VECTOR_CONTAINER_SIZE, BENCHMARK_ITERATIONS, THREAD_COUNT,
                                  (threadId, i) -> client.insertAt(key, hint, List.of(v("new_" + i)), i % VECTOR_CONTAINER_SIZE));
    }

    @Test
    @DisplayName("Stress test: removeAt (multi-threaded)")
    void stressTestVectorRemoveAtMultiThreaded() throws Exception {
        String key = "stress_v_mt_remove_" + UUID.randomUUID();
        KeyHintData hint = client.createVector(key, generateListData(VECTOR_CONTAINER_SIZE)).get();
        waitForCondition(() -> client.setMode(Mode.BACKUP).containsKey(key).get()
                && client.setMode(Mode.MASTER).containsKey(key).get());

        runMetricsBenchmarkMT("Vector.removeAt", VECTOR_CONTAINER_SIZE, BENCHMARK_ITERATIONS, THREAD_COUNT, (threadId, i) -> {
            var future = client.removeAt(key, hint, i % VECTOR_CONTAINER_SIZE);
            if (i % 10 == 0) client.pushTail(key, hint, List.of(v("item_mt_" + i)));
            return future;
        });
    }
}