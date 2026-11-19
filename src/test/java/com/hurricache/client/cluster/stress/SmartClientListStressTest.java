package com.hurricache.client.cluster.stress;

import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Mode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

public class SmartClientListStressTest extends BaseClusterStressTest {
    // pop с немедленным возвратом на противоположный конец: размер списка стабилен
    private java.util.concurrent.CompletableFuture<com.hurricache.client.intf.Variant> popHeadAndRestore(
            String key, KeyHintData hint, int i) {
        return client.popHead(key, hint)
                .thenCompose(old -> client.pushTail(key, hint, java.util.List.of(v("item_" + i)))
                                        .thenApply(x -> old));
    }


    private java.util.concurrent.CompletableFuture<Integer> removeAtAndRestore(
            String key, KeyHintData hint, int i) {
        // см. SmartClientVectorStressTest: ограничение позиции с учётом in-flight операций
        return client.removeAt(key, hint, i % (LIST_CONTAINER_SIZE - CONCURRENCY))
                .thenCompose(removed -> removed != null && removed > 0
                        ? client.pushTail(key, hint, java.util.List.of(v("item_" + i)))
                                  .thenApply(x -> removed)
                        : java.util.concurrent.CompletableFuture.completedFuture(removed));
    }

    private java.util.concurrent.CompletableFuture<com.hurricache.client.intf.Variant> popTailAndRestore(
            String key, KeyHintData hint, int i) {
        return client.popTail(key, hint)
                .thenCompose(old -> client.pushHead(key, hint, java.util.List.of(v("item_" + i)))
                                        .thenApply(x -> old));
    }


    private static final int LIST_CONTAINER_SIZE = 10000;

    @Test
    @DisplayName("Stress test: peekHead")
    void stressTestListPeekHead() throws Exception {
        String key = "stress_l_peek_head_" + UUID.randomUUID();
        KeyHintData hint = client.createList(key, generateListData(LIST_CONTAINER_SIZE)).get();
        System.out.println("Created: " + key + " with hint: " + hint);
        waitForCondition(() -> client.setMode(Mode.BACKUP).containsKey(key).get()
                && client.setMode(Mode.MASTER).containsKey(key).get());

        runWarmup(WARMUP_ITERATIONS, i -> client.peekHead(key, hint));
        runMetricsBenchmark("List.peekHead", LIST_CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> client.peekHead(key, hint));
    }

    @Test
    @DisplayName("Stress test: peekHead (no hint)")
    void stressTestListPeekHeadNoHint() throws Exception {
        String key = "stress_l_peek_head_nh_" + UUID.randomUUID();
        client.createList(key, generateListData(LIST_CONTAINER_SIZE)).get();
        waitForCondition(() -> client.setMode(Mode.BACKUP).containsKey(key).get()
                && client.setMode(Mode.MASTER).containsKey(key).get());

        runWarmup(WARMUP_ITERATIONS, i -> client.peekHead(key));
        runMetricsBenchmark("List.peekHead (no hint)", LIST_CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> client.peekHead(key));
    }

    @Test
    @DisplayName("Stress test: peekTail")
    void stressTestListPeekTail() throws Exception {
        String key = "stress_l_peek_tail_" + UUID.randomUUID();
        KeyHintData hint = client.createList(key, generateListData(LIST_CONTAINER_SIZE)).get();
        System.out.println("Created: " + key + " with hint: " + hint);
        waitForCondition(() -> client.setMode(Mode.BACKUP).containsKey(key).get()
                && client.setMode(Mode.MASTER).containsKey(key).get());

        runWarmup(WARMUP_ITERATIONS, i -> client.peekTail(key, hint));
        runMetricsBenchmark("List.peekTail", LIST_CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> client.peekTail(key, hint));
    }

    @Test
    @DisplayName("Stress test: pushTail")
    void stressTestListPushTail() throws Exception {
        String key = "stress_l_push_tail_" + UUID.randomUUID();
        KeyHintData hint = client.createList(key, generateListData(LIST_CONTAINER_SIZE / 10)).get();
        waitForCondition(() -> client.setMode(Mode.BACKUP).containsKey(key).get()
                && client.setMode(Mode.MASTER).containsKey(key).get());

        runWarmup(WARMUP_ITERATIONS, i -> client.pushTail(key, hint, List.of(v("item_" + i))));
        runMetricsBenchmark("List.pushTail", LIST_CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> client.pushTail(key, hint, List.of(v("item_" + i))));
    }

    @Test
    @DisplayName("Stress test: pushHead")
    void stressTestListPushHead() throws Exception {
        String key = "stress_l_push_head_" + UUID.randomUUID();
        KeyHintData hint = client.createList(key, generateListData(LIST_CONTAINER_SIZE / 10)).get();
        waitForCondition(() -> client.setMode(Mode.BACKUP).containsKey(key).get()
                && client.setMode(Mode.MASTER).containsKey(key).get());

        runWarmup(WARMUP_ITERATIONS, i -> client.pushHead(key, hint, List.of(v("item_" + i))));
        runMetricsBenchmark("List.pushHead", LIST_CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> client.pushHead(key, hint, List.of(v("item_" + i))));
    }

    @Test
    @DisplayName("Stress test: popHead")
    void stressTestListPopHead() throws Exception {
        String key = "stress_l_pop_head_" + UUID.randomUUID();
        KeyHintData hint = client.createList(key, generateListData(LIST_CONTAINER_SIZE)).get();
        waitForCondition(() -> client.setMode(Mode.BACKUP).containsKey(key).get()
                && client.setMode(Mode.MASTER).containsKey(key).get());

        runWarmup(WARMUP_ITERATIONS, i -> popHeadAndRestore(key, hint, i));

        runMetricsBenchmark("List.popHead", LIST_CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> popHeadAndRestore(key, hint, i));
    }

    @Test
    @DisplayName("Stress test: popTail")
    void stressTestListPopTail() throws Exception {
        String key = "stress_l_pop_tail_" + UUID.randomUUID();
        KeyHintData hint = client.createList(key, generateListData(LIST_CONTAINER_SIZE)).get();
        waitForCondition(() -> client.setMode(Mode.BACKUP).containsKey(key).get()
                && client.setMode(Mode.MASTER).containsKey(key).get());

        runWarmup(WARMUP_ITERATIONS, i -> popTailAndRestore(key, hint, i));

        runMetricsBenchmark("List.popTail", LIST_CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> popTailAndRestore(key, hint, i));
    }

    @Test
    @DisplayName("Stress test: insertAt")
    void stressTestInsertAt() throws Exception {
        String key = "stress_l_insert_" + UUID.randomUUID();
        KeyHintData hint = client.createList(key, generateListData(LIST_CONTAINER_SIZE)).get();
        waitForCondition(() -> client.setMode(Mode.BACKUP).containsKey(key).get()
                && client.setMode(Mode.MASTER).containsKey(key).get());

        runWarmup(WARMUP_ITERATIONS, i -> client.insertAt(key, hint, List.of(v("new_" + i)), i % LIST_CONTAINER_SIZE));
        runMetricsBenchmark("List.insertAt", LIST_CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> client.insertAt(key, hint, List.of(v("new_" + i)), i % LIST_CONTAINER_SIZE));
    }

    @Test
    @DisplayName("Stress test: removeAt")
    void stressTestRemoveAt() throws Exception {
        String key = "stress_l_remove_" + UUID.randomUUID();
        KeyHintData hint = client.createList(key, generateListData(LIST_CONTAINER_SIZE)).get();
        waitForCondition(() -> client.setMode(Mode.BACKUP).containsKey(key).get()
                && client.setMode(Mode.MASTER).containsKey(key).get());

        runWarmup(WARMUP_ITERATIONS, i -> removeAtAndRestore(key, hint, i));
        runMetricsBenchmark("List.removeAt", LIST_CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> removeAtAndRestore(key, hint, i));
    }

    @Test
    @DisplayName("Stress test: getSize")
    void stressTestListGetSize() throws Exception {
        String key = "stress_l_size_" + UUID.randomUUID();
        KeyHintData hint = client.createList(key, generateListData(LIST_CONTAINER_SIZE)).get();
        waitForCondition(() -> client.setMode(Mode.BACKUP).containsKey(key).get()
                && client.setMode(Mode.MASTER).containsKey(key).get());

        runWarmup(WARMUP_ITERATIONS, i -> client.getSize(key, hint));
        runMetricsBenchmark("List.getSize", LIST_CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> client.getSize(key, hint));
    }

    // MULTI-THREADED TESTS

    @Test
    @DisplayName("Stress test: pushHead (multi-threaded)")
    void stressTestListPushHeadMultiThreaded() throws Exception {
        String key = "stress_l_mt_push_head_" + UUID.randomUUID();
        KeyHintData hint = client.createList(key, generateListData(LIST_CONTAINER_SIZE / 10)).get();
        waitForCondition(() -> client.setMode(Mode.BACKUP).containsKey(key).get()
                && client.setMode(Mode.MASTER).containsKey(key).get());

        runMetricsBenchmarkMT("List.pushHead", LIST_CONTAINER_SIZE, BENCHMARK_ITERATIONS, THREAD_COUNT,
                                  (threadId, i) -> client.pushHead(key, hint, List.of(v("item_mt_" + i))));
    }

    @Test
    @DisplayName("Stress test: pushTail (multi-threaded)")
    void stressTestListPushTailMultiThreaded() throws Exception {
        String key = "stress_l_mt_push_tail_" + UUID.randomUUID();
        KeyHintData hint = client.createList(key, generateListData(LIST_CONTAINER_SIZE / 10)).get();
        waitForCondition(() -> client.setMode(Mode.BACKUP).containsKey(key).get()
                && client.setMode(Mode.MASTER).containsKey(key).get());

        runMetricsBenchmarkMT("List.pushTail", LIST_CONTAINER_SIZE, BENCHMARK_ITERATIONS, THREAD_COUNT,
                                  (threadId, i) -> client.pushTail(key, hint, List.of(v("item_mt_" + i))));
    }

    @Test
    @DisplayName("Stress test: popHead (multi-threaded)")
    void stressTestListPopHeadMultiThreaded() throws Exception {
        String key = "stress_l_mt_pop_head_" + UUID.randomUUID();
        KeyHintData hint = client.createList(key, generateListData(LIST_CONTAINER_SIZE)).get();
        waitForCondition(() -> client.setMode(Mode.BACKUP).containsKey(key).get()
                && client.setMode(Mode.MASTER).containsKey(key).get());

        runMetricsBenchmarkMT("List.popHead", LIST_CONTAINER_SIZE, BENCHMARK_ITERATIONS, THREAD_COUNT,
                              (threadId, i) -> popHeadAndRestore(key, hint, i));
    }

    @Test
    @DisplayName("Stress test: popTail (multi-threaded)")
    void stressTestListPopTailMultiThreaded() throws Exception {
        String key = "stress_l_mt_pop_tail_" + UUID.randomUUID();
        KeyHintData hint = client.createList(key, generateListData(LIST_CONTAINER_SIZE)).get();
        waitForCondition(() -> client.setMode(Mode.BACKUP).containsKey(key).get()
                && client.setMode(Mode.MASTER).containsKey(key).get());

        runMetricsBenchmarkMT("List.popTail", LIST_CONTAINER_SIZE, BENCHMARK_ITERATIONS, THREAD_COUNT,
                              (threadId, i) -> popTailAndRestore(key, hint, i));
    }

    @Test
    @DisplayName("Stress test: insertAt (multi-threaded)")
    void stressTestListInsertAtMultiThreaded() throws Exception {
        String key = "stress_l_mt_insert_" + UUID.randomUUID();
        KeyHintData hint = client.createList(key, generateListData(LIST_CONTAINER_SIZE)).get();
        waitForCondition(() -> client.setMode(Mode.BACKUP).containsKey(key).get()
                && client.setMode(Mode.MASTER).containsKey(key).get());

        runMetricsBenchmarkMT("List.insertAt", LIST_CONTAINER_SIZE, BENCHMARK_ITERATIONS, THREAD_COUNT,
                                  (threadId, i) -> client.insertAt(key, hint, List.of(v("new_" + i)), i % LIST_CONTAINER_SIZE));
    }

    @Test
    @DisplayName("Stress test: removeAt (multi-threaded)")
    void stressTestListRemoveAtMultiThreaded() throws Exception {
        String key = "stress_l_mt_remove_" + UUID.randomUUID();
        KeyHintData hint = client.createList(key, generateListData(LIST_CONTAINER_SIZE)).get();
        waitForCondition(() -> client.setMode(Mode.BACKUP).containsKey(key).get()
                && client.setMode(Mode.MASTER).containsKey(key).get());

        runMetricsBenchmarkMT("List.removeAt", LIST_CONTAINER_SIZE, BENCHMARK_ITERATIONS, THREAD_COUNT,
                              (threadId, i) -> removeAtAndRestore(key, hint, i));
    }
}