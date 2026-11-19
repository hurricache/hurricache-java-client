package com.hurricache.client.cluster.stress;

import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Mode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

public class SmartClientQueueStressTest extends BaseClusterStressTest {
    // pop/delete с немедленным возвратом на противоположный конец: размер очереди стабилен
    private java.util.concurrent.CompletableFuture<com.hurricache.client.intf.Variant> popHeadAndRestore(
            String key, KeyHintData hint, int i) {
        return client.popHead(key, hint)
                .thenCompose(old -> client.pushTail(key, hint, java.util.List.of(v("item_" + i)))
                                        .thenApply(x -> old));
    }

    private java.util.concurrent.CompletableFuture<com.hurricache.client.intf.Variant> popTailAndRestore(
            String key, KeyHintData hint, int i) {
        return client.popTail(key, hint)
                .thenCompose(old -> client.pushHead(key, hint, java.util.List.of(v("item_" + i)))
                                        .thenApply(x -> old));
    }

    private java.util.concurrent.CompletableFuture<Integer> deleteHeadAndRestore(
            String key, KeyHintData hint, int i) {
        return client.deleteHead(key, hint)
                .thenCompose(removed -> removed != null && removed > 0
                        ? client.pushTail(key, hint, java.util.List.of(v("item_" + i)))
                                  .thenApply(x -> removed)
                        : java.util.concurrent.CompletableFuture.completedFuture(removed));
    }

    private java.util.concurrent.CompletableFuture<Integer> deleteTailAndRestore(
            String key, KeyHintData hint, int i) {
        return client.deleteTail(key, hint)
                .thenCompose(removed -> removed != null && removed > 0
                        ? client.pushHead(key, hint, java.util.List.of(v("item_" + i)))
                                  .thenApply(x -> removed)
                        : java.util.concurrent.CompletableFuture.completedFuture(removed));
    }


    private static final int QUEUE_CONTAINER_SIZE = 10000;

    @Test
    @DisplayName("Stress test: peekHead")
    void stressTestPeekHead() throws Exception {
        String key = "stress_q_peek_" + UUID.randomUUID();
        KeyHintData hint = client.createQueue(key, generateListData(QUEUE_CONTAINER_SIZE)).get();
        waitForCondition(() -> client.setMode(Mode.BACKUP).containsKey(key).get()
                && client.setMode(Mode.MASTER).containsKey(key).get());

        runWarmup(WARMUP_ITERATIONS, i -> client.peekHead(key, hint));
        runMetricsBenchmark("Queue.peekHead", QUEUE_CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> client.peekHead(key, hint));
    }

    @Test
    @DisplayName("Stress test: peekHead (no hint)")
    void stressTestQueuePeekHeadNoHint() throws Exception {
        String key = "stress_q_peek_head_nh_" + UUID.randomUUID();
        client.createQueue(key, generateListData(CONTAINER_SIZE)).get();
        waitForCondition(() -> client.setMode(Mode.BACKUP).containsKey(key).get()
                && client.setMode(Mode.MASTER).containsKey(key).get());

        runWarmup(WARMUP_ITERATIONS, i -> client.peekHead(key));
        runMetricsBenchmark("Queue.peekHead (no hint)", CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> client.peekHead(key));
    }

    @Test
    @DisplayName("Stress test: peekTail")
    void stressTestPeekTail() throws Exception {
        String key = "stress_q_tail_" + UUID.randomUUID();
        KeyHintData hint = client.createQueue(key, generateListData(QUEUE_CONTAINER_SIZE)).get();
        waitForCondition(() -> client.setMode(Mode.BACKUP).containsKey(key).get()
                && client.setMode(Mode.MASTER).containsKey(key).get());

        runWarmup(WARMUP_ITERATIONS, i -> client.peekTail(key, hint));
        runMetricsBenchmark("Queue.peekTail", QUEUE_CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> client.peekTail(key, hint));
    }

    @Test
    @DisplayName("Stress test: pushTail")
    void stressTestPushTail() throws Exception {
        String key = "stress_q_push_tail_" + UUID.randomUUID();
        KeyHintData hint = client.createQueue(key, generateListData(QUEUE_CONTAINER_SIZE / 10)).get();
        waitForCondition(() -> client.setMode(Mode.BACKUP).containsKey(key).get()
                && client.setMode(Mode.MASTER).containsKey(key).get());

        runWarmup(WARMUP_ITERATIONS, i -> client.pushTail(key, hint, List.of(v("item_" + i))));
        runMetricsBenchmark("Queue.pushTail", QUEUE_CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> client.pushTail(key, hint, List.of(v("item_" + i))));
    }

    @Test
    @DisplayName("Stress test: popHead")
    void stressTestPopHead() throws Exception {
        String key = "stress_q_pop_head_" + UUID.randomUUID();
        KeyHintData hint = client.createQueue(key, generateListData(QUEUE_CONTAINER_SIZE)).get();
        waitForCondition(() -> client.setMode(Mode.BACKUP).containsKey(key).get()
                && client.setMode(Mode.MASTER).containsKey(key).get());

        runWarmup(WARMUP_ITERATIONS, i -> popHeadAndRestore(key, hint, i));

        runMetricsBenchmark("Queue.popHead", QUEUE_CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> popHeadAndRestore(key, hint, i));
    }

    @Test
    @DisplayName("Stress test: popTail")
    void stressTestPopTail() throws Exception {
        String key = "stress_q_pop_tail_" + UUID.randomUUID();
        KeyHintData hint = client.createQueue(key, generateListData(QUEUE_CONTAINER_SIZE)).get();
        waitForCondition(() -> client.setMode(Mode.BACKUP).containsKey(key).get()
                && client.setMode(Mode.MASTER).containsKey(key).get());

        runWarmup(WARMUP_ITERATIONS, i -> popTailAndRestore(key, hint, i));

        runMetricsBenchmark("Queue.popTail", QUEUE_CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> popTailAndRestore(key, hint, i));
    }

    @Test
    @DisplayName("Stress test: deleteHead")
    void stressTestDeleteHead() throws Exception {
        String key = "stress_q_del_head_" + UUID.randomUUID();
        KeyHintData hint = client.createQueue(key, generateListData(QUEUE_CONTAINER_SIZE)).get();
        waitForCondition(() -> client.setMode(Mode.BACKUP).containsKey(key).get()
                && client.setMode(Mode.MASTER).containsKey(key).get());

        runWarmup(WARMUP_ITERATIONS, i -> deleteHeadAndRestore(key, hint, i));

        runMetricsBenchmark("Queue.deleteHead", QUEUE_CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> deleteHeadAndRestore(key, hint, i));
    }

    @Test
    @DisplayName("Stress test: deleteTail")
    void stressTestDeleteTail() throws Exception {
        String key = "stress_q_del_tail_" + UUID.randomUUID();
        KeyHintData hint = client.createQueue(key, generateListData(QUEUE_CONTAINER_SIZE)).get();
        waitForCondition(() -> client.setMode(Mode.BACKUP).containsKey(key).get()
                && client.setMode(Mode.MASTER).containsKey(key).get());

        runWarmup(WARMUP_ITERATIONS, i -> deleteTailAndRestore(key, hint, i));

        runMetricsBenchmark("Queue.deleteTail", QUEUE_CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> deleteTailAndRestore(key, hint, i));
    }

    // MULTI-THREADED TESTS

    @Test
    @DisplayName("Stress test: peekHead (multi-threaded)")
    void stressTestPeekHeadMultiThreaded() throws Exception {
        String key = "stress_q_mt_" + UUID.randomUUID();
        KeyHintData hint = client.createQueue(key, generateListData(QUEUE_CONTAINER_SIZE)).get();
        waitForCondition(() -> client.setMode(Mode.BACKUP).containsKey(key).get()
                && client.setMode(Mode.MASTER).containsKey(key).get());

        runMetricsBenchmarkMT("Queue.peekHead", QUEUE_CONTAINER_SIZE, BENCHMARK_ITERATIONS, THREAD_COUNT,
                                  (threadId, i) -> client.peekHead(key, hint));
    }

    @Test
    @DisplayName("Stress test: pushHead (multi-threaded)")
    void stressTestPushHeadMultiThreaded() throws Exception {
        String key = "stress_q_mt_push_head_" + UUID.randomUUID();
        KeyHintData hint = client.createQueue(key, generateListData(QUEUE_CONTAINER_SIZE / 10)).get();
        waitForCondition(() -> client.setMode(Mode.BACKUP).containsKey(key).get()
                && client.setMode(Mode.MASTER).containsKey(key).get());

        runMetricsBenchmarkMT("Queue.pushHead", QUEUE_CONTAINER_SIZE, BENCHMARK_ITERATIONS, THREAD_COUNT,
                                  (threadId, i) -> client.pushHead(key, hint, List.of(v("item_mt_" + i))));
    }

    @Test
    @DisplayName("Stress test: pushTail (multi-threaded)")
    void stressTestPushTailMultiThreaded() throws Exception {
        String key = "stress_q_mt_push_tail_" + UUID.randomUUID();
        KeyHintData hint = client.createQueue(key, generateListData(QUEUE_CONTAINER_SIZE / 10)).get();
        waitForCondition(() -> client.setMode(Mode.BACKUP).containsKey(key).get()
                && client.setMode(Mode.MASTER).containsKey(key).get());

        runMetricsBenchmarkMT("Queue.pushTail", QUEUE_CONTAINER_SIZE, BENCHMARK_ITERATIONS, THREAD_COUNT,
                                  (threadId, i) -> client.pushTail(key, hint, List.of(v("item_mt_" + i))));
    }

    @Test
    @DisplayName("Stress test: popHead (multi-threaded)")
    void stressTestPopHeadMultiThreaded() throws Exception {
        String key = "stress_q_mt_pop_head_" + UUID.randomUUID();
        KeyHintData hint = client.createQueue(key, generateListData(QUEUE_CONTAINER_SIZE)).get();
        waitForCondition(() -> client.setMode(Mode.BACKUP).containsKey(key).get()
                && client.setMode(Mode.MASTER).containsKey(key).get());

        runMetricsBenchmarkMT("Queue.popHead", QUEUE_CONTAINER_SIZE, BENCHMARK_ITERATIONS, THREAD_COUNT,
                              (threadId, i) -> popHeadAndRestore(key, hint, i));
    }

    @Test
    @DisplayName("Stress test: popTail (multi-threaded)")
    void stressTestPopTailMultiThreaded() throws Exception {
        String key = "stress_q_mt_pop_tail_" + UUID.randomUUID();
        KeyHintData hint = client.createQueue(key, generateListData(QUEUE_CONTAINER_SIZE)).get();
        waitForCondition(() -> client.setMode(Mode.BACKUP).containsKey(key).get()
                && client.setMode(Mode.MASTER).containsKey(key).get());

        runMetricsBenchmarkMT("Queue.popTail", QUEUE_CONTAINER_SIZE, BENCHMARK_ITERATIONS, THREAD_COUNT,
                              (threadId, i) -> popTailAndRestore(key, hint, i));
    }

    @Test
    @DisplayName("Stress test: deleteHead (multi-threaded)")
    void stressTestDeleteHeadMultiThreaded() throws Exception {
        String key = "stress_q_mt_del_head_" + UUID.randomUUID();
        KeyHintData hint = client.createQueue(key, generateListData(QUEUE_CONTAINER_SIZE)).get();
        waitForCondition(() -> client.setMode(Mode.BACKUP).containsKey(key).get()
                && client.setMode(Mode.MASTER).containsKey(key).get());

        runMetricsBenchmarkMT("Queue.deleteHead", QUEUE_CONTAINER_SIZE, BENCHMARK_ITERATIONS, THREAD_COUNT,
                              (threadId, i) -> deleteHeadAndRestore(key, hint, i));
    }

    @Test
    @DisplayName("Stress test: deleteTail (multi-threaded)")
    void stressTestDeleteTailMultiThreaded() throws Exception {
        String key = "stress_q_mt_del_tail_" + UUID.randomUUID();
        KeyHintData hint = client.createQueue(key, generateListData(QUEUE_CONTAINER_SIZE)).get();
        waitForCondition(() -> client.setMode(Mode.BACKUP).containsKey(key).get()
                && client.setMode(Mode.MASTER).containsKey(key).get());

        runMetricsBenchmarkMT("Queue.deleteTail", QUEUE_CONTAINER_SIZE, BENCHMARK_ITERATIONS, THREAD_COUNT,
                              (threadId, i) -> deleteTailAndRestore(key, hint, i));
    }
}