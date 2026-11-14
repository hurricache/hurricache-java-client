package com.hurricache.client.cluster.stress;

import com.hurricache.client.intf.KeyHintData;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

public class SmartClientQueueStressTest extends BaseClusterStressTest {

    private static final int QUEUE_CONTAINER_SIZE = 10000;

    @Test
    @DisplayName("Stress test: peekHead")
    void stressTestPeekHead() throws Exception {
        String key = "stress_q_peek_" + UUID.randomUUID();
        KeyHintData hint = client.createQueue(key, generateListData(QUEUE_CONTAINER_SIZE)).get();
        Thread.sleep(500);

        runWarmup(WARMUP_ITERATIONS, i -> client.peekHead(key, hint));
        runBenchmark("Queue.peekHead", QUEUE_CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> client.peekHead(key, hint));
    }

    @Test
    @DisplayName("Stress test: peekTail")
    void stressTestPeekTail() throws Exception {
        String key = "stress_q_tail_" + UUID.randomUUID();
        KeyHintData hint = client.createQueue(key, generateListData(QUEUE_CONTAINER_SIZE)).get();
        Thread.sleep(500);

        runWarmup(WARMUP_ITERATIONS, i -> client.peekTail(key, hint));
        runBenchmark("Queue.peekTail", QUEUE_CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> client.peekTail(key, hint));
    }

    @Test
    @DisplayName("Stress test: pushTail")
    void stressTestPushTail() throws Exception {
        String key = "stress_q_push_tail_" + UUID.randomUUID();
        KeyHintData hint = client.createQueue(key, generateListData(QUEUE_CONTAINER_SIZE / 10)).get();
        Thread.sleep(500);

        runWarmup(WARMUP_ITERATIONS, i -> client.pushTail(key, hint, List.of(v("item_" + i))));
        runBenchmark("Queue.pushTail", QUEUE_CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> client.pushTail(key, hint, List.of(v("item_" + i))));
    }

    @Test
    @DisplayName("Stress test: popHead")
    void stressTestPopHead() throws Exception {
        String key = "stress_q_pop_head_" + UUID.randomUUID();
        KeyHintData hint = client.createQueue(key, generateListData(QUEUE_CONTAINER_SIZE)).get();
        Thread.sleep(500);

        runWarmup(WARMUP_ITERATIONS, i -> {
            client.popHead(key, hint).get();
            if (i % 10 == 0) client.pushTail(key, hint, List.of(v("item_" + i))).get();
            return null;
        });

        runBenchmark("Queue.popHead", QUEUE_CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> {
            var future = client.popHead(key, hint);
            if (i % 10 == 0) client.pushTail(key, hint, List.of(v("item_" + i)));
            return future;
        });
    }

    @Test
    @DisplayName("Stress test: popTail")
    void stressTestPopTail() throws Exception {
        String key = "stress_q_pop_tail_" + UUID.randomUUID();
        KeyHintData hint = client.createQueue(key, generateListData(QUEUE_CONTAINER_SIZE)).get();
        Thread.sleep(500);

        runWarmup(WARMUP_ITERATIONS, i -> {
            client.popTail(key, hint).get();
            if (i % 10 == 0) client.pushHead(key, hint, List.of(v("item_" + i))).get();
            return null;
        });

        runBenchmark("Queue.popTail", QUEUE_CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> {
            var future = client.popTail(key, hint);
            if (i % 10 == 0) client.pushHead(key, hint, List.of(v("item_" + i)));
            return future;
        });
    }

    @Test
    @DisplayName("Stress test: deleteHead")
    void stressTestDeleteHead() throws Exception {
        String key = "stress_q_del_head_" + UUID.randomUUID();
        KeyHintData hint = client.createQueue(key, generateListData(QUEUE_CONTAINER_SIZE)).get();
        Thread.sleep(500);

        runWarmup(WARMUP_ITERATIONS, i -> {
            client.deleteHead(key, hint).get();
            if (i % 10 == 0) client.pushTail(key, hint, List.of(v("item_" + i))).get();
            return null;
        });

        runBenchmark("Queue.deleteHead", QUEUE_CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> {
            var future = client.deleteHead(key, hint);
            if (i % 10 == 0) client.pushTail(key, hint, List.of(v("item_" + i)));
            return future;
        });
    }

    @Test
    @DisplayName("Stress test: deleteTail")
    void stressTestDeleteTail() throws Exception {
        String key = "stress_q_del_tail_" + UUID.randomUUID();
        KeyHintData hint = client.createQueue(key, generateListData(QUEUE_CONTAINER_SIZE)).get();
        Thread.sleep(500);

        runWarmup(WARMUP_ITERATIONS, i -> {
            client.deleteTail(key, hint).get();
            if (i % 10 == 0) client.pushHead(key, hint, List.of(v("item_" + i))).get();
            return null;
        });

        runBenchmark("Queue.deleteTail", QUEUE_CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> {
            var future = client.deleteTail(key, hint);
            if (i % 10 == 0) client.pushHead(key, hint, List.of(v("item_" + i)));
            return future;
        });
    }

    // MULTI-THREADED TESTS

    @Test
    @DisplayName("Stress test: peekHead (multi-threaded)")
    void stressTestPeekHeadMultiThreaded() throws Exception {
        String key = "stress_q_mt_" + UUID.randomUUID();
        KeyHintData hint = client.createQueue(key, generateListData(QUEUE_CONTAINER_SIZE)).get();
        Thread.sleep(500);

        runMultiThreadedBenchmark("Queue.peekHead", QUEUE_CONTAINER_SIZE, BENCHMARK_ITERATIONS, THREAD_COUNT,
                                  (threadId, i) -> client.peekHead(key, hint));
    }

    @Test
    @DisplayName("Stress test: pushHead (multi-threaded)")
    void stressTestPushHeadMultiThreaded() throws Exception {
        String key = "stress_q_mt_push_head_" + UUID.randomUUID();
        KeyHintData hint = client.createQueue(key, generateListData(QUEUE_CONTAINER_SIZE / 10)).get();
        Thread.sleep(500);

        runMultiThreadedBenchmark("Queue.pushHead", QUEUE_CONTAINER_SIZE, BENCHMARK_ITERATIONS, THREAD_COUNT,
                                  (threadId, i) -> client.pushHead(key, hint, List.of(v("item_mt_" + i))));
    }

    @Test
    @DisplayName("Stress test: pushTail (multi-threaded)")
    void stressTestPushTailMultiThreaded() throws Exception {
        String key = "stress_q_mt_push_tail_" + UUID.randomUUID();
        KeyHintData hint = client.createQueue(key, generateListData(QUEUE_CONTAINER_SIZE / 10)).get();
        Thread.sleep(500);

        runMultiThreadedBenchmark("Queue.pushTail", QUEUE_CONTAINER_SIZE, BENCHMARK_ITERATIONS, THREAD_COUNT,
                                  (threadId, i) -> client.pushTail(key, hint, List.of(v("item_mt_" + i))));
    }

    @Test
    @DisplayName("Stress test: popHead (multi-threaded)")
    void stressTestPopHeadMultiThreaded() throws Exception {
        String key = "stress_q_mt_pop_head_" + UUID.randomUUID();
        KeyHintData hint = client.createQueue(key, generateListData(QUEUE_CONTAINER_SIZE)).get();
        Thread.sleep(500);

        runMultiThreadedBenchmark("Queue.popHead", QUEUE_CONTAINER_SIZE, BENCHMARK_ITERATIONS, THREAD_COUNT, (threadId, i) -> {
            var future = client.popHead(key, hint);
            if (i % 10 == 0) client.pushTail(key, hint, List.of(v("item_mt_" + i)));
            return future;
        });
    }

    @Test
    @DisplayName("Stress test: popTail (multi-threaded)")
    void stressTestPopTailMultiThreaded() throws Exception {
        String key = "stress_q_mt_pop_tail_" + UUID.randomUUID();
        KeyHintData hint = client.createQueue(key, generateListData(QUEUE_CONTAINER_SIZE)).get();
        Thread.sleep(500);

        runMultiThreadedBenchmark("Queue.popTail", QUEUE_CONTAINER_SIZE, BENCHMARK_ITERATIONS, THREAD_COUNT, (threadId, i) -> {
            var future = client.popTail(key, hint);
            if (i % 10 == 0) client.pushHead(key, hint, List.of(v("item_mt_" + i)));
            return future;
        });
    }

    @Test
    @DisplayName("Stress test: deleteHead (multi-threaded)")
    void stressTestDeleteHeadMultiThreaded() throws Exception {
        String key = "stress_q_mt_del_head_" + UUID.randomUUID();
        KeyHintData hint = client.createQueue(key, generateListData(QUEUE_CONTAINER_SIZE)).get();
        Thread.sleep(500);

        runMultiThreadedBenchmark("Queue.deleteHead", QUEUE_CONTAINER_SIZE, BENCHMARK_ITERATIONS, THREAD_COUNT, (threadId, i) -> {
            var future = client.deleteHead(key, hint);
            if (i % 10 == 0) client.pushTail(key, hint, List.of(v("item_mt_" + i)));
            return future;
        });
    }

    @Test
    @DisplayName("Stress test: deleteTail (multi-threaded)")
    void stressTestDeleteTailMultiThreaded() throws Exception {
        String key = "stress_q_mt_del_tail_" + UUID.randomUUID();
        KeyHintData hint = client.createQueue(key, generateListData(QUEUE_CONTAINER_SIZE)).get();
        Thread.sleep(500);

        runMultiThreadedBenchmark("Queue.deleteTail", QUEUE_CONTAINER_SIZE, BENCHMARK_ITERATIONS, THREAD_COUNT, (threadId, i) -> {
            var future = client.deleteTail(key, hint);
            if (i % 10 == 0) client.pushHead(key, hint, List.of(v("item_mt_" + i)));
            return future;
        });
    }
}