package com.hurricache.client.cluster.stress;

import com.hurricache.client.intf.KeyHintData;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

public class SmartClientListStressTest extends BaseClusterStressTest {

    private static final int LIST_CONTAINER_SIZE = 10000;

    @Test
    @DisplayName("Stress test: getList")
    void stressTestGetList() throws Exception {
        String key = "stress_l_get_" + UUID.randomUUID();
        KeyHintData hint = client.createList(key, generateListData(LIST_CONTAINER_SIZE)).get();
        Thread.sleep(10000);

        runWarmup(WARMUP_ITERATIONS, i -> client.getList(key, hint));
        runBenchmark("List.getList", LIST_CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> client.getList(key, hint));
    }

    @Test
    @DisplayName("Stress test: peekHead")
    void stressTestListPeekHead() throws Exception {
        String key = "stress_l_peek_head_" + UUID.randomUUID();
        KeyHintData hint = client.createList(key, generateListData(LIST_CONTAINER_SIZE)).get();
        System.out.println("Created: " + key + " with hint: " + hint);
        Thread.sleep(1000);

        runWarmup(WARMUP_ITERATIONS, i -> client.peekHead(key, hint));
        runBenchmark("List.peekHead", LIST_CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> client.peekHead(key, hint));
    }

    @Test
    @DisplayName("Stress test: peekTail")
    void stressTestListPeekTail() throws Exception {
        String key = "stress_l_peek_tail_" + UUID.randomUUID();
        KeyHintData hint = client.createList(key, generateListData(LIST_CONTAINER_SIZE)).get();
        System.out.println("Created: " + key + " with hint: " + hint);
        Thread.sleep(1000);

        runWarmup(WARMUP_ITERATIONS, i -> client.peekTail(key, hint));
        runBenchmark("List.peekTail", LIST_CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> client.peekTail(key, hint));
    }

    @Test
    @DisplayName("Stress test: pushTail")
    void stressTestListPushTail() throws Exception {
        String key = "stress_l_push_tail_" + UUID.randomUUID();
        KeyHintData hint = client.createList(key, generateListData(LIST_CONTAINER_SIZE / 10)).get();
        Thread.sleep(1000);

        runWarmup(WARMUP_ITERATIONS, i -> client.pushTail(key, hint, List.of(v("item_" + i))));
        runBenchmark("List.pushTail", LIST_CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> client.pushTail(key, hint, List.of(v("item_" + i))));
    }

    @Test
    @DisplayName("Stress test: pushHead")
    void stressTestListPushHead() throws Exception {
        String key = "stress_l_push_head_" + UUID.randomUUID();
        KeyHintData hint = client.createList(key, generateListData(LIST_CONTAINER_SIZE / 10)).get();
        Thread.sleep(1000);

        runWarmup(WARMUP_ITERATIONS, i -> client.pushHead(key, hint, List.of(v("item_" + i))));
        runBenchmark("List.pushHead", LIST_CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> client.pushHead(key, hint, List.of(v("item_" + i))));
    }

    @Test
    @DisplayName("Stress test: popHead")
    void stressTestListPopHead() throws Exception {
        String key = "stress_l_pop_head_" + UUID.randomUUID();
        KeyHintData hint = client.createList(key, generateListData(LIST_CONTAINER_SIZE)).get();
        Thread.sleep(1000);

        runWarmup(WARMUP_ITERATIONS, i -> {
            client.popHead(key, hint).get();
            if (i % 10 == 0) client.pushTail(key, hint, List.of(v("item_" + i))).get();
            return null;
        });

        runBenchmark("List.popHead", LIST_CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> {
            var future = client.popHead(key, hint);
            if (i % 10 == 0) client.pushTail(key, hint, List.of(v("item_" + i)));
            return future;
        });
    }

    @Test
    @DisplayName("Stress test: popTail")
    void stressTestListPopTail() throws Exception {
        String key = "stress_l_pop_tail_" + UUID.randomUUID();
        KeyHintData hint = client.createList(key, generateListData(LIST_CONTAINER_SIZE)).get();
        Thread.sleep(1000);

        runWarmup(WARMUP_ITERATIONS, i -> {
            client.popTail(key, hint).get();
            if (i % 10 == 0) client.pushHead(key, hint, List.of(v("item_" + i))).get();
            return null;
        });

        runBenchmark("List.popTail", LIST_CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> {
            var future = client.popTail(key, hint);
            if (i % 10 == 0) client.pushHead(key, hint, List.of(v("item_" + i)));
            return future;
        });
    }

    @Test
    @DisplayName("Stress test: insertAt")
    void stressTestInsertAt() throws Exception {
        String key = "stress_l_insert_" + UUID.randomUUID();
        KeyHintData hint = client.createList(key, generateListData(LIST_CONTAINER_SIZE)).get();
        Thread.sleep(1000);

        runWarmup(WARMUP_ITERATIONS, i -> client.insertAt(key, hint, List.of(v("new_" + i)), i % LIST_CONTAINER_SIZE));
        runBenchmark("List.insertAt", LIST_CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> client.insertAt(key, hint, List.of(v("new_" + i)), i % LIST_CONTAINER_SIZE));
    }

    @Test
    @DisplayName("Stress test: removeAt")
    void stressTestRemoveAt() throws Exception {
        String key = "stress_l_remove_" + UUID.randomUUID();
        KeyHintData hint = client.createList(key, generateListData(LIST_CONTAINER_SIZE)).get();
        Thread.sleep(1000);

        runWarmup(WARMUP_ITERATIONS, i -> client.removeAt(key, hint, i % LIST_CONTAINER_SIZE));
        runBenchmark("List.removeAt", LIST_CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> client.removeAt(key, hint, i % LIST_CONTAINER_SIZE));
    }

    @Test
    @DisplayName("Stress test: getSize")
    void stressTestListGetSize() throws Exception {
        String key = "stress_l_size_" + UUID.randomUUID();
        KeyHintData hint = client.createList(key, generateListData(LIST_CONTAINER_SIZE)).get();
        Thread.sleep(1000);

        runWarmup(WARMUP_ITERATIONS, i -> client.getSize(key, hint));
        runBenchmark("List.getSize", LIST_CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> client.getSize(key, hint));
    }

    // MULTI-THREADED TESTS

    @Test
    @DisplayName("Stress test: getList (multi-threaded)")
    void stressTestGetListMultiThreaded() throws Exception {
        String key = "stress_l_mt_" + UUID.randomUUID();
        KeyHintData hint = client.createList(key, generateListData(LIST_CONTAINER_SIZE)).get();
        Thread.sleep(1000);

        runMultiThreadedBenchmark("List.getList", LIST_CONTAINER_SIZE, BENCHMARK_ITERATIONS, THREAD_COUNT,
                                  (threadId, i) -> client.getList(key, hint));
    }

    @Test
    @DisplayName("Stress test: pushHead (multi-threaded)")
    void stressTestListPushHeadMultiThreaded() throws Exception {
        String key = "stress_l_mt_push_head_" + UUID.randomUUID();
        KeyHintData hint = client.createList(key, generateListData(LIST_CONTAINER_SIZE / 10)).get();
        Thread.sleep(1000);

        runMultiThreadedBenchmark("List.pushHead", LIST_CONTAINER_SIZE, BENCHMARK_ITERATIONS, THREAD_COUNT,
                                  (threadId, i) -> client.pushHead(key, hint, List.of(v("item_mt_" + i))));
    }

    @Test
    @DisplayName("Stress test: pushTail (multi-threaded)")
    void stressTestListPushTailMultiThreaded() throws Exception {
        String key = "stress_l_mt_push_tail_" + UUID.randomUUID();
        KeyHintData hint = client.createList(key, generateListData(LIST_CONTAINER_SIZE / 10)).get();
        Thread.sleep(1000);

        runMultiThreadedBenchmark("List.pushTail", LIST_CONTAINER_SIZE, BENCHMARK_ITERATIONS, THREAD_COUNT,
                                  (threadId, i) -> client.pushTail(key, hint, List.of(v("item_mt_" + i))));
    }

    @Test
    @DisplayName("Stress test: popHead (multi-threaded)")
    void stressTestListPopHeadMultiThreaded() throws Exception {
        String key = "stress_l_mt_pop_head_" + UUID.randomUUID();
        KeyHintData hint = client.createList(key, generateListData(LIST_CONTAINER_SIZE)).get();
        Thread.sleep(1000);

        runMultiThreadedBenchmark("List.popHead", LIST_CONTAINER_SIZE, BENCHMARK_ITERATIONS, THREAD_COUNT, (threadId, i) -> {
            var future = client.popHead(key, hint);
            if (i % 10 == 0) client.pushTail(key, hint, List.of(v("item_mt_" + i)));
            return future;
        });
    }

    @Test
    @DisplayName("Stress test: popTail (multi-threaded)")
    void stressTestListPopTailMultiThreaded() throws Exception {
        String key = "stress_l_mt_pop_tail_" + UUID.randomUUID();
        KeyHintData hint = client.createList(key, generateListData(LIST_CONTAINER_SIZE)).get();
        Thread.sleep(1000);

        runMultiThreadedBenchmark("List.popTail", LIST_CONTAINER_SIZE, BENCHMARK_ITERATIONS, THREAD_COUNT, (threadId, i) -> {
            var future = client.popTail(key, hint);
            if (i % 10 == 0) client.pushHead(key, hint, List.of(v("item_mt_" + i)));
            return future;
        });
    }

    @Test
    @DisplayName("Stress test: insertAt (multi-threaded)")
    void stressTestListInsertAtMultiThreaded() throws Exception {
        String key = "stress_l_mt_insert_" + UUID.randomUUID();
        KeyHintData hint = client.createList(key, generateListData(LIST_CONTAINER_SIZE)).get();
        Thread.sleep(1000);

        runMultiThreadedBenchmark("List.insertAt", LIST_CONTAINER_SIZE, BENCHMARK_ITERATIONS, THREAD_COUNT,
                                  (threadId, i) -> client.insertAt(key, hint, List.of(v("new_" + i)), i % LIST_CONTAINER_SIZE));
    }

    @Test
    @DisplayName("Stress test: removeAt (multi-threaded)")
    void stressTestListRemoveAtMultiThreaded() throws Exception {
        String key = "stress_l_mt_remove_" + UUID.randomUUID();
        KeyHintData hint = client.createList(key, generateListData(LIST_CONTAINER_SIZE)).get();
        Thread.sleep(1000);

        runMultiThreadedBenchmark("List.removeAt", LIST_CONTAINER_SIZE, BENCHMARK_ITERATIONS, THREAD_COUNT, (threadId, i) -> {
            var future = client.removeAt(key, hint, i % LIST_CONTAINER_SIZE);
            if (i % 10 == 0) client.pushTail(key, hint, List.of(v("item_mt_" + i)));
            return future;
        });
    }
}