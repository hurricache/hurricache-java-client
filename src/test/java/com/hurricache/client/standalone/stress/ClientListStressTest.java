package com.hurricache.client.standalone.stress;

import com.hurricache.client.intf.KeyHintData;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

/**
 * Standalone List stress test with latency/error metrics.
 * Whole-container getList intentionally omitted; peekHead in hint and no-hint variants.
 */
public class ClientListStressTest extends BaseStandaloneStressTest {

    @Test
    @DisplayName("Stress test: peekHead (KeyHint)")
    void stressTestPeekHead() throws Exception {
        String key = "s_l_peek_head_" + UUID.randomUUID();
        KeyHintData hint = client.createList(key, generateListData(CONTAINER_SIZE)).get();
        runMetricsBenchmark("List.peekHead (KeyHint)", CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> client.peekHead(key, hint));
    }

    @Test
    @DisplayName("Stress test: peekHead (no hint)")
    void stressTestPeekHeadNoHint() throws Exception {
        String key = "s_l_peek_head_nh_" + UUID.randomUUID();
        client.createList(key, generateListData(CONTAINER_SIZE)).get();
        runMetricsBenchmark("List.peekHead (no hint)", CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> client.peekHead(key));
    }

    @Test
    @DisplayName("Stress test: peekTail")
    void stressTestPeekTail() throws Exception {
        String key = "s_l_peek_tail_" + UUID.randomUUID();
        KeyHintData hint = client.createList(key, generateListData(CONTAINER_SIZE)).get();
        runMetricsBenchmark("List.peekTail", CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> client.peekTail(key, hint));
    }

    @Test
    @DisplayName("Stress test: pushTail")
    void stressTestPushTail() throws Exception {
        String key = "s_l_push_tail_" + UUID.randomUUID();
        KeyHintData hint = client.createList(key, generateListData(CONTAINER_SIZE / 10)).get();
        runMetricsBenchmark("List.pushTail", CONTAINER_SIZE, BENCHMARK_ITERATIONS,
                            i -> client.pushTail(key, hint, List.of(v("item_" + i))));
    }

    @Test
    @DisplayName("Stress test: pushHead")
    void stressTestPushHead() throws Exception {
        String key = "s_l_push_head_" + UUID.randomUUID();
        KeyHintData hint = client.createList(key, generateListData(CONTAINER_SIZE / 10)).get();
        runMetricsBenchmark("List.pushHead", CONTAINER_SIZE, BENCHMARK_ITERATIONS,
                            i -> client.pushHead(key, hint, List.of(v("item_" + i))));
    }

    @Test
    @DisplayName("Stress test: popHead")
    void stressTestPopHead() throws Exception {
        String key = "s_l_pop_head_" + UUID.randomUUID();
        KeyHintData hint = client.createList(key, generateListData(CONTAINER_SIZE)).get();
        runMetricsBenchmark("List.popHead", CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> {
            var future = client.popHead(key, hint);
            if (i % 10 == 0) client.pushTail(key, hint, List.of(v("item_" + i)));
            return future;
        });
    }

    @Test
    @DisplayName("Stress test: popTail")
    void stressTestPopTail() throws Exception {
        String key = "s_l_pop_tail_" + UUID.randomUUID();
        KeyHintData hint = client.createList(key, generateListData(CONTAINER_SIZE)).get();
        runMetricsBenchmark("List.popTail", CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> {
            var future = client.popTail(key, hint);
            if (i % 10 == 0) client.pushHead(key, hint, List.of(v("item_" + i)));
            return future;
        });
    }

    @Test
    @DisplayName("Stress test: insertAt")
    void stressTestInsertAt() throws Exception {
        String key = "s_l_insert_" + UUID.randomUUID();
        KeyHintData hint = client.createList(key, generateListData(CONTAINER_SIZE)).get();
        runMetricsBenchmark("List.insertAt", CONTAINER_SIZE, BENCHMARK_ITERATIONS,
                            i -> client.insertAt(key, hint, List.of(v("item_" + i)), 0));
    }

    @Test
    @DisplayName("Stress test: removeAt")
    void stressTestRemoveAt() throws Exception {
        String key = "s_l_remove_" + UUID.randomUUID();
        KeyHintData hint = client.createList(key, generateListData(CONTAINER_SIZE)).get();
        runMetricsBenchmark("List.removeAt", CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> {
            var future = client.removeAt(key, hint, 0, 0);
            if (i % 10 == 0) client.pushTail(key, hint, List.of(v("item_" + i)));
            return future;
        });
    }

    @Test
    @DisplayName("Stress test: getSize")
    void stressTestGetSize() throws Exception {
        String key = "s_l_size_" + UUID.randomUUID();
        KeyHintData hint = client.createList(key, generateListData(CONTAINER_SIZE)).get();
        runMetricsBenchmark("List.getSize", CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> client.getSize(key, hint));
    }

    @Test
    @DisplayName("Stress test: pushTail (multi-threaded)")
    void stressTestPushTailMT() throws Exception {
        String key = "s_l_mt_push_" + UUID.randomUUID();
        KeyHintData hint = client.createList(key, generateListData(CONTAINER_SIZE / 10)).get();
        runMetricsBenchmarkMT("List.pushTail", CONTAINER_SIZE, BENCHMARK_ITERATIONS, THREAD_COUNT,
                              (threadId, i) -> client.pushTail(key, hint, List.of(v("item_" + i))));
    }

    @Test
    @DisplayName("Stress test: popHead (multi-threaded)")
    void stressTestPopHeadMT() throws Exception {
        String key = "s_l_mt_pop_" + UUID.randomUUID();
        KeyHintData hint = client.createList(key, generateListData(CONTAINER_SIZE)).get();
        runMetricsBenchmarkMT("List.popHead", CONTAINER_SIZE, BENCHMARK_ITERATIONS, THREAD_COUNT, (threadId, i) -> {
            var future = client.popHead(key, hint);
            if (i % 10 == 0) client.pushTail(key, hint, List.of(v("item_" + i)));
            return future;
        });
    }
}
