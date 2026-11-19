package com.hurricache.client.standalone.stress;

import com.hurricache.client.intf.KeyHintData;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

/**
 * Standalone Queue stress test with latency/error metrics (peekHead hint/no-hint).
 */
public class ClientQueueStressTest extends BaseStandaloneStressTest {

    @Test
    @DisplayName("Stress test: peekHead (KeyHint)")
    void stressTestPeekHead() throws Exception {
        String key = "s_q_peek_head_" + UUID.randomUUID();
        KeyHintData hint = client.createQueue(key, generateListData(CONTAINER_SIZE)).get();
        runMetricsBenchmark("Queue.peekHead (KeyHint)", CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> client.peekHead(key, hint));
    }

    @Test
    @DisplayName("Stress test: peekHead (no hint)")
    void stressTestPeekHeadNoHint() throws Exception {
        String key = "s_q_peek_head_nh_" + UUID.randomUUID();
        client.createQueue(key, generateListData(CONTAINER_SIZE)).get();
        runMetricsBenchmark("Queue.peekHead (no hint)", CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> client.peekHead(key));
    }

    @Test
    @DisplayName("Stress test: peekTail")
    void stressTestPeekTail() throws Exception {
        String key = "s_q_peek_tail_" + UUID.randomUUID();
        KeyHintData hint = client.createQueue(key, generateListData(CONTAINER_SIZE)).get();
        runMetricsBenchmark("Queue.peekTail", CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> client.peekTail(key, hint));
    }

    @Test
    @DisplayName("Stress test: pushTail")
    void stressTestPushTail() throws Exception {
        String key = "s_q_push_tail_" + UUID.randomUUID();
        KeyHintData hint = client.createQueue(key, generateListData(CONTAINER_SIZE / 10)).get();
        runMetricsBenchmark("Queue.pushTail", CONTAINER_SIZE, BENCHMARK_ITERATIONS,
                            i -> client.pushTail(key, hint, List.of(v("item_" + i))));
    }

    @Test
    @DisplayName("Stress test: pushHead")
    void stressTestPushHead() throws Exception {
        String key = "s_q_push_head_" + UUID.randomUUID();
        KeyHintData hint = client.createQueue(key, generateListData(CONTAINER_SIZE / 10)).get();
        runMetricsBenchmark("Queue.pushHead", CONTAINER_SIZE, BENCHMARK_ITERATIONS,
                            i -> client.pushHead(key, hint, List.of(v("item_" + i))));
    }

    @Test
    @DisplayName("Stress test: popHead")
    void stressTestPopHead() throws Exception {
        String key = "s_q_pop_head_" + UUID.randomUUID();
        KeyHintData hint = client.createQueue(key, generateListData(CONTAINER_SIZE)).get();
        runMetricsBenchmark("Queue.popHead", CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> {
            var future = client.popHead(key, hint);
            if (i % 10 == 0) client.pushTail(key, hint, List.of(v("item_" + i)));
            return future;
        });
    }

    @Test
    @DisplayName("Stress test: popTail")
    void stressTestPopTail() throws Exception {
        String key = "s_q_pop_tail_" + UUID.randomUUID();
        KeyHintData hint = client.createQueue(key, generateListData(CONTAINER_SIZE)).get();
        runMetricsBenchmark("Queue.popTail", CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> {
            var future = client.popTail(key, hint);
            if (i % 10 == 0) client.pushHead(key, hint, List.of(v("item_" + i)));
            return future;
        });
    }

    @Test
    @DisplayName("Stress test: deleteHead")
    void stressTestDeleteHead() throws Exception {
        String key = "s_q_del_head_" + UUID.randomUUID();
        KeyHintData hint = client.createQueue(key, generateListData(CONTAINER_SIZE)).get();
        runMetricsBenchmark("Queue.deleteHead", CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> {
            var future = client.deleteHead(key, hint);
            if (i % 10 == 0) client.pushTail(key, hint, List.of(v("item_" + i)));
            return future;
        });
    }

    @Test
    @DisplayName("Stress test: deleteTail")
    void stressTestDeleteTail() throws Exception {
        String key = "s_q_del_tail_" + UUID.randomUUID();
        KeyHintData hint = client.createQueue(key, generateListData(CONTAINER_SIZE)).get();
        runMetricsBenchmark("Queue.deleteTail", CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> {
            var future = client.deleteTail(key, hint);
            if (i % 10 == 0) client.pushHead(key, hint, List.of(v("item_" + i)));
            return future;
        });
    }
}
