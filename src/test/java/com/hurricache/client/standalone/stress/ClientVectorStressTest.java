package com.hurricache.client.standalone.stress;

import com.hurricache.client.intf.KeyHintData;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

/**
 * Standalone Vector stress test with latency/error metrics.
 * Whole-container getVector intentionally omitted; getAt hint/no-hint.
 */
public class ClientVectorStressTest extends BaseStandaloneStressTest {

    @Test
    @DisplayName("Stress test: getAt (KeyHint)")
    void stressTestGetAt() throws Exception {
        String key = "s_v_get_at_" + UUID.randomUUID();
        KeyHintData hint = client.createVector(key, generateListData(CONTAINER_SIZE)).get();
        runMetricsBenchmark("Vector.getAt (KeyHint)", CONTAINER_SIZE, BENCHMARK_ITERATIONS,
                            i -> client.getAt(key, hint, i % CONTAINER_SIZE));
    }

    @Test
    @DisplayName("Stress test: getAt (no hint)")
    void stressTestGetAtNoHint() throws Exception {
        String key = "s_v_get_at_nh_" + UUID.randomUUID();
        client.createVector(key, generateListData(CONTAINER_SIZE)).get();
        runMetricsBenchmark("Vector.getAt (no hint)", CONTAINER_SIZE, BENCHMARK_ITERATIONS,
                            i -> client.getAt(key, i % CONTAINER_SIZE));
    }

    @Test
    @DisplayName("Stress test: pushTail")
    void stressTestPushTail() throws Exception {
        String key = "s_v_push_" + UUID.randomUUID();
        KeyHintData hint = client.createVector(key, generateListData(CONTAINER_SIZE / 10)).get();
        runMetricsBenchmark("Vector.pushTail", CONTAINER_SIZE, BENCHMARK_ITERATIONS,
                            i -> client.pushTail(key, hint, List.of(v("item_" + i))));
    }

    @Test
    @DisplayName("Stress test: popAt")
    void stressTestPopAt() throws Exception {
        String key = "s_v_pop_" + UUID.randomUUID();
        KeyHintData hint = client.createVector(key, generateListData(CONTAINER_SIZE)).get();
        runMetricsBenchmark("Vector.popAt", CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> {
            var future = client.popAt(key, hint, 0);
            if (i % 10 == 0) client.pushTail(key, hint, List.of(v("item_" + i)));
            return future;
        });
    }

    @Test
    @DisplayName("Stress test: insertAt")
    void stressTestInsertAt() throws Exception {
        String key = "s_v_insert_" + UUID.randomUUID();
        KeyHintData hint = client.createVector(key, generateListData(CONTAINER_SIZE)).get();
        runMetricsBenchmark("Vector.insertAt", CONTAINER_SIZE, BENCHMARK_ITERATIONS,
                            i -> client.insertAt(key, hint, List.of(v("item_" + i)), 0));
    }

    @Test
    @DisplayName("Stress test: removeAt")
    void stressTestRemoveAt() throws Exception {
        String key = "s_v_remove_" + UUID.randomUUID();
        KeyHintData hint = client.createVector(key, generateListData(CONTAINER_SIZE)).get();
        runMetricsBenchmark("Vector.removeAt", CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> {
            var future = client.removeAt(key, hint, 0, 0);
            if (i % 10 == 0) client.pushTail(key, hint, List.of(v("item_" + i)));
            return future;
        });
    }

    @Test
    @DisplayName("Stress test: getSize")
    void stressTestGetSize() throws Exception {
        String key = "s_v_size_" + UUID.randomUUID();
        KeyHintData hint = client.createVector(key, generateListData(CONTAINER_SIZE)).get();
        runMetricsBenchmark("Vector.getSize", CONTAINER_SIZE, BENCHMARK_ITERATIONS, i -> client.getSize(key, hint));
    }

    @Test
    @DisplayName("Stress test: getAt (multi-threaded)")
    void stressTestGetAtMT() throws Exception {
        String key = "s_v_mt_get_" + UUID.randomUUID();
        KeyHintData hint = client.createVector(key, generateListData(CONTAINER_SIZE)).get();
        runMetricsBenchmarkMT("Vector.getAt", CONTAINER_SIZE, BENCHMARK_ITERATIONS, THREAD_COUNT,
                              (threadId, i) -> client.getAt(key, hint, i % CONTAINER_SIZE));
    }
}
