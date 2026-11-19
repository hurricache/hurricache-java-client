package com.hurricache.client.cluster.stress;

import com.hurricache.client.intf.Variant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.util.UUID;

/**
 * Scalar stress test with real error counting and per-operation latency/throughput metrics.
 *
 * Mirrors the functional coverage of {@link SmartClientScalarStressTest}
 * (create/get/replace/remove, single- and multi-threaded), but every dependent
 * operation reads only from a fully confirmed keyspace (each create is awaited),
 * so there are no spurious NOT_FOUND errors.
 *
 * GET scenarios exist in two variants: with KeyHint and without.
 */
@Execution(ExecutionMode.SAME_THREAD)
public class SmartClientScalarGetMetricsStressTest extends BaseClusterStressTest {

    // =========================================================================
    // CREATE (single-threaded)
    // =========================================================================

    @Test
    @DisplayName("Metrics: create (bytes)")
    void metricsCreateBytes() throws Exception {
        runMetricsBenchmark("Create(bytes)", BENCHMARK_ITERATIONS, i ->
                client.create("m_create_bytes_" + UUID.randomUUID(), v("value_bytes_" + (i % 100))));
    }

    @Test
    @DisplayName("Metrics: create (int)")
    void metricsCreateInt() throws Exception {
        runMetricsBenchmark("Create(int)", BENCHMARK_ITERATIONS, i ->
                client.create("m_create_int_" + UUID.randomUUID(), vi(i)));
    }

    @Test
    @DisplayName("Metrics: create (double)")
    void metricsCreateDouble() throws Exception {
        runMetricsBenchmark("Create(double)", BENCHMARK_ITERATIONS, i ->
                client.create("m_create_double_" + UUID.randomUUID(), vd(i * 1.5)));
    }

    @Test
    @DisplayName("Metrics: create (heterogeneous)")
    void metricsCreateHeterogeneous() throws Exception {
        runMetricsBenchmark("Create(heterogeneous)", BENCHMARK_ITERATIONS, i -> {
            Variant value = (i % 3 == 0) ? v("mixed_bytes_" + i) : (i % 3 == 1) ? vi(i) : vd(i * 1.5);
            return client.create("m_create_mix_" + UUID.randomUUID(), value);
        });
    }

    // =========================================================================
    // GET (single-threaded)
    // =========================================================================

    @Test
    @DisplayName("Metrics: get random (bytes, KeyHint)")
    void metricsGetBytesWithKeyHint() throws Exception {
        Keyspace ks = createBytesKeyspace("m_get_b_hint_" + UUID.randomUUID() + "_");
        runMetricsBenchmark("Get(bytes, KeyHint)", BENCHMARK_ITERATIONS, i -> {
            int idx = ks.randomIndex();
            return client.get(ks.keys[idx], ks.hints[idx]);
        });
    }

    @Test
    @DisplayName("Metrics: get random (bytes, no KeyHint)")
    void metricsGetBytesNoKeyHint() throws Exception {
        Keyspace ks = createBytesKeyspace("m_get_b_nohint_" + UUID.randomUUID() + "_");
        Thread.sleep(1000);
        runMetricsBenchmark("Get(bytes, no KeyHint)", BENCHMARK_ITERATIONS, i ->
                client.get(ks.keys[ks.randomIndex()]));
    }

    @Test
    @DisplayName("Metrics: get random (int, KeyHint)")
    void metricsGetIntWithKeyHint() throws Exception {
        Keyspace ks = createKeyspace("m_get_i_hint_" + UUID.randomUUID() + "_", i -> vi(i));
        runMetricsBenchmark("Get(int, KeyHint)", BENCHMARK_ITERATIONS, i -> {
            int idx = ks.randomIndex();
            return client.get(ks.keys[idx], ks.hints[idx]);
        });
    }

    @Test
    @DisplayName("Metrics: get random (int, no KeyHint)")
    void metricsGetIntNoKeyHint() throws Exception {
        Keyspace ks = createKeyspace("m_get_i_nohint_" + UUID.randomUUID() + "_", i -> vi(i));
        runMetricsBenchmark("Get(int, no KeyHint)", BENCHMARK_ITERATIONS, i ->
                client.get(ks.keys[ks.randomIndex()]));
    }

    @Test
    @DisplayName("Metrics: get random (double, KeyHint)")
    void metricsGetDoubleWithKeyHint() throws Exception {
        Keyspace ks = createKeyspace("m_get_d_hint_" + UUID.randomUUID() + "_", i -> vd(i * 1.5));
        runMetricsBenchmark("Get(double, KeyHint)", BENCHMARK_ITERATIONS, i -> {
            int idx = ks.randomIndex();
            return client.get(ks.keys[idx], ks.hints[idx]);
        });
    }

    @Test
    @DisplayName("Metrics: get random (double, no KeyHint)")
    void metricsGetDoubleNoKeyHint() throws Exception {
        Keyspace ks = createKeyspace("m_get_d_nohint_" + UUID.randomUUID() + "_", i -> vd(i * 1.5));
        runMetricsBenchmark("Get(double, no KeyHint)", BENCHMARK_ITERATIONS, i ->
                client.get(ks.keys[ks.randomIndex()]));
    }

    @Test
    @DisplayName("Metrics: get random (heterogeneous, KeyHint)")
    void metricsGetHeterogeneousWithKeyHint() throws Exception {
        Keyspace kb = createBytesKeyspace("m_get_mix_b_" + UUID.randomUUID() + "_");
        Keyspace ki = createKeyspace("m_get_mix_i_" + UUID.randomUUID() + "_", i -> vi(i));
        Keyspace kd = createKeyspace("m_get_mix_d_" + UUID.randomUUID() + "_", i -> vd(i * 2.5));
        runMetricsBenchmark("Get(heterogeneous, KeyHint)", BENCHMARK_ITERATIONS, i -> {
            if (i % 3 == 0) {
                int idx = kb.randomIndex();
                return client.get(kb.keys[idx], kb.hints[idx]);
            } else if (i % 3 == 1) {
                int idx = ki.randomIndex();
                return client.get(ki.keys[idx], ki.hints[idx]);
            } else {
                int idx = kd.randomIndex();
                return client.get(kd.keys[idx], kd.hints[idx]);
            }
        });
    }

    // =========================================================================
    // REPLACE (single-threaded)
    // =========================================================================

    @Test
    @DisplayName("Metrics: replace random (bytes, KeyHint)")
    void metricsReplaceBytesWithKeyHint() throws Exception {
        Keyspace ks = createBytesKeyspace("m_replace_b_" + UUID.randomUUID() + "_");
        runMetricsBenchmark("Replace(bytes, KeyHint)", BENCHMARK_ITERATIONS, i -> {
            int idx = ks.randomIndex();
            return client.replace(ks.keys[idx], ks.hints[idx], v("new_bytes_" + i));
        });
    }

    @Test
    @DisplayName("Metrics: replace random (bytes, no KeyHint)")
    void metricsReplaceBytesNoKeyHint() throws Exception {
        Keyspace ks = createBytesKeyspace("m_replace_b_nh_" + UUID.randomUUID() + "_");
        runMetricsBenchmark("Replace(bytes, no KeyHint)", BENCHMARK_ITERATIONS, i ->
                client.replace(ks.keys[ks.randomIndex()], v("new_bytes_" + i)));
    }

    @Test
    @DisplayName("Metrics: replace random (int, KeyHint)")
    void metricsReplaceIntWithKeyHint() throws Exception {
        Keyspace ks = createKeyspace("m_replace_i_" + UUID.randomUUID() + "_", i -> vi(i));
        runMetricsBenchmark("Replace(int, KeyHint)", BENCHMARK_ITERATIONS, i -> {
            int idx = ks.randomIndex();
            return client.replace(ks.keys[idx], ks.hints[idx], vi(i));
        });
    }

    @Test
    @DisplayName("Metrics: replace random (double, KeyHint)")
    void metricsReplaceDoubleWithKeyHint() throws Exception {
        Keyspace ks = createKeyspace("m_replace_d_" + UUID.randomUUID() + "_", i -> vd(i * 1.1));
        runMetricsBenchmark("Replace(double, KeyHint)", BENCHMARK_ITERATIONS, i -> {
            int idx = ks.randomIndex();
            return client.replace(ks.keys[idx], ks.hints[idx], vd(i * 1.1));
        });
    }

    // =========================================================================
    // REMOVE (single-threaded)
    // =========================================================================

    @Test
    @DisplayName("Metrics: remove")
    void metricsRemove() throws Exception {
        Keyspace ks = createBytesKeyspace("m_remove_" + UUID.randomUUID() + "_");
        runMetricsBenchmark("Remove", KEYSPACE_SIZE, i -> client.remove(ks.keys[i]));
    }

    // =========================================================================
    // MULTI-THREADED
    // =========================================================================

    @Test
    @DisplayName("Metrics: create (bytes, multi-threaded)")
    void metricsCreateBytesMT() throws Exception {
        runMetricsBenchmarkMT("Create(bytes)", BENCHMARK_ITERATIONS, THREAD_COUNT, (threadId, i) ->
                client.create("m_create_bytes_mt_" + UUID.randomUUID(), v("value_bytes_" + i)));
    }

    @Test
    @DisplayName("Metrics: create (int, multi-threaded)")
    void metricsCreateIntMT() throws Exception {
        runMetricsBenchmarkMT("Create(int)", BENCHMARK_ITERATIONS, THREAD_COUNT, (threadId, i) ->
                client.create("m_create_int_mt_" + UUID.randomUUID(), vi(i)));
    }

    @Test
    @DisplayName("Metrics: create (double, multi-threaded)")
    void metricsCreateDoubleMT() throws Exception {
        runMetricsBenchmarkMT("Create(double)", BENCHMARK_ITERATIONS, THREAD_COUNT, (threadId, i) ->
                client.create("m_create_double_mt_" + UUID.randomUUID(), vd(i * 1.5)));
    }

    @Test
    @DisplayName("Metrics: create (heterogeneous, multi-threaded)")
    void metricsCreateHeterogeneousMT() throws Exception {
        runMetricsBenchmarkMT("Create(heterogeneous)", BENCHMARK_ITERATIONS, THREAD_COUNT, (threadId, i) -> {
            Variant value = (i % 3 == 0) ? v("mixed_bytes_" + i) : (i % 3 == 1) ? vi(i) : vd(i * 1.5);
            return client.create("m_create_mix_mt_" + UUID.randomUUID(), value);
        });
    }

    @Test
    @DisplayName("Metrics: get random (bytes, KeyHint, multi-threaded)")
    void metricsGetBytesWithKeyHintMT() throws Exception {
        Keyspace ks = createBytesKeyspace("m_get_b_hint_mt_" + UUID.randomUUID() + "_");
        Thread.sleep(1000);
        runMetricsBenchmarkMT("Get(bytes, KeyHint)", BENCHMARK_ITERATIONS, THREAD_COUNT, (threadId, i) -> {
            int idx = ks.randomIndex();
            return client.get(ks.keys[idx], ks.hints[idx]);
        });
    }

    @Test
    @DisplayName("Metrics: get random (bytes, no KeyHint, multi-threaded)")
    void metricsGetBytesNoKeyHintMT() throws Exception {
        Keyspace ks = createBytesKeyspace("m_get_b_nohint_mt_" + UUID.randomUUID() + "_");
        runMetricsBenchmarkMT("Get(bytes, no KeyHint)", BENCHMARK_ITERATIONS, THREAD_COUNT, (threadId, i) ->
                client.get(ks.keys[ks.randomIndex()]));
    }

    @Test
    @DisplayName("Metrics: get random (int, KeyHint, multi-threaded)")
    void metricsGetIntWithKeyHintMT() throws Exception {
        Keyspace ks = createKeyspace("m_get_i_hint_mt_" + UUID.randomUUID() + "_", i -> vi(i));
        runMetricsBenchmarkMT("Get(int, KeyHint)", BENCHMARK_ITERATIONS, THREAD_COUNT, (threadId, i) -> {
            int idx = ks.randomIndex();
            return client.get(ks.keys[idx], ks.hints[idx]);
        });
    }

    @Test
    @DisplayName("Metrics: get random (double, KeyHint, multi-threaded)")
    void metricsGetDoubleWithKeyHintMT() throws Exception {
        Keyspace ks = createKeyspace("m_get_d_hint_mt_" + UUID.randomUUID() + "_", i -> vd(i * 1.5));
        runMetricsBenchmarkMT("Get(double, KeyHint)", BENCHMARK_ITERATIONS, THREAD_COUNT, (threadId, i) -> {
            int idx = ks.randomIndex();
            return client.get(ks.keys[idx], ks.hints[idx]);
        });
    }

    @Test
    @DisplayName("Metrics: get random (heterogeneous, KeyHint, multi-threaded)")
    void metricsGetHeterogeneousWithKeyHintMT() throws Exception {
        Keyspace kb = createBytesKeyspace("m_get_mix_b_mt_" + UUID.randomUUID() + "_");
        Keyspace ki = createKeyspace("m_get_mix_i_mt_" + UUID.randomUUID() + "_", i -> vi(i));
        Keyspace kd = createKeyspace("m_get_mix_d_mt_" + UUID.randomUUID() + "_", i -> vd(i * 2.5));
        runMetricsBenchmarkMT("Get(heterogeneous, KeyHint)", BENCHMARK_ITERATIONS, THREAD_COUNT, (threadId, i) -> {
            if (i % 3 == 0) {
                int idx = kb.randomIndex();
                return client.get(kb.keys[idx], kb.hints[idx]);
            } else if (i % 3 == 1) {
                int idx = ki.randomIndex();
                return client.get(ki.keys[idx], ki.hints[idx]);
            } else {
                int idx = kd.randomIndex();
                return client.get(kd.keys[idx], kd.hints[idx]);
            }
        });
    }

    @Test
    @DisplayName("Metrics: replace random (bytes, KeyHint, multi-threaded)")
    void metricsReplaceBytesWithKeyHintMT() throws Exception {
        Keyspace ks = createBytesKeyspace("m_replace_b_mt_" + UUID.randomUUID() + "_");
        runMetricsBenchmarkMT("Replace(bytes, KeyHint)", BENCHMARK_ITERATIONS, THREAD_COUNT, (threadId, i) -> {
            int idx = ks.randomIndex();
            return client.replace(ks.keys[idx], ks.hints[idx], v("new_bytes_" + i));
        });
    }

    @Test
    @DisplayName("Metrics: replace random (int, KeyHint, multi-threaded)")
    void metricsReplaceIntWithKeyHintMT() throws Exception {
        Keyspace ks = createKeyspace("m_replace_i_mt_" + UUID.randomUUID() + "_", i -> vi(i));
        runMetricsBenchmarkMT("Replace(int, KeyHint)", BENCHMARK_ITERATIONS, THREAD_COUNT, (threadId, i) -> {
            int idx = ks.randomIndex();
            return client.replace(ks.keys[idx], ks.hints[idx], vi(i));
        });
    }

    @Test
    @DisplayName("Metrics: replace random (double, KeyHint, multi-threaded)")
    void metricsReplaceDoubleWithKeyHintMT() throws Exception {
        Keyspace ks = createKeyspace("m_replace_d_mt_" + UUID.randomUUID() + "_", i -> vd(i * 1.1));
        runMetricsBenchmarkMT("Replace(double, KeyHint)", BENCHMARK_ITERATIONS, THREAD_COUNT, (threadId, i) -> {
            int idx = ks.randomIndex();
            return client.replace(ks.keys[idx], ks.hints[idx], vd(i * 1.1));
        });
    }
}