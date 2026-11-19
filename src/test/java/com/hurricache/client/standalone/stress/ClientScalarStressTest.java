package com.hurricache.client.standalone.stress;

import com.hurricache.client.intf.Variant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.UUID;

/**
 * Standalone scalar stress test with latency/error metrics.
 * GET/REPLACE scenarios exist in two variants: with KeyHint and without.
 */
public class ClientScalarStressTest extends BaseStandaloneStressTest {

    // =========================================================================
    // CREATE
    // =========================================================================

    @Test
    @DisplayName("Stress test: create (bytes)")
    void stressTestCreateBytes() throws Exception {
        runMetricsBenchmark("Create(bytes)", BENCHMARK_ITERATIONS, i ->
                client.create("s_create_bytes_" + UUID.randomUUID(), v("value_bytes_" + (i % 100))));
    }

    @Test
    @DisplayName("Stress test: create (int)")
    void stressTestCreateInt() throws Exception {
        runMetricsBenchmark("Create(int)", BENCHMARK_ITERATIONS, i ->
                client.create("s_create_int_" + UUID.randomUUID(), vi(i)));
    }

    @Test
    @DisplayName("Stress test: create (double)")
    void stressTestCreateDouble() throws Exception {
        runMetricsBenchmark("Create(double)", BENCHMARK_ITERATIONS, i ->
                client.create("s_create_double_" + UUID.randomUUID(), vd(i * 1.5)));
    }

    @Test
    @DisplayName("Stress test: create (heterogeneous)")
    void stressTestCreateHeterogeneous() throws Exception {
        runMetricsBenchmark("Create(heterogeneous)", BENCHMARK_ITERATIONS, i -> {
            Variant value = (i % 3 == 0) ? v("mixed_bytes_" + i) : (i % 3 == 1) ? vi(i) : vd(i * 1.5);
            return client.create("s_create_mix_" + UUID.randomUUID(), value);
        });
    }

    // =========================================================================
    // GET
    // =========================================================================

    @Test
    @DisplayName("Stress test: get random (bytes, KeyHint)")
    void stressTestGetBytes() throws Exception {
        Keyspace ks = createBytesKeyspace("s_get_b_hint_" + UUID.randomUUID() + "_");
        runMetricsBenchmark("Get(bytes, KeyHint)", BENCHMARK_ITERATIONS, i -> {
            int idx = ks.randomIndex();
            return client.get(ks.keys[idx], ks.hints[idx]);
        });
    }

    @Test
    @DisplayName("Stress test: get random (bytes, no hint)")
    void stressTestGetBytesNoHint() throws Exception {
        Keyspace ks = createBytesKeyspace("s_get_b_nohint_" + UUID.randomUUID() + "_");
        runMetricsBenchmark("Get(bytes, no hint)", BENCHMARK_ITERATIONS, i ->
                client.get(ks.keys[ks.randomIndex()]));
    }

    @Test
    @DisplayName("Stress test: get random (int, KeyHint)")
    void stressTestGetInt() throws Exception {
        Keyspace ks = createKeyspace("s_get_i_hint_" + UUID.randomUUID() + "_", i -> vi(i));
        runMetricsBenchmark("Get(int, KeyHint)", BENCHMARK_ITERATIONS, i -> {
            int idx = ks.randomIndex();
            return client.get(ks.keys[idx], ks.hints[idx]);
        });
    }

    @Test
    @DisplayName("Stress test: get random (double, KeyHint)")
    void stressTestGetDouble() throws Exception {
        Keyspace ks = createKeyspace("s_get_d_hint_" + UUID.randomUUID() + "_", i -> vd(i * 1.5));
        runMetricsBenchmark("Get(double, KeyHint)", BENCHMARK_ITERATIONS, i -> {
            int idx = ks.randomIndex();
            return client.get(ks.keys[idx], ks.hints[idx]);
        });
    }

    @Test
    @DisplayName("Stress test: get random (heterogeneous, KeyHint)")
    void stressTestGetHeterogeneous() throws Exception {
        Keyspace kb = createBytesKeyspace("s_get_mix_b_" + UUID.randomUUID() + "_");
        Keyspace ki = createKeyspace("s_get_mix_i_" + UUID.randomUUID() + "_", i -> vi(i));
        Keyspace kd = createKeyspace("s_get_mix_d_" + UUID.randomUUID() + "_", i -> vd(i * 2.5));
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

    @Test
    @DisplayName("Stress test: get random (heterogeneous, no hint)")
    void stressTestGetHeterogeneousNoHint() throws Exception {
        Keyspace kb = createBytesKeyspace("s_get_mix_b_nh_" + UUID.randomUUID() + "_");
        Keyspace ki = createKeyspace("s_get_mix_i_nh_" + UUID.randomUUID() + "_", i -> vi(i));
        Keyspace kd = createKeyspace("s_get_mix_d_nh_" + UUID.randomUUID() + "_", i -> vd(i * 2.5));
        runMetricsBenchmark("Get(heterogeneous, no hint)", BENCHMARK_ITERATIONS, i -> {
            if (i % 3 == 0) return client.get(kb.keys[kb.randomIndex()]);
            else if (i % 3 == 1) return client.get(ki.keys[ki.randomIndex()]);
            else return client.get(kd.keys[kd.randomIndex()]);
        });
    }

    // =========================================================================
    // REPLACE
    // =========================================================================

    @Test
    @DisplayName("Stress test: replace random (bytes, KeyHint)")
    void stressTestReplaceBytes() throws Exception {
        Keyspace ks = createBytesKeyspace("s_replace_b_" + UUID.randomUUID() + "_");
        runMetricsBenchmark("Replace(bytes, KeyHint)", BENCHMARK_ITERATIONS, i -> {
            int idx = ks.randomIndex();
            return client.replace(ks.keys[idx], ks.hints[idx], v("new_bytes_" + i));
        });
    }

    @Test
    @DisplayName("Stress test: replace random (bytes, no hint)")
    void stressTestReplaceBytesNoHint() throws Exception {
        Keyspace ks = createBytesKeyspace("s_replace_b_nh_" + UUID.randomUUID() + "_");
        runMetricsBenchmark("Replace(bytes, no hint)", BENCHMARK_ITERATIONS, i ->
                client.replace(ks.keys[ks.randomIndex()], v("new_bytes_" + i)));
    }

    @Test
    @DisplayName("Stress test: replace random (int, KeyHint)")
    void stressTestReplaceInt() throws Exception {
        Keyspace ks = createKeyspace("s_replace_i_" + UUID.randomUUID() + "_", i -> vi(i));
        runMetricsBenchmark("Replace(int, KeyHint)", BENCHMARK_ITERATIONS, i -> {
            int idx = ks.randomIndex();
            return client.replace(ks.keys[idx], ks.hints[idx], vi(i));
        });
    }

    @Test
    @DisplayName("Stress test: replace random (double, KeyHint)")
    void stressTestReplaceDouble() throws Exception {
        Keyspace ks = createKeyspace("s_replace_d_" + UUID.randomUUID() + "_", i -> vd(i * 1.1));
        runMetricsBenchmark("Replace(double, KeyHint)", BENCHMARK_ITERATIONS, i -> {
            int idx = ks.randomIndex();
            return client.replace(ks.keys[idx], ks.hints[idx], vd(i * 1.1));
        });
    }

    // =========================================================================
    // REMOVE
    // =========================================================================

    @Test
    @DisplayName("Stress test: remove")
    void stressTestRemove() throws Exception {
        Keyspace ks = createBytesKeyspace("s_remove_" + UUID.randomUUID() + "_");
        runMetricsBenchmark("Remove", KEYSPACE_SIZE, i -> client.remove(ks.keys[i]));
    }

    // =========================================================================
    // MULTI-THREADED
    // =========================================================================

    @Test
    @DisplayName("Stress test: get random (heterogeneous, KeyHint, multi-threaded)")
    void stressTestGetHeterogeneousMultiThreaded() throws Exception {
        Keyspace kb = createBytesKeyspace("s_get_mix_b_mt_" + UUID.randomUUID() + "_");
        Keyspace ki = createKeyspace("s_get_mix_i_mt_" + UUID.randomUUID() + "_", i -> vi(i));
        Keyspace kd = createKeyspace("s_get_mix_d_mt_" + UUID.randomUUID() + "_", i -> vd(i * 2.5));
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
}
