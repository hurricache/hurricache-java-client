package com.hurricache.client.cluster.stress;

import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Variant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.util.UUID;

@Execution(ExecutionMode.SAME_THREAD)
public class SmartClientScalarStressTest extends BaseClusterStressTest {

    // =========================================================================
    // CREATE TESTS (SINGLE-THREADED)
    // =========================================================================

    @Test
    @DisplayName("Stress test: create (bytes)")
    void stressTestCreateBytes() throws Exception {
        runBenchmark("Create(bytes)", BENCHMARK_ITERATIONS, i ->
                client.create("stress_create_bytes_" + UUID.randomUUID(), v("value_bytes_" + (i % 100)))
        );
    }

    @Test
    @DisplayName("Stress test: create (int)")
    void stressTestCreateInt() throws Exception {
        runBenchmark("Create(int)", BENCHMARK_ITERATIONS, i ->
                client.create("stress_create_int_" + UUID.randomUUID(), vi(i))
        );
    }

    @Test
    @DisplayName("Stress test: create (double)")
    void stressTestCreateDouble() throws Exception {
        runBenchmark("Create(double)", BENCHMARK_ITERATIONS, i ->
                client.create("stress_create_double_" + UUID.randomUUID(), vd(i * 1.5))
        );
    }

    @Test
    @DisplayName("Stress test: create (heterogeneous)")
    void stressTestCreateHeterogeneous() throws Exception {
        runBenchmark("Create(heterogeneous)", BENCHMARK_ITERATIONS, i -> {
            Variant value = (i % 3 == 0) ? v("mixed_bytes_" + i) : (i % 3 == 1) ? vi(i) : vd(i * 1.5);
            return client.create("stress_create_mix_" + UUID.randomUUID(), value);
        });
    }

    // =========================================================================
    // GET TESTS (SINGLE-THREADED)
    // =========================================================================

    @Test
    @DisplayName("Stress test: get (bytes)")
    void stressTestGetBytes() throws Exception {
        String key = "stress_get_bytes_" + UUID.randomUUID();
        KeyHintData hint = client.create(key, v("hello_world_bytes_data")).get();
        Thread.sleep(1000);

        runWarmup(WARMUP_ITERATIONS, i -> client.get(key, hint));
        runBenchmark("Get(bytes)", BENCHMARK_ITERATIONS, i -> client.get(key, hint));
    }

    @Test
    @DisplayName("Stress test: get (int)")
    void stressTestGetInt() throws Exception {
        String key = "stress_get_int_" + UUID.randomUUID();
        KeyHintData hint = client.create(key, vi(42)).get();
        Thread.sleep(1000);

        runWarmup(WARMUP_ITERATIONS, i -> client.get(key, hint));
        runBenchmark("Get(int)", BENCHMARK_ITERATIONS, i -> client.get(key, hint));
    }

    @Test
    @DisplayName("Stress test: get (double)")
    void stressTestGetDouble() throws Exception {
        String key = "stress_get_double_" + UUID.randomUUID();
        KeyHintData hint = client.create(key, vd(3.14159265)).get();
        Thread.sleep(1000);

        runWarmup(WARMUP_ITERATIONS, i -> client.get(key, hint));
        runBenchmark("Get(double)", BENCHMARK_ITERATIONS, i -> client.get(key, hint));
    }

    @Test
    @DisplayName("Stress test: get (heterogeneous)")
    void stressTestGetHeterogeneous() throws Exception {
        String kb = "stress_get_mix_b_" + UUID.randomUUID();
        String ki = "stress_get_mix_i_" + UUID.randomUUID();
        String kd = "stress_get_mix_d_" + UUID.randomUUID();

        KeyHintData hb = client.create(kb, v("mixed_bytes_data")).get();
        KeyHintData hi = client.create(ki, vi(123)).get();
        KeyHintData hd = client.create(kd, vd(2.71828)).get();
        Thread.sleep(1000);

        runWarmup(WARMUP_ITERATIONS, i -> {
            if (i % 3 == 0) client.get(kb, hb).get();
            else if (i % 3 == 1) client.get(ki, hi).get();
            else client.get(kd, hd).get();
            return null;
        });

        runBenchmark("Get(heterogeneous)", BENCHMARK_ITERATIONS, i -> {
            if (i % 3 == 0) return client.get(kb, hb);
            else if (i % 3 == 1) return client.get(ki, hi);
            else return client.get(kd, hd);
        });
    }

    // =========================================================================
    // REPLACE TESTS (SINGLE-THREADED)
    // =========================================================================

    @Test
    @DisplayName("Stress test: replace (bytes)")
    void stressTestReplaceBytes() throws Exception {
        String key = "stress_replace_bytes_" + UUID.randomUUID();
        KeyHintData hint = client.create(key, v("old_bytes_value")).get();
        Thread.sleep(1000);

        runWarmup(WARMUP_ITERATIONS, i -> client.replace(key, hint, v("w_" + i)));
        runBenchmark("Replace(bytes)", BENCHMARK_ITERATIONS, i -> client.replace(key, hint, v("new_bytes_" + i)));
    }

    @Test
    @DisplayName("Stress test: replace (int)")
    void stressTestReplaceInt() throws Exception {
        String key = "stress_replace_int_" + UUID.randomUUID();
        KeyHintData hint = client.create(key, vi(100)).get();
        Thread.sleep(1000);

        runWarmup(WARMUP_ITERATIONS, i -> client.replace(key, hint, vi(i)));
        runBenchmark("Replace(int)", BENCHMARK_ITERATIONS, i -> client.replace(key, hint, vi(i)));
    }

    @Test
    @DisplayName("Stress test: replace (double)")
    void stressTestReplaceDouble() throws Exception {
        String key = "stress_replace_double_" + UUID.randomUUID();
        KeyHintData hint = client.create(key, vd(2.71)).get();
        Thread.sleep(1000);

        runWarmup(WARMUP_ITERATIONS, i -> client.replace(key, hint, vd(i * 1.1)));
        runBenchmark("Replace(double)", BENCHMARK_ITERATIONS, i -> client.replace(key, hint, vd(i * 1.1)));
    }

    // =========================================================================
    // REMOVE TESTS (SINGLE-THREADED)
    // =========================================================================

    @Test
    @DisplayName("Stress test: remove (bytes)")
    void stressTestRemoveBytes() throws Exception {
        String[] keys = new String[BENCHMARK_ITERATIONS];
        for (int i = 0; i < BENCHMARK_ITERATIONS; i++) {
            keys[i] = "rem_key_" + i + "_" + UUID.randomUUID();
            client.create(keys[i], v("val")).get();
        }
        Thread.sleep(300);

        runBenchmark("Remove(bytes)", BENCHMARK_ITERATIONS, i -> client.remove(keys[i]));
    }

    // =========================================================================
    // MULTI-THREADED TESTS
    // =========================================================================

    @Test
    @DisplayName("Stress test: create (bytes, multi-threaded)")
    void stressTestCreateBytesMultiThreaded() throws Exception {
        runMultiThreadedBenchmark("Create(bytes)", BENCHMARK_ITERATIONS, THREAD_COUNT, (threadId, i) ->
                client.create("stress_create_bytes_mt_" + UUID.randomUUID(), v("value_bytes_" + i))
        );
    }

    @Test
    @DisplayName("Stress test: create (int, multi-threaded)")
    void stressTestCreateIntMultiThreaded() throws Exception {
        runMultiThreadedBenchmark("Create(int)", BENCHMARK_ITERATIONS, THREAD_COUNT, (threadId, i) ->
                client.create("stress_create_int_mt_" + UUID.randomUUID(), vi(i))
        );
    }

    @Test
    @DisplayName("Stress test: create (double, multi-threaded)")
    void stressTestCreateDoubleMultiThreaded() throws Exception {
        runMultiThreadedBenchmark("Create(double)", BENCHMARK_ITERATIONS, THREAD_COUNT, (threadId, i) ->
                client.create("stress_create_double_mt_" + UUID.randomUUID(), vd(i * 1.5))
        );
    }

    @Test
    @DisplayName("Stress test: create (heterogeneous, multi-threaded)")
    void stressTestCreateHeterogeneousMultiThreaded() throws Exception {
        runMultiThreadedBenchmark("Create(heterogeneous)", BENCHMARK_ITERATIONS, THREAD_COUNT, (threadId, i) -> {
            Variant value = (i % 3 == 0) ? v("mixed_bytes_" + i) : (i % 3 == 1) ? vi(i) : vd(i * 1.5);
            return client.create("stress_create_mix_mt_" + UUID.randomUUID(), value);
        });
    }

    @Test
    @DisplayName("Stress test: get (bytes, multi-threaded)")
    void stressTestGetBytesMultiThreaded() throws Exception {
        String key = "stress_get_bytes_mt_" + UUID.randomUUID();
        client.create(key, v("hello_world_bytes_data_mt")).get();
        Thread.sleep(1000);

        runMultiThreadedBenchmark("Get(bytes)", BENCHMARK_ITERATIONS, THREAD_COUNT, (threadId, i) -> client.get(key));
    }

    @Test
    @DisplayName("Stress test: get (int, multi-threaded)")
    void stressTestGetIntMultiThreaded() throws Exception {
        String key = "stress_get_int_mt_" + UUID.randomUUID();
        client.create(key, vi(42)).get();
        Thread.sleep(1000);

        runMultiThreadedBenchmark("Get(int)", BENCHMARK_ITERATIONS, THREAD_COUNT, (threadId, i) -> client.get(key));
    }

    @Test
    @DisplayName("Stress test: get (double, multi-threaded)")
    void stressTestGetDoubleMultiThreaded() throws Exception {
        String key = "stress_get_double_mt_" + UUID.randomUUID();
        client.create(key, vd(3.14159265)).get();
        Thread.sleep(1000);

        runMultiThreadedBenchmark("Get(double)", BENCHMARK_ITERATIONS, THREAD_COUNT, (threadId, i) -> client.get(key));
    }

    @Test
    @DisplayName("Stress test: get (heterogeneous, multi-threaded)")
    void stressTestGetHeterogeneousMultiThreaded() throws Exception {
        String kb = "stress_get_mix_mt_b_" + UUID.randomUUID();
        String ki = "stress_get_mix_mt_i_" + UUID.randomUUID();
        String kd = "stress_get_mix_mt_d_" + UUID.randomUUID();

        client.create(kb, v("mixed_bytes")).get();
        client.create(ki, vi(123)).get();
        client.create(kd, vd(2.71)).get();
        Thread.sleep(1000);

        runMultiThreadedBenchmark("Get(heterogeneous)", BENCHMARK_ITERATIONS, THREAD_COUNT, (threadId, i) -> {
            if (i % 3 == 0) return client.get(kb);
            else if (i % 3 == 1) return client.get(ki);
            else return client.get(kd);
        });
    }

    @Test
    @DisplayName("Stress test: replace (bytes, multi-threaded)")
    void stressTestReplaceBytesMultiThreaded() throws Exception {
        String key = "stress_replace_bytes_mt_" + UUID.randomUUID();
        client.create(key, v("old_bytes_mt")).get();
        Thread.sleep(1000);

        runMultiThreadedBenchmark("Replace(bytes)", BENCHMARK_ITERATIONS, THREAD_COUNT, (threadId, i) -> client.replace(key, v("new_bytes_" + i)));
    }

    @Test
    @DisplayName("Stress test: replace (int, multi-threaded)")
    void stressTestReplaceIntMultiThreaded() throws Exception {
        String key = "stress_replace_int_mt_" + UUID.randomUUID();
        client.create(key, vi(100)).get();
        Thread.sleep(1000);

        runMultiThreadedBenchmark("Replace(int)", BENCHMARK_ITERATIONS, THREAD_COUNT, (threadId, i) -> client.replace(key, vi(i % 100)));
    }

    @Test
    @DisplayName("Stress test: replace (double, multi-threaded)")
    void stressTestReplaceDoubleMultiThreaded() throws Exception {
        String key = "stress_replace_double_mt_" + UUID.randomUUID();
        client.create(key, vd(2.71)).get();
        Thread.sleep(1000);

        runMultiThreadedBenchmark("Replace(double)", BENCHMARK_ITERATIONS, THREAD_COUNT, (threadId, i) -> client.replace(key, vd(i * 1.1)));
    }
}