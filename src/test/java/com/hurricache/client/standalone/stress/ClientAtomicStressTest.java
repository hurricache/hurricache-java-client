package com.hurricache.client.standalone.stress;

import com.hurricache.client.intf.KeyHintData;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.UUID;

/**
 * Standalone atomics stress test with latency/error metrics (hint and no-hint variants).
 */
public class ClientAtomicStressTest extends BaseStandaloneStressTest {

    @Test
    @DisplayName("Stress test: exchange (long)")
    void stressTestExchangeLong() throws Exception {
        String key = "s_exchange_long_" + UUID.randomUUID();
        KeyHintData hint = client.create(key, vi(0)).get();
        runMetricsBenchmark("Exchange(long)", BENCHMARK_ITERATIONS, i -> client.exchange(key, hint, i));
    }

    @Test
    @DisplayName("Stress test: exchange (double)")
    void stressTestExchangeDouble() throws Exception {
        String key = "s_exchange_double_" + UUID.randomUUID();
        KeyHintData hint = client.create(key, vd(0.0)).get();
        runMetricsBenchmark("Exchange(double)", BENCHMARK_ITERATIONS, i -> client.exchange(key, hint, i * 1.0));
    }

    @Test
    @DisplayName("Stress test: add (long, KeyHint)")
    void stressTestAddLong() throws Exception {
        String key = "s_add_long_" + UUID.randomUUID();
        KeyHintData hint = client.create(key, vi(0)).get();
        runMetricsBenchmark("Add(long, KeyHint)", BENCHMARK_ITERATIONS, i -> client.add(key, hint, 1));
    }

    @Test
    @DisplayName("Stress test: add (long, no hint)")
    void stressTestAddLongNoHint() throws Exception {
        String key = "s_add_long_nh_" + UUID.randomUUID();
        client.create(key, vi(0)).get();
        runMetricsBenchmark("Add(long, no hint)", BENCHMARK_ITERATIONS, i -> client.add(key, 1));
    }

    @Test
    @DisplayName("Stress test: add (double, KeyHint)")
    void stressTestAddDouble() throws Exception {
        String key = "s_add_double_" + UUID.randomUUID();
        KeyHintData hint = client.create(key, vd(0.0)).get();
        runMetricsBenchmark("Add(double, KeyHint)", BENCHMARK_ITERATIONS, i -> client.add(key, hint, 1.0));
    }

    @Test
    @DisplayName("Stress test: add (double, no hint)")
    void stressTestAddDoubleNoHint() throws Exception {
        String key = "s_add_double_nh_" + UUID.randomUUID();
        client.create(key, vd(0.0)).get();
        runMetricsBenchmark("Add(double, no hint)", BENCHMARK_ITERATIONS, i -> client.add(key, 1.0));
    }

    @Test
    @DisplayName("Stress test: sub (long)")
    void stressTestSubLong() throws Exception {
        String key = "s_sub_long_" + UUID.randomUUID();
        KeyHintData hint = client.create(key, vi(BENCHMARK_ITERATIONS)).get();
        runMetricsBenchmark("Sub(long)", BENCHMARK_ITERATIONS, i -> client.sub(key, hint, 1));
    }

    @Test
    @DisplayName("Stress test: sub (double)")
    void stressTestSubDouble() throws Exception {
        String key = "s_sub_double_" + UUID.randomUUID();
        KeyHintData hint = client.create(key, vd(BENCHMARK_ITERATIONS * 1.0)).get();
        runMetricsBenchmark("Sub(double)", BENCHMARK_ITERATIONS, i -> client.sub(key, hint, 1.0));
    }

    @Test
    @DisplayName("Stress test: and")
    void stressTestAnd() throws Exception {
        String key = "s_and_" + UUID.randomUUID();
        KeyHintData hint = client.create(key, vi(-1)).get();
        runMetricsBenchmark("And", BENCHMARK_ITERATIONS, i -> client.and(key, hint, 0xFF));
    }

    @Test
    @DisplayName("Stress test: or")
    void stressTestOr() throws Exception {
        String key = "s_or_" + UUID.randomUUID();
        KeyHintData hint = client.create(key, vi(0)).get();
        runMetricsBenchmark("Or", BENCHMARK_ITERATIONS, i -> client.or(key, hint, 0xFF));
    }

    @Test
    @DisplayName("Stress test: xor")
    void stressTestXor() throws Exception {
        String key = "s_xor_" + UUID.randomUUID();
        KeyHintData hint = client.create(key, vi(0)).get();
        runMetricsBenchmark("Xor", BENCHMARK_ITERATIONS, i -> client.xor(key, hint, 0xFF));
    }

    @Test
    @DisplayName("Stress test: compareAndSet (long)")
    void stressTestCasLong() throws Exception {
        String key = "s_cas_long_" + UUID.randomUUID();
        KeyHintData hint = client.create(key, vi(0)).get();
        runMetricsBenchmark("CAS(long)", BENCHMARK_ITERATIONS, i -> client.compareAndSet(key, hint, i % 2, i % 2 + 1));
    }

    @Test
    @DisplayName("Stress test: compareAndSet (double)")
    void stressTestCasDouble() throws Exception {
        String key = "s_cas_double_" + UUID.randomUUID();
        KeyHintData hint = client.create(key, vd(0.0)).get();
        runMetricsBenchmark("CAS(double)", BENCHMARK_ITERATIONS, i -> client.compareAndSet(key, hint, i % 2.0, i % 2.0 + 1.0));
    }

    @Test
    @DisplayName("Stress test: add (multi-threaded)")
    void stressTestAddMultiThreaded() throws Exception {
        String key = "s_add_mt_" + UUID.randomUUID();
        KeyHintData hint = client.create(key, vi(0)).get();
        runMetricsBenchmarkMT("Add(long)", BENCHMARK_ITERATIONS, THREAD_COUNT, (threadId, i) -> client.add(key, hint, 1));
    }
}
