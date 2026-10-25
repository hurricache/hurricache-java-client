package com.hurricache.client.cluster.stress;

import com.hurricache.client.intf.KeyHintData;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.UUID;

public class SmartClientAtomicStressTest extends BaseClusterStressTest {

    @Test
    @DisplayName("Stress test: exchange (long)")
    void stressTestExchangeLong() throws Exception {
        String key = "stress_exchange_long_" + UUID.randomUUID();
        KeyHintData hint = client.create(key, vi(0)).get();
        Thread.sleep(500);

        runWarmup(WARMUP_ITERATIONS, i -> client.exchange(key, hint, i));
        runBenchmark("Exchange(long)", BENCHMARK_ITERATIONS, i -> client.exchange(key, hint, i));
    }

    @Test
    @DisplayName("Stress test: exchange (double)")
    void stressTestExchangeDouble() throws Exception {
        String key = "stress_exchange_double_" + UUID.randomUUID();
        KeyHintData hint = client.create(key, vd(0.0)).get();
        Thread.sleep(500);

        runWarmup(WARMUP_ITERATIONS, i -> client.exchange(key, hint, i * 1.0));
        runBenchmark("Exchange(double)", BENCHMARK_ITERATIONS, i -> client.exchange(key, hint, i * 1.0));
    }

    @Test
    @DisplayName("Stress test: add (long)")
    void stressTestAddLong() throws Exception {
        String key = "stress_add_long_" + UUID.randomUUID();
        KeyHintData hint = client.create(key, vi(0)).get();
        Thread.sleep(500);

        runWarmup(WARMUP_ITERATIONS, i -> client.add(key, hint, 1));
        runBenchmark("Add(long)", BENCHMARK_ITERATIONS, i -> client.add(key, hint, 1));
    }

    @Test
    @DisplayName("Stress test: add (double)")
    void stressTestAddDouble() throws Exception {
        String key = "stress_add_double_" + UUID.randomUUID();
        KeyHintData hint = client.create(key, vd(0.0)).get();
        Thread.sleep(500);

        runWarmup(WARMUP_ITERATIONS, i -> client.add(key, hint, 1.0));
        runBenchmark("Add(double)", BENCHMARK_ITERATIONS, i -> client.add(key, hint, 1.0));
    }

    @Test
    @DisplayName("Stress test: sub (long)")
    void stressTestSubLong() throws Exception {
        String key = "stress_sub_long_" + UUID.randomUUID();
        KeyHintData hint = client.create(key, vi(BENCHMARK_ITERATIONS)).get();
        Thread.sleep(500);

        runWarmup(WARMUP_ITERATIONS, i -> client.sub(key, hint, 1));
        runBenchmark("Sub(long)", BENCHMARK_ITERATIONS, i -> client.sub(key, hint, 1));
    }

    @Test
    @DisplayName("Stress test: sub (double)")
    void stressTestSubDouble() throws Exception {
        String key = "stress_sub_double_" + UUID.randomUUID();
        KeyHintData hint = client.create(key, vd(BENCHMARK_ITERATIONS * 1.0)).get();
        Thread.sleep(500);

        runWarmup(WARMUP_ITERATIONS, i -> client.sub(key, hint, 1.0));
        runBenchmark("Sub(double)", BENCHMARK_ITERATIONS, i -> client.sub(key, hint, 1.0));
    }

    @Test
    @DisplayName("Stress test: and")
    void stressTestAnd() throws Exception {
        String key = "stress_and_" + UUID.randomUUID();
        KeyHintData hint = client.create(key, vi(-1)).get();
        Thread.sleep(500);

        runWarmup(WARMUP_ITERATIONS, i -> client.and(key, hint, 0xFF));
        runBenchmark("And", BENCHMARK_ITERATIONS, i -> client.and(key, hint, 0xFF));
    }

    @Test
    @DisplayName("Stress test: or")
    void stressTestOr() throws Exception {
        String key = "stress_or_" + UUID.randomUUID();
        KeyHintData hint = client.create(key, vi(0)).get();
        Thread.sleep(500);

        runWarmup(WARMUP_ITERATIONS, i -> client.or(key, hint, 0xFF));
        runBenchmark("Or", BENCHMARK_ITERATIONS, i -> client.or(key, hint, 0xFF));
    }

    @Test
    @DisplayName("Stress test: xor")
    void stressTestXor() throws Exception {
        String key = "stress_xor_" + UUID.randomUUID();
        KeyHintData hint = client.create(key, vi(0)).get();
        Thread.sleep(500);

        runWarmup(WARMUP_ITERATIONS, i -> client.xor(key, hint, 0xFF));
        runBenchmark("Xor", BENCHMARK_ITERATIONS, i -> client.xor(key, hint, 0xFF));
    }

    @Test
    @DisplayName("Stress test: compareAndSet (long)")
    void stressTestCasLong() throws Exception {
        String key = "stress_cas_long_" + UUID.randomUUID();
        KeyHintData hint = client.create(key, vi(0)).get();
        Thread.sleep(500);

        runWarmup(WARMUP_ITERATIONS, i -> client.compareAndSet(key, hint, i % 2, i % 2 + 1));
        runBenchmark("CAS(long)", BENCHMARK_ITERATIONS, i -> client.compareAndSet(key, hint, i % 2, i % 2 + 1));
    }

    @Test
    @DisplayName("Stress test: compareAndSet (double)")
    void stressTestCasDouble() throws Exception {
        String key = "stress_cas_double_" + UUID.randomUUID();
        KeyHintData hint = client.create(key, vd(0.0)).get();
        Thread.sleep(500);

        runWarmup(WARMUP_ITERATIONS, i -> client.compareAndSet(key, hint, i % 2.0, i % 2.0 + 1.0));
        runBenchmark("CAS(double)", BENCHMARK_ITERATIONS, i -> client.compareAndSet(key, hint, i % 2.0, i % 2.0 + 1.0));
    }

    @Test
    @DisplayName("Stress test: add (multi-threaded)")
    void stressTestAddMultiThreaded() throws Exception {
        String key = "stress_add_mt_" + UUID.randomUUID();
        KeyHintData hint = client.create(key, vi(0)).get();
        Thread.sleep(500);

        runMultiThreadedBenchmark("Add(long)", BENCHMARK_ITERATIONS, THREAD_COUNT, (threadId, i) -> client.add(key, hint, 1));
    }
}