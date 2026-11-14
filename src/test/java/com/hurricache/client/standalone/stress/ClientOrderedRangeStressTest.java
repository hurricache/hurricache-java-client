package com.hurricache.client.standalone.stress;

import com.hurricache.TestBase;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.OrderedPayload;
import com.hurricache.client.intf.OrderedVariant;
import com.hurricache.client.intf.Variant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Stress tests for getAndDeleteElementsInRange operations on ordered containers (standalone mode).
 * Tests OrderedSet, OrderedMap, List, and Vector range deletion with proper concurrency control.
 */
@Execution(ExecutionMode.SAME_THREAD)
public class ClientOrderedRangeStressTest extends TestBase {

    private static final int CONTAINER_SIZE = 1000;
    private static final int BENCHMARK_ITERATIONS = 50000;
    private static final int CONCURRENCY = 100;
    private static final int THREAD_COUNT = 4;

    // =========================================================================
    // ORDERED SET RANGE DELETE TESTS
    // =========================================================================

    @Test
    @DisplayName("Stress: getAndDeleteElementInRange (OrderedSet, full range)")
    void stressTestOrderedSetFullRange() throws Exception {
        String key = "os_full_" + UUID.randomUUID();

        List<OrderedVariant> data = new ArrayList<>();
        for (int i = 1; i <= CONTAINER_SIZE; i++) {
            data.add(OrderedVariant.ofInt(i, (long) i));
        }
        KeyHintData hint = client.createOrderedSet(key, data).get();
        Thread.sleep(500);

        runBenchmark("OrderedSet full range", BENCHMARK_ITERATIONS, i ->
                client.getAndDeleteElementInRangeOrderedSet(key, hint, 0, 0, false)
        );
    }

    @Test
    @DisplayName("Stress: getAndDeleteElementInRange (OrderedSet, partial)")
    void stressTestOrderedSetPartial() throws Exception {
        String key = "os_partial_" + UUID.randomUUID();

        List<OrderedVariant> data = new ArrayList<>();
        for (int i = 1; i <= CONTAINER_SIZE; i++) {
            data.add(OrderedVariant.ofInt(i, (long) i));
        }
        KeyHintData hint = client.createOrderedSet(key, data).get();
        Thread.sleep(500);

        runBenchmark("OrderedSet partial range", BENCHMARK_ITERATIONS, i ->
                client.getAndDeleteElementInRangeOrderedSet(key, hint, 100, 200, false)
        );
    }

    @Test
    @DisplayName("Stress: getAndDeleteElementInRange (OrderedSet, start only)")
    void stressTestOrderedSetStartOnly() throws Exception {
        String key = "os_start_" + UUID.randomUUID();

        List<OrderedVariant> data = new ArrayList<>();
        for (int i = 1; i <= CONTAINER_SIZE; i++) {
            data.add(OrderedVariant.ofInt(i, (long) i));
        }
        KeyHintData hint = client.createOrderedSet(key, data).get();
        Thread.sleep(500);

        runBenchmark("OrderedSet start only", BENCHMARK_ITERATIONS, i ->
                client.getAndDeleteElementInRangeOrderedSet(key, hint, 500, 0, false)
        );
    }

    @Test
    @DisplayName("Stress: getAndDeleteElementInRange (OrderedSet, end only)")
    void stressTestOrderedSetEndOnly() throws Exception {
        String key = "os_end_" + UUID.randomUUID();

        List<OrderedVariant> data = new ArrayList<>();
        for (int i = 1; i <= CONTAINER_SIZE; i++) {
            data.add(OrderedVariant.ofInt(i, (long) i));
        }
        KeyHintData hint = client.createOrderedSet(key, data).get();
        Thread.sleep(500);

        runBenchmark("OrderedSet end only", BENCHMARK_ITERATIONS, i ->
                client.getAndDeleteElementInRangeOrderedSet(key, hint, 0, 500, false)
        );
    }

    @Test
    @DisplayName("Stress: getAndDeleteElementInRange (OrderedSet, reverse)")
    void stressTestOrderedSetReverse() throws Exception {
        String key = "os_reverse_" + UUID.randomUUID();

        List<OrderedVariant> data = new ArrayList<>();
        for (int i = 1; i <= CONTAINER_SIZE; i++) {
            data.add(OrderedVariant.ofInt(i, (long) i));
        }
        KeyHintData hint = client.createOrderedSet(key, data).get();
        Thread.sleep(500);

        runBenchmark("OrderedSet reverse range", BENCHMARK_ITERATIONS, i ->
                client.getAndDeleteElementInRangeOrderedSet(key, hint, 500, 100, true)
        );
    }

    // =========================================================================
    // ORDERED MAP RANGE DELETE TESTS
    // =========================================================================

    @Test
    @DisplayName("Stress: getAndDeleteElementInRange (OrderedMap, partial)")
    void stressTestOrderedMapPartial() throws Exception {
        String key = "om_partial_" + UUID.randomUUID();

        Map<OrderedPayload, Variant> data = new LinkedHashMap<>();
        for (int i = 1; i <= CONTAINER_SIZE; i++) {
            data.put(new OrderedPayload(("key_" + i).getBytes(), (long) i), vi(i));
        }
        KeyHintData hint = client.createOrderedMap(key, data).get();
        Thread.sleep(500);

        runBenchmark("OrderedMap partial range", BENCHMARK_ITERATIONS, i ->
                client.getAndDeleteElementInRangeOrderedMap(key, hint, 100, 200, false)
        );
    }

    @Test
    @DisplayName("Stress: getAndDeleteElementInRange (OrderedMap, full range)")
    void stressTestOrderedMapFull() throws Exception {
        String key = "om_full_" + UUID.randomUUID();

        Map<OrderedPayload, Variant> data = new LinkedHashMap<>();
        for (int i = 1; i <= CONTAINER_SIZE; i++) {
            data.put(new OrderedPayload(("key_" + i).getBytes(), (long) i), vi(i));
        }
        KeyHintData hint = client.createOrderedMap(key, data).get();
        Thread.sleep(500);

        runBenchmark("OrderedMap full range", BENCHMARK_ITERATIONS, i ->
                client.getAndDeleteElementInRangeOrderedMap(key, hint, 0, 0, false)
        );
    }

    // =========================================================================
    // LIST RANGE DELETE TESTS
    // =========================================================================

    @Test
    @DisplayName("Stress: getAndDeleteListElementsInRange (List, partial)")
    void stressTestListPartial() throws Exception {
        String key = "list_partial_" + UUID.randomUUID();

        List<Variant> data = new ArrayList<>();
        for (int i = 0; i < CONTAINER_SIZE; i++) {
            data.add(vi(i));
        }
        KeyHintData hint = client.createList(key, data).get();
        Thread.sleep(500);

        runBenchmark("List partial range", BENCHMARK_ITERATIONS, i ->
                client.getAndDeleteListElementsInRange(key, hint, 0, 100, false)
        );
    }

    @Test
    @DisplayName("Stress: getAndDeleteListElementsInRange (List, full range)")
    void stressTestListFull() throws Exception {
        String key = "list_full_" + UUID.randomUUID();

        List<Variant> data = new ArrayList<>();
        for (int i = 0; i < CONTAINER_SIZE; i++) {
            data.add(vi(i));
        }
        KeyHintData hint = client.createList(key, data).get();
        Thread.sleep(500);

        runBenchmark("List full range", BENCHMARK_ITERATIONS, i ->
                client.getAndDeleteListElementsInRange(key, hint, 0, 0, false)
        );
    }

    // =========================================================================
    // VECTOR RANGE DELETE TESTS
    // =========================================================================

    @Test
    @DisplayName("Stress: getAndDeleteVectorElementsInRange (Vector, partial)")
    void stressTestVectorPartial() throws Exception {
        String key = "vec_partial_" + UUID.randomUUID();

        List<Variant> data = new ArrayList<>();
        for (int i = 0; i < CONTAINER_SIZE; i++) {
            data.add(vi(i));
        }
        KeyHintData hint = client.createVector(key, data).get();
        Thread.sleep(500);

        runBenchmark("Vector partial range", BENCHMARK_ITERATIONS, i ->
                client.getAndDeleteVectorElementsInRange(key, hint, 0, 100, false)
        );
    }

    @Test
    @DisplayName("Stress: getAndDeleteVectorElementsInRange (Vector, full range)")
    void stressTestVectorFull() throws Exception {
        String key = "vec_full_" + UUID.randomUUID();

        List<Variant> data = new ArrayList<>();
        for (int i = 0; i < CONTAINER_SIZE; i++) {
            data.add(vi(i));
        }
        KeyHintData hint = client.createVector(key, data).get();
        Thread.sleep(500);

        runBenchmark("Vector full range", BENCHMARK_ITERATIONS, i ->
                client.getAndDeleteVectorElementsInRange(key, hint, 0, 0, false)
        );
    }

    // =========================================================================
    // EDGE CASES
    // =========================================================================

    @Test
    @DisplayName("Stress: getAndDeleteElementInRange (empty range - doesn't exist)")
    void stressTestEmptyRange() throws Exception {
        String key = "os_empty_" + UUID.randomUUID();

        List<OrderedVariant> data = new ArrayList<>();
        for (int i = 1; i <= 100; i++) {
            data.add(OrderedVariant.ofInt(i, (long) i));
        }
        KeyHintData hint = client.createOrderedSet(key, data).get();
        Thread.sleep(500);

        runBenchmark("Empty range", BENCHMARK_ITERATIONS, i ->
                client.getAndDeleteElementInRangeOrderedSet(key, hint, 1000, 2000, false)
        );
    }

    // =========================================================================
    // HELPER METHODS
    // =========================================================================

    @FunctionalInterface
    interface ThrowingFunction<T, R> {
        R apply(T t) throws Exception;
    }

    protected void runBenchmark(String name, int iterations, ThrowingFunction<Integer, CompletableFuture<?>> action) throws Exception {
        AtomicLong errors = new AtomicLong(0);
        Semaphore semaphore = new Semaphore(CONCURRENCY);
        List<CompletableFuture<?>> futures = new ArrayList<>(iterations);

        long start = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            semaphore.acquire();
            CompletableFuture<?> f;
            try {
                f = action.apply(i);
            } catch (Exception e) {
                semaphore.release();
                errors.incrementAndGet();
                continue;
            }

            if (f != null) {
                futures.add(f.whenComplete((res, ex) -> {
                    semaphore.release();
                    if (ex != null) errors.incrementAndGet();
                }));
            } else {
                semaphore.release();
            }
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        printResults(name, iterations, System.nanoTime() - start, errors.get());
    }

    protected void printResults(String operation, int iterations, long elapsedNanos, long errors) {
        double elapsed = elapsedNanos / 1_000_000_000.0;
        double ops = elapsed > 0 ? iterations / elapsed : 0;
        System.out.printf("[%s] %d iters, %.3fs, %.2f OPS, %d errors%n", operation, iterations, elapsed, ops, errors);
    }
}