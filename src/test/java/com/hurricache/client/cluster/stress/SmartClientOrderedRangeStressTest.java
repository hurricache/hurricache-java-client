package com.hurricache.client.cluster.stress;

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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Stress tests for getAndDeleteElementsInRange operations on ordered containers.
 * Tests OrderedSet, OrderedMap, List, and Vector range deletion with various patterns.
 */
@Execution(ExecutionMode.SAME_THREAD)
public class SmartClientOrderedRangeStressTest extends BaseClusterStressTest {

    private static final int CONTAINER_SIZE = 1000;
    private static final int BENCHMARK_ITERATIONS = 100000;
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

        // startWeight=0, endWeight=0 → full range
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

        // startWeight=100, endWeight=200
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

        // startWeight=500, endWeight=0 → from 500 to max
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

        // startWeight=0, endWeight=500 → from min to 500
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

    @Test
    @DisplayName("Stress: getAndDeleteElementInRange (OrderedSet, MT)")
    void stressTestOrderedSetMT() throws Exception {
        String key = "os_mt_" + UUID.randomUUID();
        
        List<OrderedVariant> data = new ArrayList<>();
        for (int i = 1; i <= CONTAINER_SIZE; i++) {
            data.add(OrderedVariant.ofInt(i, (long) i));
        }
        KeyHintData hint = client.createOrderedSet(key, data).get();
        Thread.sleep(500);

        runMultiThreadedBenchmark("OrderedSet MT", CONTAINER_SIZE, THREAD_COUNT, (threadId, i) ->
            client.getAndDeleteElementInRangeOrderedSet(key, hint,
                (long) threadId * 250 + 1, (long) threadId * 250 + 250, false)
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

    @Test
    @DisplayName("Stress: getAndDeleteElementInRange (OrderedMap, MT)")
    void stressTestOrderedMapMT() throws Exception {
        String key = "om_mt_" + UUID.randomUUID();
        
        Map<OrderedPayload, Variant> data = new LinkedHashMap<>();
        for (int i = 1; i <= CONTAINER_SIZE; i++) {
            data.put(new OrderedPayload(("key_" + i).getBytes(), (long) i), vi(i));
        }
        KeyHintData hint = client.createOrderedMap(key, data).get();
        Thread.sleep(500);

        runMultiThreadedBenchmark("OrderedMap MT", CONTAINER_SIZE, THREAD_COUNT, (threadId, i) ->
            client.getAndDeleteElementInRangeOrderedMap(key, hint,
                (long) threadId * 250 + 1, (long) threadId * 250 + 250, false)
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

    @Test
    @DisplayName("Stress: getAndDeleteListElementsInRange (List, start only)")
    void stressTestListStartOnly() throws Exception {
        String key = "list_start_" + UUID.randomUUID();
        
        List<Variant> data = new ArrayList<>();
        for (int i = 0; i < CONTAINER_SIZE; i++) {
            data.add(vi(i));
        }
        KeyHintData hint = client.createList(key, data).get();
        Thread.sleep(500);

        runBenchmark("List start only", BENCHMARK_ITERATIONS, i ->
            client.getAndDeleteListElementsInRange(key, hint, 500, 0, false)
        );
    }

    @Test
    @DisplayName("Stress: getAndDeleteListElementsInRange (List, MT)")
    void stressTestListMT() throws Exception {
        String key = "list_mt_" + UUID.randomUUID();
        
        List<Variant> data = new ArrayList<>();
        for (int i = 0; i < CONTAINER_SIZE; i++) {
            data.add(vi(i));
        }
        KeyHintData hint = client.createList(key, data).get();
        Thread.sleep(500);

        runMultiThreadedBenchmark("List MT", CONTAINER_SIZE, THREAD_COUNT, (threadId, i) ->
            client.getAndDeleteListElementsInRange(key, hint,
                (long) threadId * 250, (long) threadId * 250 + 249, false)
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

    @Test
    @DisplayName("Stress: getAndDeleteVectorElementsInRange (Vector, MT)")
    void stressTestVectorMT() throws Exception {
        String key = "vec_mt_" + UUID.randomUUID();
        
        List<Variant> data = new ArrayList<>();
        for (int i = 0; i < CONTAINER_SIZE; i++) {
            data.add(vi(i));
        }
        KeyHintData hint = client.createVector(key, data).get();
        Thread.sleep(500);

        runMultiThreadedBenchmark("Vector MT", CONTAINER_SIZE, THREAD_COUNT, (threadId, i) ->
            client.getAndDeleteVectorElementsInRange(key, hint,
                (long) threadId * 250, (long) threadId * 250 + 249, false)
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

    @Test
    @DisplayName("Stress: getAndDeleteElementInRange (no hint)")
    void stressTestNoHint() throws Exception {
        String key = "os_no_hint_" + UUID.randomUUID();
        
        List<OrderedVariant> data = new ArrayList<>();
        for (int i = 1; i <= 1000; i++) {
            data.add(OrderedVariant.ofInt(i, (long) i));
        }
        KeyHintData hint = client.createOrderedSet(key, data).get();
        Thread.sleep(500);

        runBenchmark("No hint", BENCHMARK_ITERATIONS, i ->
            client.getAndDeleteElementInRangeOrderedSet(key, null, 1, 100, false)
        );
    }
}
