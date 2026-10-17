package com.hurricache.client.standalone.simple;

import com.hurricache.TestBase;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.OrderedPayload;
import com.hurricache.client.intf.OrderedVariant;
import com.hurricache.client.intf.Payload;
import com.hurricache.client.intf.Variant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests with real KeyHintData to verify hint routing works correctly.
 * Each test creates a key first (which returns a hint), then uses that hint for subsequent operations.
 */
public class HintRoutingTest extends TestBase {

    // =========================================================================
    // 1. SCALAR - getValue with hint
    // =========================================================================

    @Test
    @DisplayName("Scalar: getValue with real hint")
    void testScalarWithHint() throws ExecutionException, InterruptedException {
        String key = "hint_scalar_" + UUID.randomUUID();
        KeyHintData hint = client.create(key, vi(42)).get();
        assertNotNull(hint);

        Variant value = client.get(key, hint).get();
        assertNotNull(value);
        assertEquals(42, value.longValue());
    }

    // =========================================================================
    // 2. LIST - streamList with hint
    // =========================================================================

    @Test
    @DisplayName("List: streamList with real hint")
    void testListWithHint() throws ExecutionException, InterruptedException {
        String key = "hint_list_" + UUID.randomUUID();
        KeyHintData hint = client.createList(key, List.of(v("a"), v("b"), v("c"))).get();
        assertNotNull(hint);

        List<Variant> stream = client.getList(key, hint).get();
        assertNotNull(stream);
        assertEquals(3, stream.size());
        assertEquals("a", new String(stream.get(0).bytesValue(), java.nio.charset.StandardCharsets.UTF_8));
    }

    // =========================================================================
    // 3. VECTOR - streamVector with hint
    // =========================================================================

    @Test
    @DisplayName("Vector: streamVector with real hint")
    void testVectorWithHint() throws ExecutionException, InterruptedException {
        String key = "hint_vector_" + UUID.randomUUID();
        KeyHintData hint = client.createVector(key, List.of(v("x"), v("y"))).get();
        assertNotNull(hint);

        List<Variant> stream = client.getVector(key, hint).get();
        assertNotNull(stream);
        assertEquals(2, stream.size());
    }

    // =========================================================================
    // 4. QUEUE - getHead with hint
    // =========================================================================

    @Test
    @DisplayName("Queue: getHead with real hint")
    void testQueueWithHint() throws ExecutionException, InterruptedException {
        String key = "hint_queue_" + UUID.randomUUID();
        KeyHintData hint = client.createQueue(key, List.of(v("head"), v("tail"))).get();
        assertNotNull(hint);

        Variant head = client.peekHead(key, hint).get();
        assertNotNull(head);
        assertEquals("head", new String(head.bytesValue(), java.nio.charset.StandardCharsets.UTF_8));
    }

    // =========================================================================
    // 5. SET - streamSet with hint
    // =========================================================================

    @Test
    @DisplayName("Set: streamSet with real hint")
    void testSetWithHint() throws ExecutionException, InterruptedException {
        String key = "hint_set_" + UUID.randomUUID();
        KeyHintData hint = client.createSet(key, List.of(v("item1"), v("item2"))).get();
        assertNotNull(hint);

        List<Variant> stream = client.getSet(key, hint).get();
        assertNotNull(stream);
        assertEquals(2, stream.size());
    }

    // =========================================================================
    // 6. ORDERED SET - streamOrderedSet with hint
    // =========================================================================

    @Test
    @DisplayName("OrderedSet: streamOrderedSet with real hint")
    void testOrderedSetWithHint() throws ExecutionException, InterruptedException {
        String key = "hint_ordered_set_" + UUID.randomUUID();
        KeyHintData hint = client.createOrderedSet(key, List.of(
                OrderedVariant.ofBytes("first".getBytes(), 1L),
                OrderedVariant.ofBytes("second".getBytes(), 2L)
        )).get();
        assertNotNull(hint);

        List<OrderedVariant> stream = client.getOrderedSet(key, hint).get();
        assertNotNull(stream);
        assertEquals(2, stream.size());
        assertEquals("first", new String(stream.get(0).bytesValue(), java.nio.charset.StandardCharsets.UTF_8));
    }

    // =========================================================================
    // 7. HASH MAP - container ops with hint
    // =========================================================================

    @Test
    @DisplayName("HashMap: getValueInContainerByKey with real hint")
    void testHashMapWithHint() throws ExecutionException, InterruptedException {
        String key = "hint_hash_map_" + UUID.randomUUID();
        Map<Payload, Variant> data = new HashMap<>();
        data.put(p("k1"), v("v1"));
        data.put(p("k2"), vi(100));

        KeyHintData hint = client.createMap(key, data).get();
        assertNotNull(hint);

        Variant value = client.getInContainer(key, hint, "k1".getBytes()).get();
        assertNotNull(value);
        assertEquals("v1", new String(value.bytesValue(), java.nio.charset.StandardCharsets.UTF_8));

        value = client.getInContainer(key, hint, "k2".getBytes()).get();
        assertNotNull(value);
        assertEquals(100, value.longValue());
    }

    // =========================================================================
    // 8. ORDERED MAP - container ops with hint
    // =========================================================================

    @Test
    @DisplayName("OrderedMap: getValueInContainerByKey with real hint")
    void testOrderedMapWithHint() throws ExecutionException, InterruptedException {
        String key = "hint_ordered_map_" + UUID.randomUUID();
        Map<OrderedPayload, Variant> data = new HashMap<>();
        data.put(OrderedPayload.of("k1".getBytes(), 1L), v("v1"));
        data.put(OrderedPayload.of("k2".getBytes(), 2L), vd(3.14));

        KeyHintData hint = client.createOrderedMap(key, data).get();
        assertNotNull(hint);

        Variant value = client.getInContainer(key, hint, "k1".getBytes()).get();
        assertNotNull(value);
        assertEquals("v1", new String(value.bytesValue(), java.nio.charset.StandardCharsets.UTF_8));

        value = client.getInContainer(key, hint, "k2".getBytes()).get();
        assertNotNull(value);
        assertEquals(3.14, value.doubleValue(), 0.001);
    }
}