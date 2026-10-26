package com.hurricache.client.standalone.simple;

import com.hurricache.TestBase;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.OrderedPayload;
import com.hurricache.client.intf.OrderedVariant;
import com.hurricache.client.intf.Payload;
import com.hurricache.client.intf.Variant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Server streaming returns containers in chunks of 64 elements; these tests verify
 * that the client reassembles all elements exactly once around chunk boundaries.
 */
public class StreamChunkBoundaryTest extends TestBase {

    private List<Variant> intElements(int count) {
        List<Variant> data = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            data.add(vi(i));
        }
        return data;
    }

    private List<OrderedVariant> orderedIntElements(int count) {
        List<OrderedVariant> data = new ArrayList<>(count);
        for (int i = 1; i <= count; i++) {
            data.add(OrderedVariant.ofInt(i, i));
        }
        return data;
    }

    private void assertListRoundTrip(String key, int size) throws ExecutionException, InterruptedException {
        KeyHintData hint = client.createList(key, intElements(size)).get();
        assertNotNull(hint);

        List<Variant> stream = client.getList(key).get();
        assertEquals(size, stream.size());
        for (int i = 0; i < size; i++) {
            assertEquals(i, stream.get(i).longValue(), "Wrong element at position " + i);
        }
        client.remove(key).get();
    }

    // =========================================================================
    // 1. LIST AT CHUNK BOUNDARIES (63 / 64 / 65 / 200)
    // =========================================================================

    @Test
    @DisplayName("List with 63 elements (below chunk boundary) - all elements once, in order")
    void testListStream63() throws ExecutionException, InterruptedException {
        assertListRoundTrip("stream_chunk_list_63_" + UUID.randomUUID(), 63);
    }

    @Test
    @DisplayName("List with 64 elements (exact chunk boundary) - all elements once, in order")
    void testListStream64() throws ExecutionException, InterruptedException {
        assertListRoundTrip("stream_chunk_list_64_" + UUID.randomUUID(), 64);
    }

    @Test
    @DisplayName("List with 65 elements (above chunk boundary) - all elements once, in order")
    void testListStream65() throws ExecutionException, InterruptedException {
        assertListRoundTrip("stream_chunk_list_65_" + UUID.randomUUID(), 65);
    }

    @Test
    @DisplayName("List with 200 elements (multiple chunks) - all elements once, in order")
    void testListStream200() throws ExecutionException, InterruptedException {
        assertListRoundTrip("stream_chunk_list_200_" + UUID.randomUUID(), 200);
    }

    // =========================================================================
    // 2. VECTOR AND SET ACROSS CHUNKS
    // =========================================================================

    @Test
    @DisplayName("Vector with 200 elements - all elements once, in order")
    void testVectorStream200() throws ExecutionException, InterruptedException {
        String key = "stream_chunk_vector_200_" + UUID.randomUUID();

        KeyHintData hint = client.createVector(key, intElements(200)).get();
        assertNotNull(hint);

        List<Variant> stream = client.getVector(key).get();
        assertEquals(200, stream.size());
        for (int i = 0; i < 200; i++) {
            assertEquals(i, stream.get(i).longValue(), "Wrong element at position " + i);
        }
        client.remove(key).get();
    }

    @Test
    @DisplayName("Set with 200 elements - all elements exactly once (as a set)")
    void testSetStream200() throws ExecutionException, InterruptedException {
        String key = "stream_chunk_set_200_" + UUID.randomUUID();

        KeyHintData hint = client.createSet(key, intElements(200)).get();
        assertNotNull(hint);

        List<Variant> stream = client.getSet(key).get();
        assertEquals(200, stream.size());

        Set<Long> values = new HashSet<>();
        for (Variant element : stream) {
            assertTrue(values.add(element.longValue()), "Duplicate element: " + element.longValue());
        }
        for (long i = 0; i < 200; i++) {
            assertTrue(values.contains(i), "Missing element: " + i);
        }
        client.remove(key).get();
    }

    // =========================================================================
    // 3. MAP AND ORDERED MAP ACROSS CHUNKS (130 ENTRIES)
    // =========================================================================

    @Test
    @DisplayName("Map with 130 entries - stream returns all pairs")
    void testMapStream130() throws ExecutionException, InterruptedException {
        String key = "stream_chunk_map_130_" + UUID.randomUUID();

        Map<Payload, Variant> data = new HashMap<>();
        for (int i = 0; i < 130; i++) {
            data.put(p("key_" + i), vi(i));
        }

        KeyHintData hint = client.createMap(key, data).get();
        assertNotNull(hint);

        Map<Payload, Variant> stream = client.getMap(key).get();
        assertEquals(130, stream.size());
        for (int i = 0; i < 130; i++) {
            assertEquals(vi(i), stream.get(p("key_" + i)), "Wrong value for key_" + i);
        }
        client.remove(key).get();
    }

    @Test
    @DisplayName("Ordered map with 130 entries - stream returns all pairs with weights")
    void testOrderedMapStream130() throws ExecutionException, InterruptedException {
        String key = "stream_chunk_ordered_map_130_" + UUID.randomUUID();

        Map<OrderedPayload, Variant> data = new HashMap<>();
        for (int i = 1; i <= 130; i++) {
            data.put(OrderedPayload.of(bytes("k_" + i), (long) i), vi(i));
        }

        KeyHintData hint = client.createOrderedMap(key, data).get();
        assertNotNull(hint);

        Map<OrderedPayload, Variant> stream = client.getOrderedMap(key).get();
        assertEquals(130, stream.size());

        boolean[] seen = new boolean[131];
        for (Map.Entry<OrderedPayload, Variant> entry : stream.entrySet()) {
            String mapKey = str(entry.getKey());
            assertTrue(mapKey.startsWith("k_"));
            int idx = Integer.parseInt(mapKey.substring(2));
            assertTrue(idx >= 1 && idx <= 130);
            assertFalse(seen[idx], "Duplicate key: " + mapKey);
            seen[idx] = true;
            assertNotNull(entry.getKey().getOrder());
            assertEquals(idx, entry.getKey().getOrder().longValue(), "Wrong weight for " + mapKey);
            assertEquals(idx, entry.getValue().longValue(), "Wrong value for " + mapKey);
        }
        for (int i = 1; i <= 130; i++) {
            assertTrue(seen[i], "Missing key k_" + i);
        }
        client.remove(key).get();
    }

    // =========================================================================
    // 4. ORDERED SET RANGES ACROSS CHUNKS (130 ELEMENTS, WEIGHTS 1..130)
    // =========================================================================

    @Test
    @DisplayName("Ordered set with 130 elements - full stream and full weight range")
    void testOrderedSetFullRangeAcrossChunks() throws ExecutionException, InterruptedException {
        String key = "stream_chunk_ordered_set_full_" + UUID.randomUUID();

        KeyHintData hint = client.createOrderedSet(key, orderedIntElements(130)).get();
        assertNotNull(hint);

        // Full container stream
        List<OrderedVariant> stream = client.getOrderedSet(key).get();
        assertEquals(130, stream.size());

        // Full range by weights
        List<OrderedVariant> range = client.getElementInRangeOrderedSet(key, null, 1, 130, false).get();
        assertEquals(130, range.size());
        for (int i = 0; i < 130; i++) {
            assertEquals(i + 1, range.get(i).intValue(), "Wrong element at position " + i);
            assertNotNull(range.get(i).getOrder());
            assertEquals(i + 1, range.get(i).getOrder().longValue(), "Wrong weight at position " + i);
        }
        client.remove(key).get();
    }

    @Test
    @DisplayName("Ordered set with 130 elements - reverse full range")
    void testOrderedSetReverseRangeAcrossChunks() throws ExecutionException, InterruptedException {
        String key = "stream_chunk_ordered_set_reverse_" + UUID.randomUUID();

        KeyHintData hint = client.createOrderedSet(key, orderedIntElements(130)).get();
        assertNotNull(hint);

        List<OrderedVariant> range = client.getElementInRangeOrderedSet(key, null, 1, 130, true).get();
        assertEquals(130, range.size());
        assertEquals(130, range.get(0).intValue());
        assertEquals(1, range.get(129).intValue());
        for (int i = 0; i < 130; i++) {
            assertEquals(130 - i, range.get(i).intValue(), "Wrong element at position " + i);
        }
        client.remove(key).get();
    }

    @Test
    @DisplayName("Ordered set range with pos == end - single element (inclusive bounds)")
    void testOrderedSetRangePosEqualsEnd() throws ExecutionException, InterruptedException {
        String key = "stream_chunk_ordered_set_point_" + UUID.randomUUID();

        KeyHintData hint = client.createOrderedSet(key, orderedIntElements(130)).get();
        assertNotNull(hint);

        // Server semantics: weight bounds are inclusive, so pos == end yields the single matching element
        List<OrderedVariant> range = client.getElementInRangeOrderedSet(key, null, 50, 50, false).get();
        assertEquals(1, range.size());
        assertEquals(50, range.get(0).intValue());
        client.remove(key).get();
    }

    @Test
    @DisplayName("Ordered set range with end beyond max weight - clamps to container end")
    void testOrderedSetRangeEndBeyondMax() throws ExecutionException, InterruptedException {
        String key = "stream_chunk_ordered_set_oob_" + UUID.randomUUID();

        KeyHintData hint = client.createOrderedSet(key, orderedIntElements(130)).get();
        assertNotNull(hint);

        List<OrderedVariant> range = client.getElementInRangeOrderedSet(key, null, 100, 100000, false).get();
        assertEquals(31, range.size()); // weights 100..130
        assertEquals(100, range.get(0).intValue());
        assertEquals(130, range.get(30).intValue());
        client.remove(key).get();
    }
}
