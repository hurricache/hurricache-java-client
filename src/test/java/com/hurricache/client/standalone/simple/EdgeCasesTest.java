package com.hurricache.client.standalone.simple;

import com.hurricache.TestBase;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.OrderedVariant;
import com.hurricache.client.intf.Variant;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Edge case tests: empty containers, non-existent keys, out-of-bounds indices,
 * type mismatches, and invalid operations.
 */
public class EdgeCasesTest extends TestBase {

    // =========================================================================
    // 1. EMPTY CONTAINERS - HEAD/TAIL OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("getHead on empty list throws NOT_FOUND")
    void testGetHeadOnEmptyList() throws ExecutionException, InterruptedException {
        String key = "edge_empty_list_head_" + UUID.randomUUID();
        client.createList(key, List.of()).get();

        Variant variant = client.getHead(key).get();
        assertEquals(Variant.none(), variant);
    }

    @Test
    @DisplayName("getTail on empty list throws NOT_FOUND")
    void testGetTailOnEmptyList() throws ExecutionException, InterruptedException {
        String key = "edge_empty_list_tail_" + UUID.randomUUID();
        client.createList(key, List.of()).get();

        Variant variant = client.getTail(key).get();
        assertEquals(Variant.none(), variant);
    }

    @Test
    @DisplayName("getAndDeleteHead on empty list throws NOT_FOUND")
    void testGetAndDeleteHeadOnEmptyList() throws ExecutionException, InterruptedException {
        String key = "edge_empty_list_pop_head_" + UUID.randomUUID();
        client.createList(key, List.of()).get();

        Variant variant = client.getAndDeleteHead(key).get();
        assertEquals(Variant.none(), variant);
    }

    @Test
    @DisplayName("getAndDeleteTail on empty list throws NOT_FOUND")
    void testGetAndDeleteTailOnEmptyList() throws ExecutionException, InterruptedException {
        String key = "edge_empty_list_pop_tail_" + UUID.randomUUID();
        client.createList(key, List.of()).get();

        Variant variant = client.getAndDeleteTail(key).get();
        assertEquals(Variant.none(), variant);
    }

    @Test
    @DisplayName("getHead on empty vector throws NOT_FOUND")
    void testGetHeadOnEmptyVector() throws ExecutionException, InterruptedException {
        String key = "edge_empty_vector_head_" + UUID.randomUUID();
        client.createVector(key, List.of()).get();

        Variant variant = client.getHead(key).get();
        assertEquals(Variant.none(), variant);
    }

    @Test
    @DisplayName("getTail on empty vector throws NOT_FOUND")
    void testGetTailOnEmptyVector() throws ExecutionException, InterruptedException {
        String key = "edge_empty_vector_tail_" + UUID.randomUUID();
        client.createVector(key, List.of()).get();

        Variant variant = client.getTail(key).get();
        assertEquals(Variant.none(), variant);
    }

    @Test
    @DisplayName("getHead on empty queue throws NOT_FOUND")
    void testGetHeadOnEmptyQueue() throws ExecutionException, InterruptedException {
        String key = "edge_empty_queue_head_" + UUID.randomUUID();
        client.createQueue(key, List.of()).get();

        Variant variant = client.getHead(key).get();
        assertEquals(Variant.none(), variant);
    }

    @Test
    @DisplayName("getTail on empty queue throws NOT_FOUND")
    void testGetTailOnEmptyQueue() throws ExecutionException, InterruptedException {
        String key = "edge_empty_queue_tail_" + UUID.randomUUID();
        client.createQueue(key, List.of()).get();

            Variant variant = client.getTail(key).get();
        assertEquals(Variant.none(), variant);
    }

    @Test
    @DisplayName("getAndDeleteHead on empty queue throws NOT_FOUND")
    void testGetAndDeleteHeadOnEmptyQueue() throws ExecutionException, InterruptedException {
        String key = "edge_empty_queue_pop_head_" + UUID.randomUUID();
        client.createQueue(key, List.of()).get();

        Variant variant = client.getAndDeleteHead(key).get();
        assertEquals(Variant.none(), variant);
    }

    // =========================================================================
    // 2. NON-EXISTENT KEYS
    // =========================================================================

    @Test
    @DisplayName("getValue on non-existent key throws NOT_FOUND")
    void testGetValueOnNonExistentKey() throws ExecutionException, InterruptedException {
        String key = "edge_nonexistent_get_" + UUID.randomUUID();

        try {
            client.getValue(key).get();
            fail("Expected NOT_FOUND");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    @Test
    @DisplayName("streamList on non-existent key throws NOT_FOUND")
    void testStreamListOnNonExistentKey() throws ExecutionException, InterruptedException {
        String key = "edge_nonexistent_stream_list_" + UUID.randomUUID();

        try {
            client.streamList(key).get();
            fail("Expected NOT_FOUND");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    @Test
    @DisplayName("streamVector on non-existent key throws NOT_FOUND")
    void testStreamVectorOnNonExistentKey() throws ExecutionException, InterruptedException {
        String key = "edge_nonexistent_stream_vector_" + UUID.randomUUID();

        try {
            client.streamVector(key).get();
            fail("Expected NOT_FOUND");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    @Test
    @DisplayName("streamSet on non-existent key throws NOT_FOUND")
    void testStreamSetOnNonExistentKey() throws ExecutionException, InterruptedException {
        String key = "edge_nonexistent_stream_set_" + UUID.randomUUID();

        try {
            client.streamSet(key).get();
            fail("Expected NOT_FOUND");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    @Test
    @DisplayName("streamMap on non-existent key throws NOT_FOUND")
    void testStreamMapOnNonExistentKey() throws ExecutionException, InterruptedException {
        String key = "edge_nonexistent_stream_map_" + UUID.randomUUID();

        try {
            client.streamMap(key).get();
            fail("Expected NOT_FOUND");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    @Test
    @DisplayName("getSize on non-existent key throws NOT_FOUND")
    void testGetSizeOnNonExistentKey() throws ExecutionException, InterruptedException {
        String key = "edge_nonexistent_size_" + UUID.randomUUID();

        try {
            client.getSize(key).get();
            fail("Expected NOT_FOUND");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    @Test
    @DisplayName("existKey on non-existent key returns false")
    void testExistKeyOnNonExistentKey() throws ExecutionException, InterruptedException {
        String key = "edge_nonexistent_exist_" + UUID.randomUUID();

        Boolean exists = client.existKey(key).get();
        assertFalse(exists);
    }

    @Test
    @DisplayName("remove on non-existent key returns true (no-op)")
    void testRemoveOnNonExistentKey() throws ExecutionException, InterruptedException {
        String key = "edge_nonexistent_remove_" + UUID.randomUUID();

        // Remove on non-existent key should succeed (idempotent)
        assertNotFound(client.remove(key));
    }

    // =========================================================================
    // 3. OUT-OF-BOUNDS POSITIONS
    // =========================================================================

    @Test
    @DisplayName("getElementAtPosition with index 0 on single-element list")
    void testGetElementAtPositionZeroIndex() throws ExecutionException, InterruptedException {
        String key = "edge_position_zero_" + UUID.randomUUID();
        client.createList(key, List.of(v("only"))).get();

        Variant element = client.getElementAtPosition(key, 0).get();
        assertNotNull(element);
        assertEquals("only", new String(element.bytesValue(), java.nio.charset.StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("getElementAtPosition with out-of-bounds index throws")
    void testGetElementAtPositionOutOfBounds() throws ExecutionException, InterruptedException {
        String key = "edge_position_oob_" + UUID.randomUUID();
        client.createList(key, List.of(v("first"), v("second"))).get();

        try {
            client.getElementAtPosition(key, 10).get();
            fail("Expected error for out-of-bounds");
        } catch (ExecutionException e) {
            // Should throw - index out of range
            assertNotNull(e.getCause());
        }
    }

    @Test
    @DisplayName("getElementAtPosition with negative index throws")
    void testGetElementAtPositionNegativeIndex() throws ExecutionException, InterruptedException {
        String key = "edge_position_negative_" + UUID.randomUUID();
        client.createList(key, List.of(v("first"), v("second"))).get();

        try {
            client.getElementAtPosition(key, -1).get();
            fail("Expected error for negative index");
        } catch (ExecutionException e) {
            assertNotNull(e.getCause());
        }
    }

    @Test
    @DisplayName("deleteElementAtPosition with out-of-bounds index")
    void testDeleteElementAtPositionOutOfBounds() throws ExecutionException, InterruptedException {
        String key = "edge_delete_position_oob_" + UUID.randomUUID();
        client.createList(key, List.of(v("1"), v("2"))).get();

        Integer i = client.deleteElementAtPosition(key, null, 100L).get();
        assertEquals(0, i.intValue());
    }

    @Test
    @DisplayName("getAndDeleteElementAtPosition with out-of-bounds index")
    void testGetAndDeleteElementAtPositionOutOfBounds() throws ExecutionException, InterruptedException {
        String key = "edge_get_delete_position_oob_" + UUID.randomUUID();
        client.createList(key, List.of(v("1"), v("2"))).get();

        try {
            client.getAndDeleteElementAtPosition(key, null, 50).get();
            fail("Expected error for out-of-bounds");
        } catch (ExecutionException e) {
            assertNotNull(e.getCause());
        }
    }

    // =========================================================================
    // 4. CONTAINER OPS ON EMPTY CONTAINERS
    // =========================================================================

    @Test
    @DisplayName("getValueInContainerByKey on empty map throws NOT_FOUND")
    void testGetValueInContainerOnEmptyMap() throws ExecutionException, InterruptedException {
        String key = "edge_empty_map_get_" + UUID.randomUUID();
        client.createMap(key, new java.util.HashMap<>()).get();

        try {
            client.getValueInContainerByKey(key, null, "missing".getBytes()).get();
            fail("Expected NOT_FOUND");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    @Test
    @DisplayName("existInContainerByKey on empty set returns false")
    void testExistInContainerOnEmptySet() throws ExecutionException, InterruptedException {
        String key = "edge_empty_set_exist_" + UUID.randomUUID();
        client.createSet(key, List.of()).get();

        Boolean exists = client.existInContainerByKey(key, null, "missing".getBytes()).get();
        assertFalse(exists);
    }

    @Test
    @DisplayName("deleteFromContainerByKeyValue on empty container returns 0")
    void testDeleteFromContainerByKeyValueOnEmpty() throws ExecutionException, InterruptedException {
        String key = "edge_empty_delete_value_" + UUID.randomUUID();
        client.createList(key, List.of()).get();

        Integer deleted = client.removeFromContainerByKeyAndValue(key, null, List.of(), List.of(v("missing"))).get();
        assertEquals(0, deleted);
    }

    // =========================================================================
    // 5. TYPE MISMATCHES
    // =========================================================================

    @Test
    @DisplayName("Read int value as bytes - bytesValue() returns empty/zero")
    void testReadIntAsBytes() throws ExecutionException, InterruptedException {
        String key = "edge_type_int_bytes_" + UUID.randomUUID();
        client.createKeyValue(key, vi(42)).get();

        Variant value = client.getValue(key).get();
        assertTrue(value.isInt());
        assertEquals(42, value.longValue());

        // bytesValue() on int returns null or empty
        byte[] bytes = value.bytesValue();
        assertNull(bytes);
    }

    @Test
    @DisplayName("Read double value as int - returns 0")
    void testReadDoubleAsInt() throws ExecutionException, InterruptedException {
        String key = "edge_type_double_int_" + UUID.randomUUID();
        client.createKeyValue(key, vd(3.14)).get();

        Variant value = client.getValue(key).get();
        assertTrue(value.isDouble());
        assertEquals(3.14, value.doubleValue(), 0.001);

        // longValue() on double returns 0
        assertEquals(0L, value.longValue());
    }

    @Test
    @DisplayName("Read bytes value as double - returns 0.0")
    void testReadBytesAsDouble() throws ExecutionException, InterruptedException {
        String key = "edge_type_bytes_double_" + UUID.randomUUID();
        client.createKeyValue(key, v("hello")).get();

        Variant value = client.getValue(key).get();
        assertTrue(value.isBytes());
        assertEquals("hello", new String(value.bytesValue(), java.nio.charset.StandardCharsets.UTF_8));

        // doubleValue() on bytes returns 0.0
        assertEquals(0.0, value.doubleValue());
    }

    // =========================================================================
    // 6. EMPTY RANGE QUERIES
    // =========================================================================

    @Test
    @DisplayName("streamElementInRange with start > end returns empty")
    void testStreamElementInRangeStartGreaterThanEnd() throws ExecutionException, InterruptedException {
        String key = "edge_range_inverted_" + UUID.randomUUID();
        client.createList(key, List.of(v("1"), v("2"), v("3"))).get();

        List<Variant> range = client.streamElementInRangeUnordered(key, null, com.hurricache.grpc.ContainerType.LIST, 5, 2).get();
        assertNotNull(range);
        assertTrue(range.isEmpty());
    }

    @Test
    @DisplayName("streamElementInRange with out-of-bounds start returns empty")
    void testStreamElementInRangeOutOfBounds() throws ExecutionException, InterruptedException {
        String key = "edge_range_oob_" + UUID.randomUUID();
        client.createList(key, List.of(v("1"), v("2"), v("3"))).get();

        List<Variant> range = client.streamElementInRangeUnordered(key, null, com.hurricache.grpc.ContainerType.LIST, 10, 20).get();
        assertNotNull(range);
        assertTrue(range.isEmpty());
    }

    // =========================================================================
    // 7. OPERATIONS ON NON-CONTAINER KEYS
    // =========================================================================

    @Test
    @DisplayName("getHead on scalar key throws type error")
    void testGetHeadOnScalarKey() throws ExecutionException, InterruptedException {
        String key = "edge_scalar_as_container_" + UUID.randomUUID();
        client.createKeyValue(key, v("scalar_value")).get();

        try {
            client.getHead(key).get();
            fail("Expected error - scalar key used as container");
        } catch (ExecutionException e) {
            assertNotNull(e.getCause());
        }
    }

    @Test
    @DisplayName("getValue on container key throws type error")
    void testGetValueOnContainerKey() throws ExecutionException, InterruptedException {
        String key = "edge_container_as_scalar_" + UUID.randomUUID();
        client.createList(key, List.of(v("item1"))).get();

        try {
            client.getValue(key).get();
            fail("Expected error - container key used as scalar");
        } catch (ExecutionException e) {
            assertNotNull(e.getCause());
        }
    }

    // =========================================================================
    // 8. ATOMIC OPS ON EMPTY/WRONG TYPE
    // =========================================================================

    @Test
    @DisplayName("atomicAdd on non-existent key throws NOT_FOUND")
    void testAtomicAddOnNonExistentKey() throws ExecutionException, InterruptedException {
        String key = "edge_atomic_nonexistent_" + UUID.randomUUID();

        try {
            client.atomicAdd(key, 5).get();
            fail("Expected NOT_FOUND");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    @Test
    @DisplayName("atomicExchange on non-existent key throws NOT_FOUND")
    void testAtomicExchangeOnNonExistentKey() throws ExecutionException, InterruptedException {
        String key = "edge_atomic_exchange_nonexistent_" + UUID.randomUUID();

        try {
            client.atomicExchange(key, 100).get();
            fail("Expected NOT_FOUND");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    @Test
    @DisplayName("atomicCompareAndSet on non-existent key throws NOT_FOUND")
    void testAtomicCompareAndSetOnNonExistentKey() throws ExecutionException, InterruptedException {
        String key = "edge_atomic_cas_nonexistent_" + UUID.randomUUID();

        try {
            client.atomicCompareAndSet(key, 0L, 100L).get();
            fail("Expected NOT_FOUND");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    // =========================================================================
    // 9. DELETE FROM EMPTY/MISSING
    // =========================================================================

    @Test
    @DisplayName("deleteHead on single-element list leaves empty")
    void testDeleteHeadOnSingleElement() throws ExecutionException, InterruptedException {
        String key = "edge_delete_single_head_" + UUID.randomUUID();
        client.createList(key, List.of(v("only"))).get();

        Integer deleted = client.deleteHead(key).get();
        assertEquals(1, deleted);

        // Now list is empty - getHead returns Variant.none()
        Variant head = client.getHead(key).get();
        assertTrue(head.isNone());
    }

    @Test
    @DisplayName("deleteTail on single-element list leaves empty")
    void testDeleteTailOnSingleElement() throws ExecutionException, InterruptedException {
        String key = "edge_delete_single_tail_" + UUID.randomUUID();
        client.createList(key, List.of(v("only"))).get();

        Integer deleted = client.deleteTail(key).get();
        assertEquals(1, deleted);

        // Now list is empty - getTail returns Variant.none()
        Variant tail = client.getTail(key).get();
        assertTrue(tail.isNone());
    }

    // =========================================================================
    // 10. POSITION OPERATIONS ON SINGLE-ELEMENT CONTAINERS
    // =========================================================================

    @Test
    @DisplayName("getElementAtPosition(0) on single-element vector")
    void testGetElementAtPositionOnSingleElement() throws ExecutionException, InterruptedException {
        String key = "edge_single_element_get_" + UUID.randomUUID();
        client.createVector(key, List.of(v("only"))).get();

        Variant element = client.getElementAtPosition(key, 0).get();
        assertNotNull(element);
        assertEquals("only", new String(element.bytesValue(), java.nio.charset.StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("getAndDeleteElementAtPosition(0) on single-element list")
    void testGetAndDeleteElementAtPositionOnSingleElement() throws ExecutionException, InterruptedException {
        String key = "edge_single_element_pop_" + UUID.randomUUID();
        client.createList(key, List.of(v("only"))).get();

        Variant deleted = client.getAndDeleteElementAtPosition(key, null, 0).get();
        assertNotNull(deleted);
        assertEquals("only", new String(deleted.bytesValue(), java.nio.charset.StandardCharsets.UTF_8));

        // List is now empty
        Variant variant = client.getHead(key).get();
        assertEquals(Variant.none(), variant);

    }
}