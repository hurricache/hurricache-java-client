package com.hurricache.client.standalone.simple;

import com.hurricache.TestBase;
import com.hurricache.client.intf.Variant;
import com.hurricache.grpc.ContainerType;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Exact gRPC status codes for error scenarios (not just presence of an exception).
 */
public class ErrorCodeTest extends TestBase {

    private static void assertErrorCode(CompletableFuture<?> future, Status.Code expected) {
        try {
            future.get();
            fail("Expected " + expected);
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(expected, cause.getStatus().getCode());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail(e.getMessage());
        }
    }

    // =========================================================================
    // 1. ALREADY_EXISTS
    // =========================================================================

    @Test
    @DisplayName("createKeyValue on existing key - ALREADY_EXISTS")
    void testDuplicateCreateKeyValueAlreadyExists() throws ExecutionException, InterruptedException {
        String key = "errcode_create_dup_" + UUID.randomUUID();

        client.create(key, v("first")).get();

        assertErrorCode(client.create(key, v("second")), Status.Code.ALREADY_EXISTS);

        // Original value should remain unchanged
        Variant current = client.get(key).get();
        assertEquals("first", str(current));
    }

    @Test
    @DisplayName("createContainer on existing key - ALREADY_EXISTS")
    void testDuplicateCreateContainerAlreadyExists() throws ExecutionException, InterruptedException {
        String key = "errcode_create_container_dup_" + UUID.randomUUID();

        client.createList(key, List.of(v("item1"))).get();

        assertErrorCode(client.createList(key, List.of(v("item2"))), Status.Code.ALREADY_EXISTS);

        // Original container should remain unchanged
        List<Variant> stream = client.getList(key).get();
        assertEquals(1, stream.size());
        assertEquals("item1", str(stream.get(0)));
    }

    // =========================================================================
    // 2. SCALAR READ ON CONTAINER / CONTAINER READ ON SCALAR
    // =========================================================================

    @Test
    @DisplayName("getValue on container key (list, map) - NOT_FOUND")
    void testGetValueOnContainerKey() throws ExecutionException, InterruptedException {
        String listKey = "errcode_get_on_list_" + UUID.randomUUID();
        client.createList(listKey, List.of(v("item1"))).get();

        // Server semantics: getValue on non-RAW type returns NOT_FOUND ("Key not found or type is not correct")
        assertNotFound(client.get(listKey));

        String mapKey = "errcode_get_on_map_" + UUID.randomUUID();
        Map<com.hurricache.client.intf.Payload, Variant> data = new HashMap<>();
        data.put(p("k1"), v("v1"));
        client.createMap(mapKey, data).get();

        assertNotFound(client.get(mapKey));
    }

    @Test
    @DisplayName("getHead on scalar key - INTERNAL")
    void testPeekHeadOnScalarKey() throws ExecutionException, InterruptedException {
        String key = "errcode_head_on_scalar_" + UUID.randomUUID();

        client.create(key, v("scalar_value")).get();

        // Server semantics: getHead supports LIST/VECTOR/QUEUE only -> INTERNAL
        assertErrorCode(client.peekHead(key), Status.Code.INTERNAL);
    }

    // =========================================================================
    // 3. TYPE MISMATCHES
    // =========================================================================

    @Test
    @DisplayName("atomicAdd on bytes value - INVALID_ARGUMENT")
    void testAtomicAddOnBytesInvalidArgument() throws ExecutionException, InterruptedException {
        String key = "errcode_add_on_bytes_" + UUID.randomUUID();

        client.create(key, v("hello")).get();

        assertInvalidArgument(client.add(key, 10));

        // Value should remain unchanged
        Variant current = client.get(key).get();
        assertTrue(current.isBytes());
    }

    @Test
    @DisplayName("getElementsInRange with wrong container type - INVALID_ARGUMENT")
    void testGetRangeTypeMismatchInvalidArgument() throws ExecutionException, InterruptedException {
        String key = "errcode_range_type_mismatch_" + UUID.randomUUID();

        client.createList(key, List.of(v("1"), v("2"), v("3"))).get();

        // Declared VECTOR type does not match stored LIST -> "Type mismatch" INVALID_ARGUMENT
        assertErrorCode(client.getRange(key, null, ContainerType.VECTOR, 0, 1), Status.Code.INVALID_ARGUMENT);
    }

    // =========================================================================
    // 4. MAP-ONLY OPERATIONS ON OTHER CONTAINERS
    // =========================================================================

    @Test
    @DisplayName("getValueInContainerByKey on LIST - INTERNAL")
    void testGetInContainerOnListInternal() throws ExecutionException, InterruptedException {
        String key = "errcode_get_in_list_" + UUID.randomUUID();

        client.createList(key, List.of(v("item1"))).get();

        // Server semantics: getValueInContainerByKey supports MAP and ORDERED_MAP only -> INTERNAL
        assertErrorCode(client.getInContainer(key, null, bytes("item1")), Status.Code.INTERNAL);
    }

    @Test
    @DisplayName("updateValueInContainer on SET - INTERNAL")
    void testUpdateInContainerOnSetInternal() throws ExecutionException, InterruptedException {
        String key = "errcode_update_in_set_" + UUID.randomUUID();

        client.createSet(key, List.of(v("item1"))).get();

        // Server semantics: updateValueInContainer supports MAP and ORDERED_MAP only -> INTERNAL
        assertErrorCode(client.update(key, null, bytes("item1"), v("new")), Status.Code.INTERNAL);
    }

    // =========================================================================
    // 5. CONTROL CASE
    // =========================================================================

    @Test
    @DisplayName("getValue on non-existent key - NOT_FOUND")
    void testGetValueOnNonExistentKey() {
        String key = "errcode_get_missing_" + UUID.randomUUID();

        assertNotFound(client.get(key));
    }
}
