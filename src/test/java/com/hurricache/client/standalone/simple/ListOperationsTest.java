package com.hurricache.client.standalone.simple;

import com.hurricache.TestBase;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Variant;
import com.hurricache.grpc.ContainerType;
import com.hurricache.grpc.LockStatus;
import com.hurricache.grpc.LockType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ListOperationsTest extends TestBase {

    private static final int OWNER_CLIENT_ID = 100;
    private static final int INTRUDER_CLIENT_ID = 200;

    // =========================================================================
    // 1. CREATE + STREAM LIST
    // =========================================================================

    @Test
    @DisplayName("Create empty list")
    void testCreateEmptyList() throws ExecutionException, InterruptedException {
        String key = "list_empty_" + UUID.randomUUID();

        KeyHintData hint = client.createList(key, new ArrayList<>()).get();
        assertNotNull(hint);

        Integer size = client.getSize(key).get();
        assertEquals(0, size);
    }

    @Test
    @DisplayName("Create list with bytes")
    void testCreateListWithBytes() throws ExecutionException, InterruptedException {
        String key = "list_bytes_" + UUID.randomUUID();
        List<Variant> data = List.of(v("item1"), v("item2"), v("item3"));

        KeyHintData hint = client.createList(key, data).get();
        assertNotNull(hint);

        List<Variant> stream = client.getList(key).get();
        assertEquals(3, stream.size());
        assertEquals("item1", new String(stream.get(0).bytesValue(), java.nio.charset.StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("Create list with int")
    void testCreateListWithInt() throws ExecutionException, InterruptedException {
        String key = "list_int_" + UUID.randomUUID();
        List<Variant> data = List.of(vi(10), vi(20), vi(30));

        KeyHintData hint = client.createList(key, data).get();
        assertNotNull(hint);

        List<Variant> stream = client.getList(key).get();
        assertEquals(3, stream.size());
        assertEquals(10, stream.get(0).longValue());
        assertEquals(20, stream.get(1).longValue());
    }

    @Test
    @DisplayName("Create list with double")
    void testCreateListWithDouble() throws ExecutionException, InterruptedException {
        String key = "list_double_" + UUID.randomUUID();
        List<Variant> data = List.of(vd(1.1), vd(2.2), vd(3.3));

        KeyHintData hint = client.createList(key, data).get();
        assertNotNull(hint);

        List<Variant> stream = client.getList(key).get();
        assertEquals(3, stream.size());
        assertEquals(1.1, stream.get(0).doubleValue(), 0.001);
    }

    @Test
    @DisplayName("Create list with mixed types")
    void testCreateListMixedTypes() throws ExecutionException, InterruptedException {
        String key = "list_mixed_" + UUID.randomUUID();
        List<Variant> data = List.of(v("hello"), vi(42), vd(3.14));

        KeyHintData hint = client.createList(key, data).get();
        assertNotNull(hint);

        List<Variant> stream = client.getList(key).get();
        assertEquals(3, stream.size());
        assertEquals("hello", new String(stream.get(0).bytesValue(), java.nio.charset.StandardCharsets.UTF_8));
        assertEquals(42, stream.get(1).longValue());
        assertEquals(3.14, stream.get(2).doubleValue(), 0.001);
    }

    // =========================================================================
    // 2. ADD/REMOVE ENDS
    // =========================================================================

    @Test
    @DisplayName("addElementToTail and addElementToHead")
    void testAddToTailAndHead() throws ExecutionException, InterruptedException {
        String key = "list_add_ends_" + UUID.randomUUID();

        KeyHintData hint = client.createList(key, List.of(v("middle"))).get();

        // Add to tail
        Integer added = client.pushTail(key, List.of(v("tail1"), v("tail2"))).get();
        assertEquals(2, added);

        // Add to head
        added = client.pushHead(key, List.of(v("head1"))).get();
        assertEquals(1, added);

        List<Variant> stream = client.getList(key).get();
        assertEquals(4, stream.size());
        assertEquals("head1", new String(stream.get(0).bytesValue(), java.nio.charset.StandardCharsets.UTF_8));
        assertEquals("middle", new String(stream.get(1).bytesValue(), java.nio.charset.StandardCharsets.UTF_8));
        assertEquals("tail1", new String(stream.get(2).bytesValue(), java.nio.charset.StandardCharsets.UTF_8));
        assertEquals("tail2", new String(stream.get(3).bytesValue(), java.nio.charset.StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("getHead and getTail")
    void testGetHeadAndTail() throws ExecutionException, InterruptedException {
        String key = "list_head_tail_" + UUID.randomUUID();
        List<Variant> data = List.of(v("first"), v("middle"), v("last"));

        KeyHintData hint = client.createList(key, data).get();

        Variant head = client.peekHead(key).get();
        assertNotNull(head);
        assertEquals("first", new String(head.bytesValue(), java.nio.charset.StandardCharsets.UTF_8));

        Variant tail = client.peekTail(key).get();
        assertNotNull(tail);
        assertEquals("last", new String(tail.bytesValue(), java.nio.charset.StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("getAndDeleteHead and getAndDeleteTail")
    void testGetAndDeleteHeadAndTail() throws ExecutionException, InterruptedException {
        String key = "list_pop_ends_" + UUID.randomUUID();
        List<Variant> data = List.of(v("first"), v("middle"), v("last"));

        KeyHintData hint = client.createList(key, data).get();

        // Delete head
        Variant deleted = client.popHead(key).get();
        assertNotNull(deleted);
        assertEquals("first", new String(deleted.bytesValue(), java.nio.charset.StandardCharsets.UTF_8));

        // Delete tail
        deleted = client.popTail(key).get();
        assertNotNull(deleted);
        assertEquals("last", new String(deleted.bytesValue(), java.nio.charset.StandardCharsets.UTF_8));

        List<Variant> stream = client.getList(key).get();
        assertEquals(1, stream.size());
        assertEquals("middle", new String(stream.get(0).bytesValue(), java.nio.charset.StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("deleteHead and deleteTail")
    void testDeleteHeadAndTail() throws ExecutionException, InterruptedException {
        String key = "list_delete_ends_" + UUID.randomUUID();
        List<Variant> data = List.of(v("first"), v("middle"), v("last"));

        KeyHintData hint = client.createList(key, data).get();

        Integer deleted = client.deleteHead(key).get();
        assertEquals(1, deleted);

        deleted = client.deleteTail(key).get();
        assertEquals(1, deleted);

        List<Variant> stream = client.getList(key).get();
        assertEquals(1, stream.size());
    }

    // =========================================================================
    // 3. POSITION-BASED OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("getElementAtPosition and getAndDeleteElementAtPosition")
    void testElementAtPosition() throws ExecutionException, InterruptedException {
        String key = "list_position_" + UUID.randomUUID();
        List<Variant> data = List.of(v("first"), v("second"), v("third"));

        KeyHintData hint = client.createList(key, data).get();

        Variant element = client.getAt(key, 1).get();
        assertNotNull(element);
        assertEquals("second", new String(element.bytesValue(), java.nio.charset.StandardCharsets.UTF_8));

        Variant deleted = client.popAt(key, null, 1).get();
        assertNotNull(deleted);
        assertEquals("second", new String(deleted.bytesValue(), java.nio.charset.StandardCharsets.UTF_8));

        List<Variant> stream = client.getList(key).get();
        assertEquals(2, stream.size());
    }

    @Test
    @DisplayName("deleteElementAtPosition and deleteElementAtPosition range")
    void testDeleteElementAtPosition() throws ExecutionException, InterruptedException {
        String key = "list_delete_position_" + UUID.randomUUID();
        List<Variant> data = List.of(v("1"), v("2"), v("3"), v("4"), v("5"));

        KeyHintData hint = client.createList(key, data).get();

        // Delete single element
        Integer deleted = client.removeAt(key, null, 2L).get();
        assertEquals(1, deleted);

        List<Variant> stream = client.getList(key).get();
        assertEquals(4, stream.size());

        // Delete range
        deleted = client.removeAt(key, null, 1L, 2L).get();
        assertEquals(2, deleted);

        stream = client.getList(key).get();
        assertEquals(2, stream.size());
    }

    @Test
    @DisplayName("deleteFromContainerByKeyValue")
    void testDeleteFromContainerByKeyValue() throws ExecutionException, InterruptedException {
        String key = "list_delete_value_" + UUID.randomUUID();
        List<Variant> data = List.of(v("a"), v("b"), v("a"), v("c"));

        KeyHintData hint = client.createList(key, data).get();

        Integer deleted = client.removeFromContainerByKeyAndValue(key, ContainerType.LIST, List.of(), List.of(v("a"))).get();
        assertEquals(2, deleted);

        List<Variant> stream = client.getList(key).get();
        assertEquals(2, stream.size());
    }

    // =========================================================================
    // 4. POSITION-BASED INSERTION
    // =========================================================================

    @Test
    @DisplayName("addElementToPositionBefore and addElementToPositionAfter")
    void testAddElementToPosition() throws ExecutionException, InterruptedException {
        String key = "list_position_insert_" + UUID.randomUUID();
        List<Variant> data = List.of(v("first"), v("last"));

        KeyHintData hint = client.createList(key, data).get();

        // Insert before "last"
        Integer added = client.insertBefore(key, hint, List.of(v("before")), v("last")).get();
        assertEquals(1, added);

        // Insert after "first"
        added = client.insertAfter(key, hint, List.of(v("after")), v("first")).get();
        assertEquals(1, added);

        List<Variant> stream = client.getList(key).get();
        assertEquals(4, stream.size());
        assertEquals("first", new String(stream.get(0).bytesValue(), java.nio.charset.StandardCharsets.UTF_8));
        assertEquals("after", new String(stream.get(1).bytesValue(), java.nio.charset.StandardCharsets.UTF_8));
        assertEquals("before", new String(stream.get(2).bytesValue(), java.nio.charset.StandardCharsets.UTF_8));
        assertEquals("last", new String(stream.get(3).bytesValue(), java.nio.charset.StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("addElementToPositionBefore/After with mixed types")
    void testAddElementToPositionMixedTypes() throws ExecutionException, InterruptedException {
        String key = "list_position_mixed_" + UUID.randomUUID();
        List<Variant> data = List.of(vi(100));

        KeyHintData hint = client.createList(key, data).get();

        client.insertBefore(key, hint, List.of(v("str"), vi(200), vd(3.14)), vi(100)).get();

        List<Variant> stream = client.getList(key).get();
        assertEquals(4, stream.size());
        assertEquals("str", new String(stream.get(0).bytesValue(), java.nio.charset.StandardCharsets.UTF_8));
        assertEquals(200, stream.get(1).longValue());
        assertEquals(3.14, stream.get(2).doubleValue(), 0.001);
        assertEquals(100, stream.get(3).longValue());
    }

    // =========================================================================
    // 5. RANGE QUERIES
    // =========================================================================

    @Test
    @DisplayName("getElementsInRange")
    void testGetElementsInRange() throws ExecutionException, InterruptedException {
        String key = "list_range_" + UUID.randomUUID();
        List<Variant> data = List.of(v("1"), v("2"), v("3"), v("4"), v("5"));

        KeyHintData hint = client.createList(key, data).get();

        List<Variant> range = client.getRange(key, null, ContainerType.LIST, 1, 3).get();
        assertEquals(3, range.size());
        assertEquals("2", new String(range.get(0).bytesValue(), java.nio.charset.StandardCharsets.UTF_8));
        assertEquals("3", new String(range.get(1).bytesValue(), java.nio.charset.StandardCharsets.UTF_8));
        assertEquals("4", new String(range.get(2).bytesValue(), java.nio.charset.StandardCharsets.UTF_8));
    }

    // =========================================================================
    // 6. SIZE
    // =========================================================================

    @Test
    @DisplayName("getSize")
    void testGetSize() throws ExecutionException, InterruptedException {
        String key = "list_size_" + UUID.randomUUID();
        List<Variant> data = List.of(v("1"), v("2"), v("3"));

        KeyHintData hint = client.createList(key, data).get();

        Integer size = client.getSize(key).get();
        assertEquals(3, size);
    }

    // =========================================================================
    // 7. LOCK OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("READ_LOCK on list: owner and intruder can both read, neither can write")
    void testReadLock() throws ExecutionException, InterruptedException {
        String key = "list_read_lock_" + UUID.randomUUID();
        List<Variant> data = List.of(v("value"));

        KeyHintData hint = client.createList(key, data).get();

        LockStatus lock = client.lock(key, LockType.READ_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        // Owner can read
        List<Variant> val = client.getList(key, OWNER_CLIENT_ID).get();
        assertNotNull(val);

        // Intruder can ALSO read (shared access)
        List<Variant> intruderVal = client.getList(key, INTRUDER_CLIENT_ID).get();
        assertNotNull(intruderVal);

        // Neither can write
        assertDenied(client.pushTail(key, null, List.of(v("added")), OWNER_CLIENT_ID));
        assertDenied(client.pushTail(key, null, List.of(v("added")), INTRUDER_CLIENT_ID));

        client.unlock(key, OWNER_CLIENT_ID).get();
    }

    @Test
    @DisplayName("WRITE_LOCK on list: owner can read/write, intruder denied")
    void testWriteLock() throws ExecutionException, InterruptedException {
        String key = "list_write_lock_" + UUID.randomUUID();
        List<Variant> data = List.of(v("value"));

        KeyHintData hint = client.createList(key, data).get();

        LockStatus lock = client.lock(key, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        List<Variant> val = client.getList(key, OWNER_CLIENT_ID).get();
        assertNotNull(val);

        client.pushTail(key, null, List.of(v("added")), OWNER_CLIENT_ID).get();

        assertDenied(client.getList(key, INTRUDER_CLIENT_ID));

        client.unlock(key, OWNER_CLIENT_ID).get();
    }

    @Test
    @DisplayName("GLOBAL_LOCK on list: only owner can access")
    void testGlobalLock() throws ExecutionException, InterruptedException {
        String key = "list_global_lock_" + UUID.randomUUID();
        List<Variant> data = List.of(v("value"));

        KeyHintData hint = client.createList(key, data).get();

        LockStatus lock = client.lock(key, LockType.GLOBAL, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        List<Variant> val = client.getList(key, OWNER_CLIENT_ID).get();
        assertNotNull(val);

        assertDenied(client.getList(key, INTRUDER_CLIENT_ID));

        client.unlock(key, OWNER_CLIENT_ID).get();
    }
    @Test
    @DisplayName("Lock expiration on list")
    void testLockExpiration() throws ExecutionException, InterruptedException {
        String key = "list_lock_expire_" + UUID.randomUUID();
        List<Variant> data = List.of(v("value"));

        KeyHintData hint = client.createList(key, data).get();

        LockStatus lock = client.lock(key, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(1)).get();
        assertEquals(LockStatus.OK, lock);

        Thread.sleep(2000);

        List<Variant> val = client.getList(key, INTRUDER_CLIENT_ID).get();
        assertNotNull(val);
    }

    // =========================================================================
    // 5. GET AND DELETE ELEMENTS IN RANGE (LIST)
    // =========================================================================

    @Test
    @DisplayName("getAndDeleteListElementsInRange - partial range")
    void testGetAndDeleteListElementsInRangePartial() throws ExecutionException, InterruptedException {
        String key = "list_getdel_range_" + UUID.randomUUID();
        List<Variant> data = List.of(v("1"), v("2"), v("3"), v("4"), v("5"));

        KeyHintData hint = client.createList(key, data).get();

        // Delete range [1, 3]
        List<Variant> deleted = client.getAndDeleteListElementsInRange(key, hint, 1, 3, false).get();
        assertEquals(3, deleted.size());
        assertEquals("2", new String(deleted.get(0).bytesValue(), java.nio.charset.StandardCharsets.UTF_8));
        assertEquals("3", new String(deleted.get(1).bytesValue(), java.nio.charset.StandardCharsets.UTF_8));
        assertEquals("4", new String(deleted.get(2).bytesValue(), java.nio.charset.StandardCharsets.UTF_8));

        // Verify remaining elements
        List<Variant> remaining = client.getList(key).get();
        assertEquals(2, remaining.size());
        assertEquals("1", new String(remaining.get(0).bytesValue(), java.nio.charset.StandardCharsets.UTF_8));
        assertEquals("5", new String(remaining.get(1).bytesValue(), java.nio.charset.StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("getAndDeleteListElementsInRange - full range (0,0)")
    void testGetAndDeleteListElementsInRangeFullRange() throws ExecutionException, InterruptedException {
        String key = "list_getdel_full_" + UUID.randomUUID();
        List<Variant> data = List.of(v("a"), v("b"), v("c"));

        KeyHintData hint = client.createList(key, data).get();

        // Delete full range with 0,0
        List<Variant> deleted = client.getAndDeleteListElementsInRange(key, hint, 0, 0, false).get();
        assertEquals(3, deleted.size());

        // Verify container is empty
        List<Variant> remaining = client.getList(key).get();
        assertEquals(0, remaining.size());
    }

    @Test
    @DisplayName("getAndDeleteListElementsInRange - start only (end=0)")
    void testGetAndDeleteListElementsInRangeStartOnly() throws ExecutionException, InterruptedException {
        String key = "list_getdel_start_" + UUID.randomUUID();
        List<Variant> data = List.of(v("1"), v("2"), v("3"), v("4"));

        KeyHintData hint = client.createList(key, data).get();

        // Delete from position 2 to end
        List<Variant> deleted = client.getAndDeleteListElementsInRange(key, hint, 2, 0, false).get();
        assertEquals(2, deleted.size());

        // Verify only position 0 remains
        List<Variant> remaining = client.getList(key).get();
        assertEquals(2, remaining.size());
        assertEquals("1", new String(remaining.get(0).bytesValue(), java.nio.charset.StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("getAndDeleteListElementsInRange - end only (start=0)")
    void testGetAndDeleteListElementsInRangeEndOnly() throws ExecutionException, InterruptedException {
        String key = "list_getdel_end_" + UUID.randomUUID();
        List<Variant> data = List.of(v("1"), v("2"), v("3"), v("4"));

        KeyHintData hint = client.createList(key, data).get();

        // Delete from start to position 1
        List<Variant> deleted = client.getAndDeleteListElementsInRange(key, hint, 0, 1, false).get();
        assertEquals(2, deleted.size());

        // Verify positions 2, 3 remain
        List<Variant> remaining = client.getList(key).get();
        assertEquals(2, remaining.size());
        assertEquals("3", new String(remaining.get(0).bytesValue(), java.nio.charset.StandardCharsets.UTF_8));
        assertEquals("4", new String(remaining.get(1).bytesValue(), java.nio.charset.StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("getAndDeleteListElementsInRange - empty range (no match)")
    void testGetAndDeleteListElementsInRangeEmptyRange() throws ExecutionException, InterruptedException {
        String key = "list_getdel_empty_" + UUID.randomUUID();
        List<Variant> data = List.of(v("1"), v("2"));

        KeyHintData hint = client.createList(key, data).get();

        // Delete range [10, 20] - doesn't exist
        List<Variant> deleted = client.getAndDeleteListElementsInRange(key, hint, 10, 20, false).get();
        assertEquals(0, deleted.size());

        // Verify original elements unchanged
        List<Variant> remaining = client.getList(key).get();
        assertEquals(2, remaining.size());
    }

    @Test
    @DisplayName("getAndDeleteListElementsInRange - with no hint")
    void testGetAndDeleteListElementsInRangeNoHint() throws ExecutionException, InterruptedException {
        String key = "list_getdel_no_hint_" + UUID.randomUUID();
        List<Variant> data = List.of(v("a"), v("b"), v("c"));

        KeyHintData hint = client.createList(key, data).get();

        // Delete without hint - client resolves routing
        List<Variant> deleted = client.getAndDeleteListElementsInRange(key, null, 0, 1, false).get();
        assertEquals(2, deleted.size());

        // Verify only position 2 remains
        List<Variant> remaining = client.getList(key).get();
        assertEquals(1, remaining.size());
        assertEquals("c", new String(remaining.get(0).bytesValue(), java.nio.charset.StandardCharsets.UTF_8));
    }
}