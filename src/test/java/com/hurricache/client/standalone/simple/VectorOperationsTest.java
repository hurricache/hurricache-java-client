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

public class VectorOperationsTest extends TestBase {

    private static final int OWNER_CLIENT_ID = 100;
    private static final int INTRUDER_CLIENT_ID = 200;

    // =========================================================================
    // 1. CREATE + STREAM VECTOR
    // =========================================================================

    @Test
    @DisplayName("Create empty vector")
    void testCreateEmptyVector() throws ExecutionException, InterruptedException {
        String key = "vector_empty_" + UUID.randomUUID();

        KeyHintData hint = client.createVector(key, new ArrayList<>()).get();
        assertNotNull(hint);

        Integer size = client.getSize(key).get();
        assertEquals(0, size);
    }

    @Test
    @DisplayName("Create vector with bytes")
    void testCreateVectorWithBytes() throws ExecutionException, InterruptedException {
        String key = "vector_bytes_" + UUID.randomUUID();
        List<Variant> data = List.of(v("item1"), v("item2"), v("item3"));

        KeyHintData hint = client.createVector(key, data).get();
        assertNotNull(hint);

        List<Variant> stream = client.getVector(key).get();
        assertEquals(3, stream.size());
        assertEquals("item1", new String(stream.get(0).bytesValue(), java.nio.charset.StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("Create vector with int")
    void testCreateVectorWithInt() throws ExecutionException, InterruptedException {
        String key = "vector_int_" + UUID.randomUUID();
        List<Variant> data = List.of(vi(10), vi(20), vi(30));

        KeyHintData hint = client.createVector(key, data).get();
        assertNotNull(hint);

        List<Variant> stream = client.getVector(key).get();
        assertEquals(3, stream.size());
        assertEquals(10, stream.get(0).longValue());
    }

    @Test
    @DisplayName("Create vector with double")
    void testCreateVectorWithDouble() throws ExecutionException, InterruptedException {
        String key = "vector_double_" + UUID.randomUUID();
        List<Variant> data = List.of(vd(1.1), vd(2.2), vd(3.3));

        KeyHintData hint = client.createVector(key, data).get();
        assertNotNull(hint);

        List<Variant> stream = client.getVector(key).get();
        assertEquals(3, stream.size());
        assertEquals(1.1, stream.get(0).doubleValue(), 0.001);
    }

    @Test
    @DisplayName("Create vector with mixed types")
    void testCreateVectorMixedTypes() throws ExecutionException, InterruptedException {
        String key = "vector_mixed_" + UUID.randomUUID();
        List<Variant> data = List.of(v("hello"), vi(42), vd(3.14));

        KeyHintData hint = client.createVector(key, data).get();
        assertNotNull(hint);

        List<Variant> stream = client.getVector(key).get();
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
        String key = "vector_add_ends_" + UUID.randomUUID();

        KeyHintData hint = client.createVector(key, List.of(v("middle"))).get();

        Integer added = client.pushTail(key, List.of(v("tail1"), v("tail2"))).get();
        assertEquals(2, added);

        added = client.pushHead(key, List.of(v("head1"))).get();
        assertEquals(1, added);

        List<Variant> stream = client.getVector(key).get();
        assertEquals(4, stream.size());
        assertEquals("head1", new String(stream.get(0).bytesValue(), java.nio.charset.StandardCharsets.UTF_8));
        assertEquals("tail2", new String(stream.get(3).bytesValue(), java.nio.charset.StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("getHead and getTail")
    void testGetHeadAndTail() throws ExecutionException, InterruptedException {
        String key = "vector_head_tail_" + UUID.randomUUID();
        List<Variant> data = List.of(v("first"), v("middle"), v("last"));

        KeyHintData hint = client.createVector(key, data).get();

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
        String key = "vector_pop_ends_" + UUID.randomUUID();
        List<Variant> data = List.of(v("first"), v("middle"), v("last"));

        KeyHintData hint = client.createVector(key, data).get();

        Variant deleted = client.popHead(key).get();
        assertNotNull(deleted);
        assertEquals("first", new String(deleted.bytesValue(), java.nio.charset.StandardCharsets.UTF_8));

        deleted = client.popTail(key).get();
        assertNotNull(deleted);
        assertEquals("last", new String(deleted.bytesValue(), java.nio.charset.StandardCharsets.UTF_8));

        List<Variant> stream = client.getVector(key).get();
        assertEquals(1, stream.size());
    }

    @Test
    @DisplayName("deleteHead and deleteTail")
    void testDeleteHeadAndTail() throws ExecutionException, InterruptedException {
        String key = "vector_delete_ends_" + UUID.randomUUID();
        List<Variant> data = List.of(v("first"), v("middle"), v("last"));

        KeyHintData hint = client.createVector(key, data).get();

        Integer deleted = client.deleteHead(key).get();
        assertEquals(1, deleted);

        deleted = client.deleteTail(key).get();
        assertEquals(1, deleted);

        List<Variant> stream = client.getVector(key).get();
        assertEquals(1, stream.size());
    }

    // =========================================================================
    // 3. INDEX OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("getElementAtPosition with mixed types")
    void testGetElementAtPosition() throws ExecutionException, InterruptedException {
        String key = "vector_get_position_" + UUID.randomUUID();
        List<Variant> data = List.of(v("first"), vi(42), vd(3.14));

        KeyHintData hint = client.createVector(key, data).get();

        Variant element = client.getAt(key, 1).get();
        assertNotNull(element);
        assertEquals(42, element.longValue());

        element = client.getAt(key, 2).get();
        assertNotNull(element);
        assertEquals(3.14, element.doubleValue(), 0.001);
    }

    @Test
    @DisplayName("getAndDeleteElementAtPosition")
    void testGetAndDeleteElementAtPosition() throws ExecutionException, InterruptedException {
        String key = "vector_delete_position_" + UUID.randomUUID();
        List<Variant> data = List.of(v("1"), v("2"), v("3"), v("4"), v("5"));

        KeyHintData hint = client.createVector(key, data).get();

        Variant deleted = client.popAt(key, null, 2).get();
        assertNotNull(deleted);
        assertEquals("3", new String(deleted.bytesValue(), java.nio.charset.StandardCharsets.UTF_8));

        List<Variant> stream = client.getVector(key).get();
        assertEquals(4, stream.size());
    }

    @Test
    @DisplayName("deleteElementAtPosition and deleteElementAtPosition range")
    void testDeleteElementAtPosition() throws ExecutionException, InterruptedException {
        String key = "vector_delete_range_" + UUID.randomUUID();
        List<Variant> data = List.of(v("1"), v("2"), v("3"), v("4"), v("5"));

        KeyHintData hint = client.createVector(key, data).get();

        Integer deleted = client.removeAt(key, null, 2L).get();
        assertEquals(1, deleted);

        List<Variant> stream = client.getVector(key).get();
        assertEquals(4, stream.size());

        deleted = client.removeAt(key, null, 1L, 2L).get();
        assertEquals(2, deleted);

        stream = client.getVector(key).get();
        assertEquals(2, stream.size());
    }

    @Test
    @DisplayName("deleteFromContainerByKeyValue")
    void testDeleteFromContainerByKeyValue() throws ExecutionException, InterruptedException {
        String key = "vector_delete_value_" + UUID.randomUUID();
        List<Variant> data = List.of(v("a"), v("b"), v("a"), v("c"));

        KeyHintData hint = client.createVector(key, data).get();

        Integer deleted = client.removeFromContainerByKeyAndValue(key, ContainerType.VECTOR, List.of(), List.of(v("a"))).get();
        assertEquals(2, deleted);

        List<Variant> stream = client.getVector(key).get();
        assertEquals(2, stream.size());
    }

    // =========================================================================
    // 4. POSITION-BASED INSERTION
    // =========================================================================

    @Test
    @DisplayName("addElementToPositionBefore and addElementToPositionAfter")
    void testAddElementToPosition() throws ExecutionException, InterruptedException {
        String key = "vector_position_insert_" + UUID.randomUUID();
        List<Variant> data = List.of(v("first"), v("last"));

        KeyHintData hint = client.createVector(key, data).get();

        Integer added = client.insertBefore(key, hint, List.of(v("before")), v("last")).get();
        assertEquals(1, added);

        added = client.insertAfter(key, hint, List.of(v("after")), v("first")).get();
        assertEquals(1, added);

        List<Variant> stream = client.getVector(key).get();
        assertEquals(4, stream.size());
        assertEquals("first", new String(stream.get(0).bytesValue(), java.nio.charset.StandardCharsets.UTF_8));
        assertEquals("after", new String(stream.get(1).bytesValue(), java.nio.charset.StandardCharsets.UTF_8));
        assertEquals("before", new String(stream.get(2).bytesValue(), java.nio.charset.StandardCharsets.UTF_8));
        assertEquals("last", new String(stream.get(3).bytesValue(), java.nio.charset.StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("addElementToPositionBefore/After with mixed types")
    void testAddElementToPositionMixedTypes() throws ExecutionException, InterruptedException {
        String key = "vector_position_mixed_" + UUID.randomUUID();
        List<Variant> data = List.of(vi(100));

        KeyHintData hint = client.createVector(key, data).get();

        client.insertBefore(key, hint, List.of(v("str"), vi(200), vd(3.14)), vi(100)).get();

        List<Variant> stream = client.getVector(key).get();
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
        String key = "vector_range_" + UUID.randomUUID();
        List<Variant> data = List.of(v("1"), v("2"), v("3"), v("4"), v("5"));

        KeyHintData hint = client.createVector(key, data).get();

        List<Variant> range = client.getRange(key, null, ContainerType.VECTOR, 1, 3).get();
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
        String key = "vector_size_" + UUID.randomUUID();
        List<Variant> data = List.of(v("1"), v("2"), v("3"));

        KeyHintData hint = client.createVector(key, data).get();

        Integer size = client.getSize(key).get();
        assertEquals(3, size);
    }

    // =========================================================================
    // 7. LOCK OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("READ_LOCK on vector: owner and intruder can both read, neither can write")
    void testReadLock() throws ExecutionException, InterruptedException {
        String key = "vector_read_lock_" + UUID.randomUUID();
        List<Variant> data = List.of(v("value"));

        KeyHintData hint = client.createVector(key, data).get();

        LockStatus lock = client.lock(key, LockType.READ_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        // Owner can read
        List<Variant> val = client.getVector(key.getBytes(), OWNER_CLIENT_ID).get();
        assertNotNull(val);

        // Intruder can ALSO read (shared access)
        List<Variant> intruderVal = client.getVector(key.getBytes(), INTRUDER_CLIENT_ID).get();
        assertNotNull(intruderVal);

        // Neither can write
        assertDenied(client.pushTail(key, null, List.of(v("added")), OWNER_CLIENT_ID));
        assertDenied(client.pushTail(key, null, List.of(v("added")), INTRUDER_CLIENT_ID));

        client.unlock(key, OWNER_CLIENT_ID).get();
    }

    @Test
    @DisplayName("WRITE_LOCK on vector: owner can read/write, intruder denied")
    void testWriteLock() throws ExecutionException, InterruptedException {
        String key = "vector_write_lock_" + UUID.randomUUID();
        List<Variant> data = List.of(v("value"));

        KeyHintData hint = client.createVector(key, data).get();

        LockStatus lock = client.lock(key, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        List<Variant> val = client.getVector(key.getBytes(), OWNER_CLIENT_ID).get();
        assertNotNull(val);

        client.pushTail(key, null, List.of(v("added")), OWNER_CLIENT_ID).get();

        assertDenied(client.getVector(key.getBytes(), INTRUDER_CLIENT_ID));

        client.unlock(key, OWNER_CLIENT_ID).get();
    }

    @Test
    @DisplayName("GLOBAL_LOCK on vector: only owner can access")
    void testGlobalLock() throws ExecutionException, InterruptedException {
        String key = "vector_global_lock_" + UUID.randomUUID();
        List<Variant> data = List.of(v("value"));

        KeyHintData hint = client.createVector(key, data).get();

        LockStatus lock = client.lock(key, LockType.GLOBAL, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        List<Variant> val = client.getVector(key.getBytes(), OWNER_CLIENT_ID).get();
        assertNotNull(val);

        assertDenied(client.getVector(key.getBytes(), INTRUDER_CLIENT_ID));

        client.unlock(key, OWNER_CLIENT_ID).get();
    }
    @Test
    @DisplayName("Lock expiration on vector")
    void testLockExpiration() throws ExecutionException, InterruptedException {
        String key = "vector_lock_expire_" + UUID.randomUUID();
        List<Variant> data = List.of(v("value"));

        KeyHintData hint = client.createVector(key, data).get();

        LockStatus lock = client.lock(key, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(1)).get();
        assertEquals(LockStatus.OK, lock);

        Thread.sleep(2000);

        List<Variant> val = client.getVector(key.getBytes(), INTRUDER_CLIENT_ID).get();
        assertNotNull(val);
    }
}