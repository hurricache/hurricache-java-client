package com.hurricache.client.cluster.smart;

import com.hurricache.TestBaseCluster;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Mode;
import com.hurricache.client.intf.Variant;
import com.hurricache.grpc.ContainerType;
import com.hurricache.grpc.LockStatus;
import com.hurricache.grpc.LockType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * SmartClient routing tests for Vector operations WITHOUT keyHint.
 * Tests default routing behavior when keyHint is null.
 */
public class SmartClientVectorNoHintRoutingTest extends TestBaseCluster {

    // =========================================================================
    // 1. CREATE + STREAM VECTOR WITHOUT KEYHINT
    // =========================================================================

    @Test
    @DisplayName("Create empty vector, read on MASTER without keyHint")
    void testCreateEmptyVectorWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "vector_no_hint_empty_" + UUID.randomUUID();

        KeyHintData hint = client.createVector(key, new ArrayList<>()).get();
        assertNotNull(hint);
        Thread.sleep(500);

        Integer size = client.setMode(Mode.MASTER).getSize(key).get();
        assertEquals(0, size);
    }

    @Test
    @DisplayName("Create vector with bytes, read on MASTER without keyHint")
    void testCreateVectorWithBytesWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "vector_no_hint_bytes_" + UUID.randomUUID();
        List<Variant> data = List.of(v("item1"), v("item2"), v("item3"));

        KeyHintData hint = client.createVector(key, data).get();
        assertNotNull(hint);
        Thread.sleep(500);

        List<Variant> stream = client.setMode(Mode.MASTER).getVector(key).get();
        assertEquals(3, stream.size());
        assertEquals("item1", new String(stream.get(0).bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("Create vector with int, read on MASTER without keyHint")
    void testCreateVectorWithIntWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "vector_no_hint_int_" + UUID.randomUUID();
        List<Variant> data = List.of(vi(10), vi(20), vi(30));

        KeyHintData hint = client.createVector(key, data).get();
        assertNotNull(hint);
        Thread.sleep(500);

        List<Variant> stream = client.setMode(Mode.MASTER).getVector(key).get();
        assertEquals(3, stream.size());
        assertEquals(10, stream.get(0).longValue());
    }

    @Test
    @DisplayName("Create vector with mixed types, read on MASTER without keyHint")
    void testCreateVectorMixedTypesWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "vector_no_hint_mixed_" + UUID.randomUUID();
        List<Variant> data = List.of(v("hello"), vi(42), vd(3.14));

        KeyHintData hint = client.createVector(key, data).get();
        assertNotNull(hint);
        Thread.sleep(500);

        List<Variant> stream = client.setMode(Mode.MASTER).getVector(key).get();
        assertEquals(3, stream.size());
        assertEquals("hello", new String(stream.get(0).bytesValue(), StandardCharsets.UTF_8));
        assertEquals(42, stream.get(1).longValue());
        assertEquals(3.14, stream.get(2).doubleValue(), 0.001);
    }

    // =========================================================================
    // 2. ADD/REMOVE ENDS WITHOUT KEYHINT
    // =========================================================================

    @Test
    @DisplayName("addElementToTail and addElementToHead on MASTER without keyHint")
    void testAddToTailAndHeadWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "vector_no_hint_add_ends_" + UUID.randomUUID();

        KeyHintData hint = client.createVector(key, List.of(v("middle"))).get();
        Thread.sleep(500);

        // Add to tail on MASTER without keyHint
        client.setMode(Mode.MASTER);
        Integer added = client.pushTail(key, List.of(v("tail1"), v("tail2"))).get();
        assertEquals(2, added);

        // Add to head on MASTER without keyHint
        added = client.pushHead(key, List.of(v("head1"))).get();
        assertEquals(1, added);
        Thread.sleep(500);

        // Verify on BACKUP without keyHint
        List<Variant> stream = client.setMode(Mode.BACKUP).getVector(key).get();
        assertEquals(4, stream.size());
        assertEquals("head1", new String(stream.get(0).bytesValue(), StandardCharsets.UTF_8));
        assertEquals("tail2", new String(stream.get(3).bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("getHead and getTail on MASTER without keyHint")
    void testGetHeadAndTailWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "vector_no_hint_head_tail_" + UUID.randomUUID();
        List<Variant> data = List.of(v("first"), v("middle"), v("last"));

        KeyHintData hint = client.createVector(key, data).get();
        Thread.sleep(500);

        Variant head = client.setMode(Mode.MASTER).peekHead(key).get();
        assertNotNull(head);
        assertEquals("first", new String(head.bytesValue(), StandardCharsets.UTF_8));

        Variant tail = client.setMode(Mode.MASTER).peekTail(key).get();
        assertNotNull(tail);
        assertEquals("last", new String(tail.bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("getAndDeleteHead and getAndDeleteTail on MASTER without keyHint")
    void testGetAndDeleteHeadAndTailWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "vector_no_hint_pop_ends_" + UUID.randomUUID();
        List<Variant> data = List.of(v("first"), v("middle"), v("last"));

        KeyHintData hint = client.createVector(key, data).get();
        Thread.sleep(500);

        // GetAndDeleteHead on MASTER without keyHint
        client.setMode(Mode.MASTER);
        Variant deleted = client.popHead(key).get();
        assertNotNull(deleted);
        assertEquals("first", new String(deleted.bytesValue(), StandardCharsets.UTF_8));

        // GetAndDeleteTail on MASTER without keyHint
        deleted = client.popTail(key).get();
        assertNotNull(deleted);
        assertEquals("last", new String(deleted.bytesValue(), StandardCharsets.UTF_8));
        Thread.sleep(500);

        // Verify on BACKUP without keyHint
        List<Variant> stream = client.setMode(Mode.BACKUP).getVector(key).get();
        assertEquals(1, stream.size());
        assertEquals("middle", new String(stream.get(0).bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("deleteHead and deleteTail on MASTER without keyHint")
    void testDeleteHeadAndTailWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "vector_no_hint_delete_ends_" + UUID.randomUUID();
        List<Variant> data = List.of(v("first"), v("middle"), v("last"));

        KeyHintData hint = client.createVector(key, data).get();
        Thread.sleep(500);

        // DeleteHead on MASTER without keyHint
        client.setMode(Mode.MASTER);
        Integer deleted = client.deleteHead(key).get();
        assertEquals(1, deleted);

        // DeleteTail on MASTER without keyHint
        deleted = client.deleteTail(key).get();
        assertEquals(1, deleted);
        Thread.sleep(500);

        // Verify on BACKUP without keyHint
        List<Variant> stream = client.setMode(Mode.BACKUP).getVector(key).get();
        assertEquals(1, stream.size());
        assertEquals("middle", new String(stream.get(0).bytesValue(), StandardCharsets.UTF_8));
    }

    // =========================================================================
    // 3. INDEX OPERATIONS WITHOUT KEYHINT
    // =========================================================================

    @Test
    @DisplayName("getElementAtPosition with mixed types on MASTER without keyHint")
    void testGetElementAtPositionMixedTypesWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "vector_no_hint_get_position_" + UUID.randomUUID();
        List<Variant> data = List.of(v("first"), vi(42), vd(3.14));

        KeyHintData hint = client.createVector(key, data).get();
        Thread.sleep(500);

        Variant element = client.setMode(Mode.MASTER).getAt(key, 1).get();
        assertNotNull(element);
        assertEquals(42, element.longValue());
    }

    @Test
    @DisplayName("getAndDeleteElementAtPosition on MASTER without keyHint")
    void testGetAndDeleteElementAtPositionWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "vector_no_hint_delete_position_" + UUID.randomUUID();
        List<Variant> data = List.of(v("1"), v("2"), v("3"), v("4"), v("5"));

        KeyHintData hint = client.createVector(key, data).get();
        Thread.sleep(500);

        // GetAndDeleteElementAtPosition on MASTER without keyHint
        client.setMode(Mode.MASTER);
        Variant deleted = client.popAt(key, null, 2).get();
        assertNotNull(deleted);
        assertEquals("3", new String(deleted.bytesValue(), StandardCharsets.UTF_8));
        Thread.sleep(500);

        // Verify on BACKUP without keyHint
        List<Variant> stream = client.setMode(Mode.BACKUP).getVector(key).get();
        assertEquals(4, stream.size());
    }

    @Test
    @DisplayName("deleteFromContainerByKeyValue on MASTER without keyHint")
    void testDeleteFromContainerByKeyValueWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "vector_no_hint_delete_value_" + UUID.randomUUID();
        List<Variant> data = List.of(v("a"), v("b"), v("a"), v("c"));

        KeyHintData hint = client.createVector(key, data).get();
        Thread.sleep(500);

        // DeleteFromContainerByKeyValue on MASTER without keyHint
        client.setMode(Mode.MASTER);
        Integer deleted = client.removeFromContainerByKeyAndValue(key, ContainerType.VECTOR, List.of(), List.of(v("a"))).get();
        assertEquals(2, deleted);
        Thread.sleep(500);

        // Verify on BACKUP without keyHint
        List<Variant> stream = client.setMode(Mode.BACKUP).getVector(key).get();
        assertEquals(2, stream.size());
    }

    // =========================================================================
    // 4. POSITION-BASED INSERTION WITHOUT KEYHINT
    // =========================================================================

    @Test
    @DisplayName("addElementToPositionBefore and addElementToPositionAfter on MASTER without keyHint")
    void testAddElementToPositionWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "vector_no_hint_position_insert_" + UUID.randomUUID();
        List<Variant> data = List.of(v("first"), v("last"));

        KeyHintData hint = client.createVector(key, data).get();
        Thread.sleep(500);

        // Add to position on MASTER without keyHint
        client.setMode(Mode.MASTER);
        Integer added = client.insertBefore(key, List.of(v("before")), v("last")).get();
        assertEquals(1, added);

        added = client.insertAfter(key, List.of(v("after")), v("first")).get();
        assertEquals(1, added);
        Thread.sleep(500);

        // Verify on BACKUP without keyHint
        List<Variant> stream = client.setMode(Mode.BACKUP).getVector(key).get();
        assertEquals(4, stream.size());
        assertEquals("first", new String(stream.get(0).bytesValue(), StandardCharsets.UTF_8));
        assertEquals("after", new String(stream.get(1).bytesValue(), StandardCharsets.UTF_8));
        assertEquals("before", new String(stream.get(2).bytesValue(), StandardCharsets.UTF_8));
        assertEquals("last", new String(stream.get(3).bytesValue(), StandardCharsets.UTF_8));
    }

    // =========================================================================
    // 5. RANGE QUERIES WITHOUT KEYHINT
    // =========================================================================

    @Test
    @DisplayName("getElementsInRange on MASTER without keyHint")
    void testGetElementsInRangeWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "vector_no_hint_range_" + UUID.randomUUID();
        List<Variant> data = List.of(v("1"), v("2"), v("3"), v("4"), v("5"));

        KeyHintData hint = client.createVector(key, data).get();
        Thread.sleep(500);

        List<Variant> range = client.setMode(Mode.MASTER).getRange(key, null, ContainerType.VECTOR, 1, 3).get();
        assertEquals(3, range.size());
        assertEquals("2", new String(range.get(0).bytesValue(), StandardCharsets.UTF_8));
        assertEquals("3", new String(range.get(1).bytesValue(), StandardCharsets.UTF_8));
        assertEquals("4", new String(range.get(2).bytesValue(), StandardCharsets.UTF_8));
    }

    // =========================================================================
    // 6. SIZE WITHOUT KEYHINT
    // =========================================================================

    @Test
    @DisplayName("getSize on MASTER without keyHint")
    void testGetSizeWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "vector_no_hint_size_" + UUID.randomUUID();
        List<Variant> data = List.of(v("1"), v("2"), v("3"));

        KeyHintData hint = client.createVector(key, data).get();
        Thread.sleep(500);

        Integer size = client.setMode(Mode.MASTER).getSize(key).get();
        assertEquals(3, size);
    }

    // =========================================================================
    // 7. LOCK OPERATIONS WITHOUT KEYHINT
    // =========================================================================

    @Test
    @DisplayName("READ_LOCK on vector on MASTER without keyHint")
    void testReadLockOnVectorWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "vector_no_hint_read_lock_" + UUID.randomUUID();
        List<Variant> data = List.of(v("value"));

        KeyHintData hint = client.createVector(key, data).get();
        Thread.sleep(500);

        // Lock on MASTER without keyHint
        client.setMode(Mode.MASTER);
        LockStatus lock = client.lock(key, LockType.READ_LOCK, 100, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);
        Thread.sleep(500);

        // Owner can read on BACKUP without keyHint
        List<Variant> val = client.setMode(Mode.BACKUP).getVector(key, 100).get();
        assertNotNull(val);

        // Intruder can ALSO read on BACKUP (shared access)
        List<Variant> intruderVal = client.setMode(Mode.BACKUP).getVector(key, 999).get();
        assertNotNull(intruderVal);

        // Unlock
        client.setMode(Mode.MASTER);
        client.unlock(key, 100).get();
    }

    @Test
    @DisplayName("WRITE_LOCK on vector on MASTER without keyHint")
    void testWriteLockOnVectorWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "vector_no_hint_write_lock_" + UUID.randomUUID();
        List<Variant> data = List.of(v("value"));

        KeyHintData hint = client.createVector(key, data).get();
        Thread.sleep(500);

        // Lock on MASTER without keyHint
        client.setMode(Mode.MASTER);
        LockStatus lock = client.lock(key, LockType.WRITE_LOCK, 100, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);
        Thread.sleep(500);

        // Owner can read on BACKUP without keyHint
        List<Variant> val = client.setMode(Mode.BACKUP).getVector(key, 100).get();
        assertNotNull(val);

        // Owner can write on MASTER without keyHint
        client.pushTail(key, List.of(v("added")), 100).get();
        Thread.sleep(500);

        // Verify on BACKUP without keyHint
        val = client.setMode(Mode.BACKUP).getVector(key, 100).get();
        assertEquals(2, val.size());

        // Unlock
        client.setMode(Mode.MASTER);
        client.unlock(key, 100).get();
    }

    @Test
    @DisplayName("GLOBAL_LOCK on vector on MASTER without keyHint")
    void testGlobalLockOnVectorWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "vector_no_hint_global_lock_" + UUID.randomUUID();
        List<Variant> data = List.of(v("value"));

        KeyHintData hint = client.createVector(key, data).get();
        Thread.sleep(500);

        // Lock on MASTER without keyHint
        client.setMode(Mode.MASTER);
        LockStatus lock = client.lock(key, LockType.GLOBAL, 100, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);
        Thread.sleep(500);

        // Owner can read on BACKUP without keyHint
        List<Variant> val = client.setMode(Mode.BACKUP).getVector(key, 100).get();
        assertNotNull(val);

        // Unlock
        client.setMode(Mode.MASTER);
        client.unlock(key, 100).get();
    }

    @Test
    @DisplayName("Lock expiration on vector without keyHint")
    void testLockExpirationOnVectorWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "vector_no_hint_lock_expire_" + UUID.randomUUID();
        List<Variant> data = List.of(v("value"));

        KeyHintData hint = client.createVector(key, data).get();
        Thread.sleep(500);

        // Lock with short TTL on MASTER without keyHint
        client.setMode(Mode.MASTER);
        LockStatus lock = client.lock(key, null, LockType.WRITE_LOCK, 100, Duration.ofSeconds(1)).get();
        assertEquals(LockStatus.OK, lock);
        Thread.sleep(2000);

        // Lock expired, can access on BACKUP without keyHint
        List<Variant> val = client.setMode(Mode.BACKUP).getVector(key, 100).get();
        assertNotNull(val);
    }
}