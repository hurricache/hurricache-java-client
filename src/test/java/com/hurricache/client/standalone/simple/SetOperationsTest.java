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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SetOperationsTest extends TestBase {

    private static final int OWNER_CLIENT_ID = 100;
    private static final int INTRUDER_CLIENT_ID = 200;

    // =========================================================================
    // 1. CREATE + STREAM SET
    // =========================================================================

    @Test
    @DisplayName("Create empty set")
    void testCreateEmptySet() throws ExecutionException, InterruptedException {
        String key = "set_empty_" + UUID.randomUUID();

        KeyHintData hint = client.createSet(key, new ArrayList<>()).get();
        assertNotNull(hint);

        Integer size = client.getSize(key).get();
        assertEquals(0, size);
    }

    @Test
    @DisplayName("Create set with bytes")
    void testCreateSetWithBytes() throws ExecutionException, InterruptedException {
        String key = "set_bytes_" + UUID.randomUUID();
        List<Variant> data = List.of(v("item1"), v("item2"), v("item3"));

        KeyHintData hint = client.createSet(key, data).get();
        assertNotNull(hint);

        List<Variant> stream = client.getSet(key).get();
        assertEquals(3, stream.size());
    }

    @Test
    @DisplayName("Create set with int")
    void testCreateSetWithInt() throws ExecutionException, InterruptedException {
        String key = "set_int_" + UUID.randomUUID();
        List<Variant> data = List.of(vi(10), vi(20), vi(30));

        KeyHintData hint = client.createSet(key, data).get();
        assertNotNull(hint);

        List<Variant> stream = client.getSet(key).get();
        assertEquals(3, stream.size());
        assertTrue(stream.stream().anyMatch(v -> v.longValue() == 10));
        assertTrue(stream.stream().anyMatch(v -> v.longValue() == 20));
        assertTrue(stream.stream().anyMatch(v -> v.longValue() == 30));
    }

    @Test
    @DisplayName("Create set with double")
    void testCreateSetWithDouble() throws ExecutionException, InterruptedException {
        String key = "set_double_" + UUID.randomUUID();
        List<Variant> data = List.of(vd(1.1), vd(2.2), vd(3.3));

        KeyHintData hint = client.createSet(key, data).get();
        assertNotNull(hint);

        List<Variant> stream = client.getSet(key).get();
        assertEquals(3, stream.size());
        assertTrue(stream.stream().anyMatch(v -> Math.abs(v.doubleValue() - 1.1) < 0.001));
    }

    @Test
    @DisplayName("Create set with mixed types")
    void testCreateSetMixedTypes() throws ExecutionException, InterruptedException {
        String key = "set_mixed_" + UUID.randomUUID();
        List<Variant> data = List.of(v("hello"), vi(42), vd(3.14));

        KeyHintData hint = client.createSet(key, data).get();
        assertNotNull(hint);

        List<Variant> stream = client.getSet(key).get();
        assertEquals(3, stream.size());
    }

    @Test
    @DisplayName("Set enforces uniqueness - duplicate bytes ignored")
    void testSetUniquenessBytes() throws ExecutionException, InterruptedException {
        String key = "set_unique_bytes_" + UUID.randomUUID();
        List<Variant> data = List.of(v("item1"), v("item2"), v("item1"));

        KeyHintData hint = client.createSet(key, data).get();
        assertNotNull(hint);

        List<Variant> stream = client.getSet(key).get();
        assertEquals(2, stream.size());
    }

    @Test
    @DisplayName("Set enforces uniqueness - duplicate int ignored")
    void testSetUniquenessInt() throws ExecutionException, InterruptedException {
        String key = "set_unique_int_" + UUID.randomUUID();
        List<Variant> data = List.of(vi(10), vi(20), vi(10));

        KeyHintData hint = client.createSet(key, data).get();
        assertNotNull(hint);

        List<Variant> stream = client.getSet(key).get();
        assertEquals(2, stream.size());
        assertEquals(10, stream.stream().filter(v -> v.longValue() == 10).findFirst().get().longValue());
        assertEquals(20, stream.stream().filter(v -> v.longValue() == 20).findFirst().get().longValue());
    }

    // =========================================================================
    // 2. ADD/REMOVE ELEMENTS
    // =========================================================================

    @Test
    @DisplayName("addElement to set")
    void testAddElement() throws ExecutionException, InterruptedException {
        String key = "set_add_" + UUID.randomUUID();

        KeyHintData hint = client.createSet(key, List.of(v("item1"))).get();

        Integer added = client.addElementUnordered(key, List.of(v("item2"), v("item3"))).get();
        assertEquals(2, added);

        List<Variant> stream = client.getSet(key).get();
        assertEquals(3, stream.size());
    }

    @Test
    @DisplayName("addElement ignores duplicates")
    void testAddElementDuplicate() throws ExecutionException, InterruptedException {
        String key = "set_add_dup_" + UUID.randomUUID();

        KeyHintData hint = client.createSet(key, List.of(v("item1"))).get();

        Integer added = client.addElementUnordered(key, List.of(v("item1"))).get();
        assertEquals(0, added);

        List<Variant> stream = client.getSet(key).get();
        assertEquals(1, stream.size());
    }

    @Test
    @DisplayName("deleteFromContainerByKeyValue")
    void testDeleteFromContainerByKeyValue() throws ExecutionException, InterruptedException {
        String key = "set_delete_value_" + UUID.randomUUID();
        List<Variant> data = List.of(v("a"), v("b"), v("c"));

        KeyHintData hint = client.createSet(key, data).get();

        Integer deleted = client.removeFromContainerByKeyAndValue(key, ContainerType.SET, List.of(), List.of(v("b"))).get();
        assertEquals(1, deleted);

        List<Variant> stream = client.getSet(key).get();
        assertEquals(2, stream.size());
        assertFalse(stream.stream().anyMatch(v -> "b".equals(new String(v.bytesValue(), java.nio.charset.StandardCharsets.UTF_8))));
    }

    @Test
    @DisplayName("deleteFromContainerByKey")
    void testDeleteFromContainerByKey() throws ExecutionException, InterruptedException {
        String key = "set_delete_key_" + UUID.randomUUID();
        List<Variant> data = List.of(v("a"), v("b"), v("c"));

        KeyHintData hint = client.createSet(key, data).get();

        Integer deleted = client.removeFromContainerByKey(key, v("b")).get();
        assertEquals(1, deleted);

        List<Variant> stream = client.getSet(key).get();
        assertEquals(2, stream.size());
    }

    // =========================================================================
    // 3. EXIST CHECK
    // =========================================================================

    @Test
    @DisplayName("existInContainerByKey - element exists")
    void testExistInContainerExists() throws ExecutionException, InterruptedException {
        String key = "set_exist_yes_" + UUID.randomUUID();
        List<Variant> data = List.of(v("a"), v("b"), v("c"));

        KeyHintData hint = client.createSet(key, data).get();

        Boolean exists = client.containsKey(key, null, v("b").bytesValue()).get();
        assertTrue(exists);
    }

    @Test
    @DisplayName("existInContainerByKey - element does not exist")
    void testExistInContainerNotExists() throws ExecutionException, InterruptedException {
        String key = "set_exist_no_" + UUID.randomUUID();
        List<Variant> data = List.of(v("a"), v("b"));

        KeyHintData hint = client.createSet(key, data).get();

        Boolean exists = client.containsKey(key, null, v("c").bytesValue()).get();
        assertFalse(exists);
    }

    // =========================================================================
    // 4. SIZE
    // =========================================================================

    @Test
    @DisplayName("getSize")
    void testGetSize() throws ExecutionException, InterruptedException {
        String key = "set_size_" + UUID.randomUUID();
        List<Variant> data = List.of(v("1"), v("2"), v("3"));

        KeyHintData hint = client.createSet(key, data).get();

        Integer size = client.getSize(key).get();
        assertEquals(3, size);
    }

    // =========================================================================
    // 5. LOCK OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("READ_LOCK on set: owner and intruder can both read, neither can write")
    void testReadLock() throws ExecutionException, InterruptedException {
        String key = "set_read_lock_" + UUID.randomUUID();
        List<Variant> data = List.of(v("value"));

        KeyHintData hint = client.createSet(key, data).get();

        LockStatus lock = client.lock(key, LockType.READ_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        // Owner can read
        List<Variant> val = client.getSet(key, null, OWNER_CLIENT_ID).get();
        assertNotNull(val);

        // Intruder can ALSO read (shared access)
        List<Variant> intruderVal = client.getSet(key, null, INTRUDER_CLIENT_ID).get();
        assertNotNull(intruderVal);

        // Neither can write
        assertDenied(client.addElementUnordered(key, null, List.of(v("added")), OWNER_CLIENT_ID));
        assertDenied(client.addElementUnordered(key, null, List.of(v("added")), INTRUDER_CLIENT_ID));

        client.unlock(key, OWNER_CLIENT_ID).get();
    }

    @Test
    @DisplayName("WRITE_LOCK on set: owner can read/write, intruder denied")
    void testWriteLock() throws ExecutionException, InterruptedException {
        String key = "set_write_lock_" + UUID.randomUUID();
        List<Variant> data = List.of(v("value"));

        KeyHintData hint = client.createSet(key, data).get();

        LockStatus lock = client.lock(key, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        List<Variant> val = client.getSet(key, null, OWNER_CLIENT_ID).get();
        assertNotNull(val);

        client.addElementUnordered(key, null, List.of(v("added")), OWNER_CLIENT_ID).get();

        assertDenied(client.getSet(key, null, INTRUDER_CLIENT_ID));

        client.unlock(key, OWNER_CLIENT_ID).get();
    }

    @Test
    @DisplayName("GLOBAL_LOCK on set: only owner can access")
    void testGlobalLock() throws ExecutionException, InterruptedException {
        String key = "set_global_lock_" + UUID.randomUUID();
        List<Variant> data = List.of(v("value"));

        KeyHintData hint = client.createSet(key, data).get();

        LockStatus lock = client.lock(key, LockType.GLOBAL, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        List<Variant> val = client.getSet(key, null, OWNER_CLIENT_ID).get();
        assertNotNull(val);

        assertDenied(client.getSet(key, null, INTRUDER_CLIENT_ID));

        client.unlock(key, OWNER_CLIENT_ID).get();
    }

    @Test
    @DisplayName("Lock expiration on set")
    void testLockExpiration() throws ExecutionException, InterruptedException {
        String key = "set_lock_expire_" + UUID.randomUUID();
        List<Variant> data = List.of(v("value"));

        KeyHintData hint = client.createSet(key, data).get();

        LockStatus lock = client.lock(key, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(1)).get();
        assertEquals(LockStatus.OK, lock);

        Thread.sleep(2000);

        List<Variant> val = client.getSet(key, null, INTRUDER_CLIENT_ID).get();
        assertNotNull(val);
    }
}