package com.hurricache.client.cluster.smart;

import com.hurricache.TestBaseCluster;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Mode;
import com.hurricache.client.intf.Variant;
import com.hurricache.grpc.LockStatus;
import com.hurricache.grpc.LockType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * SmartClient routing tests for Set operations.
 * Mirrors SetOperationsTest for cluster routing.
 */
public class SmartClientSetRoutingTest extends TestBaseCluster {

    // =========================================================================
    // 1. CREATE + STREAM SET WITH ROUTING
    // =========================================================================

    @Test
    @DisplayName("Create empty set, validate on MASTER and BACKUP")
    void testCreateEmptySetOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "set_empty_" + UUID.randomUUID();

        KeyHintData hint = client.createSet(key, List.of()).get();
        assertNotNull(hint);
        Thread.sleep(500);

        Integer size = client.setMode(Mode.MASTER).getSize(key, hint).get();
        assertEquals(0, size);

        size = client.setMode(Mode.BACKUP).getSize(key, hint).get();
        assertEquals(0, size);
    }

    @Test
    @DisplayName("Create set with bytes, read on MASTER then BACKUP")
    void testCreateSetWithBytesOnMasterThenBackup() throws ExecutionException, InterruptedException {
        String key = "set_bytes_" + UUID.randomUUID();
        List<Variant> data = List.of(v("item1"), v("item2"), v("item3"));

        KeyHintData hint = client.createSet(key, data).get();
        assertNotNull(hint);
        Thread.sleep(500);

        List<Variant> stream = client.setMode(Mode.MASTER).getSet(key, hint).get();
        assertEquals(3, stream.size());
        assertTrue(stream.stream().anyMatch(v -> "item1".equals(new String(v.bytesValue(), StandardCharsets.UTF_8))));
        assertTrue(stream.stream().anyMatch(v -> "item2".equals(new String(v.bytesValue(), StandardCharsets.UTF_8))));
        assertTrue(stream.stream().anyMatch(v -> "item3".equals(new String(v.bytesValue(), StandardCharsets.UTF_8))));

        stream = client.setMode(Mode.BACKUP).getSet(key, hint).get();
        assertEquals(3, stream.size());
        assertTrue(stream.stream().anyMatch(v -> "item1".equals(new String(v.bytesValue(), StandardCharsets.UTF_8))));
        assertTrue(stream.stream().anyMatch(v -> "item2".equals(new String(v.bytesValue(), StandardCharsets.UTF_8))));
        assertTrue(stream.stream().anyMatch(v -> "item3".equals(new String(v.bytesValue(), StandardCharsets.UTF_8))));
    }

    @Test
    @DisplayName("Create set with int, read on MASTER then BACKUP")
    void testCreateSetWithIntOnMasterThenBackup() throws ExecutionException, InterruptedException {
        String key = "set_int_" + UUID.randomUUID();
        List<Variant> data = List.of(vi(10), vi(20), vi(30));

        KeyHintData hint = client.createSet(key, data).get();
        assertNotNull(hint);
        Thread.sleep(500);

        List<Variant> stream = client.setMode(Mode.MASTER).getSet(key, hint).get();
        assertEquals(3, stream.size());
        List<Long> ints = stream.stream().map(Variant::longValue).sorted().collect(Collectors.toList());
        assertEquals(10, ints.get(0));
        assertEquals(20, ints.get(1));
        assertEquals(30, ints.get(2));

        stream = client.setMode(Mode.BACKUP).getSet(key, hint).get();
        assertEquals(3, stream.size());
        ints = stream.stream().map(Variant::longValue).sorted().collect(Collectors.toList());
        assertEquals(10, ints.get(0));
        assertEquals(20, ints.get(1));
        assertEquals(30, ints.get(2));
    }

    @Test
    @DisplayName("Create set with double, read on MASTER then BACKUP")
    void testCreateSetWithDoubleOnMasterThenBackup() throws ExecutionException, InterruptedException {
        String key = "set_double_" + UUID.randomUUID();
        List<Variant> data = List.of(vd(1.1), vd(2.2), vd(3.3));

        KeyHintData hint = client.createSet(key, data).get();
        assertNotNull(hint);
        Thread.sleep(500);

        List<Variant> stream = client.setMode(Mode.MASTER).getSet(key, hint).get();
        assertEquals(3, stream.size());
        List<Double> doubles = stream.stream().map(Variant::doubleValue).sorted().collect(Collectors.toList());
        assertEquals(1.1, doubles.get(0), 0.001);
        assertEquals(2.2, doubles.get(1), 0.001);
        assertEquals(3.3, doubles.get(2), 0.001);

        stream = client.setMode(Mode.BACKUP).getSet(key, hint).get();
        assertEquals(3, stream.size());
        doubles = stream.stream().map(Variant::doubleValue).sorted().collect(Collectors.toList());
        assertEquals(1.1, doubles.get(0), 0.001);
        assertEquals(2.2, doubles.get(1), 0.001);
        assertEquals(3.3, doubles.get(2), 0.001);
    }

    @Test
    @DisplayName("Create set with mixed types, validate on MASTER and BACKUP")
    void testCreateSetMixedTypesOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "set_mixed_" + UUID.randomUUID();
        List<Variant> data = List.of(v("hello"), vi(42), vd(3.14));

        KeyHintData hint = client.createSet(key, data).get();
        assertNotNull(hint);
        Thread.sleep(500);

        List<Variant> stream = client.setMode(Mode.MASTER).getSet(key, hint).get();
        assertEquals(3, stream.size());
        assertTrue(stream.stream().anyMatch(v -> v.type() == Variant.Type.BYTES && "hello".equals(new String(v.bytesValue(), StandardCharsets.UTF_8))));
        assertTrue(stream.stream().anyMatch(v -> v.type() == Variant.Type.INT && v.longValue() == 42));
        assertTrue(stream.stream().anyMatch(v -> v.type() == Variant.Type.DOUBLE && Math.abs(v.doubleValue() - 3.14) < 0.001));

        stream = client.setMode(Mode.BACKUP).getSet(key, hint).get();
        assertEquals(3, stream.size());
        assertTrue(stream.stream().anyMatch(v -> v.type() == Variant.Type.BYTES && "hello".equals(new String(v.bytesValue(), StandardCharsets.UTF_8))));
        assertTrue(stream.stream().anyMatch(v -> v.type() == Variant.Type.INT && v.longValue() == 42));
        assertTrue(stream.stream().anyMatch(v -> v.type() == Variant.Type.DOUBLE && Math.abs(v.doubleValue() - 3.14) < 0.001));
    }

    @Test
    @DisplayName("Create set with int, validate uniqueness on MASTER and BACKUP")
    void testCreateSetWithIntUniquenessOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "set_unique_int_" + UUID.randomUUID();
        List<Variant> data = List.of(vi(10), vi(20), vi(10));

        KeyHintData hint = client.createSet(key, data).get();
        assertNotNull(hint);
        Thread.sleep(500);

        // On MASTER - should have 2 unique elements
        List<Variant> stream = client.setMode(Mode.MASTER).getSet(key, hint).get();
        assertEquals(2, stream.size());

        // On BACKUP - should also have 2 unique elements
        stream = client.setMode(Mode.BACKUP).getSet(key, hint).get();
        assertEquals(2, stream.size());
    }

    // =========================================================================
    // 2. ADD ELEMENT TO SET WITH ROUTING
    // =========================================================================

    @Test
    @DisplayName("addElementUnordered on MASTER, validate on BACKUP")
    void testAddElementUnorderedOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "set_add_" + UUID.randomUUID();
        List<Variant> data = List.of(v("item1"));

        KeyHintData hint = client.createSet(key, data).get();
        Thread.sleep(500);

        // AddElementUnordered on MASTER
        client.setMode(Mode.MASTER);
        Integer added = client.addElementUnordered(key, hint, List.of(v("item2"), v("item3"))).get();
        assertEquals(2, added);
        Thread.sleep(500);

        // Verify on BACKUP
        List<Variant> stream = client.setMode(Mode.BACKUP).getSet(key, hint).get();
        assertEquals(3, stream.size());
    }

    @Test
    @DisplayName("addElementUnordered ignores duplicates on MASTER, verify on BACKUP")
    void testAddElementUnorderedDuplicateOnMasterVerifyOnBackup() throws ExecutionException, InterruptedException {
        String key = "set_add_dup_" + UUID.randomUUID();
        List<Variant> data = List.of(v("item1"));

        KeyHintData hint = client.createSet(key, data).get();
        Thread.sleep(500);

        // Try to add duplicate on MASTER
        client.setMode(Mode.MASTER);
        Integer added = client.addElementUnordered(key, hint, List.of(v("item1"))).get();
        assertEquals(0, added);
        Thread.sleep(500);

        // Verify on BACKUP - should still have 1 element
        List<Variant> stream = client.setMode(Mode.BACKUP).getSet(key, hint).get();
        assertEquals(1, stream.size());
    }

    @Test
    @DisplayName("deleteFromContainerByKeyValue on MASTER, verify on BACKUP")
    void testDeleteFromContainerByKeyValueOnMasterVerifyOnBackup() throws ExecutionException, InterruptedException {
        String key = "set_delete_value_" + UUID.randomUUID();
        List<Variant> data = List.of(v("a"), v("b"), v("c"));

        KeyHintData hint = client.createSet(key, data).get();
        Thread.sleep(500);

        // DeleteFromContainerByKeyValue on MASTER
        client.setMode(Mode.MASTER);
        Integer deleted = client.removeFromContainerByKeyAndValue(key, com.hurricache.grpc.ContainerType.SET, List.of(), List.of(v("b"))).get();
        assertEquals(1, deleted);
        Thread.sleep(500);

        // Verify on BACKUP
        List<Variant> stream = client.setMode(Mode.BACKUP).getSet(key, hint).get();
        assertEquals(2, stream.size());
        assertFalse(stream.stream().anyMatch(v -> "b".equals(new String(v.bytesValue(), StandardCharsets.UTF_8))));
    }

    @Test
    @DisplayName("deleteFromContainerByKey on MASTER, verify on BACKUP")
    void testDeleteFromContainerByKeyOnMasterVerifyOnBackup() throws ExecutionException, InterruptedException {
        String key = "set_delete_key_" + UUID.randomUUID();
        List<Variant> data = List.of(v("a"), v("b"), v("c"));

        KeyHintData hint = client.createSet(key, data).get();
        Thread.sleep(500);

        // RemoveFromContainerByKey on MASTER
        client.setMode(Mode.MASTER);
        Integer deleted = client.removeFromContainerByKey(key, hint, v("b")).get();
        assertEquals(1, deleted);
        Thread.sleep(500);

        // Verify on BACKUP
        List<Variant> stream = client.setMode(Mode.BACKUP).getSet(key, hint).get();
        assertEquals(2, stream.size());
    }

    @Test
    @DisplayName("existInContainerByKey - element exists on MASTER then BACKUP")
    void testExistInContainerExistsOnMasterThenBackup() throws ExecutionException, InterruptedException {
        String key = "set_exist_yes_" + UUID.randomUUID();
        List<Variant> data = List.of(v("a"), v("b"), v("c"));

        KeyHintData hint = client.createSet(key, data).get();
        Thread.sleep(500);

        // Exist on MASTER
        Boolean exists = client.setMode(Mode.MASTER).containsKey(key, hint, "b".getBytes()).get();
        assertTrue(exists);

        // Exist on BACKUP
        exists = client.setMode(Mode.BACKUP).containsKey(key, hint, "b".getBytes()).get();
        assertTrue(exists);
    }

    @Test
    @DisplayName("existInContainerByKey - element does not exist on MASTER then BACKUP")
    void testExistInContainerNotExistsOnMasterThenBackup() throws ExecutionException, InterruptedException {
        String key = "set_exist_no_" + UUID.randomUUID();
        List<Variant> data = List.of(v("a"), v("b"), v("c"));

        KeyHintData hint = client.createSet(key, data).get();
        Thread.sleep(500);

        // Exist on MASTER
        Boolean exists = client.setMode(Mode.MASTER).containsKey(key, hint, "z".getBytes()).get();
        assertFalse(exists);

        // Exist on BACKUP
        exists = client.setMode(Mode.BACKUP).containsKey(key, hint, "z".getBytes()).get();
        assertFalse(exists);
    }

    // =========================================================================
    // 3. SIZE WITH ROUTING
    // =========================================================================

    @Test
    @DisplayName("getSize on MASTER then BACKUP")
    void testGetSizeOnMasterThenBackup() throws ExecutionException, InterruptedException {
        String key = "set_size_" + UUID.randomUUID();
        List<Variant> data = List.of(v("1"), v("2"), v("3"));

        KeyHintData hint = client.createSet(key, data).get();
        Thread.sleep(500);

        Integer size = client.setMode(Mode.MASTER).getSize(key, hint).get();
        assertEquals(3, size);

        size = client.setMode(Mode.BACKUP).getSize(key, hint).get();
        assertEquals(3, size);
    }

    // =========================================================================
    // 4. LOCK OPERATIONS WITH ROUTING
    // =========================================================================

    @Test
    @DisplayName("READ_LOCK on set on MASTER: owner and intruder can both read, neither can write")
    void testReadLockOnSetOnMaster() throws ExecutionException, InterruptedException {
        String key = "set_read_lock_" + UUID.randomUUID();
        List<Variant> data = List.of(v("value"));

        KeyHintData hint = client.createSet(key, data).get();
        Thread.sleep(500);

        // Lock on MASTER
        client.setMode(Mode.MASTER);
        LockStatus lock = client.lock(key, hint, LockType.READ_LOCK, 100, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);
        Thread.sleep(500);

        // Owner can read on BACKUP
        List<Variant> val = client.setMode(Mode.BACKUP).getSet(key, hint, 100).get();
        assertNotNull(val);

        // Intruder can ALSO read on BACKUP (shared access)
        List<Variant> intruderVal = client.setMode(Mode.BACKUP).getSet(key, hint, 999).get();
        assertNotNull(intruderVal);

        // Unlock
        client.setMode(Mode.MASTER);
        client.unlock(key, hint, 100).get();
    }

    @Test
    @DisplayName("WRITE_LOCK on set on MASTER: owner can read/write, intruder denied")
    void testWriteLockOnSetOnMaster() throws ExecutionException, InterruptedException {
        String key = "set_write_lock_" + UUID.randomUUID();
        List<Variant> data = List.of(v("value"));

        KeyHintData hint = client.createSet(key, data).get();
        Thread.sleep(500);

        // Lock on MASTER
        client.setMode(Mode.MASTER);
        LockStatus lock = client.lock(key, hint, LockType.WRITE_LOCK, 100, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);
        Thread.sleep(500);

        // Owner can read on BACKUP
        List<Variant> val = client.setMode(Mode.BACKUP).getSet(key, hint, 100).get();
        assertNotNull(val);

        // Owner can write on MASTER
        client.addElementUnordered(key, hint, List.of(v("added")), 100).get();
        Thread.sleep(500);

        // Verify on BACKUP
        val = client.setMode(Mode.BACKUP).getSet(key, hint, 100).get();
        assertEquals(2, val.size());

        // Unlock
        client.setMode(Mode.MASTER);
        client.unlock(key, hint, 100).get();
    }

    @Test
    @DisplayName("GLOBAL_LOCK on set on MASTER: only owner can access")
    void testGlobalLockOnSetOnMaster() throws ExecutionException, InterruptedException {
        String key = "set_global_lock_" + UUID.randomUUID();
        List<Variant> data = List.of(v("value"));

        KeyHintData hint = client.createSet(key, data).get();
        Thread.sleep(500);

        // Lock on MASTER
        client.setMode(Mode.MASTER);
        LockStatus lock = client.lock(key, hint, LockType.GLOBAL, 100, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);
        Thread.sleep(500);

        // Owner can read on BACKUP
        List<Variant> val = client.setMode(Mode.BACKUP).getSet(key, hint, 100).get();
        assertNotNull(val);

        // Unlock
        client.setMode(Mode.MASTER);
        client.unlock(key, hint, 100).get();
    }

    @Test
    @DisplayName("Lock expiration on set with routing")
    void testLockExpirationOnSetOnMaster() throws ExecutionException, InterruptedException {
        String key = "set_lock_expire_" + UUID.randomUUID();
        List<Variant> data = List.of(v("value"));

        KeyHintData hint = client.createSet(key, data).get();
        Thread.sleep(500);

        // Lock with short TTL on MASTER
        client.setMode(Mode.MASTER);
        LockStatus lock = client.lock(key, hint, LockType.WRITE_LOCK, 100, Duration.ofSeconds(1)).get();
        assertEquals(LockStatus.OK, lock);
        Thread.sleep(2000);

        // Lock expired, can access on BACKUP
        List<Variant> val = client.setMode(Mode.BACKUP).getSet(key, hint, 100).get();
        assertNotNull(val);
    }
}