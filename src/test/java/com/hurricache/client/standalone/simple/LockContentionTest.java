package com.hurricache.client.standalone.simple;

import com.hurricache.TestBase;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Variant;
import com.hurricache.grpc.LockStatus;
import com.hurricache.grpc.LockType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Lock behavior tests - verifying correct lock semantics:
 * READ_LOCK: owner reads, others also read (shared), no writes
 * WRITE_LOCK: owner reads+writes, others blocked completely
 * GLOBAL: owner does everything, others wait (blocked)
 * unlockObject: only owner can unlock, others get CANT_UNLOCK
 */
public class LockContentionTest extends TestBase {

    private static final int CLIENT_A = 100;
    private static final int CLIENT_B = 200;
    private static final int CLIENT_C = 300;

    // =========================================================================
    // READ_LOCK - Shared Reading
    // =========================================================================

    @Test
    @DisplayName("READ_LOCK: owner can read, other clients can also read")
    void testReadLockSharedAccess() throws ExecutionException, InterruptedException {
        String key = "lock_read_shared_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, v("value")).get();

        // Client A acquires READ_LOCK
        LockStatus lockA = client.lock(key, LockType.READ_LOCK, CLIENT_A, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lockA);

        // Client A can read
        Variant valA = client.get(key, CLIENT_A).get();
        assertNotNull(valA);

        // Client B can ALSO read (shared access)
        Variant valB = client.get(key, CLIENT_B).get();
        assertNotNull(valB);

        // Client C can ALSO read
        Variant valC = client.get(key, CLIENT_C).get();
        assertNotNull(valC);

        // Cleanup
        client.unlock(key, CLIENT_A).get();
    }

    @Test
    @DisplayName("READ_LOCK: no one can write")
    void testReadLockBlocksWrite() throws ExecutionException, InterruptedException {
        String key = "lock_read_blocks_write_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, v("original")).get();

        // Client A gets READ_LOCK
        LockStatus lockA = client.lock(key, LockType.READ_LOCK, CLIENT_A, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lockA);

        // Client A cannot write (READ_LOCK only)
        assertDenied(client.replace(key, v("updated"), CLIENT_A));

        // Client B cannot write
        assertDenied(client.replace(key, v("updated"), CLIENT_B));

        // Cleanup
        client.unlock(key, CLIENT_A).get();
    }

    @Test
    @DisplayName("READ_LOCK: no one can change the lock")
    void testReadLockBlocksLockChange() throws ExecutionException, InterruptedException {
        String key = "lock_read_blocks_lock_change_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, v("value")).get();

        // Client A gets READ_LOCK
        LockStatus lockA = client.lock(key, LockType.READ_LOCK, CLIENT_A, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lockA);

        // Client B tries to change lock to WRITE_LOCK - should fail
        LockStatus lockB = client.lock(key, LockType.WRITE_LOCK, CLIENT_B, Duration.ofSeconds(1)).get();
        assertNotEquals(LockStatus.OK, lockB);

        // Cleanup
        client.unlock(key, CLIENT_A).get();
    }

    // =========================================================================
    // WRITE_LOCK - Exclusive Access
    // =========================================================================

    @Test
    @DisplayName("WRITE_LOCK: owner can read and write")
    void testWriteLockOwnerAccess() throws ExecutionException, InterruptedException {
        String key = "lock_write_owner_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, v("original")).get();

        LockStatus lockA = client.lock(key, LockType.WRITE_LOCK, CLIENT_A, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lockA);

        // Client A can read
        Variant val = client.get(key, CLIENT_A).get();
        assertNotNull(val);

        // Client A can write
        client.replace(key, v("updated"), CLIENT_A).get();
        val = client.get(key, CLIENT_A).get();
        assertEquals("updated", new String(val.bytesValue(), java.nio.charset.StandardCharsets.UTF_8));

        // Cleanup
        client.unlock(key, CLIENT_A).get();
    }

    @Test
    @DisplayName("WRITE_LOCK: others cannot read")
    void testWriteLockBlocksOthersRead() throws ExecutionException, InterruptedException {
        String key = "lock_write_blocks_read_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, v("value")).get();

        LockStatus lockA = client.lock(key, LockType.WRITE_LOCK, CLIENT_A, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lockA);

        // Client B cannot read
        assertDenied(client.get(key, CLIENT_B));

        // Cleanup
        client.unlock(key, CLIENT_A).get();
    }

    @Test
    @DisplayName("WRITE_LOCK: others cannot write")
    void testWriteLockBlocksOthersWrite() throws ExecutionException, InterruptedException {
        String key = "lock_write_blocks_write_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, v("original")).get();

        LockStatus lockA = client.lock(key, LockType.WRITE_LOCK, CLIENT_A, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lockA);

        // Client B cannot write
        assertDenied(client.replace(key, v("updated"), CLIENT_B));

        // Cleanup
        client.unlock(key, CLIENT_A).get();
    }

    @Test
    @DisplayName("WRITE_LOCK: others cannot change lock")
    void testWriteLockBlocksLockChange() throws ExecutionException, InterruptedException {
        String key = "lock_write_blocks_lock_change_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, v("value")).get();

        LockStatus lockA = client.lock(key, LockType.WRITE_LOCK, CLIENT_A, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lockA);

        // Client B tries to get READ_LOCK - should fail
        LockStatus lockB = client.lock(key, LockType.READ_LOCK, CLIENT_B, Duration.ofSeconds(1)).get();
        assertNotEquals(LockStatus.OK, lockB);

        // Cleanup
        client.unlock(key, CLIENT_A).get();
    }

    // =========================================================================
    // GLOBAL LOCK - Total Blocking
    // =========================================================================

    @Test
    @DisplayName("GLOBAL_LOCK: owner can do everything")
    void testGlobalLockOwnerAccess() throws ExecutionException, InterruptedException {
        String key = "lock_global_owner_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, v("original")).get();

        LockStatus lockA = client.lock(key, LockType.GLOBAL, CLIENT_A, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lockA);

        // Client A can read
        Variant val = client.get(key, CLIENT_A).get();
        assertNotNull(val);

        // Client A can write
        client.replace(key, v("updated"), CLIENT_A).get();
        val = client.get(key, CLIENT_A).get();
        assertEquals("updated", new String(val.bytesValue(), java.nio.charset.StandardCharsets.UTF_8));

        // Cleanup
        client.unlock(key, CLIENT_A).get();
    }

    @Test
    @DisplayName("GLOBAL_LOCK: others cannot read")
    void testGlobalLockBlocksOthersRead() throws ExecutionException, InterruptedException {
        String key = "lock_global_blocks_read_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, v("value")).get();

        LockStatus lockA = client.lock(key, LockType.GLOBAL, CLIENT_A, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lockA);

        // Client B cannot read
        assertDenied(client.get(key, CLIENT_B));

        // Cleanup
        client.unlock(key, CLIENT_A).get();
    }

    @Test
    @DisplayName("GLOBAL_LOCK: others cannot write")
    void testGlobalLockBlocksOthersWrite() throws ExecutionException, InterruptedException {
        String key = "lock_global_blocks_write_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, v("original")).get();

        LockStatus lockA = client.lock(key, LockType.GLOBAL, CLIENT_A, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lockA);

        // Client B cannot write
        assertDenied(client.replace(key, v("updated"), CLIENT_B));

        // Cleanup
        client.unlock(key, CLIENT_A).get();
    }

    @Test
    @DisplayName("GLOBAL_LOCK: others are completely blocked")
    void testGlobalLockBlocksAll() throws ExecutionException, InterruptedException {
        String key = "lock_global_blocks_all_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, v("value")).get();

        LockStatus lockA = client.lock(key, LockType.GLOBAL, CLIENT_A, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lockA);

        // Client B tries READ_LOCK - blocked
        LockStatus readLock = client.lock(key, LockType.READ_LOCK, CLIENT_B, Duration.ofSeconds(1)).get();
        assertNotEquals(LockStatus.OK, readLock);

        // Client B tries WRITE_LOCK - blocked
        LockStatus writeLock = client.lock(key, LockType.WRITE_LOCK, CLIENT_B, Duration.ofSeconds(1)).get();
        assertNotEquals(LockStatus.OK, writeLock);

        // Cleanup
        client.unlock(key, CLIENT_A).get();
    }

    // =========================================================================
    // UNLOCK - Owner Only
    // =========================================================================

    @Test
    @DisplayName("unlockObject: owner can unlock")
    void testUnlockOwner() throws ExecutionException, InterruptedException {
        String key = "lock_unlock_owner_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, v("value")).get();

        LockStatus lockA = client.lock(key, LockType.WRITE_LOCK, CLIENT_A, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lockA);

        // Owner unlocks
        LockStatus unlock = client.unlock(key, CLIENT_A).get();
        assertEquals(LockStatus.OK, unlock);

        // Now anyone can access
        Variant val = client.get(key, CLIENT_B).get();
        assertNotNull(val);
    }

    @Test
    @DisplayName("unlockObject: intruder gets CANT_UNLOCK")
    void testUnlockIntruder() throws ExecutionException, InterruptedException {
        String key = "lock_unlock_intruder_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, v("value")).get();

        LockStatus lockA = client.lock(key, LockType.WRITE_LOCK, CLIENT_A, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lockA);

        // Intruder tries to unlock
        LockStatus unlock = client.unlock(key, CLIENT_B).get();
        assertEquals(LockStatus.CANT_UNLOCK, unlock);

        // Lock still active
        assertDenied(client.get(key, CLIENT_B));

        // Owner can still unlock
        unlock = client.unlock(key, CLIENT_A).get();
        assertEquals(LockStatus.OK, unlock);
    }

    @Test
    @DisplayName("unlockObject: intruder CANT_UNLOCK on READ_LOCK")
    void testUnlockIntruderReadLock() throws ExecutionException, InterruptedException {
        String key = "lock_unlock_intruder_read_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, v("value")).get();

        LockStatus lockA = client.lock(key, LockType.READ_LOCK, CLIENT_A, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lockA);

        LockStatus unlock = client.unlock(key, CLIENT_B).get();
        assertEquals(LockStatus.CANT_UNLOCK, unlock);

        client.unlock(key, CLIENT_A).get();
    }

    @Test
    @DisplayName("unlockObject: intruder CANT_UNLOCK on WRITE_LOCK")
    void testUnlockIntruderWriteLock() throws ExecutionException, InterruptedException {
        String key = "lock_unlock_intruder_write_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, v("value")).get();

        LockStatus lockA = client.lock(key, LockType.WRITE_LOCK, CLIENT_A, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lockA);

        LockStatus unlock = client.unlock(key, CLIENT_B).get();
        assertEquals(LockStatus.CANT_UNLOCK, unlock);

        client.unlock(key, CLIENT_A).get();
    }

    @Test
    @DisplayName("unlockObject: intruder CANT_UNLOCK on GLOBAL")
    void testUnlockIntruderGlobalLock() throws ExecutionException, InterruptedException {
        String key = "lock_unlock_intruder_global_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, v("value")).get();

        LockStatus lockA = client.lock(key, LockType.GLOBAL, CLIENT_A, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lockA);

        LockStatus unlock = client.unlock(key, CLIENT_B).get();
        assertEquals(LockStatus.CANT_UNLOCK, unlock);

        client.unlock(key, CLIENT_A).get();
    }

    // =========================================================================
    // LOCK ON CONTAINERS
    // =========================================================================

    @Test
    @DisplayName("READ_LOCK on list: multiple readers")
    void testReadLockOnListSharedAccess() throws ExecutionException, InterruptedException {
        String key = "lock_list_read_shared_" + UUID.randomUUID();

        KeyHintData hint = client.createList(key, List.of(v("item1"), v("item2"))).get();

        LockStatus lockA = client.lock(key, LockType.READ_LOCK, CLIENT_A, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lockA);

        // Both can stream
        assertNotNull(client.getList(key, CLIENT_A).get());
        assertNotNull(client.getList(key, CLIENT_B).get());

        // No one can add
        assertDenied(client.pushTail(key, null, List.of(v("item3")), CLIENT_A));
        assertDenied(client.pushTail(key, null, List.of(v("item3")), CLIENT_B));

        client.unlock(key, CLIENT_A).get();
    }

    @Test
    @DisplayName("WRITE_LOCK on map: exclusive access")
    void testWriteLockOnMap() throws ExecutionException, InterruptedException {
        String key = "lock_map_write_" + UUID.randomUUID();

        java.util.Map<com.hurricache.client.intf.Payload, Variant> data = new java.util.HashMap<>();
        data.put(p("k1"), v("v1"));

        KeyHintData hint = client.createMap(key, data).get();

        LockStatus lockA = client.lock(key, LockType.WRITE_LOCK, CLIENT_A, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lockA);

        // Owner can read
        assertNotNull(client.getMap(key, null, CLIENT_A).get());

        // Owner can write
        client.putAll(key, null, List.of(p("k2")), List.of(v("v2")), CLIENT_A).get();

        // Others cannot read
        assertDenied(client.getMap(key, null, CLIENT_B));

        client.unlock(key, CLIENT_A).get();
    }

    // =========================================================================
    // LOCK RELEASE AND REACQUIRE
    // =========================================================================

    @Test
    @DisplayName("Client B acquires lock after client A releases")
    void testLockReleaseAndReacquire() throws ExecutionException, InterruptedException {
        String key = "lock_release_reacquire_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, v("value")).get();

        LockStatus lockA = client.lock(key, LockType.WRITE_LOCK, CLIENT_A, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lockA);

        // Client B blocked
        assertDenied(client.get(key, CLIENT_B));

        // Client A releases
        client.unlock(key, CLIENT_A).get();

        // Client B can now access
        Variant val = client.get(key, CLIENT_B).get();
        assertNotNull(val);
    }
}