package com.hurricache.client.cluster.smart;

import com.hurricache.TestBaseCluster;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Mode;
import com.hurricache.client.intf.Variant;
import com.hurricache.grpc.AtomicCasRes;
import com.hurricache.grpc.LockStatus;
import com.hurricache.grpc.LockType;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * SmartClient negative replication tests.
 * Verifies that operations with shouldReplicate=false on the server side
 * (failed CAS, atomic overflow, failed lock acquire) do NOT change the BACKUP replica.
 */
public class SmartClientReplicationNegativeTest extends TestBaseCluster {

    // =========================================================================
    // 1. FAILED CAS IS NOT REPLICATED
    // =========================================================================

    @Test
    @DisplayName("Failed atomicCompareAndSet on MASTER leaves BACKUP unchanged")
    void testFailedCasNotReplicatedToBackup() throws ExecutionException, InterruptedException {
        String key = "negrep_cas_" + UUID.randomUUID();

        // Control: create replicates to BACKUP
        KeyHintData hint = client.create(key, vi(42)).get();
        assertNotNull(hint);
        Thread.sleep(500);

        Variant backupValue = client.setMode(Mode.BACKUP).get(key, hint).get();
        assertEquals(42, backupValue.longValue());

        // Failed CAS on MASTER (expected != actual) - must NOT be replicated
        client.setMode(Mode.MASTER);
        AtomicCasRes result = client.compareAndSet(key, hint, 999, 777).get();
        assertNotNull(result);
        assertFalse(result.getResult());
        Thread.sleep(500);

        // BACKUP still holds the original value
        backupValue = client.setMode(Mode.BACKUP).get(key, hint).get();
        assertEquals(42, backupValue.longValue());

        // MASTER also unchanged
        Variant masterValue = client.setMode(Mode.MASTER).get(key, hint).get();
        assertEquals(42, masterValue.longValue());
    }

    // =========================================================================
    // 2. ATOMIC OVERFLOW IS NOT REPLICATED
    // =========================================================================

    @Test
    @DisplayName("atomicAdd overflow (OUT_OF_RANGE) on MASTER leaves BACKUP unchanged")
    void testAtomicAddOverflowNotReplicatedToBackup() throws ExecutionException, InterruptedException {
        String key = "negrep_overflow_" + UUID.randomUUID();

        // Control: create replicates to BACKUP
        KeyHintData hint = client.create(key, Variant.ofLong(Long.MAX_VALUE)).get();
        assertNotNull(hint);
        Thread.sleep(500);

        Variant backupValue = client.setMode(Mode.BACKUP).get(key, hint).get();
        assertEquals(Long.MAX_VALUE, backupValue.longValue());

        // atomicAdd(+1) overflows on MASTER -> OUT_OF_RANGE, must NOT be replicated
        client.setMode(Mode.MASTER);
        try {
            client.add(key, hint, 1).get();
            fail("Expected OUT_OF_RANGE for integer overflow");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.OUT_OF_RANGE, cause.getStatus().getCode());
        }
        Thread.sleep(500);

        // BACKUP value unchanged
        backupValue = client.setMode(Mode.BACKUP).get(key, hint).get();
        assertEquals(Long.MAX_VALUE, backupValue.longValue());

        // MASTER value unchanged
        Variant masterValue = client.setMode(Mode.MASTER).get(key, hint).get();
        assertEquals(Long.MAX_VALUE, masterValue.longValue());
    }

    // =========================================================================
    // 3. FAILED LOCK ACQUIRE IS NOT REPLICATED
    // =========================================================================

    @Test
    @DisplayName("Failed WRITE_LOCK acquire by intruder on MASTER keeps owner lock on BACKUP")
    void testFailedLockAcquireNotReplicatedToBackup() throws ExecutionException, InterruptedException {
        String key = "negrep_lock_" + UUID.randomUUID();
        int ownerClientId = 100;
        int intruderClientId = 999;

        KeyHintData hint = client.create(key, v("value")).get();
        assertNotNull(hint);
        Thread.sleep(500);

        // Owner takes WRITE_LOCK on MASTER - replicated to BACKUP
        client.setMode(Mode.MASTER);
        LockStatus lock = client.lock(key, hint, LockType.WRITE_LOCK, ownerClientId, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);
        Thread.sleep(500);

        // Intruder tries to take the same lock on MASTER -> denied, must NOT be replicated
        LockStatus intruderLock = client.lock(key, hint, LockType.WRITE_LOCK, intruderClientId, Duration.ofSeconds(30)).get();
        assertNotEquals(LockStatus.OK, intruderLock);
        Thread.sleep(500);

        // On BACKUP the lock still belongs to the owner: owner can read
        Variant ownerValue = client.setMode(Mode.BACKUP).get(key, hint, ownerClientId).get();
        assertNotNull(ownerValue);

        // On BACKUP the intruder is still denied
        try {
            client.setMode(Mode.BACKUP).get(key, hint, intruderClientId).get();
            fail("Expected exception for intruder read with WRITE_LOCK");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.PERMISSION_DENIED, cause.getStatus().getCode());
        }

        // Cleanup: owner unlocks on MASTER
        client.setMode(Mode.MASTER);
        client.unlock(key, hint, ownerClientId).get();
    }
}
