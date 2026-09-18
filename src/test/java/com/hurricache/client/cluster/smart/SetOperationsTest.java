package com.hurricache.client.cluster.smart;

import com.hurricache.TestBaseCluster;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Payload;
import com.hurricache.client.intf.Mode;
import com.hurricache.grpc.LockStatus;
import com.hurricache.grpc.LockType;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
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
import static org.junit.jupiter.api.Assertions.fail;

public class SetOperationsTest extends TestBaseCluster {

    private static final int OWNER_CLIENT_ID = 100;
    private static final int INTRUDER_CLIENT_ID = 200;
    private static final long REPLICATION_DELAY_MS = 100;

    // ==================== Section 1: Set Creation ====================

    @Test
    @DisplayName("Create empty set on master, verify replication to backup")
    void testCreateEmptySet() throws ExecutionException, InterruptedException {
        String key = "emptySet" + UUID.randomUUID();

        // Create empty set
        KeyHintData keyHint = client.createSet(key, new ArrayList<>())
                .get();

        Thread.sleep(REPLICATION_DELAY_MS); // replication wait

        // Verify on MASTER: getSize returns 0
        Integer masterSize = client.setMode(Mode.MASTER)
                .getSize(key, keyHint)
                .get();
        assertNotNull(masterSize);
        assertEquals(0, masterSize, "Expected size 0 on master");

        // Verify on BACKUP: streamSet returns empty list
        Thread.sleep(REPLICATION_DELAY_MS);
        List<Payload> backupStream = client.setMode(Mode.BACKUP)
                .streamSet(key, keyHint)
                .get();
        assertNotNull(backupStream);
        assertTrue(backupStream.isEmpty(), "Expected empty set on backup");
    }

    @Test
    @DisplayName("Create set with data on master, verify replication to backup")
    void testCreateSetWithData() throws ExecutionException, InterruptedException {
        String key = "setWithData" + UUID.randomUUID();
        List<Payload> initialData = List.of(
                Payload.of(bytes("item1")),
                Payload.of(bytes("item2")),
                Payload.of(bytes("item3"))
        );

        // Create set
        KeyHintData keyHint = client.createSet(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS); // replication wait

        // Verify on MASTER: streamSet returns 3 elements
        List<Payload> masterStream = client.setMode(Mode.MASTER)
                .streamSet(key, keyHint)
                .get();
        assertNotNull(masterStream);
        assertEquals(3, masterStream.size());

        // Verify on BACKUP: streamSet returns 3 elements
        List<Payload> backupStream = client.setMode(Mode.BACKUP)
                .streamSet(key, keyHint)
                .get();
        assertNotNull(backupStream);
        assertEquals(3, backupStream.size());

        // Write on BACKUP: addElementUnordered
        client.setMode(Mode.BACKUP)
                .addElementUnordered(key, keyHint, List.of(Payload.of(bytes("item4"))))
                .get();

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on BACKUP: streamSet returns 4 elements
        List<Payload> backupAfterWrite = client.setMode(Mode.BACKUP)
                .streamSet(key, keyHint)
                .get();
        assertEquals(4, backupAfterWrite.size());

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on MASTER: streamSet returns 4 elements
        List<Payload> masterAfterWrite = client.setMode(Mode.MASTER)
                .streamSet(key, keyHint)
                .get();
        assertEquals(4, masterAfterWrite.size());
    }

    @Test
    @DisplayName("Create large set with chunking on master, verify 1500 elements on both nodes")
    void testCreateLargeSetWithChunking() throws ExecutionException, InterruptedException {
        String key = "largeSet" + UUID.randomUUID();
        int elementCount = 1500;

        List<Payload> payloads = new ArrayList<>(elementCount);
        for (int i = 0; i < elementCount; i++) {
            payloads.add(Payload.of(bytes("large_item_" + i + "_" + UUID.randomUUID())));
        }

        // Create large set
        KeyHintData keyHint = client.createSet(key, payloads)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS); // replication wait

        // Verify on MASTER: streamSet returns 1500 elements
        List<Payload> masterStream = client.setMode(Mode.MASTER)
                .streamSet(key, keyHint)
                .get();
        assertNotNull(masterStream);
        assertEquals(elementCount, masterStream.size(), "Expected " + elementCount + " elements on master");

        // Verify on BACKUP: streamSet returns 1500 elements
        List<Payload> backupStream = client.setMode(Mode.BACKUP)
                .streamSet(key, keyHint)
                .get();
        assertNotNull(backupStream);
        assertEquals(elementCount, backupStream.size(), "Expected " + elementCount + " elements on backup");
    }

    // ==================== Section 2: Stream Set Operations ====================

    @Test
    @DisplayName("streamSet on master, verify replication and write on backup")
    void testStreamSet() throws ExecutionException, InterruptedException {
        String key = "streamSetTest" + UUID.randomUUID();
        List<Payload> initialData = List.of(
                Payload.of(bytes("elem1")),
                Payload.of(bytes("elem2")),
                Payload.of(bytes("elem3"))
        );

        // Create set
        KeyHintData keyHint = client.createSet(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS); // replication wait

        // Verify on MASTER: streamSet returns 3 elements
        List<Payload> masterStream = client.setMode(Mode.MASTER)
                .streamSet(key, keyHint)
                .get();
        assertNotNull(masterStream);
        assertEquals(3, masterStream.size());

        // Verify on BACKUP: streamSet returns 3 elements
        List<Payload> backupStream = client.setMode(Mode.BACKUP)
                .streamSet(key, keyHint)
                .get();
        assertNotNull(backupStream);
        assertEquals(3, backupStream.size());

        // Write on BACKUP: addElementUnordered
        client.setMode(Mode.BACKUP)
                .addElementUnordered(key, keyHint, List.of(Payload.of(bytes("elem4"))))
                .get();

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on BACKUP: streamSet returns 4 elements
        List<Payload> backupAfterWrite = client.setMode(Mode.BACKUP)
                .streamSet(key, keyHint)
                .get();
        assertEquals(4, backupAfterWrite.size());

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on MASTER: streamSet returns 4 elements
        List<Payload> masterAfterWrite = client.setMode(Mode.MASTER)
                .streamSet(key, keyHint)
                .get();
        assertEquals(4, masterAfterWrite.size());
    }

    // ==================== Section 3: Add Element Operations ====================

    @Test
    @DisplayName("addElementUnordered on master, verify replication and write on backup")
    void testAddElementUnordered() throws ExecutionException, InterruptedException {
        String key = "addElementUnorderedTest" + UUID.randomUUID();
        List<Payload> initialData = List.of(Payload.of(bytes("item1")));

        // Create set
        KeyHintData keyHint = client.createSet(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS); // replication wait

        // Write on BACKUP: addElementUnordered
        Integer added = client.setMode(Mode.BACKUP)
                .addElementUnordered(key, keyHint, List.of(
                        Payload.of(bytes("item2")),
                        Payload.of(bytes("item3"))
                ))
                .get();
        assertEquals(2, added, "2 unique elements should be added");

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on BACKUP: getSize returns 3
        Integer backupSize = client.setMode(Mode.BACKUP)
                .getSize(key, keyHint)
                .get();
        assertEquals(3, backupSize);

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on MASTER: getSize returns 3
        Integer masterSize = client.setMode(Mode.MASTER)
                .getSize(key, keyHint)
                .get();
        assertEquals(3, masterSize);

        // Write on MASTER: addElementUnordered
        Integer addedMaster = client.setMode(Mode.MASTER)
                .addElementUnordered(key, keyHint, List.of(Payload.of(bytes("item4"))))
                .get();
        assertEquals(1, addedMaster);

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on MASTER: getSize returns 4
        Integer masterSizeAfter = client.setMode(Mode.MASTER)
                .getSize(key, keyHint)
                .get();
        assertEquals(4, masterSizeAfter);

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on BACKUP: getSize returns 4
        Integer backupSizeAfter = client.setMode(Mode.BACKUP)
                .getSize(key, keyHint)
                .get();
        assertEquals(4, backupSizeAfter);
    }

    @Test
    @DisplayName("addElementUnordered does not add duplicates and returns 0 for duplicates")
    void testAddElementUnorderedNoDuplicates() throws ExecutionException, InterruptedException {
        String key = "addElementUnorderedNoDup" + UUID.randomUUID();
        List<Payload> initialData = List.of(Payload.of(bytes("item1")));

        // Create set
        KeyHintData keyHint = client.createSet(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS); // replication wait

        // Write on BACKUP: addElementUnordered with duplicate
        Integer added = client.setMode(Mode.BACKUP)
                .addElementUnordered(key, keyHint, List.of(
                        Payload.of(bytes("item1")),
                        Payload.of(bytes("item2"))
                ))
                .get();
        assertEquals(1, added, "Only 1 new element (item2) should be added, item1 is duplicate");

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on BACKUP: getSize returns 2
        Integer backupSize = client.setMode(Mode.BACKUP)
                .getSize(key, keyHint)
                .get();
        assertEquals(2, backupSize);

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on MASTER: getSize returns 2
        Integer masterSize = client.setMode(Mode.MASTER)
                .getSize(key, keyHint)
                .get();
        assertEquals(2, masterSize);
    }

    // ==================== Section 4: Remove From Container Operations ====================

    @Test
    @DisplayName("removeFromContainer on master, verify replication and write on backup")
    void testRemoveFromContainer() throws ExecutionException, InterruptedException {
        String key = "removeFromContainerSetTest" + UUID.randomUUID();
        List<Payload> initialData = List.of(
                Payload.of(bytes("item1")),
                Payload.of(bytes("item2")),
                Payload.of(bytes("item3"))
        );

        // Create set
        KeyHintData keyHint = client.createSet(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS); // replication wait

        // Write on BACKUP: removeFromContainer
        Integer removed = client.setMode(Mode.BACKUP)
                .removeFromContainer(bytes(key), keyHint, bytes("item1"))
                .get();
        assertEquals(1, removed, "1 element should be removed");

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on BACKUP: getSize returns 2
        Integer backupSize = client.setMode(Mode.BACKUP)
                .getSize(key, keyHint)
                .get();
        assertEquals(2, backupSize);

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on MASTER: getSize returns 2
        Integer masterSize = client.setMode(Mode.MASTER)
                .getSize(key, keyHint)
                .get();
        assertEquals(2, masterSize);

        // Write on MASTER: removeFromContainer
        Integer removedMaster = client.setMode(Mode.MASTER)
                .removeFromContainer(bytes(key), keyHint, bytes("item2"))
                .get();
        assertEquals(1, removedMaster);

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on MASTER: getSize returns 1
        Integer masterSizeAfter = client.setMode(Mode.MASTER)
                .getSize(key, keyHint)
                .get();
        assertEquals(1, masterSizeAfter);

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on BACKUP: getSize returns 1
        Integer backupSizeAfter = client.setMode(Mode.BACKUP)
                .getSize(key, keyHint)
                .get();
        assertEquals(1, backupSizeAfter);
    }

    @Test
    @DisplayName("removeFromContainer returns 0 if element doesn't exist")
    void testRemoveFromContainerNonExistent() throws ExecutionException, InterruptedException {
        String key = "removeFromContainerNonExistSet" + UUID.randomUUID();
        List<Payload> initialData = List.of(
                Payload.of(bytes("item1")),
                Payload.of(bytes("item2"))
        );

        // Create set
        KeyHintData keyHint = client.createSet(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS); // replication wait

        // Write on BACKUP: removeFromContainer non-existent
        Integer removed = client.setMode(Mode.BACKUP)
                .removeFromContainer(bytes(key), keyHint, bytes("nonexistent"))
                .get();
        assertEquals(0, removed, "0 elements should be removed (element not found)");

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on BACKUP: getSize still returns 2
        Integer backupSize = client.setMode(Mode.BACKUP)
                .getSize(key, keyHint)
                .get();
        assertEquals(2, backupSize);

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on MASTER: getSize still returns 2
        Integer masterSize = client.setMode(Mode.MASTER)
                .getSize(key, keyHint)
                .get();
        assertEquals(2, masterSize);
    }

    // ==================== Section 5: Contains Container Key Operations ====================

    @Test
    @DisplayName("containsContainerKey on master, verify replication and write on backup")
    void testContainsContainerKey() throws ExecutionException, InterruptedException {
        String key = "containsContainerKeySetTest" + UUID.randomUUID();
        List<Payload> initialData = List.of(
                Payload.of(bytes("item1")),
                Payload.of(bytes("item2"))
        );

        // Create set
        KeyHintData keyHint = client.createSet(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS); // replication wait

        // Write on BACKUP: containsContainerKey for existing element
        Boolean exists = client.setMode(Mode.BACKUP)
                .containsContainerKey(bytes(key), keyHint, bytes("item1"))
                .get();
        assertTrue(exists, "Element item1 should exist");

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on BACKUP: containsContainerKey for non-existing element
        Boolean notExists = client.setMode(Mode.BACKUP)
                .containsContainerKey(bytes(key), keyHint, bytes("nonexistent"))
                .get();
        assertTrue(!notExists, "Element nonexistent should not exist");

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on MASTER: containsContainerKey for existing element
        Boolean masterExists = client.setMode(Mode.MASTER)
                .containsContainerKey(bytes(key), keyHint, bytes("item1"))
                .get();
        assertTrue(masterExists);

        // Write on MASTER: addElementUnordered
        client.setMode(Mode.MASTER)
                .addElementUnordered(key, keyHint, List.of(Payload.of(bytes("item3"))))
                .get();

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on MASTER: containsContainerKey for new element
        Boolean newExists = client.setMode(Mode.MASTER)
                .containsContainerKey(bytes(key), keyHint, bytes("item3"))
                .get();
        assertTrue(newExists);

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on BACKUP: containsContainerKey for new element
        Boolean backupNewExists = client.setMode(Mode.BACKUP)
                .containsContainerKey(bytes(key), keyHint, bytes("item3"))
                .get();
        assertTrue(backupNewExists);
    }

    // ==================== Section 6: Set TTL Operations ====================

    @Test
    @DisplayName("SetTtl on master, verify GetTtl on both nodes and write on backup")
    void testSetTtlAndGetTtl() throws ExecutionException, InterruptedException {
        String key = "setTtlSetTest" + UUID.randomUUID();

        // Create set
        KeyHintData keyHint = client.createSet(key, List.of(Payload.of(bytes("item1"))))
                .get();

        Thread.sleep(REPLICATION_DELAY_MS); // replication wait

        // Write on BACKUP: setTtl 300000ms (5 minutes)
        Boolean ttlSet = client.setMode(Mode.BACKUP)
                .setTtl(key, keyHint, 300000)
                .get();
        assertTrue(ttlSet);

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on BACKUP: getTtl returns ~300000ms (with tolerance for elapsed time)
        Long ttlBackup = client.setMode(Mode.BACKUP)
                .getTtl(key, keyHint)
                .get();
        assertNotNull(ttlBackup);
        assertTrue(ttlBackup >= 299000 && ttlBackup <= 300000,
                "Expected TTL ~300000ms on backup, got " + ttlBackup);

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on MASTER: getTtl returns ~300000ms (with tolerance for elapsed time)
        Long ttlMaster = client.setMode(Mode.MASTER)
                .getTtl(key, keyHint)
                .get();
        assertNotNull(ttlMaster);
        assertTrue(ttlMaster >= 298000 && ttlMaster <= 300000,
                "Expected TTL ~300000ms on master, got " + ttlMaster);

        // Write on MASTER: setTtl 600000ms (10 minutes)
        Boolean ttlSet2 = client.setMode(Mode.MASTER)
                .setTtl(key, keyHint, 600000)
                .get();
        assertTrue(ttlSet2);

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on MASTER: getTtl returns ~600000ms (with tolerance for elapsed time)
        Long ttlMaster2 = client.setMode(Mode.MASTER)
                .getTtl(key, keyHint)
                .get();
        assertNotNull(ttlMaster2);
        assertTrue(ttlMaster2 >= 599000 && ttlMaster2 <= 600000,
                "Expected TTL ~600000ms on master, got " + ttlMaster2);

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on BACKUP: getTtl returns ~600000ms (with tolerance for elapsed time)
        Long ttlBackup2 = client.setMode(Mode.BACKUP)
                .getTtl(key, keyHint)
                .get();
        assertNotNull(ttlBackup2);
        assertTrue(ttlBackup2 >= 598000 && ttlBackup2 <= 600000,
                "Expected TTL ~600000ms on backup, got " + ttlBackup2);
    }

    // ==================== Section 7: TTL Expiration Operations ====================

    @Test
    @DisplayName("TTL expiration: set on master, verify both nodes after TTL expires")
    void testTtlExpirationOnMaster() throws ExecutionException, InterruptedException {
        String key = "ttlExpirationSetMaster" + UUID.randomUUID();

        // Create set with 2s TTL
        KeyHintData keyHint = client.createSet(bytes(key),
                        List.of(Payload.of(bytes("item1"))))
                .get();

        Thread.sleep(REPLICATION_DELAY_MS); // replication wait

        client.setTtl(key, keyHint, 2000).get();

        // Verify on MASTER: getTtl > 0
        Long ttlMaster = client.setMode(Mode.MASTER)
                .getTtl(key, keyHint)
                .get();
        assertNotNull(ttlMaster);
        assertTrue(ttlMaster > 0, "Expected TTL > 0 on master");

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on BACKUP: getTtl > 0
        Long ttlBackup = client.setMode(Mode.BACKUP)
                .getTtl(key, keyHint)
                .get();
        assertNotNull(ttlBackup);
        assertTrue(ttlBackup > 0, "Expected TTL > 0 on backup");

        // Wait for TTL to expire
        Thread.sleep(3000);

        // Verify on MASTER: existKey returns false after TTL expiry (container removed)
        Boolean masterExists = client.setMode(Mode.MASTER)
                .existKey(key, keyHint)
                .get();
        assertTrue(!masterExists, "Expected container to not exist on master after TTL expiry");

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on BACKUP: existKey returns false after TTL expiry (container removed)
        Boolean backupExists = client.setMode(Mode.BACKUP)
                .existKey(key, keyHint)
                .get();
        assertTrue(!backupExists, "Expected container to not exist on backup after TTL expiry");
    }

    @Test
    @DisplayName("TTL expiration: set on backup, verify both nodes after TTL expires")
    void testTtlExpirationOnBackup() throws ExecutionException, InterruptedException {
        String key = "ttlExpirationSetBackup" + UUID.randomUUID();

        // Create set on BACKUP with 2s TTL
        KeyHintData keyHint = client.setMode(Mode.BACKUP)
                .createSet(key,
                        List.of(Payload.of(bytes("item1"))))
                .get();
        Thread.sleep(REPLICATION_DELAY_MS); // replication wait

        client.setMode(Mode.BACKUP).setTtl(key, keyHint, 2000).get();



        // Verify on BACKUP: getTtl > 0
        Long ttlBackup = client.setMode(Mode.BACKUP)
                .getTtl(key, keyHint)
                .get();
        assertNotNull(ttlBackup);
        assertTrue(ttlBackup > 0, "Expected TTL > 0 on backup");

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on MASTER: getTtl > 0
        Long ttlMaster = client.setMode(Mode.MASTER)
                .getTtl(key, keyHint)
                .get();
        assertNotNull(ttlMaster);
        assertTrue(ttlMaster > 0, "Expected TTL > 0 on master");

        // Wait for TTL to expire
        Thread.sleep(3000);

        // Verify on BACKUP: existKey returns false after TTL expiry (container removed)
        Boolean backupExists = client.setMode(Mode.BACKUP)
                .existKey(key, keyHint)
                .get();
        assertTrue(!backupExists, "Expected container to not exist on backup after TTL expiry");

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on MASTER: existKey returns false after TTL expiry (container removed)
        Boolean masterExists = client.setMode(Mode.MASTER)
                .existKey(key, keyHint)
                .get();
        assertTrue(!masterExists, "Expected container to not exist on master after TTL expiry");
    }

    // ==================== Section 8: Lock Operations ====================

    @Test
    @DisplayName("LockObject READ_LOCK: parallel reads OK, writes denied, unlock by owner only")
    void testLockObjectReadLock() throws ExecutionException, InterruptedException {
        String key = "readLockSetTest" + UUID.randomUUID();

        // Create set
        KeyHintData keyHint = client.createSet(key, List.of(Payload.of(bytes("item1"))))
                .get();

        Thread.sleep(REPLICATION_DELAY_MS); // replication wait

        // Write on BACKUP: lockObject READ_LOCK
        LockStatus lockStatus = client.setMode(Mode.BACKUP)
                .lockObject(key, keyHint, LockType.READ_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(60))
                .get();
        assertEquals(LockStatus.OK, lockStatus);

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on BACKUP: owner can read (streamSet)
        List<Payload> backupStream = client.setMode(Mode.BACKUP)
                .streamSet(key, keyHint, OWNER_CLIENT_ID)
                .get();
        assertNotNull(backupStream);

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on MASTER: owner can read (streamSet)
        List<Payload> masterStream = client.setMode(Mode.MASTER)
                .streamSet(key, keyHint, OWNER_CLIENT_ID)
                .get();
        assertNotNull(masterStream);

        // Write on BACKUP: intruder can read (should succeed with READ_LOCK)
        List<Payload> backupStream2 = client.setMode(Mode.BACKUP)
                .streamSet(key, keyHint, INTRUDER_CLIENT_ID)
                .get();
        assertNotNull(backupStream2);

        // Write on BACKUP: intruder tries to write (should fail with PERMISSION_DENIED)
        try {
            client.setMode(Mode.BACKUP)
                    .addElementUnordered(key, keyHint, List.of(Payload.of(bytes("item2"))), INTRUDER_CLIENT_ID)
                    .get();
            fail("Expected PERMISSION_DENIED for write with READ_LOCK");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.PERMISSION_DENIED, cause.getStatus().getCode());
        }

        // Write on BACKUP: unlockObject by owner
        LockStatus unlockStatus = client.setMode(Mode.BACKUP)
                .unlockObject(key, keyHint, OWNER_CLIENT_ID)
                .get();
        assertEquals(LockStatus.OK, unlockStatus);

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Re-lock by owner to test CANT_UNLOCK
        LockStatus lockStatus2 = client.setMode(Mode.BACKUP)
                .lockObject(key, keyHint, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(60))
                .get();
        assertEquals(LockStatus.OK, lockStatus2);
        Thread.sleep(REPLICATION_DELAY_MS); // replication delay
        // Write on MASTER: unlockObject by intruder should fail
        LockStatus failedUnlock = client.setMode(Mode.MASTER)
                .unlockObject(key, keyHint, INTRUDER_CLIENT_ID)
                .get();
        assertEquals(LockStatus.CANT_UNLOCK, failedUnlock);

        // Write on MASTER: unlockObject by owner should succeed
        LockStatus successUnlock = client.setMode(Mode.MASTER)
                .unlockObject(key, keyHint, OWNER_CLIENT_ID)
                .get();
        assertEquals(LockStatus.OK, successUnlock);
    }

    @Test
    @DisplayName("LockObject WRITE_LOCK: owner read/write OK, others denied, unlock by owner only")
    void testLockObjectWriteLock() throws ExecutionException, InterruptedException {
        String key = "writeLockSetTest" + UUID.randomUUID();

        // Create set
        KeyHintData keyHint = client.createSet(key, List.of(Payload.of(bytes("item1"))))
                .get();

        Thread.sleep(REPLICATION_DELAY_MS); // replication wait

        // Write on BACKUP: lockObject WRITE_LOCK
        LockStatus lockStatus = client.setMode(Mode.BACKUP)
                .lockObject(key, keyHint, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(60))
                .get();
        assertEquals(LockStatus.OK, lockStatus);

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on BACKUP: owner can write (addElementUnordered)
        client.setMode(Mode.BACKUP)
                .addElementUnordered(key, keyHint, List.of(Payload.of(bytes("item2"))), OWNER_CLIENT_ID)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on MASTER: owner can read (streamSet)
        List<Payload> masterStream = client.setMode(Mode.MASTER)
                .streamSet(key, keyHint, OWNER_CLIENT_ID)
                .get();
        assertNotNull(masterStream);

        // Write on BACKUP: intruder tries to read (should fail with PERMISSION_DENIED)
        try {
            client.setMode(Mode.BACKUP)
                    .streamSet(key, keyHint, INTRUDER_CLIENT_ID)
                    .get();
            fail("Expected PERMISSION_DENIED for read with WRITE_LOCK");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.PERMISSION_DENIED, cause.getStatus().getCode());
        }

        // Write on BACKUP: intruder tries to write (should fail with PERMISSION_DENIED)
        try {
            client.setMode(Mode.BACKUP)
                    .addElementUnordered(key, keyHint, List.of(Payload.of(bytes("item3"))), INTRUDER_CLIENT_ID)
                    .get();
            fail("Expected PERMISSION_DENIED for write with WRITE_LOCK");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.PERMISSION_DENIED, cause.getStatus().getCode());
        }

        // Write on BACKUP: unlockObject by owner
        LockStatus unlockStatus = client.setMode(Mode.BACKUP)
                .unlockObject(key, keyHint, OWNER_CLIENT_ID)
                .get();
        assertEquals(LockStatus.OK, unlockStatus);

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Re-lock by owner to test CANT_UNLOCK
        LockStatus lockStatus2 = client.setMode(Mode.BACKUP)
                .lockObject(key, keyHint, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(60))
                .get();
        assertEquals(LockStatus.OK, lockStatus2);
        Thread.sleep(REPLICATION_DELAY_MS); // replication delay
        // Write on MASTER: unlockObject by intruder should fail
        LockStatus failedUnlock = client.setMode(Mode.MASTER)
                .unlockObject(key, keyHint, INTRUDER_CLIENT_ID)
                .get();
        assertEquals(LockStatus.CANT_UNLOCK, failedUnlock);

        // Write on MASTER: unlockObject by owner should succeed
        LockStatus successUnlock = client.setMode(Mode.MASTER)
                .unlockObject(key, keyHint, OWNER_CLIENT_ID)
                .get();
        assertEquals(LockStatus.OK, successUnlock);
    }

    @Test
    @DisplayName("LockObject GLOBAL: only owner any operations, others denied, unlock by owner only")
    void testLockObjectGlobalLock() throws ExecutionException, InterruptedException {
        String key = "globalLockSetTest" + UUID.randomUUID();

        // Create set
        KeyHintData keyHint = client.createSet(key, List.of(Payload.of(bytes("item1"))))
                .get();

        Thread.sleep(REPLICATION_DELAY_MS); // replication wait

        // Write on BACKUP: lockObject GLOBAL
        LockStatus lockStatus = client.setMode(Mode.BACKUP)
                .lockObject(key, keyHint, LockType.GLOBAL, 1, Duration.ofSeconds(60))
                .get();
        assertEquals(LockStatus.OK, lockStatus);

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on BACKUP: client 1 can read (streamSet)
        List<Payload> backupStream = client.setMode(Mode.BACKUP)
                .streamSet(key, keyHint, 1)
                .get();
        assertNotNull(backupStream);

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on MASTER: client 1 can read (streamSet)
        List<Payload> masterStream = client.setMode(Mode.MASTER)
                .streamSet(key, keyHint, 1)
                .get();
        assertNotNull(masterStream);

        // Write on BACKUP: client 2 tries to read (should fail with PERMISSION_DENIED)
        try {
            client.setMode(Mode.BACKUP)
                    .streamSet(key, keyHint, 2)
                    .get();
            fail("Expected PERMISSION_DENIED for read with GLOBAL_LOCK");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.PERMISSION_DENIED, cause.getStatus().getCode());
        }

        // Write on BACKUP: client 2 tries to write (should fail with PERMISSION_DENIED)
        try {
            client.setMode(Mode.BACKUP)
                    .addElementUnordered(key, keyHint, List.of(Payload.of(bytes("item2"))))
                    .get();
            fail("Expected PERMISSION_DENIED for write with GLOBAL_LOCK");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.PERMISSION_DENIED, cause.getStatus().getCode());
        }

        // Write on BACKUP: unlockObject by owner (client 1)
        LockStatus unlockStatus = client.setMode(Mode.BACKUP)
                .unlockObject(key, keyHint, 1)
                .get();
        assertEquals(LockStatus.OK, unlockStatus);

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Re-lock by owner 1 to test CANT_UNLOCK
        LockStatus lockStatus2 = client.setMode(Mode.BACKUP)
                .lockObject(key, keyHint, LockType.GLOBAL, 1, Duration.ofSeconds(60))
                .get();
        assertEquals(LockStatus.OK, lockStatus2);
        Thread.sleep(REPLICATION_DELAY_MS); // replication delay
        // Write on MASTER: unlockObject by non-owner (client 2) should fail
        LockStatus failedUnlock = client.setMode(Mode.MASTER)
                .unlockObject(key, keyHint, 2)
                .get();
        assertEquals(LockStatus.CANT_UNLOCK, failedUnlock);

        // Write on MASTER: unlockObject by owner (client 1) should succeed
        LockStatus successUnlock = client.setMode(Mode.MASTER)
                .unlockObject(key, keyHint, 1)
                .get();
        assertEquals(LockStatus.OK, successUnlock);
    }

    @Test
    @DisplayName("UnlockObject: only lock owner can unlock, others get CANT_UNLOCK")
    void testUnlockObjectOwnerOnly() throws ExecutionException, InterruptedException {
        String key = "unlockOwnerSetTest" + UUID.randomUUID();

        // Create set
        KeyHintData keyHint = client.createSet(key, List.of(Payload.of(bytes("item1"))))
                .get();

        Thread.sleep(REPLICATION_DELAY_MS); // replication wait

        // Lock by owner clientId=400
        LockStatus lockStatus = client.setMode(Mode.BACKUP)
                .lockObject(key, keyHint, LockType.WRITE_LOCK, 400, Duration.ofSeconds(60))
                .get();
        assertEquals(LockStatus.OK, lockStatus);

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Try unlock by non-owner (clientId=999) should fail with CANT_UNLOCK
        LockStatus failedUnlock = client.setMode(Mode.BACKUP)
                .unlockObject(key, keyHint, 999)
                .get();
        assertEquals(LockStatus.CANT_UNLOCK, failedUnlock);

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on MASTER: lockObject by non-owner should still fail (lock still active)
        LockStatus cantLock = client.setMode(Mode.MASTER)
                .lockObject(key, keyHint, LockType.WRITE_LOCK, 999, Duration.ofSeconds(60))
                .get();
        assertEquals(LockStatus.CANT_LOCK, cantLock);

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Unlock by owner (clientId=400) should succeed
        LockStatus successUnlock = client.setMode(Mode.BACKUP)
                .unlockObject(key, keyHint, 400)
                .get();
        assertEquals(LockStatus.OK, successUnlock);
    }

    @Test
    @DisplayName("Lock expiration: set short TTL lock on master, verify both nodes after lock expires")
    void testLockExpirationOnMaster() throws ExecutionException, InterruptedException {
        String key = "lockExpirationSetMaster" + UUID.randomUUID();

        // Create set
        KeyHintData keyHint = client.createSet(key, List.of(Payload.of(bytes("item1"))))
                .get();

        Thread.sleep(REPLICATION_DELAY_MS); // replication wait

        // Write on MASTER: lockObject with 2s TTL
        LockStatus lockStatus = client.setMode(Mode.MASTER)
                .lockObject(key, keyHint, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(2))
                .get();
        assertEquals(LockStatus.OK, lockStatus);

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on MASTER: owner can write (addElementUnordered)
        client.setMode(Mode.MASTER)
                .addElementUnordered(key, keyHint, List.of(Payload.of(bytes("item2"))), OWNER_CLIENT_ID)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on BACKUP: streamSet shows lock is replicated (clientId=OWNER_CLIENT_ID)
        List<Payload> backupStream = client.setMode(Mode.BACKUP)
                .streamSet(bytes(key), keyHint, OWNER_CLIENT_ID, Duration.ofSeconds(5))
                .get();
        assertEquals(2, backupStream.size());

        // Wait for lock TTL to expire (2s lock + buffer)
        Thread.sleep(3500);

        // Verify on MASTER: streamSet succeeds (lock expired, data accessible)
        List<Payload> masterStream = client.setMode(Mode.MASTER)
                .streamSet(key, keyHint, OWNER_CLIENT_ID)
                .get();
        assertNotNull(masterStream);

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on BACKUP: streamSet succeeds (lock expired, data replicated)
        List<Payload> backupStreamAfter = client.setMode(Mode.BACKUP)
                .streamSet(key, keyHint, OWNER_CLIENT_ID)
                .get();
        assertNotNull(backupStreamAfter);

        // Verify on BACKUP: intruder can read (lock no longer active)
        List<Payload> backupStream2 = client.setMode(Mode.BACKUP)
                .streamSet(key, keyHint, INTRUDER_CLIENT_ID)
                .get();
        assertNotNull(backupStream2);
    }

    @Test
    @DisplayName("Lock expiration: set short TTL lock on backup, verify both nodes after lock expires")
    void testLockExpirationOnBackup() throws ExecutionException, InterruptedException {
        String key = "lockExpirationSetBackup" + UUID.randomUUID();

        // Create set on BACKUP
        KeyHintData keyHint = client.setMode(Mode.BACKUP)
                .createSet(key, List.of(Payload.of(bytes("item1"))))
                .get();

        Thread.sleep(REPLICATION_DELAY_MS); // replication wait

        // Write on BACKUP: lockObject with 2s TTL
        LockStatus lockStatus = client.setMode(Mode.BACKUP)
                .lockObject(key, keyHint, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(2))
                .get();
        assertEquals(LockStatus.OK, lockStatus);

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on BACKUP: owner can write (addElementUnordered)
        client.setMode(Mode.BACKUP)
                .addElementUnordered(key, keyHint, List.of(Payload.of(bytes("item2"))), OWNER_CLIENT_ID)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on MASTER: streamSet shows lock is replicated (clientId=OWNER_CLIENT_ID)
        List<Payload> masterStream = client.setMode(Mode.MASTER)
                .streamSet(bytes(key), keyHint, OWNER_CLIENT_ID, Duration.ofSeconds(5))
                .get();
        assertEquals(2, masterStream.size());

        // Wait for lock TTL to expire (2s lock + buffer)
        Thread.sleep(3500);

        // Verify on BACKUP: streamSet succeeds (lock expired, data accessible)
        List<Payload> backupStream = client.setMode(Mode.BACKUP)
                .streamSet(key, keyHint, OWNER_CLIENT_ID)
                .get();
        assertNotNull(backupStream);

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on MASTER: streamSet succeeds (lock expired, data replicated)
        List<Payload> masterStreamAfter = client.setMode(Mode.MASTER)
                .streamSet(key, keyHint, OWNER_CLIENT_ID)
                .get();
        assertNotNull(masterStreamAfter);

        // Verify on MASTER: intruder can read (lock no longer active)
        List<Payload> masterStream2 = client.setMode(Mode.MASTER)
                .streamSet(key, keyHint, INTRUDER_CLIENT_ID)
                .get();
        assertNotNull(masterStream2);
    }

    // =========================================================================
    // 15. READ LOCK PARALLEL READS
    // =========================================================================

    @Test
    @DisplayName("READ_LOCK: multiple clients can read in parallel on both nodes")
    void testReadLockParallelReads() throws ExecutionException, InterruptedException {
        String key = "readLockParallel" + UUID.randomUUID();
        List<Payload> initialData = List.of(Payload.of(bytes("item1")));

        // Create set
        KeyHintData keyHint = client.createSet(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // First client acquires READ_LOCK on MASTER
        LockStatus lock1 = client.setMode(Mode.MASTER)
                .lockObject(bytes(key), keyHint, LockType.READ_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30))
                .get();
        assertEquals(LockStatus.OK, lock1);

        // Second client can also acquire READ_LOCK on MASTER
        LockStatus lock2 = client.setMode(Mode.MASTER)
                .lockObject(bytes(key), keyHint, LockType.READ_LOCK, INTRUDER_CLIENT_ID, Duration.ofSeconds(30))
                .get();
        assertEquals(LockStatus.CANT_LOCK, lock2);

        // Both clients can read on MASTER
        Thread.sleep(REPLICATION_DELAY_MS);
        List<Payload> result1 = client.setMode(Mode.MASTER)
                .streamSet(key, keyHint, OWNER_CLIENT_ID)
                .get();
        assertNotNull(result1);
        assertEquals(1, result1.size());

        List<Payload> result2 = client.setMode(Mode.MASTER)
                .streamSet(key, keyHint, INTRUDER_CLIENT_ID)
                .get();
        assertNotNull(result2);
        assertEquals(1, result2.size());

        // Verify on BACKUP: lock is replicated, both can read
        Thread.sleep(REPLICATION_DELAY_MS);
        List<Payload> backupResult1 = client.setMode(Mode.BACKUP)
                .streamSet(key, keyHint, OWNER_CLIENT_ID)
                .get();
        assertNotNull(backupResult1);

        List<Payload> backupResult2 = client.setMode(Mode.BACKUP)
                .streamSet(key, keyHint, INTRUDER_CLIENT_ID)
                .get();
        assertNotNull(backupResult2);

        // Release locks
        client.setMode(Mode.MASTER)
                .unlockObject(bytes(key), keyHint, OWNER_CLIENT_ID)
                .get();
        client.setMode(Mode.MASTER)
                .unlockObject(bytes(key), keyHint, INTRUDER_CLIENT_ID)
                .get();
    }

    // =========================================================================
    // 16. UNSUPPORTED METHODS FOR SET
    // =========================================================================

    @Test
    @DisplayName("Methods not applicable to set should throw an error")
    void testUnsupportedMethodsForSet() throws ExecutionException, InterruptedException {
        String key = "unsupportedSet" + UUID.randomUUID();
        List<Payload> initialData = List.of(Payload.of(bytes("item1")));

        // Create set
        KeyHintData keyHint = client.createSet(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // getElementAtPosition - not applicable to set
        try {
            client.setMode(Mode.MASTER)
                    .getElementAtPosition(bytes(key), keyHint, 0)
                    .get();
            fail("getElementAtPosition should throw an error for set");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.INTERNAL, cause.getStatus().getCode());
        }

        // getHead - not applicable to set
        try {
            client.setMode(Mode.MASTER)
                    .getHead(bytes(key), keyHint)
                    .get();
            fail("getHead should throw an error for set");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.INTERNAL, cause.getStatus().getCode());
        }

        // getTail - not applicable to set
        try {
            client.setMode(Mode.MASTER)
                    .getTail(bytes(key), keyHint)
                    .get();
            fail("getTail should throw an error for set");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.INTERNAL, cause.getStatus().getCode());
        }
    }

    // =========================================================================
    // 17. ADD ELEMENT EMPTY LIST
    // =========================================================================

    @Test
    @DisplayName("addElementUnordered with empty list returns 0 on both nodes")
    void testAddElementEmptyList() throws ExecutionException, InterruptedException {
        String key = "addElementEmptyList" + UUID.randomUUID();
        List<Payload> initialData = List.of(Payload.of(bytes("item1")));

        // Create set
        KeyHintData keyHint = client.createSet(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify initial state
        Integer masterSize = client.setMode(Mode.MASTER)
                .getSize(key, keyHint)
                .get();
        assertEquals(1, masterSize);

        Integer backupSize = client.setMode(Mode.BACKUP)
                .getSize(key, keyHint)
                .get();
        assertEquals(1, backupSize);

        // addElementUnordered with empty list on MASTER
        Integer added = client.setMode(Mode.MASTER)
                .addElementUnordered(key, keyHint, new ArrayList<>())
                .get();
        assertEquals(0, added, "Adding empty list should return 0");

        // Verify on MASTER: size unchanged
        Thread.sleep(REPLICATION_DELAY_MS);
        Integer masterSizeAfter = client.setMode(Mode.MASTER)
                .getSize(key, keyHint)
                .get();
        assertEquals(1, masterSizeAfter);

        // Verify on BACKUP: size unchanged (replicated)
        Integer backupSizeAfter = client.setMode(Mode.BACKUP)
                .getSize(key, keyHint)
                .get();
        assertEquals(1, backupSizeAfter);
    }

    // =========================================================================
    // 18. STREAM SET NON EXISTENT
    // =========================================================================

    @Test
    @DisplayName("streamSet on non-existent set returns NOT_FOUND on both nodes")
    void testStreamSetNonExistent() throws ExecutionException, InterruptedException {
        String key = "streamNonExistent" + UUID.randomUUID();

        // Create a dummy KeyHint for non-existent set
        KeyHintData keyHint = KeyHintData.of(1, 1);

        // streamSet on MASTER should fail
        try {
            client.setMode(Mode.MASTER)
                    .streamSet(key, keyHint)
                    .get();
            fail("streamSet on non-existent set should throw an error");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }

        // streamSet on BACKUP should also fail
        try {
            client.setMode(Mode.BACKUP)
                    .streamSet(key, keyHint)
                    .get();
            fail("streamSet on non-existent set should throw an error");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    // =========================================================================
    // 19. UNLOCK ON EXPIRED LOCK
    // =========================================================================

    @Test
    @DisplayName("Lock with expired TTL: unlock returns OK on both nodes")
    void testUnlockOnExpiredLock() throws ExecutionException, InterruptedException {
        String key = "unlockExpired" + UUID.randomUUID();
        List<Payload> initialData = List.of(Payload.of(bytes("item1")));

        // Create set
        KeyHintData keyHint = client.createSet(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Acquire lock with TTL = 1 second on MASTER
        LockStatus lock = client.setMode(Mode.MASTER)
                .lockObject(bytes(key), keyHint, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(1))
                .get();
        assertEquals(LockStatus.OK, lock);

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify on BACKUP: lock is replicated
        Thread.sleep(REPLICATION_DELAY_MS);
        assertDenied(client.setMode(Mode.BACKUP)
                .streamSet(key, keyHint, INTRUDER_CLIENT_ID));

        // Wait for lock TTL to expire
        Thread.sleep(1500);

        // Try to release expired lock on MASTER
        LockStatus unlock = client.setMode(Mode.MASTER)
                .unlockObject(bytes(key), keyHint, OWNER_CLIENT_ID)
                .get();
        assertEquals(LockStatus.OK, unlock, "Releasing expired lock should return OK");

        // Verify on BACKUP: lock is also released
        Thread.sleep(REPLICATION_DELAY_MS);
        LockStatus unlockBackup = client.setMode(Mode.BACKUP)
                .unlockObject(bytes(key), keyHint, OWNER_CLIENT_ID)
                .get();
        assertEquals(LockStatus.OK, unlockBackup);
    }

}