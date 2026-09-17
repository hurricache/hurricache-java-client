package com.hurricache.client.cluster.smart;

import com.hurricache.TestBaseCluster;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Mode;
import com.hurricache.client.intf.Payload;
import com.hurricache.grpc.LockStatus;
import com.hurricache.grpc.LockType;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class QueueOperationsTest extends TestBaseCluster {

    private static final int OWNER_CLIENT_ID = 100;
    private static final int INTRUDER_CLIENT_ID = 200;
    private static final long REPLICATION_DELAY_MS = 100;

    // =========================================================================
    // 1. CREATE QUEUE & BASIC OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("Create empty queue, verify replication to backup")
    void testCreateEmptyQueue() throws ExecutionException, InterruptedException {
        String key = "emptyQueue" + UUID.randomUUID();

        // Create empty queue
        KeyHintData keyHint = client.createQueue(key, List.of())
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify on MASTER: getHead returns empty
        Payload masterHead = client.setMode(Mode.MASTER)
                .getHead(bytes(key), keyHint)
                .get();
        assertNotNull(masterHead);
        assertEquals(0, masterHead.getValue().length, "Expected empty payload on master");

        // Verify on BACKUP: getHead returns empty
        Payload backupHead = client.setMode(Mode.BACKUP)
                .getHead(bytes(key), keyHint)
                .get();
        assertNotNull(backupHead);
        assertEquals(0, backupHead.getValue().length, "Expected empty payload on backup");
    }

    @Test
    @DisplayName("Create queue with initial data, verify replication to backup")
    void testCreateQueueWithInitialData() throws ExecutionException, InterruptedException {
        String key = "queueWithData" + UUID.randomUUID();
        List<Payload> initialData = List.of(
                Payload.of(bytes("k1")),
                Payload.of(bytes("k2")),
                Payload.of(bytes("k3"))
        );

        // Create queue
        KeyHintData keyHint = client.createQueue(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify on MASTER: getAndRemoveFront returns k1
        Payload masterFront = client.setMode(Mode.MASTER)
                .getAndRemoveFront(bytes(key), keyHint)
                .get();
        assertNotNull(masterFront);
        assertEquals("k1", new String(masterFront.getValue(), StandardCharsets.UTF_8));

        // Verify on BACKUP: getHead returns k2 (k1 was removed by replication)
        Thread.sleep(REPLICATION_DELAY_MS);
        Payload backupNext = client.setMode(Mode.BACKUP)
                .getHead(bytes(key), keyHint)
                .get();
        assertNotNull(backupNext);
        assertEquals("k2", new String(backupNext.getValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("Create large queue with chunking, verify 1500 elements on both nodes")
    void testCreateLargeQueueWithChunking() throws ExecutionException, InterruptedException {
        String key = "largeQueue" + UUID.randomUUID();
        int elementCount = 1500;

        List<Payload> payloads = new java.util.ArrayList<>();
        for (int i = 0; i < elementCount; i++) {
            payloads.add(Payload.of(bytes("elem_" + i)));
        }

        // Create large queue
        KeyHintData keyHint = client.createQueue(key, payloads)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify on MASTER: existKey returns true
        Boolean masterExists = client.setMode(Mode.MASTER)
                .existKey(bytes(key))
                .get();
        assertTrue(masterExists);

        // Verify on BACKUP: existKey returns true
        Boolean backupExists = client.setMode(Mode.BACKUP)
                .existKey(bytes(key))
                .get();
        assertTrue(backupExists);

        // Verify first and last elements on MASTER
        Payload masterFirst = client.setMode(Mode.MASTER)
                .getHead(bytes(key), keyHint)
                .get();
        assertEquals("elem_0", new String(masterFirst.getValue(), StandardCharsets.UTF_8));

        Payload masterLast = client.setMode(Mode.MASTER)
                .getTail(bytes(key), keyHint)
                .get();
        assertEquals("elem_" + (elementCount - 1), new String(masterLast.getValue(), StandardCharsets.UTF_8));

        // Verify first and last elements on BACKUP
        Payload backupFirst = client.setMode(Mode.BACKUP)
                .getHead(bytes(key), keyHint)
                .get();
        assertEquals("elem_0", new String(backupFirst.getValue(), StandardCharsets.UTF_8));

        Payload backupLast = client.setMode(Mode.BACKUP)
                .getTail(bytes(key), keyHint)
                .get();
        assertEquals("elem_" + (elementCount - 1), new String(backupLast.getValue(), StandardCharsets.UTF_8));
    }

    // =========================================================================
    // 2. EMPTY QUEUE EDGE CASES
    // =========================================================================

    @Test
    @DisplayName("Get head on empty queue, verify replication to backup")
    void testGetHeadOnEmptyQueue() throws ExecutionException, InterruptedException {
        String key = "emptyQueueHead" + UUID.randomUUID();

        // Create empty queue
        KeyHintData keyHint = client.createQueue(key, List.of())
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify on MASTER: getHead returns empty
        Payload masterHead = client.setMode(Mode.MASTER)
                .getHead(bytes(key), keyHint)
                .get();
        assertNotNull(masterHead);
        assertEquals(0, masterHead.getValue().length);

        // Verify on BACKUP: getHead returns empty
        Payload backupHead = client.setMode(Mode.BACKUP)
                .getHead(bytes(key), keyHint)
                .get();
        assertNotNull(backupHead);
        assertEquals(0, backupHead.getValue().length);
    }

    @Test
    @DisplayName("Get and remove front on empty queue, verify replication to backup")
    void testGetAndRemoveFrontOnEmptyQueue() throws ExecutionException, InterruptedException {
        String key = "emptyQueueFront" + UUID.randomUUID();

        // Create empty queue
        KeyHintData keyHint = client.createQueue(key, List.of())
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify on MASTER: getAndRemoveFront returns empty
        Payload masterFront = client.setMode(Mode.MASTER)
                .getAndRemoveFront(bytes(key), keyHint)
                .get();
        assertNotNull(masterFront);
        assertEquals(0, masterFront.getValue().length);

        // Verify on BACKUP: getAndRemoveFront returns empty
        Payload backupFront = client.setMode(Mode.BACKUP)
                .getAndRemoveFront(bytes(key), keyHint)
                .get();
        assertNotNull(backupFront);
        assertEquals(0, backupFront.getValue().length);
    }

    // =========================================================================
    // 3. ADD ELEMENT TO TAIL (write!)
    // =========================================================================

    @Test
    @DisplayName("addElementToTail on master, verify replication to backup")
    void testAddElementToTailOnMaster() throws ExecutionException, InterruptedException {
        String key = "addTailMaster" + UUID.randomUUID();
        List<Payload> initialData = List.of(Payload.of(bytes("k1")));

        // Create queue
        KeyHintData keyHint = client.createQueue(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify initial state on both nodes
        Payload masterInitial = client.setMode(Mode.MASTER)
                .getTail(bytes(key), keyHint)
                .get();
        assertEquals("k1", new String(masterInitial.getValue(), StandardCharsets.UTF_8));

        Payload backupInitial = client.setMode(Mode.BACKUP)
                .getTail(bytes(key), keyHint)
                .get();
        assertEquals("k1", new String(backupInitial.getValue(), StandardCharsets.UTF_8));

        // addElementToTail on MASTER
        List<Payload> newElements = List.of(
                Payload.of(bytes("k2")),
                Payload.of(bytes("k3"))
        );
        client.setMode(Mode.MASTER)
                .addElementToTail(bytes(key), keyHint, newElements)
                .get();

        // Verify on MASTER: getTail returns k3
        Thread.sleep(REPLICATION_DELAY_MS);
        Payload masterTail = client.setMode(Mode.MASTER)
                .getTail(bytes(key), keyHint)
                .get();
        assertEquals("k3", new String(masterTail.getValue(), StandardCharsets.UTF_8));

        // Verify on BACKUP: getTail returns k3 (replicated)
        Payload backupTail = client.setMode(Mode.BACKUP)
                .getTail(bytes(key), keyHint)
                .get();
        assertEquals("k3", new String(backupTail.getValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("addElementToTail on backup, verify replication to master")
    void testAddElementToTailOnBackup() throws ExecutionException, InterruptedException {
        String key = "addTailBackup" + UUID.randomUUID();
        List<Payload> initialData = List.of(Payload.of(bytes("k1")));

        // Create queue
        KeyHintData keyHint = client.createQueue(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify initial state on both nodes
        Payload masterInitial = client.setMode(Mode.MASTER)
                .getTail(bytes(key), keyHint)
                .get();
        assertEquals("k1", new String(masterInitial.getValue(), StandardCharsets.UTF_8));

        Payload backupInitial = client.setMode(Mode.BACKUP)
                .getTail(bytes(key), keyHint)
                .get();
        assertEquals("k1", new String(backupInitial.getValue(), StandardCharsets.UTF_8));

        // addElementToTail on BACKUP
        List<Payload> newElements = List.of(
                Payload.of(bytes("k2")),
                Payload.of(bytes("k3"))
        );
        client.setMode(Mode.BACKUP)
                .addElementToTail(bytes(key), keyHint, newElements)
                .get();

        // Verify on BACKUP: getTail returns k3
        Thread.sleep(REPLICATION_DELAY_MS);
        Payload backupTail = client.setMode(Mode.BACKUP)
                .getTail(bytes(key), keyHint)
                .get();
        assertEquals("k3", new String(backupTail.getValue(), StandardCharsets.UTF_8));

        // Verify on MASTER: getTail returns k3 (replicated)
        Payload masterTail = client.setMode(Mode.MASTER)
                .getTail(bytes(key), keyHint)
                .get();
        assertEquals("k3", new String(masterTail.getValue(), StandardCharsets.UTF_8));
    }

    // =========================================================================
    // 4. GET AND REMOVE FRONT (write!)
    // =========================================================================

    @Test
    @DisplayName("getAndRemoveFront on master, verify replication to backup")
    void testGetAndRemoveFrontOnMaster() throws ExecutionException, InterruptedException {
        String key = "removeFrontMaster" + UUID.randomUUID();
        List<Payload> initialData = List.of(
                Payload.of(bytes("k1")),
                Payload.of(bytes("k2")),
                Payload.of(bytes("k3"))
        );

        // Create queue
        KeyHintData keyHint = client.createQueue(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify initial state on both nodes
        Payload masterFront = client.setMode(Mode.MASTER)
                .getHead(bytes(key), keyHint)
                .get();
        assertEquals("k1", new String(masterFront.getValue(), StandardCharsets.UTF_8));

        Payload backupFront = client.setMode(Mode.BACKUP)
                .getHead(bytes(key), keyHint)
                .get();
        assertEquals("k1", new String(backupFront.getValue(), StandardCharsets.UTF_8));

        // getAndRemoveFront on MASTER
        Payload removed = client.setMode(Mode.MASTER)
                .getAndRemoveFront(bytes(key), keyHint)
                .get();
        assertNotNull(removed);
        assertEquals("k1", new String(removed.getValue(), StandardCharsets.UTF_8));

        // Verify on MASTER: getHead returns k2
        Thread.sleep(REPLICATION_DELAY_MS);
        Payload masterNext = client.setMode(Mode.MASTER)
                .getHead(bytes(key), keyHint)
                .get();
        assertEquals("k2", new String(masterNext.getValue(), StandardCharsets.UTF_8));

        // Verify on BACKUP: getHead returns k2 (replicated)
        Payload backupNext = client.setMode(Mode.BACKUP)
                .getHead(bytes(key), keyHint)
                .get();
        assertEquals("k2", new String(backupNext.getValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("getAndRemoveFront on backup, verify replication to master")
    void testGetAndRemoveFrontOnBackup() throws ExecutionException, InterruptedException {
        String key = "removeFrontBackup" + UUID.randomUUID();
        List<Payload> initialData = List.of(
                Payload.of(bytes("k1")),
                Payload.of(bytes("k2")),
                Payload.of(bytes("k3"))
        );

        // Create queue
        KeyHintData keyHint = client.createQueue(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify initial state on both nodes
        Payload masterFront = client.setMode(Mode.MASTER)
                .getHead(bytes(key), keyHint)
                .get();
        assertEquals("k1", new String(masterFront.getValue(), StandardCharsets.UTF_8));

        Payload backupFront = client.setMode(Mode.BACKUP)
                .getHead(bytes(key), keyHint)
                .get();
        assertEquals("k1", new String(backupFront.getValue(), StandardCharsets.UTF_8));

        // getAndRemoveFront on BACKUP
        Payload removed = client.setMode(Mode.BACKUP)
                .getAndRemoveFront(bytes(key), keyHint)
                .get();
        assertNotNull(removed);
        assertEquals("k1", new String(removed.getValue(), StandardCharsets.UTF_8));

        // Verify on BACKUP: getHead returns k2
        Thread.sleep(REPLICATION_DELAY_MS);
        Payload backupNext = client.setMode(Mode.BACKUP)
                .getHead(bytes(key), keyHint)
                .get();
        assertEquals("k2", new String(backupNext.getValue(), StandardCharsets.UTF_8));

        // Verify on MASTER: getHead returns k2 (replicated)
        Payload masterNext = client.setMode(Mode.MASTER)
                .getHead(bytes(key), keyHint)
                .get();
        assertEquals("k2", new String(masterNext.getValue(), StandardCharsets.UTF_8));
    }

    // =========================================================================
    // 5. GET AND REMOVE TAIL (write!)
    // =========================================================================

    @Test
    @DisplayName("getAndRemoveTail on master, verify replication to backup")
    void testGetAndRemoveTailOnMaster() throws ExecutionException, InterruptedException {
        String key = "removeTailMaster" + UUID.randomUUID();
        List<Payload> initialData = List.of(
                Payload.of(bytes("k1")),
                Payload.of(bytes("k2")),
                Payload.of(bytes("k3"))
        );

        // Create queue
        KeyHintData keyHint = client.createQueue(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify initial state on both nodes
        Payload masterTail = client.setMode(Mode.MASTER)
                .getTail(bytes(key), keyHint)
                .get();
        assertEquals("k3", new String(masterTail.getValue(), StandardCharsets.UTF_8));

        Payload backupTail = client.setMode(Mode.BACKUP)
                .getTail(bytes(key), keyHint)
                .get();
        assertEquals("k3", new String(backupTail.getValue(), StandardCharsets.UTF_8));

        // getAndRemoveTail on MASTER
        Payload removed = client.setMode(Mode.MASTER)
                .getAndRemoveTail(bytes(key), keyHint)
                .get();
        assertNotNull(removed);
        assertEquals("k3", new String(removed.getValue(), StandardCharsets.UTF_8));

        // Verify on MASTER: getTail returns k2
        Thread.sleep(REPLICATION_DELAY_MS);
        Payload masterNext = client.setMode(Mode.MASTER)
                .getTail(bytes(key), keyHint)
                .get();
        assertEquals("k2", new String(masterNext.getValue(), StandardCharsets.UTF_8));

        // Verify on BACKUP: getTail returns k2 (replicated)
        Thread.sleep(REPLICATION_DELAY_MS);
        Payload backupNext = client.setMode(Mode.BACKUP)
                .getTail(bytes(key), keyHint)
                .get();
        assertEquals("k2", new String(backupNext.getValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("getAndRemoveTail on backup, verify replication to master")
    void testGetAndRemoveTailOnBackup() throws ExecutionException, InterruptedException {
        String key = "removeTailBackup" + UUID.randomUUID();
        List<Payload> initialData = List.of(
                Payload.of(bytes("k1")),
                Payload.of(bytes("k2")),
                Payload.of(bytes("k3"))
        );

        // Create queue
        KeyHintData keyHint = client.createQueue(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify initial state on both nodes
        Payload masterTail = client.setMode(Mode.MASTER)
                .getTail(bytes(key), keyHint)
                .get();
        assertEquals("k3", new String(masterTail.getValue(), StandardCharsets.UTF_8));

        Payload backupTail = client.setMode(Mode.BACKUP)
                .getTail(bytes(key), keyHint)
                .get();
        assertEquals("k3", new String(backupTail.getValue(), StandardCharsets.UTF_8));

        // getAndRemoveTail on BACKUP
        Payload removed = client.setMode(Mode.BACKUP)
                .getAndRemoveTail(bytes(key), keyHint)
                .get();
        assertNotNull(removed);
        assertEquals("k3", new String(removed.getValue(), StandardCharsets.UTF_8));

        // Verify on BACKUP: getTail returns k2
        Thread.sleep(REPLICATION_DELAY_MS);
        Payload backupNext = client.setMode(Mode.BACKUP)
                .getTail(bytes(key), keyHint)
                .get();
        assertEquals("k2", new String(backupNext.getValue(), StandardCharsets.UTF_8));

        // Verify on MASTER: getTail returns k2 (replicated)
        Thread.sleep(REPLICATION_DELAY_MS);
        Payload masterNext = client.setMode(Mode.MASTER)
                .getTail(bytes(key), keyHint)
                .get();
        assertEquals("k2", new String(masterNext.getValue(), StandardCharsets.UTF_8));
    }

    // =========================================================================
    // 6. REMOVE HEAD (write!)
    // =========================================================================

    @Test
    @DisplayName("removeHead on master, verify replication to backup")
    void testRemoveHeadOnMaster() throws ExecutionException, InterruptedException {
        String key = "removeHeadMaster" + UUID.randomUUID();
        List<Payload> initialData = List.of(
                Payload.of(bytes("k1")),
                Payload.of(bytes("k2")),
                Payload.of(bytes("k3"))
        );

        // Create queue
        KeyHintData keyHint = client.createQueue(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify initial state on both nodes
        Payload masterHead = client.setMode(Mode.MASTER)
                .getHead(bytes(key), keyHint)
                .get();
        assertEquals("k1", new String(masterHead.getValue(), StandardCharsets.UTF_8));

        Payload backupHead = client.setMode(Mode.BACKUP)
                .getHead(bytes(key), keyHint)
                .get();
        assertEquals("k1", new String(backupHead.getValue(), StandardCharsets.UTF_8));

        // removeHead on MASTER
        client.setMode(Mode.MASTER)
                .removeHead(bytes(key), keyHint)
                .get();

        // Verify on MASTER: getHead returns k2
        Thread.sleep(REPLICATION_DELAY_MS);
        Payload masterNext = client.setMode(Mode.MASTER)
                .getHead(bytes(key), keyHint)
                .get();
        assertEquals("k2", new String(masterNext.getValue(), StandardCharsets.UTF_8));

        // Verify on BACKUP: getHead returns k2 (replicated)
        Payload backupNext = client.setMode(Mode.BACKUP)
                .getHead(bytes(key), keyHint)
                .get();
        assertEquals("k2", new String(backupNext.getValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("removeHead on backup, verify replication to master")
    void testRemoveHeadOnBackup() throws ExecutionException, InterruptedException {
        String key = "removeHeadBackup" + UUID.randomUUID();
        List<Payload> initialData = List.of(
                Payload.of(bytes("k1")),
                Payload.of(bytes("k2")),
                Payload.of(bytes("k3"))
        );

        // Create queue
        KeyHintData keyHint = client.createQueue(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify initial state on both nodes
        Payload masterHead = client.setMode(Mode.MASTER)
                .getHead(bytes(key), keyHint)
                .get();
        assertEquals("k1", new String(masterHead.getValue(), StandardCharsets.UTF_8));

        Payload backupHead = client.setMode(Mode.BACKUP)
                .getHead(bytes(key), keyHint)
                .get();
        assertEquals("k1", new String(backupHead.getValue(), StandardCharsets.UTF_8));

        // removeHead on BACKUP
        client.setMode(Mode.BACKUP)
                .removeHead(bytes(key), keyHint)
                .get();

        // Verify on BACKUP: getHead returns k2
        Thread.sleep(REPLICATION_DELAY_MS);
        Payload backupNext = client.setMode(Mode.BACKUP)
                .getHead(bytes(key), keyHint)
                .get();
        assertEquals("k2", new String(backupNext.getValue(), StandardCharsets.UTF_8));

        // Verify on MASTER: getHead returns k2 (replicated)
        Payload masterNext = client.setMode(Mode.MASTER)
                .getHead(bytes(key), keyHint)
                .get();
        assertEquals("k2", new String(masterNext.getValue(), StandardCharsets.UTF_8));
    }

    // =========================================================================
    // 7. GET FRONT / GET HEAD (read)
    // =========================================================================

    @Test
    @DisplayName("getHead on master, verify replication to backup")
    void testGetHeadOnMaster() throws ExecutionException, InterruptedException {
        String key = "getHeadMaster" + UUID.randomUUID();
        List<Payload> initialData = List.of(
                Payload.of(bytes("k1")),
                Payload.of(bytes("k2")),
                Payload.of(bytes("k3"))
        );

        // Create queue
        KeyHintData keyHint = client.createQueue(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // getHead on MASTER
        Payload masterHead = client.setMode(Mode.MASTER)
                .getHead(bytes(key), keyHint)
                .get();
        assertNotNull(masterHead);
        assertEquals("k1", new String(masterHead.getValue(), StandardCharsets.UTF_8));

        // Verify on BACKUP: getHead returns k1 (replicated)
        Payload backupHead = client.setMode(Mode.BACKUP)
                .getHead(bytes(key), keyHint)
                .get();
        assertNotNull(backupHead);
        assertEquals("k1", new String(backupHead.getValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("getHead on backup, verify replication to master")
    void testGetHeadOnBackup() throws ExecutionException, InterruptedException {
        String key = "getHeadBackup" + UUID.randomUUID();
        List<Payload> initialData = List.of(
                Payload.of(bytes("k1")),
                Payload.of(bytes("k2")),
                Payload.of(bytes("k3"))
        );

        // Create queue
        KeyHintData keyHint = client.createQueue(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // getHead on BACKUP
        Payload backupHead = client.setMode(Mode.BACKUP)
                .getHead(bytes(key), keyHint)
                .get();
        assertNotNull(backupHead);
        assertEquals("k1", new String(backupHead.getValue(), StandardCharsets.UTF_8));

        // Verify on MASTER: getHead returns k1 (replicated)
        Payload masterHead = client.setMode(Mode.MASTER)
                .getHead(bytes(key), keyHint)
                .get();
        assertNotNull(masterHead);
        assertEquals("k1", new String(masterHead.getValue(), StandardCharsets.UTF_8));
    }

    // =========================================================================
    // 8. GET TAIL (read)
    // =========================================================================

    @Test
    @DisplayName("getTail on master, verify replication to backup")
    void testGetTailOnMaster() throws ExecutionException, InterruptedException {
        String key = "getTailMaster" + UUID.randomUUID();
        List<Payload> initialData = List.of(
                Payload.of(bytes("k1")),
                Payload.of(bytes("k2")),
                Payload.of(bytes("k3"))
        );

        // Create queue
        KeyHintData keyHint = client.createQueue(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // getTail on MASTER
        Payload masterTail = client.setMode(Mode.MASTER)
                .getTail(bytes(key), keyHint)
                .get();
        assertNotNull(masterTail);
        assertEquals("k3", new String(masterTail.getValue(), StandardCharsets.UTF_8));

        // Verify on BACKUP: getTail returns k3 (replicated)
        Payload backupTail = client.setMode(Mode.BACKUP)
                .getTail(bytes(key), keyHint)
                .get();
        assertNotNull(backupTail);
        assertEquals("k3", new String(backupTail.getValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("getTail on backup, verify replication to master")
    void testGetTailOnBackup() throws ExecutionException, InterruptedException {
        String key = "getTailBackup" + UUID.randomUUID();
        List<Payload> initialData = List.of(
                Payload.of(bytes("k1")),
                Payload.of(bytes("k2")),
                Payload.of(bytes("k3"))
        );

        // Create queue
        KeyHintData keyHint = client.createQueue(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // getTail on BACKUP
        Payload backupTail = client.setMode(Mode.BACKUP)
                .getTail(bytes(key), keyHint)
                .get();
        assertNotNull(backupTail);
        assertEquals("k3", new String(backupTail.getValue(), StandardCharsets.UTF_8));

        // Verify on MASTER: getTail returns k3 (replicated)
        Payload masterTail = client.setMode(Mode.MASTER)
                .getTail(bytes(key), keyHint)
                .get();
        assertNotNull(masterTail);
        assertEquals("k3", new String(masterTail.getValue(), StandardCharsets.UTF_8));
    }

    // =========================================================================
    // 9. EXIST KEY & GET SIZE (read)
    // =========================================================================

    @Test
    @DisplayName("existKey on master, verify replication to backup")
    void testExistKeyOnMaster() throws ExecutionException, InterruptedException {
        String key = "existKeyMaster" + UUID.randomUUID();
        List<Payload> initialData = List.of(Payload.of(bytes("k1")));

        // Create queue
        KeyHintData keyHint = client.createQueue(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // existKey on MASTER
        Boolean masterExists = client.setMode(Mode.MASTER)
                .existKey(bytes(key))
                .get();
        assertTrue(masterExists);

        // Verify on BACKUP: existKey returns true (replicated)
        Boolean backupExists = client.setMode(Mode.BACKUP)
                .existKey(bytes(key))
                .get();
        assertTrue(backupExists);

        // Non-existent key on both nodes
        Boolean notExistsMaster = client.setMode(Mode.MASTER)
                .existKey(bytes("nonexistent"))
                .get();
        assertFalse(notExistsMaster);
    }

    @Test
    @DisplayName("existKey on backup, verify replication to master")
    void testExistKeyOnBackup() throws ExecutionException, InterruptedException {
        String key = "existKeyBackup" + UUID.randomUUID();
        List<Payload> initialData = List.of(Payload.of(bytes("k1")));

        // Create queue
        KeyHintData keyHint = client.createQueue(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // existKey on BACKUP
        Boolean backupExists = client.setMode(Mode.BACKUP)
                .existKey(bytes(key))
                .get();
        assertTrue(backupExists);

        // Verify on MASTER: existKey returns true (replicated)
        Boolean masterExists = client.setMode(Mode.MASTER)
                .existKey(bytes(key))
                .get();
        assertTrue(masterExists);
    }

    @Test
    @DisplayName("getSize returns 0 on both master and backup")
    void testGetSizeReturnsZero() throws ExecutionException, InterruptedException {
        String key = "getSizeQueue" + UUID.randomUUID();
        List<Payload> initialData = List.of(
                Payload.of(bytes("k1")),
                Payload.of(bytes("k2")),
                Payload.of(bytes("k3"))
        );

        // Create queue
        KeyHintData keyHint = client.createQueue(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // getSize on MASTER returns 0
        Integer masterSize = client.setMode(Mode.MASTER)
                .getSize(bytes(key), keyHint)
                .get();
        assertEquals(0, masterSize);

        // getSize on BACKUP returns 0
        Integer backupSize = client.setMode(Mode.BACKUP)
                .getSize(bytes(key), keyHint)
                .get();
        assertEquals(0, backupSize);
    }

    // =========================================================================
    // 10. REMOVE QUEUE (write!)
    // =========================================================================

    @Test
    @DisplayName("remove queue on master, verify replication to backup")
    void testRemoveQueueOnMaster() throws ExecutionException, InterruptedException {
        String key = "removeQueueMaster" + UUID.randomUUID();
        List<Payload> initialData = List.of(Payload.of(bytes("k1")));

        // Create queue
        KeyHintData keyHint = client.createQueue(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify initial state on both nodes
        Boolean masterExists = client.setMode(Mode.MASTER)
                .existKey(bytes(key))
                .get();
        assertTrue(masterExists);

        Boolean backupExists = client.setMode(Mode.BACKUP)
                .existKey(bytes(key))
                .get();
        assertTrue(backupExists);

        // remove queue on MASTER
        Boolean removed = client.setMode(Mode.MASTER)
                .remove(bytes(key), keyHint, OWNER_CLIENT_ID)
                .get();
        assertTrue(removed);

        // Verify on MASTER: existKey returns false
        Thread.sleep(REPLICATION_DELAY_MS);
        Boolean masterExistsAfter = client.setMode(Mode.MASTER)
                .existKey(bytes(key))
                .get();
        assertFalse(masterExistsAfter);

        // Verify on BACKUP: existKey returns false (replicated)
        Boolean backupExistsAfter = client.setMode(Mode.BACKUP)
                .existKey(bytes(key))
                .get();
        assertFalse(backupExistsAfter);
    }

    @Test
    @DisplayName("remove queue on backup, verify replication to master")
    void testRemoveQueueOnBackup() throws ExecutionException, InterruptedException {
        String key = "removeQueueBackup" + UUID.randomUUID();
        List<Payload> initialData = List.of(Payload.of(bytes("k1")));

        // Create queue
        KeyHintData keyHint = client.createQueue(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify initial state on both nodes
        Boolean masterExists = client.setMode(Mode.MASTER)
                .existKey(bytes(key))
                .get();
        assertTrue(masterExists);

        Boolean backupExists = client.setMode(Mode.BACKUP)
                .existKey(bytes(key))
                .get();
        assertTrue(backupExists);

        // remove queue on BACKUP
        Boolean removed = client.setMode(Mode.BACKUP)
                .remove(bytes(key), keyHint)
                .get();
        assertTrue(removed);

        // Verify on BACKUP: existKey returns false
        Thread.sleep(REPLICATION_DELAY_MS);
        Boolean backupExistsAfter = client.setMode(Mode.BACKUP)
                .existKey(bytes(key))
                .get();
        assertFalse(backupExistsAfter);

        // Verify on MASTER: existKey returns false (replicated)
        Boolean masterExistsAfter = client.setMode(Mode.MASTER)
                .existKey(bytes(key))
                .get();
        assertFalse(masterExistsAfter);
    }

    // =========================================================================
    // 11. SET TTL OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("setTtl on master, verify getTtl on both nodes")
    void testSetTtlOnMaster() throws ExecutionException, InterruptedException {
        String key = "setTtlMaster" + UUID.randomUUID();
        List<Payload> initialData = List.of(Payload.of(bytes("k1")));

        // Create queue
        KeyHintData keyHint = client.createQueue(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // setTtl on MASTER
        Boolean ttlSet = client.setMode(Mode.MASTER)
                .setTtl(bytes(key), keyHint, 300000, OWNER_CLIENT_ID)
                .get();
        assertTrue(ttlSet);

        // Verify on MASTER: getTtl > 0
        Thread.sleep(REPLICATION_DELAY_MS);
        Long ttlMaster = client.setMode(Mode.MASTER)
                .getTtl(bytes(key), keyHint, OWNER_CLIENT_ID)
                .get();
        assertNotNull(ttlMaster);
        assertTrue(ttlMaster > 0);

        // Verify on BACKUP: getTtl > 0 (replicated)
        Long ttlBackup = client.setMode(Mode.BACKUP)
                .getTtl(bytes(key), keyHint, OWNER_CLIENT_ID)
                .get();
        assertNotNull(ttlBackup);
        assertTrue(ttlBackup > 0);
    }

    @Test
    @DisplayName("setTtl on backup, verify getTtl on both nodes")
    void testSetTtlOnBackup() throws ExecutionException, InterruptedException {
        String key = "setTtlBackup" + UUID.randomUUID();
        List<Payload> initialData = List.of(Payload.of(bytes("k1")));

        // Create queue
        KeyHintData keyHint = client.createQueue(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // setTtl on BACKUP
        Boolean ttlSet = client.setMode(Mode.BACKUP)
                .setTtl(bytes(key), keyHint, 300000, OWNER_CLIENT_ID)
                .get();
        assertTrue(ttlSet);

        // Verify on BACKUP: getTtl > 0
        Thread.sleep(REPLICATION_DELAY_MS);
        Long ttlBackup = client.setMode(Mode.BACKUP)
                .getTtl(bytes(key), keyHint, OWNER_CLIENT_ID)
                .get();
        assertNotNull(ttlBackup);
        assertTrue(ttlBackup > 0);

        // Verify on MASTER: getTtl > 0 (replicated)
        Long ttlMaster = client.setMode(Mode.MASTER)
                .getTtl(bytes(key), keyHint, OWNER_CLIENT_ID)
                .get();
        assertNotNull(ttlMaster);
        assertTrue(ttlMaster > 0);
    }

    // =========================================================================
    // 12. TTL EXPIRATION
    // =========================================================================

    @Test
    @DisplayName("TTL expiration: set short TTL on master, verify both nodes expire")
    void testTtlExpirationOnMaster() throws ExecutionException, InterruptedException {
        String key = "ttlExpMaster" + UUID.randomUUID();
        List<Payload> initialData = List.of(Payload.of(bytes("k1")));

        // Create queue
        KeyHintData keyHint = client.createQueue(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // setTtl = 2 seconds on MASTER
        Boolean ttlSet = client.setMode(Mode.MASTER)
                .setTtl(bytes(key), keyHint, 2000, OWNER_CLIENT_ID)
                .get();
        assertTrue(ttlSet);

        Thread.sleep(REPLICATION_DELAY_MS);

        // Wait for TTL to expire
        Thread.sleep(2500);

        // Verify on MASTER: getHead throws NOT_FOUND
        try {
            client.setMode(Mode.MASTER)
                    .getHead(bytes(key), keyHint)
                    .get();
            fail("Expected NOT_FOUND on master after TTL expiry");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }

        // Verify on BACKUP: getHead throws NOT_FOUND (replicated)
        Thread.sleep(REPLICATION_DELAY_MS);
        try {
            client.setMode(Mode.BACKUP)
                    .getHead(bytes(key), keyHint)
                    .get();
            fail("Expected NOT_FOUND on backup after TTL expiry");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    @Test
    @DisplayName("TTL expiration: set short TTL on backup, verify both nodes expire")
    void testTtlExpirationOnBackup() throws ExecutionException, InterruptedException {
        String key = "ttlExpBackup" + UUID.randomUUID();
        List<Payload> initialData = List.of(Payload.of(bytes("k1")));

        // Create queue
        KeyHintData keyHint = client.createQueue(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // setTtl = 2 seconds on BACKUP
        Boolean ttlSet = client.setMode(Mode.BACKUP)
                .setTtl(bytes(key), keyHint, 2000, OWNER_CLIENT_ID)
                .get();
        assertTrue(ttlSet);

        Thread.sleep(REPLICATION_DELAY_MS);

        // Wait for TTL to expire
        Thread.sleep(2500);

        // Verify on BACKUP: getHead throws NOT_FOUND
        try {
            client.setMode(Mode.BACKUP)
                    .getHead(bytes(key), keyHint)
                    .get();
            fail("Expected NOT_FOUND on backup after TTL expiry");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }

        // Verify on MASTER: getHead throws NOT_FOUND (replicated)
        Thread.sleep(REPLICATION_DELAY_MS);
        try {
            client.setMode(Mode.MASTER)
                    .getHead(bytes(key), keyHint)
                    .get();
            fail("Expected NOT_FOUND on master after TTL expiry");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    // =========================================================================
    // 13. LOCK OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("lockObject on master, verify lock replicates to backup")
    void testLockObjectOnMaster() throws ExecutionException, InterruptedException {
        String key = "lockMaster" + UUID.randomUUID();
        List<Payload> initialData = List.of(Payload.of(bytes("k1")));

        // Create queue
        KeyHintData keyHint = client.createQueue(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // lockObject WRITE_LOCK on MASTER
        LockStatus lockStatus = client.setMode(Mode.MASTER)
                .lockObject(bytes(key), keyHint, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30))
                .get();
        assertEquals(LockStatus.OK, lockStatus);

        // Verify on MASTER: owner can read
        Thread.sleep(REPLICATION_DELAY_MS);
        Payload masterVal = client.setMode(Mode.MASTER)
                .getHead(bytes(key), keyHint, OWNER_CLIENT_ID, Duration.ofSeconds(30))
                .get();
        assertNotNull(masterVal);
        assertEquals("k1", new String(masterVal.getValue(), StandardCharsets.UTF_8));

        // Verify on BACKUP: lock is replicated (intruder cannot read)
        Thread.sleep(REPLICATION_DELAY_MS);
        assertDenied(client.setMode(Mode.BACKUP)
                .getHead(bytes(key), keyHint, INTRUDER_CLIENT_ID, Duration.ofSeconds(30)));

        // Unlock by owner
        LockStatus unlockStatus = client.setMode(Mode.MASTER)
                .unlockObject(bytes(key), keyHint, OWNER_CLIENT_ID)
                .get();
        assertEquals(LockStatus.OK, unlockStatus);
    }

    @Test
    @DisplayName("lockObject on backup, verify lock replicates to master")
    void testLockObjectOnBackup() throws ExecutionException, InterruptedException {
        String key = "lockBackup" + UUID.randomUUID();
        List<Payload> initialData = List.of(Payload.of(bytes("k1")));

        // Create queue
        KeyHintData keyHint = client.createQueue(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // lockObject WRITE_LOCK on BACKUP
        LockStatus lockStatus = client.setMode(Mode.BACKUP)
                .lockObject(bytes(key), keyHint, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30))
                .get();
        assertEquals(LockStatus.OK, lockStatus);

        // Verify on BACKUP: owner can read
        Thread.sleep(REPLICATION_DELAY_MS);
        Payload backupVal = client.setMode(Mode.BACKUP)
                .getHead(bytes(key), keyHint, OWNER_CLIENT_ID, Duration.ofSeconds(30))
                .get();
        assertNotNull(backupVal);
        assertEquals("k1", new String(backupVal.getValue(), StandardCharsets.UTF_8));

        // Verify on MASTER: lock is replicated (intruder cannot read)
        Thread.sleep(REPLICATION_DELAY_MS);
        assertDenied(client.setMode(Mode.MASTER)
                .getHead(bytes(key), keyHint, INTRUDER_CLIENT_ID, Duration.ofSeconds(30)));

        // Unlock by owner
        LockStatus unlockStatus = client.setMode(Mode.BACKUP)
                .unlockObject(bytes(key), keyHint, OWNER_CLIENT_ID)
                .get();
        assertEquals(LockStatus.OK, unlockStatus);
    }

    // =========================================================================
    // 14. LOCK EXPIRATION
    // =========================================================================

    @Test
    @DisplayName("Lock expiration: set short TTL lock on master, verify both nodes expire")
    void testLockExpirationOnMaster() throws ExecutionException, InterruptedException {
        String key = "lockExpMaster" + UUID.randomUUID();
        List<Payload> initialData = List.of(Payload.of(bytes("k1")));

        // Create queue
        KeyHintData keyHint = client.createQueue(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // lockObject WRITE_LOCK with 2s TTL on MASTER
        LockStatus lockStatus = client.setMode(Mode.MASTER)
                .lockObject(bytes(key), keyHint, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(2))
                .get();
        assertEquals(LockStatus.OK, lockStatus);

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify on MASTER: owner can read
        Payload masterVal = client.setMode(Mode.MASTER)
                .getHead(bytes(key), keyHint, OWNER_CLIENT_ID, Duration.ofSeconds(30))
                .get();
        assertNotNull(masterVal);

        // Verify on BACKUP: lock is replicated (intruder cannot read)
        Thread.sleep(REPLICATION_DELAY_MS);
        assertDenied(client.setMode(Mode.BACKUP)
                .getHead(bytes(key), keyHint, INTRUDER_CLIENT_ID, Duration.ofSeconds(30)));

        // Wait for lock TTL to expire
        Thread.sleep(3500);

        // Verify on MASTER: lock expired, intruder can read
        Payload masterValAfter = client.setMode(Mode.MASTER)
                .getHead(bytes(key), keyHint, INTRUDER_CLIENT_ID, Duration.ofSeconds(30))
                .get();
        assertNotNull(masterValAfter);

        // Verify on BACKUP: lock expired, intruder can read (replicated)
        Thread.sleep(REPLICATION_DELAY_MS);
        Payload backupValAfter = client.setMode(Mode.BACKUP)
                .getHead(bytes(key), keyHint, INTRUDER_CLIENT_ID, Duration.ofSeconds(30))
                .get();
        assertNotNull(backupValAfter);
    }

    @Test
    @DisplayName("Lock expiration: set short TTL lock on backup, verify both nodes expire")
    void testLockExpirationOnBackup() throws ExecutionException, InterruptedException {
        String key = "lockExpBackup" + UUID.randomUUID();
        List<Payload> initialData = List.of(Payload.of(bytes("k1")));

        // Create queue
        KeyHintData keyHint = client.createQueue(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // lockObject WRITE_LOCK with 2s TTL on BACKUP
        LockStatus lockStatus = client.setMode(Mode.BACKUP)
                .lockObject(bytes(key), keyHint, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(2))
                .get();
        assertEquals(LockStatus.OK, lockStatus);

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify on BACKUP: owner can read
        Payload backupVal = client.setMode(Mode.BACKUP)
                .getHead(bytes(key), keyHint, OWNER_CLIENT_ID, Duration.ofSeconds(30))
                .get();
        assertNotNull(backupVal);

        // Verify on MASTER: lock is replicated (intruder cannot read)
        Thread.sleep(REPLICATION_DELAY_MS);
        assertDenied(client.setMode(Mode.MASTER)
                .getHead(bytes(key), keyHint, INTRUDER_CLIENT_ID, Duration.ofSeconds(30)));

        // Wait for lock TTL to expire
        Thread.sleep(3500);

        // Verify on BACKUP: lock expired, intruder can read
        Payload backupValAfter = client.setMode(Mode.BACKUP)
                .getHead(bytes(key), keyHint, INTRUDER_CLIENT_ID, Duration.ofSeconds(30))
                .get();
        assertNotNull(backupValAfter);

        // Verify on MASTER: lock expired, intruder can read (replicated)
        Thread.sleep(REPLICATION_DELAY_MS);
        Payload masterValAfter = client.setMode(Mode.MASTER)
                .getHead(bytes(key), keyHint, INTRUDER_CLIENT_ID, Duration.ofSeconds(30))
                .get();
        assertNotNull(masterValAfter);
    }

}