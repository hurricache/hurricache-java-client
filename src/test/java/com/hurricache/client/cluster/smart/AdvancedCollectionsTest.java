package com.hurricache.client.cluster.smart;

import com.hurricache.TestBaseCluster;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Payload;
import com.hurricache.client.intf.Mode;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class AdvancedCollectionsTest extends TestBaseCluster {

    private static final long REPLICATION_DELAY_MS = 100;

    // =========================================================================
    // 1. HEAD AND POSITIONAL ADDITION
    // =========================================================================

    @Test
    @DisplayName("addElementToHead and addElementToPosition on List, verify replication")
    void testHeadAndPositionalAddition() throws ExecutionException, InterruptedException {
        String listKey = "headPosKeyCluster" + UUID.randomUUID();

        // Create list with middle element
        KeyHintData hint = client.createList(listKey, List.of(Payload.of(bytes("Middle"))))
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify initial state on both nodes
        Payload masterHead = client.setMode(Mode.MASTER)
                .getHead(bytes(listKey), hint)
                .get();
        assertEquals("Middle", new String(masterHead.getValue(), StandardCharsets.UTF_8));

        Payload backupHead = client.setMode(Mode.BACKUP)
                .getHead(bytes(listKey), hint)
                .get();
        assertEquals("Middle", new String(backupHead.getValue(), StandardCharsets.UTF_8));

        // addElementToHead on MASTER
        client.setMode(Mode.MASTER)
                .addElementToHead(bytes(listKey), hint, List.of(Payload.of(bytes("Head"))))
                .get();

        // Verify on MASTER: getHead returns Head
        Thread.sleep(REPLICATION_DELAY_MS);
        Payload masterHeadAfter = client.setMode(Mode.MASTER)
                .getHead(bytes(listKey), hint)
                .get();
        assertEquals("Head", new String(masterHeadAfter.getValue(), StandardCharsets.UTF_8));

        // Verify on BACKUP: getHead returns Head (replicated)
        Payload backupHeadAfter = client.setMode(Mode.BACKUP)
                .getHead(bytes(listKey), hint)
                .get();
        assertEquals("Head", new String(backupHeadAfter.getValue(), StandardCharsets.UTF_8));

        // addElementToPosition on MASTER
        client.setMode(Mode.MASTER)
                .addElementToPosition(listKey, List.of(Payload.of(bytes("NewPos1"))), 1, 0)
                .get();

        // Verify on MASTER: getElementAtPosition(1) returns NewPos1
        Thread.sleep(REPLICATION_DELAY_MS);
        Payload pos1 = client.setMode(Mode.MASTER)
                .getElementAtPosition(bytes(listKey), hint, 1)
                .get();
        assertNotNull(pos1);
        assertEquals("NewPos1", new String(pos1.getValue(), StandardCharsets.UTF_8));

        // Verify on BACKUP: getElementAtPosition(1) returns NewPos1 (replicated)
        Payload backupPos1 = client.setMode(Mode.BACKUP)
                .getElementAtPosition(bytes(listKey), hint, 1)
                .get();
        assertNotNull(backupPos1);
        assertEquals("NewPos1", new String(backupPos1.getValue(), StandardCharsets.UTF_8));
    }

    // =========================================================================
    // 2. TAIL AND POSITIONAL REMOVAL
    // =========================================================================

    @Test
    @DisplayName("removeTail and removeElementAtPosition on Vector, verify replication")
    void testTailAndPositionalRemoval() throws ExecutionException, InterruptedException {
        String vecKey = "removePosKeyCluster" + UUID.randomUUID();

        // Create vector with initial element
        KeyHintData hint = client.createVector(vecKey, List.of(Payload.of(bytes("0"))))
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // addElementToTail on MASTER
        client.setMode(Mode.MASTER)
                .addElementToTail(bytes(vecKey), hint, Arrays.asList(
                        Payload.of(bytes("1")),
                        Payload.of(bytes("2"))))
                .get();

        // Verify on MASTER: 3 elements
        Thread.sleep(REPLICATION_DELAY_MS);
        Payload masterTail = client.setMode(Mode.MASTER)
                .getTail(bytes(vecKey), hint)
                .get();
        assertEquals("2", new String(masterTail.getValue(), StandardCharsets.UTF_8));

        // Verify on BACKUP: 3 elements (replicated)
        Payload backupTail = client.setMode(Mode.BACKUP)
                .getTail(bytes(vecKey), hint)
                .get();
        assertEquals("2", new String(backupTail.getValue(), StandardCharsets.UTF_8));

        // removeTail on MASTER (removes "2")
        client.setMode(Mode.MASTER)
                .removeTail(bytes(vecKey), hint)
                .get();

        // Verify on MASTER: getTail returns "1"
        Thread.sleep(REPLICATION_DELAY_MS);
        Payload masterTailAfter = client.setMode(Mode.MASTER)
                .getTail(bytes(vecKey), hint)
                .get();
        assertEquals("1", new String(masterTailAfter.getValue(), StandardCharsets.UTF_8));

        // Verify on BACKUP: getTail returns "1" (replicated)
        Payload backupTailAfter = client.setMode(Mode.BACKUP)
                .getTail(bytes(vecKey), hint)
                .get();
        assertEquals("1", new String(backupTailAfter.getValue(), StandardCharsets.UTF_8));

        // removeElementAtPosition(0, 0) on MASTER (removes "0")
        client.setMode(Mode.MASTER)
                .removeElementAtPosition(bytes(vecKey), hint, 0L, 0L)
                .get();

        // Verify on MASTER: getElementAtPosition(0) returns "1"
        Thread.sleep(REPLICATION_DELAY_MS);
        Payload masterRemaining = client.setMode(Mode.MASTER)
                .getElementAtPosition(bytes(vecKey), hint, 0)
                .get();
        assertNotNull(masterRemaining);
        assertEquals("1", new String(masterRemaining.getValue(), StandardCharsets.UTF_8));

        // Verify on BACKUP: getElementAtPosition(0) returns "1" (replicated)
        Payload backupRemaining = client.setMode(Mode.BACKUP)
                .getElementAtPosition(bytes(vecKey), hint, 0)
                .get();
        assertNotNull(backupRemaining);
        assertEquals("1", new String(backupRemaining.getValue(), StandardCharsets.UTF_8));
    }

    // =========================================================================
    // 3. REMOVE ELEMENT IN RANGE SUCCESS
    // =========================================================================

    @Test
    @DisplayName("removeElementAtPosition with range on Vector, verify replication")
    void testRemoveElementInRangeSuccess() throws ExecutionException, InterruptedException {
        String key = "boolRangeKeyCluster" + UUID.randomUUID();

        // Create vector with initial element
        KeyHintData hint = client.createVector(key, List.of(Payload.of(bytes("0"))))
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // addElementToTail on MASTER
        for (int i = 1; i < 5; i++) {
            client.setMode(Mode.MASTER)
                    .addElementToTail(bytes(key), hint, List.of(Payload.of(bytes(String.valueOf(i)))))
                    .get();
        }

        // Verify on MASTER: 5 elements
        Thread.sleep(REPLICATION_DELAY_MS);
        Payload masterTail = client.setMode(Mode.MASTER)
                .getTail(bytes(key), hint)
                .get();
        assertEquals("4", new String(masterTail.getValue(), StandardCharsets.UTF_8));

        // Verify on BACKUP: 5 elements (replicated)
        Payload backupTail = client.setMode(Mode.BACKUP)
                .getTail(bytes(key), hint)
                .get();
        assertEquals("4", new String(backupTail.getValue(), StandardCharsets.UTF_8));

        // removeElementAtPosition(0, 2) on MASTER (removes positions 0-2: "0", "1", "2")
        Boolean removed = client.setMode(Mode.MASTER)
                .removeElementAtPosition(bytes(key), hint, 0L, 2L)
                .get();
        assertTrue(removed);

        // Verify on MASTER: getTail returns "4" (3 elements remain: "3", "4")
        Thread.sleep(REPLICATION_DELAY_MS);
        Payload masterTailAfter = client.setMode(Mode.MASTER)
                .getTail(bytes(key), hint)
                .get();
        assertNotNull(masterTailAfter);
        assertEquals("4", new String(masterTailAfter.getValue(), StandardCharsets.UTF_8));

        // Verify on BACKUP: getTail returns "4" (replicated)
        Payload backupTailAfter = client.setMode(Mode.BACKUP)
                .getTail(bytes(key), hint)
                .get();
        assertNotNull(backupTailAfter);
        assertEquals("4", new String(backupTailAfter.getValue(), StandardCharsets.UTF_8));
    }

    // =========================================================================
    // 4. QUEUE TYPE SAFETY
    // =========================================================================

    @Test
    @DisplayName("addElementToPosition not supported on Queue, verify on both nodes")
    void testQueueTypeSafety() throws ExecutionException, InterruptedException {
        String qKey = "strictQueueCluster" + UUID.randomUUID();

        // Create queue
        KeyHintData hint = client.createQueue(qKey, List.of(Payload.of(bytes("q1"))))
                .get();

        Thread.sleep(REPLICATION_DELAY_MS);

        // Verify initial state on both nodes
        Payload masterHead = client.setMode(Mode.MASTER)
                .getHead(bytes(qKey), hint)
                .get();
        assertEquals("q1", new String(masterHead.getValue(), StandardCharsets.UTF_8));

        Payload backupHead = client.setMode(Mode.BACKUP)
                .getHead(bytes(qKey), hint)
                .get();
        assertEquals("q1", new String(backupHead.getValue(), StandardCharsets.UTF_8));

        // addElementToPosition on MASTER should fail for Queue
        try {
            client.setMode(Mode.MASTER)
                    .addElementToPosition(qKey, List.of(Payload.of(bytes("fail"))), 1, 0)
                    .get();
            fail("addElementToPosition should throw an error for Queue");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertNotEquals(Status.Code.OK, cause.getStatus().getCode());
        }

        // addElementToPosition on BACKUP should also fail
        try {
            client.setMode(Mode.BACKUP)
                    .addElementToPosition(qKey, List.of(Payload.of(bytes("fail"))), 1, 0)
                    .get();
            fail("addElementToPosition should throw an error for Queue");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertNotEquals(Status.Code.OK, cause.getStatus().getCode());
        }
    }

}