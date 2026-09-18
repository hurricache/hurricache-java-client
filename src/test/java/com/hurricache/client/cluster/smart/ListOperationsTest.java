package com.hurricache.client.cluster.smart;

import com.hurricache.TestBaseCluster;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Mode;
import com.hurricache.client.intf.Payload;
import com.hurricache.grpc.ContainerType;
import com.hurricache.grpc.LockStatus;
import com.hurricache.grpc.LockType;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class ListOperationsTest extends TestBaseCluster {

    // ==================== Section 1: List Creation ====================

    @Test
    @DisplayName("Create empty list on master, verify replication to backup")
    void testCreateEmptyList() throws ExecutionException, InterruptedException {
        String key = "emptyList" + UUID.randomUUID();

        // Create empty list
        KeyHintData keyHint = client.createList(key, Collections.emptyList())
                .get();

        Thread.sleep(500); // replication wait

        // Verify on BACKUP: streamList returns empty list
        List<Payload> backupStream = client.setMode(Mode.BACKUP)
                .streamList(key, keyHint)
                .get();
        assertNotNull(backupStream);
        assertTrue(backupStream.isEmpty(), "Expected empty list on backup");

        // Verify on MASTER: getSize returns 0
        Integer masterSize = client.setMode(Mode.MASTER)
                .getSize(key, keyHint)
                .get();
        assertNotNull(masterSize);
        assertEquals(0, masterSize, "Expected size 0 on master");
    }

    @Test
    @DisplayName("Create list with data on master, verify replication to backup and write on backup")
    void testCreateListWithData() throws ExecutionException, InterruptedException {
        String key = "listWithData" + UUID.randomUUID();
        List<Payload> initialData = Arrays.asList(
                Payload.of("first".getBytes(StandardCharsets.UTF_8)),
                Payload.of("second".getBytes(StandardCharsets.UTF_8))
        );

        // Create list
        KeyHintData keyHint = client.createList(key, initialData)
                .get();

        Thread.sleep(500); // replication wait

        // Verify on MASTER: streamList returns [first, second]
        List<Payload> masterStream = client.setMode(Mode.MASTER)
                .streamList(key, keyHint)
                .get();
        assertNotNull(masterStream);
        assertEquals(2, masterStream.size());
        assertEquals("first", new String(masterStream.get(0).getValue(), StandardCharsets.UTF_8));
        assertEquals("second", new String(masterStream.get(1).getValue(), StandardCharsets.UTF_8));

        // Verify on BACKUP: streamList returns [first, second]
        List<Payload> backupStream = client.setMode(Mode.BACKUP)
                .streamList(key, keyHint)
                .get();
        assertNotNull(backupStream);
        assertEquals(2, backupStream.size());
        assertEquals("first", new String(backupStream.get(0).getValue(), StandardCharsets.UTF_8));
        assertEquals("second", new String(backupStream.get(1).getValue(), StandardCharsets.UTF_8));

        // Write on BACKUP: addElementToTail
        client.setMode(Mode.BACKUP)
                .addElementToTail(key, keyHint, List.of(Payload.of("third".getBytes(StandardCharsets.UTF_8))))
                .get();

        Thread.sleep(300); // replication delay

        // Verify on BACKUP: streamList returns [first, second, third]
        List<Payload> backupAfterWrite = client.setMode(Mode.BACKUP)
                .streamList(key, keyHint)
                .get();
        assertEquals(3, backupAfterWrite.size());
        assertEquals("third", new String(backupAfterWrite.get(2).getValue(), StandardCharsets.UTF_8));

        Thread.sleep(300); // replication delay

        // Verify on MASTER: streamList returns [first, second, third]
        List<Payload> masterAfterWrite = client.setMode(Mode.MASTER)
                .streamList(key, keyHint)
                .get();
        assertEquals(3, masterAfterWrite.size());
        assertEquals("third", new String(masterAfterWrite.get(2).getValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("Create large list with chunking on master, verify 1500 elements on both nodes")
    void testCreateLargeListWithChunking() throws ExecutionException, InterruptedException {
        String key = "largeList" + UUID.randomUUID();
        int elementCount = 1500;
        int payloadSize = 8192; // 8KB per element

        List<Payload> largePayloads = new ArrayList<>(elementCount);
        for (int i = 0; i < elementCount; i++) {
            largePayloads.add(Payload.of(createLargePayload(payloadSize)));
        }

        // Create large list
        KeyHintData keyHint = client.createList(key, largePayloads)
                .get();

        Thread.sleep(1500); // replication wait

        // Verify on MASTER: streamList returns 1500 elements
        List<Payload> masterStream = client.setMode(Mode.MASTER)
                .streamList(key, keyHint)
                .get();
        assertNotNull(masterStream);
        assertEquals(elementCount, masterStream.size(), "Expected " + elementCount + " elements on master");

        // Verify on BACKUP: streamList returns 1500 elements
        List<Payload> backupStream = client.setMode(Mode.BACKUP)
                .streamList(key, keyHint)
                .get();
        assertNotNull(backupStream);
        assertEquals(elementCount, backupStream.size(), "Expected " + elementCount + " elements on backup");

        // Verify integrity: first and last elements match
        assertArrayEquals(largePayloads.get(0).getValue(), masterStream.get(0).getValue(), "First element mismatch on master");
        assertArrayEquals(largePayloads.get(elementCount - 1).getValue(), masterStream.get(elementCount - 1).getValue(), "Last element mismatch on master");
        assertArrayEquals(largePayloads.get(0).getValue(), backupStream.get(0).getValue(), "First element mismatch on backup");
        assertArrayEquals(largePayloads.get(elementCount - 1).getValue(), backupStream.get(elementCount - 1).getValue(), "Last element mismatch on backup");
    }

    // ==================== Section 2: Read Operations ====================

    @Test
    @DisplayName("Stream list on master, verify replication and write on backup")
    void testStreamList() throws ExecutionException, InterruptedException {
        String key = "streamListTest" + UUID.randomUUID();
        List<Payload> initialData = Arrays.asList(
                Payload.of("a".getBytes(StandardCharsets.UTF_8)),
                Payload.of("b".getBytes(StandardCharsets.UTF_8)),
                Payload.of("c".getBytes(StandardCharsets.UTF_8))
        );

        // Create list
        KeyHintData keyHint = client.createList(key, initialData)
                .get();

        Thread.sleep(500); // replication wait

        // Verify on MASTER: streamList returns 3 elements [a, b, c]
        List<Payload> masterStream = client.setMode(Mode.MASTER)
                .streamList(key, keyHint)
                .get();
        assertNotNull(masterStream);
        assertEquals(3, masterStream.size());

        // Verify on BACKUP: streamList returns 3 elements [a, b, c]
        List<Payload> backupStream = client.setMode(Mode.BACKUP)
                .streamList(key, keyHint)
                .get();
        assertNotNull(backupStream);
        assertEquals(3, backupStream.size());

        // Write on BACKUP: addElementToTail
        client.setMode(Mode.BACKUP)
                .addElementToTail(key, keyHint, List.of(Payload.of("d".getBytes(StandardCharsets.UTF_8))))
                .get();

        Thread.sleep(300); // replication delay

        // Verify on BACKUP: streamList returns 4 elements
        List<Payload> backupAfterWrite = client.setMode(Mode.BACKUP)
                .streamList(key, keyHint)
                .get();
        assertEquals(4, backupAfterWrite.size());

        Thread.sleep(300); // replication delay

        // Verify on MASTER: streamList returns 4 elements
        List<Payload> masterAfterWrite = client.setMode(Mode.MASTER)
                .streamList(key, keyHint)
                .get();
        assertEquals(4, masterAfterWrite.size());
    }

    @Test
    @DisplayName("GetAndRemoveFront on master, verify replication and write on backup")
    void testGetAndRemoveFront() throws ExecutionException, InterruptedException {
        String key = "getAndRemoveFrontTest" + UUID.randomUUID();
        List<Payload> initialData = Arrays.asList(
                Payload.of("head".getBytes(StandardCharsets.UTF_8)),
                Payload.of("middle".getBytes(StandardCharsets.UTF_8)),
                Payload.of("tail".getBytes(StandardCharsets.UTF_8))
        );

        // Create list
        KeyHintData keyHint = client.createList(key, initialData)
                .get();

        Thread.sleep(500); // replication wait

        // Write on BACKUP: getAndRemoveFront returns "head"
        Payload removed = client.setMode(Mode.BACKUP)
                .getAndRemoveFront(key, keyHint)
                .get();
        assertNotNull(removed);
        assertEquals("head", new String(removed.getValue(), StandardCharsets.UTF_8));

        Thread.sleep(300); // replication delay

        // Verify on BACKUP: getHead returns "middle", streamList returns [middle, tail]
        Payload backupHead = client.setMode(Mode.BACKUP)
                .getHead(key, keyHint)
                .get();
        assertNotNull(backupHead);
        assertEquals("middle", new String(backupHead.getValue(), StandardCharsets.UTF_8));

        List<Payload> backupStream = client.setMode(Mode.BACKUP)
                .streamList(key, keyHint)
                .get();
        assertEquals(2, backupStream.size());

        Thread.sleep(300); // replication delay

        // Verify on MASTER: streamList returns [middle, tail]
        List<Payload> masterStream = client.setMode(Mode.MASTER)
                .streamList(key, keyHint)
                .get();
        assertEquals(2, masterStream.size());
        assertEquals("middle", new String(masterStream.get(0).getValue(), StandardCharsets.UTF_8));

        // Write on MASTER: getAndRemoveFront returns "middle"
        Payload removedMaster = client.setMode(Mode.MASTER)
                .getAndRemoveFront(key, keyHint)
                .get();
        assertNotNull(removedMaster);
        assertEquals("middle", new String(removedMaster.getValue(), StandardCharsets.UTF_8));

        Thread.sleep(300); // replication delay

        // Verify on MASTER: getHead returns "tail"
        Payload masterHead = client.setMode(Mode.MASTER)
                .getHead(key, keyHint)
                .get();
        assertNotNull(masterHead);
        assertEquals("tail", new String(masterHead.getValue(), StandardCharsets.UTF_8));

        Thread.sleep(300); // replication delay

        // Verify on BACKUP: getHead returns "tail"
        Payload backupHeadAfter = client.setMode(Mode.BACKUP)
                .getHead(key, keyHint)
                .get();
        assertNotNull(backupHeadAfter);
        assertEquals("tail", new String(backupHeadAfter.getValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("GetHead on master, verify replication and write on backup")
    void testGetHead() throws ExecutionException, InterruptedException {
        String key = "getHeadTest" + UUID.randomUUID();
        List<Payload> initialData = Arrays.asList(
                Payload.of("first".getBytes(StandardCharsets.UTF_8)),
                Payload.of("second".getBytes(StandardCharsets.UTF_8))
        );

        // Create list
        KeyHintData keyHint = client.createList(key, initialData)
                .get();

        Thread.sleep(500); // replication wait

        // Write on BACKUP: addElementToTail
        client.setMode(Mode.BACKUP)
                .addElementToTail(key, keyHint, List.of(Payload.of("third".getBytes(StandardCharsets.UTF_8))))
                .get();

        Thread.sleep(300); // replication delay

        // Verify on BACKUP: getHead returns "first"
        Payload backupHead = client.setMode(Mode.BACKUP)
                .getHead(key, keyHint)
                .get();
        assertNotNull(backupHead);
        assertEquals("first", new String(backupHead.getValue(), StandardCharsets.UTF_8));

        Thread.sleep(300); // replication delay

        // Verify on MASTER: getHead returns "first"
        Payload masterHead = client.setMode(Mode.MASTER)
                .getHead(key, keyHint)
                .get();
        assertNotNull(masterHead);
        assertEquals("first", new String(masterHead.getValue(), StandardCharsets.UTF_8));

        // Write on MASTER: addElementToHead
        client.setMode(Mode.MASTER)
                .addElementToHead(key, keyHint, List.of(Payload.of("zero".getBytes(StandardCharsets.UTF_8))))
                .get();

        Thread.sleep(300); // replication delay

        // Verify on MASTER: getHead returns "zero"
        Payload masterHeadAfter = client.setMode(Mode.MASTER)
                .getHead(key, keyHint)
                .get();
        assertNotNull(masterHeadAfter);
        assertEquals("zero", new String(masterHeadAfter.getValue(), StandardCharsets.UTF_8));

        Thread.sleep(300); // replication delay

        // Verify on BACKUP: getHead returns "zero"
        Payload backupHeadAfter = client.setMode(Mode.BACKUP)
                .getHead(key, keyHint)
                .get();
        assertNotNull(backupHeadAfter);
        assertEquals("zero", new String(backupHeadAfter.getValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("GetTail on master, verify replication and write on backup")
    void testGetTail() throws ExecutionException, InterruptedException {
        String key = "getTailTest" + UUID.randomUUID();
        List<Payload> initialData = Arrays.asList(
                Payload.of("first".getBytes(StandardCharsets.UTF_8)),
                Payload.of("second".getBytes(StandardCharsets.UTF_8))
        );

        // Create list
        KeyHintData keyHint = client.createList(key, initialData)
                .get();

        Thread.sleep(500); // replication wait

        // Write on BACKUP: addElementToHead
        client.setMode(Mode.BACKUP)
                .addElementToHead(key, keyHint, List.of(Payload.of("zero".getBytes(StandardCharsets.UTF_8))))
                .get();

        Thread.sleep(300); // replication delay

        // Verify on BACKUP: getTail returns "second"
        Payload backupTail = client.setMode(Mode.BACKUP)
                .getTail(key, keyHint)
                .get();
        assertNotNull(backupTail);
        assertEquals("second", new String(backupTail.getValue(), StandardCharsets.UTF_8));

        Thread.sleep(300); // replication delay

        // Verify on MASTER: getTail returns "second"
        Payload masterTail = client.setMode(Mode.MASTER)
                .getTail(key, keyHint)
                .get();
        assertNotNull(masterTail);
        assertEquals("second", new String(masterTail.getValue(), StandardCharsets.UTF_8));

        // Write on MASTER: addElementToTail
        client.setMode(Mode.MASTER)
                .addElementToTail(key, keyHint, List.of(Payload.of("last".getBytes(StandardCharsets.UTF_8))))
                .get();

        Thread.sleep(300); // replication delay

        // Verify on MASTER: getTail returns "last"
        Payload masterTailAfter = client.setMode(Mode.MASTER)
                .getTail(key, keyHint)
                .get();
        assertNotNull(masterTailAfter);
        assertEquals("last", new String(masterTailAfter.getValue(), StandardCharsets.UTF_8));

        Thread.sleep(300); // replication delay

        // Verify on BACKUP: getTail returns "last"
        Payload backupTailAfter = client.setMode(Mode.BACKUP)
                .getTail(key, keyHint)
                .get();
        assertNotNull(backupTailAfter);
        assertEquals("last", new String(backupTailAfter.getValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("GetElementAtPosition on master, verify replication and write on backup")
    void testGetElementAtPosition() throws ExecutionException, InterruptedException {
        String key = "getElementAtPositionTest" + UUID.randomUUID();
        List<Payload> initialData = Arrays.asList(
                Payload.of("pos0".getBytes(StandardCharsets.UTF_8)),
                Payload.of("pos1".getBytes(StandardCharsets.UTF_8)),
                Payload.of("pos2".getBytes(StandardCharsets.UTF_8))
        );

        // Create list
        KeyHintData keyHint = client.createList(key, initialData)
                .get();

        Thread.sleep(500); // replication wait

        // Write on BACKUP: getElementAtPosition(key, 1) returns "pos1"
        Payload pos1 = client.setMode(Mode.BACKUP)
                .getElementAtPosition(key, keyHint, 1)
                .get();
        assertNotNull(pos1);
        assertEquals("pos1", new String(pos1.getValue(), StandardCharsets.UTF_8));

        Thread.sleep(300); // replication delay

        // Verify on BACKUP: getElementAtPosition(key, 0) returns "pos0"
        Payload pos0 = client.setMode(Mode.BACKUP)
                .getElementAtPosition(key, keyHint, 0)
                .get();
        assertNotNull(pos0);
        assertEquals("pos0", new String(pos0.getValue(), StandardCharsets.UTF_8));

        Thread.sleep(300); // replication delay

        // Verify on MASTER: getElementAtPosition(key, 1) returns "pos1"
        Payload masterPos1 = client.setMode(Mode.MASTER)
                .getElementAtPosition(key, keyHint, 1)
                .get();
        assertNotNull(masterPos1);
        assertEquals("pos1", new String(masterPos1.getValue(), StandardCharsets.UTF_8));

        // Write on MASTER: addElementToPosition at 2
        client.setMode(Mode.MASTER)
                .addElementToPosition(key, keyHint, List.of(Payload.of("new".getBytes(StandardCharsets.UTF_8))), 2)
                .get();

        Thread.sleep(300); // replication delay

        // Verify on MASTER: getElementAtPosition(key, 2) returns "new"
        Payload masterNew = client.setMode(Mode.MASTER)
                .getElementAtPosition(key, keyHint, 2)
                .get();
        assertNotNull(masterNew);
        assertEquals("new", new String(masterNew.getValue(), StandardCharsets.UTF_8));

        Thread.sleep(300); // replication delay

        // Verify on BACKUP: getElementAtPosition(key, 2) returns "new"
        Payload backupNew = client.setMode(Mode.BACKUP)
                .getElementAtPosition(key, keyHint, 2)
                .get();
        assertNotNull(backupNew);
        assertEquals("new", new String(backupNew.getValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("StreamElementInRangeUnordered on master, verify replication and write on backup")
    void testStreamElementInRangeUnordered() throws ExecutionException, InterruptedException {
        String key = "streamRangeTest" + UUID.randomUUID();
        List<Payload> initialData = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            initialData.add(Payload.of(String.valueOf(i).getBytes(StandardCharsets.UTF_8)));
        }

        // Create list
        KeyHintData keyHint = client.createList(key, initialData)
                .get();

        Thread.sleep(500); // replication wait

        // Write on BACKUP: streamElementInRangeUnordered(key, LIST, 2, 5) returns 4 elements [2,3,4,5]
        List<Payload> rangeData = client.setMode(Mode.BACKUP)
                .streamElementInRangeUnordered(key, keyHint, ContainerType.LIST, 2, 5)
                .get();
        assertNotNull(rangeData);
        assertEquals(4, rangeData.size());

        Thread.sleep(300); // replication delay

        // Verify on MASTER: streamElementInRangeUnordered returns 4 elements
        List<Payload> masterRange = client.setMode(Mode.MASTER)
                .streamElementInRangeUnordered(key, keyHint, ContainerType.LIST, 2, 5)
                .get();
        assertNotNull(masterRange);
        assertEquals(4, masterRange.size());

        // Write on MASTER: addElementToTail
        client.setMode(Mode.MASTER)
                .addElementToTail(key, keyHint, List.of(Payload.of("10".getBytes(StandardCharsets.UTF_8))))
                .get();

        Thread.sleep(300); // replication delay

        // Verify on MASTER: streamElementInRangeUnordered still returns 4 elements (range 2-5)
        List<Payload> masterRangeAfter = client.setMode(Mode.MASTER)
                .streamElementInRangeUnordered(key, keyHint, ContainerType.LIST, 2, 5)
                .get();
        assertEquals(4, masterRangeAfter.size());

        Thread.sleep(300); // replication delay

        // Verify on BACKUP: streamElementInRangeUnordered still returns 4 elements
        List<Payload> backupRangeAfter = client.setMode(Mode.BACKUP)
                .streamElementInRangeUnordered(key, keyHint, ContainerType.LIST, 2, 5)
                .get();
        assertEquals(4, backupRangeAfter.size());
    }

    // ==================== Section 3: Write Operations ====================

    @Test
    @DisplayName("GetAndRemoveTail on master, verify replication and write on backup")
    void testGetAndRemoveTail() throws ExecutionException, InterruptedException {
        String key = "getAndRemoveTailTest" + UUID.randomUUID();
        List<Payload> initialData = Arrays.asList(
                Payload.of("head".getBytes(StandardCharsets.UTF_8)),
                Payload.of("middle".getBytes(StandardCharsets.UTF_8)),
                Payload.of("tail".getBytes(StandardCharsets.UTF_8))
        );

        // Create list
        KeyHintData keyHint = client.createList(key, initialData)
                .get();

        Thread.sleep(500); // replication wait

        // Write on BACKUP: getAndRemoveTail returns "tail"
        Payload removed = client.setMode(Mode.BACKUP)
                .getAndRemoveTail(key, keyHint)
                .get();
        assertNotNull(removed);
        assertEquals("tail", new String(removed.getValue(), StandardCharsets.UTF_8));

        Thread.sleep(300); // replication delay

        // Verify on BACKUP: getTail returns "middle", streamList returns [head, middle]
        Payload backupTail = client.setMode(Mode.BACKUP)
                .getTail(key, keyHint)
                .get();
        assertNotNull(backupTail);
        assertEquals("middle", new String(backupTail.getValue(), StandardCharsets.UTF_8));

        List<Payload> backupStream = client.setMode(Mode.BACKUP)
                .streamList(key, keyHint)
                .get();
        assertEquals(2, backupStream.size());

        Thread.sleep(300); // replication delay

        // Verify on MASTER: streamList returns [head, middle]
        List<Payload> masterStream = client.setMode(Mode.MASTER)
                .streamList(key, keyHint)
                .get();
        assertEquals(2, masterStream.size());

        // Write on MASTER: getAndRemoveTail returns "middle"
        Payload removedMaster = client.setMode(Mode.MASTER)
                .getAndRemoveTail(key, keyHint)
                .get();
        assertNotNull(removedMaster);
        assertEquals("middle", new String(removedMaster.getValue(), StandardCharsets.UTF_8));

        Thread.sleep(300); // replication delay

        // Verify on MASTER: getTail returns "head"
        Payload masterTail = client.setMode(Mode.MASTER)
                .getTail(key, keyHint)
                .get();
        assertNotNull(masterTail);
        assertEquals("head", new String(masterTail.getValue(), StandardCharsets.UTF_8));

        Thread.sleep(300); // replication delay

        // Verify on BACKUP: getTail returns "head"
        Payload backupTailAfter = client.setMode(Mode.BACKUP)
                .getTail(key, keyHint)
                .get();
        assertNotNull(backupTailAfter);
        assertEquals("head", new String(backupTailAfter.getValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("GetAndRemoveElementAtPosition on master, verify replication and write on backup")
    void testGetAndRemoveElementAtPosition() throws ExecutionException, InterruptedException {
        String key = "getAndRemoveElementAtPositionTest" + UUID.randomUUID();
        List<Payload> initialData = Arrays.asList(
                Payload.of("pos0".getBytes(StandardCharsets.UTF_8)),
                Payload.of("pos1".getBytes(StandardCharsets.UTF_8)),
                Payload.of("pos2".getBytes(StandardCharsets.UTF_8))
        );

        // Create list
        KeyHintData keyHint = client.createList(key, initialData)
                .get();

        Thread.sleep(500); // replication wait

        // Write on BACKUP: getAndRemoveElementAtPosition(key, 1) returns "pos1"
        Payload removed = client.setMode(Mode.BACKUP)
                .getAndRemoveElementAtPosition(key, keyHint, 1)
                .get();
        assertNotNull(removed);
        assertEquals("pos1", new String(removed.getValue(), StandardCharsets.UTF_8));

        Thread.sleep(300); // replication delay

        // Verify on BACKUP: getElementAtPosition(key, 1) returns "pos2", streamList returns [pos0, pos2]
        Payload pos1 = client.setMode(Mode.BACKUP)
                .getElementAtPosition(key, keyHint, 1)
                .get();
        assertNotNull(pos1);
        assertEquals("pos2", new String(pos1.getValue(), StandardCharsets.UTF_8));

        List<Payload> backupStream = client.setMode(Mode.BACKUP)
                .streamList(key, keyHint)
                .get();
        assertEquals(2, backupStream.size());

        Thread.sleep(300); // replication delay

        // Verify on MASTER: streamList returns [pos0, pos2]
        List<Payload> masterStream = client.setMode(Mode.MASTER)
                .streamList(key, keyHint)
                .get();
        assertEquals(2, masterStream.size());

        // Write on MASTER: getAndRemoveElementAtPosition(key, 0) returns "pos0"
        Payload removedMaster = client.setMode(Mode.MASTER)
                .getAndRemoveElementAtPosition(key, keyHint, 0)
                .get();
        assertNotNull(removedMaster);
        assertEquals("pos0", new String(removedMaster.getValue(), StandardCharsets.UTF_8));

        Thread.sleep(300); // replication delay

        // Verify on MASTER: getElementAtPosition(key, 0) returns "pos2"
        Payload masterPos0 = client.setMode(Mode.MASTER)
                .getElementAtPosition(key, keyHint, 0)
                .get();
        assertNotNull(masterPos0);
        assertEquals("pos2", new String(masterPos0.getValue(), StandardCharsets.UTF_8));

        Thread.sleep(300); // replication delay

        // Verify on BACKUP: getElementAtPosition(key, 0) returns "pos2"
        Payload backupPos0 = client.setMode(Mode.BACKUP)
                .getElementAtPosition(key, keyHint, 0)
                .get();
        assertNotNull(backupPos0);
        assertEquals("pos2", new String(backupPos0.getValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("AddElementToPosition on master, verify replication and write on backup")
    void testAddElementToPosition() throws ExecutionException, InterruptedException {
        String key = "addElementToPositionTest" + UUID.randomUUID();
        List<Payload> initialData = Arrays.asList(
                Payload.of("first".getBytes(StandardCharsets.UTF_8)),
                Payload.of("third".getBytes(StandardCharsets.UTF_8))
        );

        // Create list
        KeyHintData keyHint = client.createList(key, initialData)
                .get();

        Thread.sleep(500); // replication wait

        // Write on BACKUP: addElementToPosition at 1
        Integer added = client.setMode(Mode.BACKUP)
                .addElementToPosition(key, keyHint, List.of(Payload.of("second".getBytes(StandardCharsets.UTF_8))), 1)
                .get();
        assertNotNull(added);
        assertEquals(1, added);

        Thread.sleep(300); // replication delay

        // Verify on BACKUP: streamList returns [first, second, third]
        List<Payload> backupStream = client.setMode(Mode.BACKUP)
                .streamList(key, keyHint)
                .get();
        assertEquals(3, backupStream.size());
        assertEquals("second", new String(backupStream.get(1).getValue(), StandardCharsets.UTF_8));

        Thread.sleep(300); // replication delay

        // Verify on MASTER: streamList returns [first, second, third]
        List<Payload> masterStream = client.setMode(Mode.MASTER)
                .streamList(key, keyHint)
                .get();
        assertEquals(3, masterStream.size());
        assertEquals("second", new String(masterStream.get(1).getValue(), StandardCharsets.UTF_8));

        // Write on MASTER: addElementToPosition at 0
        client.setMode(Mode.MASTER)
                .addElementToPosition(key, keyHint, List.of(Payload.of("zero".getBytes(StandardCharsets.UTF_8))), 0)
                .get();

        Thread.sleep(300); // replication delay

        // Verify on MASTER: streamList returns [zero, first, second, third]
        List<Payload> masterStreamAfter = client.setMode(Mode.MASTER)
                .streamList(key, keyHint)
                .get();
        assertEquals(4, masterStreamAfter.size());
        assertEquals("zero", new String(masterStreamAfter.get(0).getValue(), StandardCharsets.UTF_8));

        Thread.sleep(300); // replication delay

        // Verify on BACKUP: streamList returns [zero, first, second, third]
        List<Payload> backupStreamAfter = client.setMode(Mode.BACKUP)
                .streamList(key, keyHint)
                .get();
        assertEquals(4, backupStreamAfter.size());
        assertEquals("zero", new String(backupStreamAfter.get(0).getValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("RemoveElementAtPosition on master, verify replication and write on backup")
    void testRemoveElementAtPosition() throws ExecutionException, InterruptedException {
        String key = "removeElementAtPositionTest" + UUID.randomUUID();
        List<Payload> initialData = Arrays.asList(
                Payload.of("0".getBytes(StandardCharsets.UTF_8)),
                Payload.of("1".getBytes(StandardCharsets.UTF_8)),
                Payload.of("2".getBytes(StandardCharsets.UTF_8)),
                Payload.of("3".getBytes(StandardCharsets.UTF_8))
        );

        // Create list
        KeyHintData keyHint = client.createList(key, initialData)
                .get();

        Thread.sleep(500); // replication wait

        // Write on BACKUP: removeElementAtPosition(key, 1)
        Boolean removed = client.setMode(Mode.BACKUP)
                .removeElementAtPosition(key, keyHint, 1)
                .get();
        assertTrue(removed);

        Thread.sleep(300); // replication delay

        // Verify on BACKUP: streamList returns [0, 2, 3]
        List<Payload> backupStream = client.setMode(Mode.BACKUP)
                .streamList(key, keyHint)
                .get();
        assertEquals(3, backupStream.size());

        Thread.sleep(300); // replication delay

        // Verify on MASTER: streamList returns [0, 2, 3]
        List<Payload> masterStream = client.setMode(Mode.MASTER)
                .streamList(key, keyHint)
                .get();
        assertEquals(3, masterStream.size());

        // Write on MASTER: removeElementAtPosition(key, 0, 1) - removes range [0, 1]
        Boolean rangeRemoved = client.setMode(Mode.MASTER)
                .removeElementAtPosition(key, keyHint, 0, 1)
                .get();
        assertTrue(rangeRemoved);

        Thread.sleep(300); // replication delay

        // Verify on MASTER: streamList returns [3] (removed 0, 2, left 3)
        List<Payload> masterStreamAfter = client.setMode(Mode.MASTER)
                .streamList(key, keyHint)
                .get();
        assertEquals(1, masterStreamAfter.size());
        assertEquals("3", new String(masterStreamAfter.get(0).getValue(), StandardCharsets.UTF_8));

        Thread.sleep(300); // replication delay

        // Verify on BACKUP: streamList returns [3]
        List<Payload> backupStreamAfter = client.setMode(Mode.BACKUP)
                .streamList(key, keyHint)
                .get();
        assertEquals(1, backupStreamAfter.size());
        assertEquals("3", new String(backupStreamAfter.get(0).getValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("AddElementToPositionBefore on master, verify replication and write on backup")
    void testAddElementToPositionBefore() throws ExecutionException, InterruptedException {
        String key = "addElementToPositionBeforeTest" + UUID.randomUUID();
        List<Payload> initialData = List.of(Payload.of("pivot".getBytes(StandardCharsets.UTF_8)));

        // Create list
        KeyHintData keyHint = client.createList(key, initialData)
                .get();

        Thread.sleep(500); // replication wait

        // Write on BACKUP: addElementToPositionBefore
        Integer added = client.setMode(Mode.BACKUP)
                .addElementToPositionBefore(key, keyHint,
                        Arrays.asList(
                                Payload.of("before1".getBytes(StandardCharsets.UTF_8)),
                                Payload.of("before2".getBytes(StandardCharsets.UTF_8))
                        ),
                        Payload.of("pivot".getBytes(StandardCharsets.UTF_8)))
                .get();
        assertNotNull(added);
        assertEquals(2, added);

        Thread.sleep(300); // replication delay

        // Verify on BACKUP: streamList returns [before1, before2, pivot]
        List<Payload> backupStream = client.setMode(Mode.BACKUP)
                .streamList(key, keyHint)
                .get();
        assertEquals(3, backupStream.size());
        assertEquals("before1", new String(backupStream.get(0).getValue(), StandardCharsets.UTF_8));

        Thread.sleep(300); // replication delay

        // Verify on MASTER: streamList returns [before1, before2, pivot]
        List<Payload> masterStream = client.setMode(Mode.MASTER)
                .streamList(key, keyHint)
                .get();
        assertEquals(3, masterStream.size());
        assertEquals("before1", new String(masterStream.get(0).getValue(), StandardCharsets.UTF_8));

        // Write on MASTER: addElementToPositionBefore before before1
        client.setMode(Mode.MASTER)
                .addElementToPositionBefore(key, keyHint,
                        Arrays.asList(Payload.of("before3".getBytes(StandardCharsets.UTF_8))),
                        Payload.of("before1".getBytes(StandardCharsets.UTF_8)))
                .get();

        Thread.sleep(300); // replication delay

        // Verify on MASTER: streamList returns [before3, before1, before2, pivot]
        List<Payload> masterStreamAfter = client.setMode(Mode.MASTER)
                .streamList(key, keyHint)
                .get();
        assertEquals(4, masterStreamAfter.size());
        assertEquals("before3", new String(masterStreamAfter.get(0).getValue(), StandardCharsets.UTF_8));

        Thread.sleep(300); // replication delay

        // Verify on BACKUP: streamList returns [before3, before1, before2, pivot]
        List<Payload> backupStreamAfter = client.setMode(Mode.BACKUP)
                .streamList(key, keyHint)
                .get();
        assertEquals(4, backupStreamAfter.size());
        assertEquals("before3", new String(backupStreamAfter.get(0).getValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("AddElementToPositionAfter on master, verify replication and write on backup")
    void testAddElementToPositionAfter() throws ExecutionException, InterruptedException {
        String key = "addElementToPositionAfterTest" + UUID.randomUUID();
        List<Payload> initialData = Arrays.asList(
                Payload.of("pivot".getBytes(StandardCharsets.UTF_8))
        );

        // Create list
        KeyHintData keyHint = client.createList(key, initialData)
                .get();

        Thread.sleep(500); // replication wait

        // Write on BACKUP: addElementToPositionAfter
        Integer added = client.setMode(Mode.BACKUP)
                .addElementToPositionAfter(key, keyHint,
                        Arrays.asList(
                                Payload.of("after1".getBytes(StandardCharsets.UTF_8)),
                                Payload.of("after2".getBytes(StandardCharsets.UTF_8))
                        ),
                        Payload.of("pivot".getBytes(StandardCharsets.UTF_8)))
                .get();
        assertNotNull(added);
        assertEquals(2, added);

        Thread.sleep(300); // replication delay

        // Verify on BACKUP: streamList returns [pivot, after1, after2]
        List<Payload> backupStream = client.setMode(Mode.BACKUP)
                .streamList(key, keyHint)
                .get();
        assertEquals(3, backupStream.size());
        assertEquals("pivot", new String(backupStream.get(0).getValue(), StandardCharsets.UTF_8));

        Thread.sleep(300); // replication delay

        // Verify on MASTER: streamList returns [pivot, after1, after2]
        List<Payload> masterStream = client.setMode(Mode.MASTER)
                .streamList(key, keyHint)
                .get();
        assertEquals(3, masterStream.size());
        assertEquals("pivot", new String(masterStream.get(0).getValue(), StandardCharsets.UTF_8));

        // Write on MASTER: addElementToPositionAfter after after1
        client.setMode(Mode.MASTER)
                .addElementToPositionAfter(key, keyHint,
                        Arrays.asList(Payload.of("after3".getBytes(StandardCharsets.UTF_8))),
                        Payload.of("after1".getBytes(StandardCharsets.UTF_8)))
                .get();

        Thread.sleep(300); // replication delay

        // Verify on MASTER: streamList returns [pivot, after1, after3, after2]
        List<Payload> masterStreamAfter = client.setMode(Mode.MASTER)
                .streamList(key, keyHint)
                .get();
        assertEquals(4, masterStreamAfter.size());
        assertEquals("after3", new String(masterStreamAfter.get(2).getValue(), StandardCharsets.UTF_8));

        Thread.sleep(300); // replication delay

        // Verify on BACKUP: streamList returns [pivot, after1, after3, after2]
        List<Payload> backupStreamAfter = client.setMode(Mode.BACKUP)
                .streamList(key, keyHint)
                .get();
        assertEquals(4, backupStreamAfter.size());
        assertEquals("after3", new String(backupStreamAfter.get(2).getValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("Remove list on master, verify deletion on backup and write on backup")
    void testRemoveList() throws ExecutionException, InterruptedException {
        String key = "removeListTest" + UUID.randomUUID();

        // Create list
        KeyHintData keyHint = client.createList(key, Arrays.asList(Payload.of("data".getBytes(StandardCharsets.UTF_8))))
                .get();

        Thread.sleep(500); // replication wait

        // Write on BACKUP: remove
        Boolean removed = client.setMode(Mode.BACKUP)
                .remove(key, keyHint)
                .get();
        assertTrue(removed);

        Thread.sleep(300); // replication delay

        // Verify on BACKUP: streamList throws NOT_FOUND
        try {
            client.setMode(Mode.BACKUP)
                    .streamList(key, keyHint)
                    .get();
            fail("Expected NOT_FOUND on backup");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }

        Thread.sleep(300); // replication delay

        // Verify on MASTER: streamList throws NOT_FOUND
        try {
            client.setMode(Mode.MASTER)
                    .streamList(key, keyHint)
                    .get();
            fail("Expected NOT_FOUND on master");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }

        // Write on MASTER: create new list
        KeyHintData keyHint2 = client.createList(key, Arrays.asList(Payload.of("data2".getBytes(StandardCharsets.UTF_8))))
                .get();

        Thread.sleep(500); // replication wait

        // Write on BACKUP: remove again
        Boolean removed2 = client.setMode(Mode.BACKUP)
                .remove(key, keyHint2)
                .get();
        assertTrue(removed2);

        Thread.sleep(300); // replication delay

        // Verify on BACKUP: streamList throws NOT_FOUND
        try {
            client.setMode(Mode.BACKUP)
                    .streamList(key, keyHint2)
                    .get();
            fail("Expected NOT_FOUND on backup");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }

        Thread.sleep(300); // replication delay

        // Verify on MASTER: streamList throws NOT_FOUND
        try {
            client.setMode(Mode.MASTER)
                    .streamList(key, keyHint2)
                    .get();
            fail("Expected NOT_FOUND on master");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    // ==================== Section 4: Admin Operations ====================

    @Test
    @DisplayName("SetTtl on master, verify GetTtl on both nodes and write on backup")
    void testSetTtlAndGetTtl() throws ExecutionException, InterruptedException {
        String key = "setTtlTest" + UUID.randomUUID();

        // Create list
        KeyHintData keyHint = client.createList(key, Arrays.asList(Payload.of("data".getBytes(StandardCharsets.UTF_8))))
                .get();

        Thread.sleep(500); // replication wait

        // Write on BACKUP: setTtl 300000ms (5 minutes)
        Boolean ttlSet = client.setMode(Mode.BACKUP)
                .setTtl(key, keyHint, 300000)
                .get();
        assertTrue(ttlSet);

        Thread.sleep(300); // replication delay

        // Verify on BACKUP: getTtl returns ~300000ms (with tolerance for elapsed time)
        Long ttlBackup = client.setMode(Mode.BACKUP)
                .getTtl(key, keyHint)
                .get();
        assertNotNull(ttlBackup);
        assertTrue(ttlBackup >= 299000 && ttlBackup <= 300000,
                "Expected TTL ~300000ms on backup, got " + ttlBackup);

        Thread.sleep(300); // replication delay

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

        Thread.sleep(300); // replication delay

        // Verify on MASTER: getTtl returns ~600000ms (with tolerance for elapsed time)
        Long ttlMaster2 = client.setMode(Mode.MASTER)
                .getTtl(key, keyHint)
                .get();
        assertNotNull(ttlMaster2);
        assertTrue(ttlMaster2 >= 599000 && ttlMaster2 <= 600000,
                "Expected TTL ~600000ms on master, got " + ttlMaster2);

        Thread.sleep(300); // replication delay

        // Verify on BACKUP: getTtl returns ~600000ms (with tolerance for elapsed time)
        Long ttlBackup2 = client.setMode(Mode.BACKUP)
                .getTtl(key, keyHint)
                .get();
        assertNotNull(ttlBackup2);
        assertTrue(ttlBackup2 >= 598000 && ttlBackup2 <= 600000,
                "Expected TTL ~600000ms on backup, got " + ttlBackup2);
    }

    @Test
    @DisplayName("TTL expiration: set on master, verify both nodes after TTL expires")
    void testTtlExpirationOnMaster() throws ExecutionException, InterruptedException {
        String key = "ttlExpirationListMaster" + UUID.randomUUID();

        // Create list with 2s TTL
        KeyHintData keyHint = client.createList(key.getBytes(StandardCharsets.UTF_8),
                        Arrays.asList(Payload.of("data".getBytes(StandardCharsets.UTF_8))),
                        Duration.ofSeconds(2))
                .get();

        Thread.sleep(500); // replication wait

        // Verify on MASTER: getTtl > 0
        Long ttlMaster = client.setMode(Mode.MASTER)
                .getTtl(key, keyHint)
                .get();
        assertNotNull(ttlMaster);
        assertTrue(ttlMaster > 0, "Expected TTL > 0 on master");

        Thread.sleep(300); // replication delay

        // Verify on BACKUP: getTtl > 0
        Long ttlBackup = client.setMode(Mode.BACKUP)
                .getTtl(key, keyHint)
                .get();
        assertNotNull(ttlBackup);
        assertTrue(ttlBackup > 0, "Expected TTL > 0 on backup");

        // Wait for TTL to expire
        Thread.sleep(3000);

        // Verify on MASTER: getHead throws NOT_FOUND
        try {
            client.setMode(Mode.MASTER)
                    .getHead(key, keyHint)
                    .get();
            fail("Expected NOT_FOUND on master after TTL expiry");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }

        Thread.sleep(300); // replication delay

        // Verify on BACKUP: getHead throws NOT_FOUND
        try {
            client.setMode(Mode.BACKUP)
                    .getHead(key, keyHint)
                    .get();
            fail("Expected NOT_FOUND on backup after TTL expiry");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    @Test
    @DisplayName("TTL expiration: set on backup, verify both nodes after TTL expires")
    void testTtlExpirationOnBackup() throws ExecutionException, InterruptedException {
        String key = "ttlExpirationListBackup" + UUID.randomUUID();

        // Create list on BACKUP with 2s TTL
        KeyHintData keyHint = client.setMode(Mode.BACKUP)
                .createList(key.getBytes(StandardCharsets.UTF_8),
                        Arrays.asList(Payload.of("data".getBytes(StandardCharsets.UTF_8))),
                        Duration.ofSeconds(2))
                .get();

        Thread.sleep(500); // replication wait

        // Verify on BACKUP: getTtl > 0
        Long ttlBackup = client.setMode(Mode.BACKUP)
                .getTtl(key, keyHint)
                .get();
        assertNotNull(ttlBackup);
        assertTrue(ttlBackup > 0, "Expected TTL > 0 on backup");

        Thread.sleep(300); // replication delay

        // Verify on MASTER: getTtl > 0
        Long ttlMaster = client.setMode(Mode.MASTER)
                .getTtl(key, keyHint)
                .get();
        assertNotNull(ttlMaster);
        assertTrue(ttlMaster > 0, "Expected TTL > 0 on master");

        // Wait for TTL to expire
        Thread.sleep(3000);

        // Verify on BACKUP: getHead throws NOT_FOUND
        try {
            client.setMode(Mode.BACKUP)
                    .getHead(key, keyHint)
                    .get();
            fail("Expected NOT_FOUND on backup after TTL expiry");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }

        Thread.sleep(300); // replication delay

        // Verify on MASTER: getHead throws NOT_FOUND
        try {
            client.setMode(Mode.MASTER)
                    .getHead(key, keyHint)
                    .get();
            fail("Expected NOT_FOUND on master after TTL expiry");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    // ==================== Section 5: Lock Operations ====================

    @Test
    @DisplayName("LockObject READ_LOCK: parallel reads OK, writes denied, unlock by owner only")
    void testLockObjectReadLock() throws ExecutionException, InterruptedException {
        String key = "readLockTest" + UUID.randomUUID();

        // Create list
        KeyHintData keyHint = client.createList(key, Arrays.asList(Payload.of("data".getBytes(StandardCharsets.UTF_8))))
                .get();

        Thread.sleep(500); // replication wait

        // Write on BACKUP: lockObject READ_LOCK
        LockStatus lockStatus = client.setMode(Mode.BACKUP)
                .lockObject(key, keyHint, LockType.READ_LOCK, 1, Duration.ofSeconds(60))
                .get();
        assertEquals(LockStatus.OK, lockStatus);

        Thread.sleep(300); // replication delay

        // Verify on BACKUP: client 1 can read (getHead)
        Payload backupHead = client.setMode(Mode.BACKUP)
                .getHead(key, keyHint, 1)
                .get();
        assertNotNull(backupHead);

        Thread.sleep(300); // replication delay

        // Verify on MASTER: client 1 can read (getHead)
        Payload masterHead = client.setMode(Mode.MASTER)
                .getHead(key, keyHint, 1)
                .get();
        assertNotNull(masterHead);

        // Write on BACKUP: client 2 tries to read (should succeed with READ_LOCK)
        Payload backupHead2 = client.setMode(Mode.BACKUP)
                .getHead(key, keyHint, 2)
                .get();
        assertNotNull(backupHead2);

        // Write on BACKUP: client 2 tries to write (should fail with PERMISSION_DENIED)
        try {
            client.setMode(Mode.BACKUP)
                    .addElementToTail(key, keyHint, Arrays.asList(Payload.of("fail".getBytes(StandardCharsets.UTF_8))))
                    .get();
            fail("Expected PERMISSION_DENIED for write with READ_LOCK");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.PERMISSION_DENIED, cause.getStatus().getCode());
        }

        // Write on BACKUP: unlockObject by owner (client 1)
        LockStatus unlockStatus = client.setMode(Mode.BACKUP)
                .unlockObject(key, keyHint, 1)
                .get();
        assertEquals(LockStatus.OK, unlockStatus);

        Thread.sleep(300); // replication delay

        // Re-lock by owner 1 to test CANT_UNLOCK
        LockStatus lockStatus2 = client.setMode(Mode.BACKUP)
                .lockObject(key, keyHint, LockType.WRITE_LOCK, 1, Duration.ofSeconds(60))
                .get();
        assertEquals(LockStatus.OK, lockStatus2);
        Thread.sleep(300); // replication delay
        // Write on MASTER: unlockObject by non-owner (client 999) should fail
        LockStatus failedUnlock = client.setMode(Mode.MASTER)
                .unlockObject(key, keyHint, 999)
                .get();
        assertEquals(LockStatus.CANT_UNLOCK, failedUnlock);

        // Write on MASTER: unlockObject by owner (client 1) should succeed
        LockStatus successUnlock = client.setMode(Mode.MASTER)
                .unlockObject(key, keyHint, 1)
                .get();
        assertEquals(LockStatus.OK, successUnlock);
    }

    @Test
    @DisplayName("LockObject WRITE_LOCK: owner read/write OK, others denied, unlock by owner only")
    void testLockObjectWriteLock() throws ExecutionException, InterruptedException {
        String key = "writeLockTest" + UUID.randomUUID();

        // Create list
        KeyHintData keyHint = client.createList(key, Arrays.asList(Payload.of("data".getBytes(StandardCharsets.UTF_8))))
                .get();

        Thread.sleep(500); // replication wait

        // Write on BACKUP: lockObject WRITE_LOCK
        LockStatus lockStatus = client.setMode(Mode.BACKUP)
                .lockObject(key, keyHint, LockType.WRITE_LOCK, 1, Duration.ofSeconds(60))
                .get();
        assertEquals(LockStatus.OK, lockStatus);

        Thread.sleep(300); // replication delay

        // Verify on BACKUP: client 1 can write (addElementToTail)
        client.setMode(Mode.BACKUP)
                .addElementToTail(key, keyHint, Arrays.asList(Payload.of("added".getBytes(StandardCharsets.UTF_8))),1)
                .get();

        Thread.sleep(300); // replication delay

        // Verify on MASTER: client 1 can read (getHead)
        Payload masterHead = client.setMode(Mode.MASTER)
                .getHead(key, keyHint, 1)
                .get();
        assertNotNull(masterHead);

        // Write on BACKUP: client 2 tries to read (should fail with PERMISSION_DENIED)
        try {
            client.setMode(Mode.BACKUP)
                    .getHead(key, keyHint, 2)
                    .get();
            fail("Expected PERMISSION_DENIED for read with WRITE_LOCK");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.PERMISSION_DENIED, cause.getStatus().getCode());
        }

        // Write on BACKUP: client 2 tries to write (should fail with PERMISSION_DENIED)
        try {
            client.setMode(Mode.BACKUP)
                    .addElementToTail(key, keyHint, Arrays.asList(Payload.of("fail".getBytes(StandardCharsets.UTF_8))),2)
                    .get();
            fail("Expected PERMISSION_DENIED for write with WRITE_LOCK");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.PERMISSION_DENIED, cause.getStatus().getCode());
        }

        // Write on BACKUP: unlockObject by owner (client 1)
        LockStatus unlockStatus = client.setMode(Mode.BACKUP)
                .unlockObject(key, keyHint, 1)
                .get();
        assertEquals(LockStatus.OK, unlockStatus);

        Thread.sleep(300); // replication delay

        // Re-lock by owner 1 to test CANT_UNLOCK
        LockStatus lockStatus2 = client.setMode(Mode.BACKUP)
                .lockObject(key, keyHint, LockType.WRITE_LOCK, 1, Duration.ofSeconds(60))
                .get();
        assertEquals(LockStatus.OK, lockStatus2);
        Thread.sleep(300); // replication delay
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
    @DisplayName("LockObject GLOBAL: only owner any operations, others denied, unlock by owner only")
    void testLockObjectGlobalLock() throws ExecutionException, InterruptedException {
        String key = "globalLockTest" + UUID.randomUUID();

        // Create list
        KeyHintData keyHint = client.createList(key, Arrays.asList(Payload.of("data".getBytes(StandardCharsets.UTF_8))))
                .get();

        Thread.sleep(500); // replication wait

        // Write on BACKUP: lockObject GLOBAL
        LockStatus lockStatus = client.setMode(Mode.BACKUP)
                .lockObject(key, keyHint, LockType.GLOBAL, 1, Duration.ofSeconds(60))
                .get();
        assertEquals(LockStatus.OK, lockStatus);

        Thread.sleep(300); // replication delay

        // Verify on BACKUP: client 1 can read (getHead)
        Payload backupHead = client.setMode(Mode.BACKUP)
                .getHead(key, keyHint, 1)
                .get();
        assertNotNull(backupHead);

        Thread.sleep(300); // replication delay

        // Verify on MASTER: client 1 can read (getHead)
        Payload masterHead = client.setMode(Mode.MASTER)
                .getHead(key, keyHint, 1)
                .get();
        assertNotNull(masterHead);

        // Write on BACKUP: client 2 tries to read (should fail with PERMISSION_DENIED)
        try {
            client.setMode(Mode.BACKUP)
                    .getHead(key, keyHint, 2)
                    .get();
            fail("Expected PERMISSION_DENIED for read with GLOBAL_LOCK");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.PERMISSION_DENIED, cause.getStatus().getCode());
        }

        // Write on BACKUP: client 2 tries to write (should fail with PERMISSION_DENIED)
        try {
            client.setMode(Mode.BACKUP)
                    .addElementToTail(key, keyHint, Arrays.asList(Payload.of("fail".getBytes(StandardCharsets.UTF_8))))
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

        Thread.sleep(300); // replication delay

        // Re-lock by owner 1 to test CANT_UNLOCK
        LockStatus lockStatus2 = client.setMode(Mode.BACKUP)
                .lockObject(key, keyHint, LockType.GLOBAL, 1, Duration.ofSeconds(60))
                .get();
        assertEquals(LockStatus.OK, lockStatus2);
        Thread.sleep(300); // replication delay
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
        String key = "unlockOwnerTest" + UUID.randomUUID();

        // Create list
        KeyHintData keyHint = client.createList(key, Arrays.asList(Payload.of("data".getBytes(StandardCharsets.UTF_8))))
                .get();

        Thread.sleep(500); // replication wait

        // Lock by owner clientId=400
        LockStatus lockStatus = client.setMode(Mode.BACKUP)
                .lockObject(key, keyHint, LockType.WRITE_LOCK, 400, Duration.ofSeconds(60))
                .get();
        assertEquals(LockStatus.OK, lockStatus);

        Thread.sleep(300); // replication delay

        // Try unlock by non-owner (clientId=999) should fail with CANT_UNLOCK
        LockStatus failedUnlock = client.setMode(Mode.BACKUP)
                .unlockObject(key, keyHint, 999)
                .get();
        assertEquals(LockStatus.CANT_UNLOCK, failedUnlock);

        Thread.sleep(300); // replication delay

        // Verify on MASTER: lockObject by non-owner should still fail (lock still active)
        LockStatus cantLock = client.setMode(Mode.MASTER)
                .lockObject(key, keyHint, LockType.WRITE_LOCK, 999, Duration.ofSeconds(60))
                .get();
        assertEquals(LockStatus.CANT_LOCK, cantLock);

        Thread.sleep(300); // replication delay

        // Unlock by owner (clientId=400) should succeed
        LockStatus successUnlock = client.setMode(Mode.BACKUP)
                .unlockObject(key, keyHint, 400)
                .get();
        assertEquals(LockStatus.OK, successUnlock);
    }

    @Test
    @DisplayName("Lock expiration: set short TTL lock on master, verify both nodes after lock expires")
    void testLockExpirationOnMaster() throws ExecutionException, InterruptedException {
        String key = "lockExpirationMaster" + UUID.randomUUID();

        // Create list
        KeyHintData keyHint = client.createList(key, Arrays.asList(Payload.of("data".getBytes(StandardCharsets.UTF_8))))
                .get();

        Thread.sleep(500); // replication wait

        // Write on MASTER: lockObject with 2s TTL
        LockStatus lockStatus = client.setMode(Mode.MASTER)
                .lockObject(key, keyHint, LockType.WRITE_LOCK, 1, Duration.ofSeconds(2))
                .get();
        assertEquals(LockStatus.OK, lockStatus);

        Thread.sleep(300); // replication delay

        // Verify on MASTER: client 1 can write (addElementToTail)
        client.setMode(Mode.MASTER)
                .addElementToTail(key, keyHint, Arrays.asList(Payload.of("added".getBytes(StandardCharsets.UTF_8))), 1)
                .get();

        Thread.sleep(300); // replication delay

        // Verify on BACKUP: streamList shows lock is replicated (clientId=1)
        List<Payload> backupStream = client.setMode(Mode.BACKUP)
                .streamList(key.getBytes(StandardCharsets.UTF_8), keyHint, 1, Duration.ofSeconds(5))
                .get();
        assertEquals(2, backupStream.size());

        // Wait for lock TTL to expire (2s lock + buffer)
        Thread.sleep(3500);

        // Verify on MASTER: getHead succeeds (lock expired, data accessible)
        Payload masterHead = client.setMode(Mode.MASTER)
                .getHead(key, keyHint, 1)
                .get();
        assertNotNull(masterHead);

        Thread.sleep(300); // replication delay

        // Verify on BACKUP: getHead succeeds (lock expired, data replicated)
        Payload backupHead = client.setMode(Mode.BACKUP)
                .getHead(key, keyHint)
                .get();
        assertNotNull(backupHead);

        // Verify on BACKUP: client 2 can read/write (lock no longer active)
        Payload backupHead2 = client.setMode(Mode.BACKUP)
                .getHead(key, keyHint, 2)
                .get();
        assertNotNull(backupHead2);
    }

    @Test
    @DisplayName("Lock expiration: set short TTL lock on backup, verify both nodes after lock expires")
    void testLockExpirationOnBackup() throws ExecutionException, InterruptedException {
        String key = "lockExpirationBackup" + UUID.randomUUID();

        // Create list on BACKUP
        KeyHintData keyHint = client.setMode(Mode.BACKUP)
                .createList(key.getBytes(StandardCharsets.UTF_8), Arrays.asList(Payload.of("data".getBytes(StandardCharsets.UTF_8))))
                .get();

        Thread.sleep(500); // replication wait

        // Write on BACKUP: lockObject with 2s TTL
        LockStatus lockStatus = client.setMode(Mode.BACKUP)
                .lockObject(key, keyHint, LockType.WRITE_LOCK, 1, Duration.ofSeconds(2))
                .get();
        assertEquals(LockStatus.OK, lockStatus);

        Thread.sleep(300); // replication delay

        // Verify on BACKUP: client 1 can write (addElementToTail)
        client.setMode(Mode.BACKUP)
                .addElementToTail(key, keyHint, Arrays.asList(Payload.of("added".getBytes(StandardCharsets.UTF_8))), 1)
                .get();

        Thread.sleep(300); // replication delay

        // Verify on MASTER: streamList shows lock is replicated (clientId=1)
        List<Payload> masterStream = client.setMode(Mode.MASTER)
                .streamList(key.getBytes(StandardCharsets.UTF_8), keyHint, 1, Duration.ofSeconds(5))
                .get();
        assertEquals(2, masterStream.size());

        // Wait for lock TTL to expire (2s lock + buffer)
        Thread.sleep(3500);

        // Verify on BACKUP: getHead succeeds (lock expired, data accessible)
        Payload backupHead = client.setMode(Mode.BACKUP)
                .getHead(key, keyHint, 1)
                .get();
        assertNotNull(backupHead);

        Thread.sleep(300); // replication delay

        // Verify on MASTER: getHead succeeds (lock expired, data replicated)
        Payload masterHead = client.setMode(Mode.MASTER)
                .getHead(key, keyHint, 1)
                .get();
        assertNotNull(masterHead);

        // Verify on MASTER: client 2 can read/write (lock no longer active)
        Payload masterHead2 = client.setMode(Mode.MASTER)
                .getHead(key, keyHint, 2)
                .get();
        assertNotNull(masterHead2);
    }

    // ==================== Section 6: Edge Cases ====================

    @Test
    @DisplayName("GetAndRemoveFront on single element: list becomes empty")
    void testGetAndRemoveFrontOnSingleElement() throws ExecutionException, InterruptedException {
        String key = "getAndRemoveFrontSingle" + UUID.randomUUID();

        // Create list with single element
        KeyHintData keyHint = client.createList(key, Arrays.asList(Payload.of("only".getBytes(StandardCharsets.UTF_8))))
                .get();

        Thread.sleep(500); // replication wait

        // Write on BACKUP: getAndRemoveFront
        Payload removed = client.setMode(Mode.BACKUP)
                .getAndRemoveFront(key, keyHint)
                .get();
        assertNotNull(removed);
        assertEquals("only", new String(removed.getValue(), StandardCharsets.UTF_8));

        Thread.sleep(300); // replication delay

        // Verify on BACKUP: streamList returns empty list
        List<Payload> backupStream = client.setMode(Mode.BACKUP)
                .streamList(key, keyHint)
                .get();
        assertNotNull(backupStream);
        assertTrue(backupStream.isEmpty());

        Thread.sleep(300); // replication delay

        // Verify on MASTER: streamList returns empty list
        List<Payload> masterStream = client.setMode(Mode.MASTER)
                .streamList(key, keyHint)
                .get();
        assertNotNull(masterStream);
        assertTrue(masterStream.isEmpty());
    }

    @Test
    @DisplayName("GetHead on single element: head equals the only element")
    void testGetHeadOnSingleElement() throws ExecutionException, InterruptedException {
        String key = "getHeadSingle" + UUID.randomUUID();

        // Create list with single element
        KeyHintData keyHint = client.createList(key, Arrays.asList(Payload.of("only".getBytes(StandardCharsets.UTF_8))))
                .get();

        Thread.sleep(500); // replication wait

        // Write on BACKUP: getHead
        Payload head = client.setMode(Mode.BACKUP)
                .getHead(key, keyHint)
                .get();
        assertNotNull(head);
        assertEquals("only", new String(head.getValue(), StandardCharsets.UTF_8));

        Thread.sleep(300); // replication delay

        // Verify on MASTER: getHead returns same element
        Payload masterHead = client.setMode(Mode.MASTER)
                .getHead(key, keyHint)
                .get();
        assertNotNull(masterHead);
        assertEquals("only", new String(masterHead.getValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("GetTail on single element: tail equals the only element")
    void testGetTailOnSingleElement() throws ExecutionException, InterruptedException {
        String key = "getTailSingle" + UUID.randomUUID();

        // Create list with single element
        KeyHintData keyHint = client.createList(key, Arrays.asList(Payload.of("only".getBytes(StandardCharsets.UTF_8))))
                .get();

        Thread.sleep(500); // replication wait

        // Write on BACKUP: getTail
        Payload tail = client.setMode(Mode.BACKUP)
                .getTail(key, keyHint)
                .get();
        assertNotNull(tail);
        assertEquals("only", new String(tail.getValue(), StandardCharsets.UTF_8));

        Thread.sleep(300); // replication delay

        // Verify on MASTER: getTail returns same element
        Payload masterTail = client.setMode(Mode.MASTER)
                .getTail(key, keyHint)
                .get();
        assertNotNull(masterTail);
        assertEquals("only", new String(masterTail.getValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("GetAndRemoveTail on single element: list becomes empty")
    void testGetAndRemoveTailOnSingleElement() throws ExecutionException, InterruptedException {
        String key = "getAndRemoveTailSingle" + UUID.randomUUID();

        // Create list with single element
        KeyHintData keyHint = client.createList(key, Arrays.asList(Payload.of("only".getBytes(StandardCharsets.UTF_8))))
                .get();

        Thread.sleep(500); // replication wait

        // Write on BACKUP: getAndRemoveTail
        Payload removed = client.setMode(Mode.BACKUP)
                .getAndRemoveTail(key, keyHint)
                .get();
        assertNotNull(removed);
        assertEquals("only", new String(removed.getValue(), StandardCharsets.UTF_8));

        Thread.sleep(300); // replication delay

        // Verify on BACKUP: streamList returns empty list
        List<Payload> backupStream = client.setMode(Mode.BACKUP)
                .streamList(key, keyHint)
                .get();
        assertNotNull(backupStream);
        assertTrue(backupStream.isEmpty());

        Thread.sleep(300); // replication delay

        // Verify on MASTER: streamList returns empty list
        List<Payload> masterStream = client.setMode(Mode.MASTER)
                .streamList(key, keyHint)
                .get();
        assertNotNull(masterStream);
        assertTrue(masterStream.isEmpty());
    }

    @Test
    @DisplayName("GetElementAtPosition position 0 (first element)")
    void testGetElementAtPositionZero() throws ExecutionException, InterruptedException {
        String key = "getElementAtPositionZero" + UUID.randomUUID();

        // Create list
        KeyHintData keyHint = client.createList(key, Arrays.asList(
                Payload.of("first".getBytes(StandardCharsets.UTF_8)),
                Payload.of("second".getBytes(StandardCharsets.UTF_8))
        )).get();

        Thread.sleep(500); // replication wait

        // Write on BACKUP: getElementAtPosition(key, 0)
        Payload pos0 = client.setMode(Mode.BACKUP)
                .getElementAtPosition(key, keyHint, 0)
                .get();
        assertNotNull(pos0);
        assertEquals("first", new String(pos0.getValue(), StandardCharsets.UTF_8));

        Thread.sleep(300); // replication delay

        // Verify on MASTER: getElementAtPosition(key, 0)
        Payload masterPos0 = client.setMode(Mode.MASTER)
                .getElementAtPosition(key, keyHint, 0)
                .get();
        assertNotNull(masterPos0);
        assertEquals("first", new String(masterPos0.getValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("GetElementAtPosition last position")
    void testGetElementAtPositionLast() throws ExecutionException, InterruptedException {
        String key = "getElementAtPositionLast" + UUID.randomUUID();

        // Create list
        KeyHintData keyHint = client.createList(key, Arrays.asList(
                Payload.of("first".getBytes(StandardCharsets.UTF_8)),
                Payload.of("second".getBytes(StandardCharsets.UTF_8))
        )).get();

        Thread.sleep(500); // replication wait

        // Write on BACKUP: getElementAtPosition(key, 1)
        Payload last = client.setMode(Mode.BACKUP)
                .getElementAtPosition(key, keyHint, 1)
                .get();
        assertNotNull(last);
        assertEquals("second", new String(last.getValue(), StandardCharsets.UTF_8));

        Thread.sleep(300); // replication delay

        // Verify on MASTER: getElementAtPosition(key, 1)
        Payload masterLast = client.setMode(Mode.MASTER)
                .getElementAtPosition(key, keyHint, 1)
                .get();
        assertNotNull(masterLast);
        assertEquals("second", new String(masterLast.getValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("GetAndRemoveElementAtPosition position 0 (first element)")
    void testGetAndRemoveElementAtPositionFirst() throws ExecutionException, InterruptedException {
        String key = "getAndRemoveElementAtPositionFirst" + UUID.randomUUID();

        // Create list
        KeyHintData keyHint = client.createList(key, Arrays.asList(
                Payload.of("first".getBytes(StandardCharsets.UTF_8)),
                Payload.of("second".getBytes(StandardCharsets.UTF_8)),
                Payload.of("third".getBytes(StandardCharsets.UTF_8))
        )).get();

        Thread.sleep(500); // replication wait

        // Write on BACKUP: getAndRemoveElementAtPosition(key, 0)
        Payload removed = client.setMode(Mode.BACKUP)
                .getAndRemoveElementAtPosition(key, keyHint, 0)
                .get();
        assertNotNull(removed);
        assertEquals("first", new String(removed.getValue(), StandardCharsets.UTF_8));

        Thread.sleep(300); // replication delay

        // Verify on BACKUP: streamList returns [second, third]
        List<Payload> backupStream = client.setMode(Mode.BACKUP)
                .streamList(key, keyHint)
                .get();
        assertEquals(2, backupStream.size());
        assertEquals("second", new String(backupStream.get(0).getValue(), StandardCharsets.UTF_8));

        Thread.sleep(300); // replication delay

        // Verify on MASTER: streamList returns [second, third]
        List<Payload> masterStream = client.setMode(Mode.MASTER)
                .streamList(key, keyHint)
                .get();
        assertEquals(2, masterStream.size());
        assertEquals("second", new String(masterStream.get(0).getValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("AddElementToPosition at end (position == size)")
    void testAddElementToPositionAtEnd() throws ExecutionException, InterruptedException {
        String key = "addElementToPositionAtEnd" + UUID.randomUUID();

        // Create list
        KeyHintData keyHint = client.createList(key, Arrays.asList(
                Payload.of("first".getBytes(StandardCharsets.UTF_8)),
                Payload.of("second".getBytes(StandardCharsets.UTF_8))
        )).get();

        Thread.sleep(500); // replication wait

        // Write on BACKUP: addElementToPosition at position 2 (end)
        Integer added = client.setMode(Mode.BACKUP)
                .addElementToPosition(key, keyHint, Arrays.asList(Payload.of("last".getBytes(StandardCharsets.UTF_8))), 2)
                .get();
        assertNotNull(added);
        assertEquals(1, added);

        Thread.sleep(300); // replication delay

        // Verify on BACKUP: streamList returns [first, second, last]
        List<Payload> backupStream = client.setMode(Mode.BACKUP)
                .streamList(key, keyHint)
                .get();
        assertEquals(3, backupStream.size());
        assertEquals("last", new String(backupStream.get(2).getValue(), StandardCharsets.UTF_8));

        Thread.sleep(300); // replication delay

        // Verify on MASTER: streamList returns [first, second, last]
        List<Payload> masterStream = client.setMode(Mode.MASTER)
                .streamList(key, keyHint)
                .get();
        assertEquals(3, masterStream.size());
        assertEquals("last", new String(masterStream.get(2).getValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("AddElementToPositionBefore first element (before head)")
    void testAddElementToPositionBeforeFirst() throws ExecutionException, InterruptedException {
        String key = "addElementToPositionBeforeFirst" + UUID.randomUUID();

        // Create list
        KeyHintData keyHint = client.createList(key, Arrays.asList(
                Payload.of("first".getBytes(StandardCharsets.UTF_8)),
                Payload.of("second".getBytes(StandardCharsets.UTF_8))
        )).get();

        Thread.sleep(500); // replication wait

        // Write on BACKUP: addElementToPositionBefore "first"
        Integer added = client.setMode(Mode.BACKUP)
                .addElementToPositionBefore(key, keyHint,
                        Arrays.asList(Payload.of("inserted".getBytes(StandardCharsets.UTF_8))),
                        Payload.of("first".getBytes(StandardCharsets.UTF_8)))
                .get();
        assertNotNull(added);
        assertEquals(1, added);

        Thread.sleep(300); // replication delay

        // Verify on BACKUP: streamList returns [inserted, first, second]
        List<Payload> backupStream = client.setMode(Mode.BACKUP)
                .streamList(key, keyHint)
                .get();
        assertEquals(3, backupStream.size());
        assertEquals("inserted", new String(backupStream.get(0).getValue(), StandardCharsets.UTF_8));

        Thread.sleep(300); // replication delay

        // Verify on MASTER: streamList returns [inserted, first, second]
        List<Payload> masterStream = client.setMode(Mode.MASTER)
                .streamList(key, keyHint)
                .get();
        assertEquals(3, masterStream.size());
        assertEquals("inserted", new String(masterStream.get(0).getValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("AddElementToPositionAfter last element (after tail)")
    void testAddElementToPositionAfterLast() throws ExecutionException, InterruptedException {
        String key = "addElementToPositionAfterLast" + UUID.randomUUID();

        // Create list
        KeyHintData keyHint = client.createList(key, Arrays.asList(
                Payload.of("first".getBytes(StandardCharsets.UTF_8)),
                Payload.of("last".getBytes(StandardCharsets.UTF_8))
        )).get();

        Thread.sleep(500); // replication wait

        // Write on BACKUP: addElementToPositionAfter "last"
        Integer added = client.setMode(Mode.BACKUP)
                .addElementToPositionAfter(key, keyHint,
                        Arrays.asList(Payload.of("inserted".getBytes(StandardCharsets.UTF_8))),
                        Payload.of("last".getBytes(StandardCharsets.UTF_8)))
                .get();
        assertNotNull(added);
        assertEquals(1, added);

        Thread.sleep(300); // replication delay

        // Verify on BACKUP: streamList returns [first, last, inserted]
        List<Payload> backupStream = client.setMode(Mode.BACKUP)
                .streamList(key, keyHint)
                .get();
        assertEquals(3, backupStream.size());
        assertEquals("inserted", new String(backupStream.get(2).getValue(), StandardCharsets.UTF_8));

        Thread.sleep(300); // replication delay

        // Verify on MASTER: streamList returns [first, last, inserted]
        List<Payload> masterStream = client.setMode(Mode.MASTER)
                .streamList(key, keyHint)
                .get();
        assertEquals(3, masterStream.size());
        assertEquals("inserted", new String(masterStream.get(2).getValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("StreamElementInRangeUnordered full range [0, size-1]")
    void testStreamElementInRangeUnorderedFullRange() throws ExecutionException, InterruptedException {
        String key = "streamRangeFullRange" + UUID.randomUUID();

        // Create list with 5 elements
        List<Payload> initialData = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            initialData.add(Payload.of(String.valueOf(i).getBytes(StandardCharsets.UTF_8)));
        }
        KeyHintData keyHint = client.createList(key, initialData)
                .get();

        Thread.sleep(500); // replication wait

        // Write on BACKUP: streamElementInRangeUnordered(key, LIST, 0, 4)
        List<Payload> rangeData = client.setMode(Mode.BACKUP)
                .streamElementInRangeUnordered(key, keyHint, ContainerType.LIST, 0, 4)
                .get();
        assertNotNull(rangeData);
        assertEquals(5, rangeData.size());

        Thread.sleep(300); // replication delay

        // Verify on MASTER: streamElementInRangeUnordered returns 5 elements
        List<Payload> masterRange = client.setMode(Mode.MASTER)
                .streamElementInRangeUnordered(key, keyHint, ContainerType.LIST, 0, 4)
                .get();
        assertNotNull(masterRange);
        assertEquals(5, masterRange.size());
    }

    // ==================== Section 7: Error Cases (Non-existent Lists) ====================

    @Test
    @DisplayName("GetAndRemoveFront on non-existent list returns NOT_FOUND")
    void testGetAndRemoveFrontOnMissingList() {
        String key = "getAndRemoveFrontMissing" + UUID.randomUUID();

        try {
            client.getAndRemoveFront(key).get();
            fail("Expected NOT_FOUND");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail(e.getMessage());
        }
    }

    @Test
    @DisplayName("GetAndRemoveTail on non-existent list returns NOT_FOUND")
    void testGetAndRemoveTailOnMissingList() {
        String key = "getAndRemoveTailMissing" + UUID.randomUUID();

        try {
            client.getAndRemoveTail(key).get();
            fail("Expected NOT_FOUND");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail(e.getMessage());
        }
    }

    @Test
    @DisplayName("GetElementAtPosition on non-existent list returns NOT_FOUND")
    void testGetElementAtPositionOnMissingList() {
        String key = "getElementAtPositionMissing" + UUID.randomUUID();

        try {
            client.getElementAtPosition(key, 0).get();
            fail("Expected NOT_FOUND");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail(e.getMessage());
        }
    }

    @Test
    @DisplayName("GetAndRemoveElementAtPosition on non-existent list returns NOT_FOUND")
    void testGetAndRemoveElementAtPositionOnMissingList() {
        String key = "getAndRemoveElementAtPositionMissing" + UUID.randomUUID();

        try {
            client.getAndRemoveElementAtPosition(key, null, 0).get();
            fail("Expected NOT_FOUND");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail(e.getMessage());
        }
    }

    @Test
    @DisplayName("AddElementToPosition on non-existent list returns NOT_FOUND")
    void testAddElementToPositionOnMissingList() {
        String key = "addElementToPositionMissing" + UUID.randomUUID();

        try {
            client.addElementToPosition(key, null,
                    List.of(Payload.of("x".getBytes(StandardCharsets.UTF_8))), 0).get();
            fail("Expected NOT_FOUND");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail(e.getMessage());
        }
    }

    @Test
    @DisplayName("RemoveElementAtPosition on non-existent list returns NOT_FOUND")
    void testRemoveElementAtPositionOnMissingList() {
        String key = "removeElementAtPositionMissing" + UUID.randomUUID();

        try {
            client.removeElementAtPosition(key, null, 0, 1).get();
            fail("Expected NOT_FOUND");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail(e.getMessage());
        }
    }

    @Test
    @DisplayName("AddElementToPositionBefore on non-existent list returns NOT_FOUND")
    void testAddElementToPositionBeforeOnMissingList() {
        String key = "addElementToPositionBeforeMissing" + UUID.randomUUID();

        try {
            client.addElementToPositionBefore(key,
                    List.of(Payload.of("x".getBytes(StandardCharsets.UTF_8))),
                    Payload.of("pivot".getBytes(StandardCharsets.UTF_8))).get();
            fail("Expected NOT_FOUND");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail(e.getMessage());
        }
    }

    @Test
    @DisplayName("AddElementToPositionAfter on non-existent list returns NOT_FOUND")
    void testAddElementToPositionAfterOnMissingList() {
        String key = "addElementToPositionAfterMissing" + UUID.randomUUID();

        try {
            client.addElementToPositionAfter(key,
                    List.of(Payload.of("x".getBytes(StandardCharsets.UTF_8))),
                    Payload.of("pivot".getBytes(StandardCharsets.UTF_8))).get();
            fail("Expected NOT_FOUND");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail(e.getMessage());
        }
    }

    @Test
    @DisplayName("Remove on non-existent list returns NOT_FOUND")
    void testRemoveOnMissingList() {
        String key = "removeMissingList" + UUID.randomUUID();

        try {
            client.remove(key).get();
            fail("Expected NOT_FOUND");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail(e.getMessage());
        }
    }

    @Test
    @DisplayName("LockObject on non-existent list returns NOT_FOUND")
    void testLockOnMissingList() {
        String key = "lockMissingList" + UUID.randomUUID();

        try {
            client.lockObject(key, LockType.WRITE_LOCK, 0, Duration.ofSeconds(60)).get();
            fail("Expected NOT_FOUND");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail(e.getMessage());
        }
    }

    @Test
    @DisplayName("UnlockObject on non-existent list returns NOT_FOUND")
    void testUnlockOnMissingList() {
        String key = "unlockMissingList" + UUID.randomUUID();

        try {
            client.unlockObject(key).get();
            fail("Expected NOT_FOUND");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail(e.getMessage());
        }
    }

    @Test
    @DisplayName("SetTtl on non-existent list returns NOT_FOUND")
    void testSetTtlOnMissingList() {
        String key = "setTtlMissingList" + UUID.randomUUID();

        try {
            client.setTtl(key, 5000L).get();
            fail("Expected NOT_FOUND");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail(e.getMessage());
        }
    }

    @Test
    @DisplayName("GetTtl on non-existent list returns NOT_FOUND")
    void testGetTtlOnMissingList() {
        String key = "getTtlMissingList" + UUID.randomUUID();

        try {
            client.getTtl(key).get();
            fail("Expected NOT_FOUND");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail(e.getMessage());
        }
    }

    @Test
    @DisplayName("GetSize on non-existent list returns NOT_FOUND")
    void testGetSizeOnMissingList() {
        String key = "getSizeMissingList" + UUID.randomUUID();

        try {
            client.getSize(key).get();
            fail("Expected NOT_FOUND");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail(e.getMessage());
        }
    }

    @Test
    @DisplayName("StreamList on non-existent list returns NOT_FOUND")
    void testStreamListOnMissingList() {
        String key = "streamListMissing" + UUID.randomUUID();

        try {
            client.streamList(key).get();
            fail("Expected NOT_FOUND");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail(e.getMessage());
        }
    }

    @Test
    @DisplayName("GetHead on non-existent list returns NOT_FOUND")
    void testGetHeadOnMissingList() {
        String key = "getHeadMissingList" + UUID.randomUUID();

        try {
            client.getHead(key).get();
            fail("Expected NOT_FOUND");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail(e.getMessage());
        }
    }

    @Test
    @DisplayName("GetTail on non-existent list returns NOT_FOUND")
    void testGetTailOnMissingList() {
        String key = "getTailMissingList" + UUID.randomUUID();

        try {
            client.getTail(key).get();
            fail("Expected NOT_FOUND");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail(e.getMessage());
        }
    }

    // ==================== Section 8: Lock Edge Cases ====================

    @Test
    @DisplayName("LockObject NO_LOCK: no lock acquired, unlock succeeds")
    void testNoLockOnList() throws ExecutionException, InterruptedException {
        String key = "noLockList" + UUID.randomUUID();

        // Create list
        KeyHintData keyHint = client.createList(key, Arrays.asList(Payload.of("data".getBytes(StandardCharsets.UTF_8))))
                .get();

        Thread.sleep(500); // replication wait

        // Write on BACKUP: lockObject NO_LOCK
        LockStatus lockStatus = client.setMode(Mode.BACKUP)
                .lockObject(key, keyHint, LockType.NO_LOCK, 4, Duration.ofSeconds(60))
                .get();
        assertEquals(LockStatus.OK, lockStatus);

        Thread.sleep(300); // replication delay

        // Write on BACKUP: unlockObject should succeed
        LockStatus unlockStatus = client.setMode(Mode.BACKUP)
                .unlockObject(key, keyHint, 4)
                .get();
        assertEquals(LockStatus.OK, unlockStatus);
    }

    @Test
    @DisplayName("CANT_LOCK when list already locked by another client")
    void testCantLockOnListByOtherClient() throws ExecutionException, InterruptedException {
        String key = "cantLockList" + UUID.randomUUID();

        // Create list
        KeyHintData keyHint = client.createList(key, Arrays.asList(Payload.of("data".getBytes(StandardCharsets.UTF_8))))
                .get();

        Thread.sleep(500); // replication wait

        // Write on BACKUP: lock by owner clientId=5
        LockStatus lockStatus = client.setMode(Mode.BACKUP)
                .lockObject(key, keyHint, LockType.WRITE_LOCK, 5, Duration.ofSeconds(60))
                .get();
        assertEquals(LockStatus.OK, lockStatus);

        Thread.sleep(300); // replication delay

        // Write on BACKUP: intruder clientId=6 tries to lock -> CANT_LOCK
        LockStatus cantLock = client.setMode(Mode.BACKUP)
                .lockObject(key, keyHint, LockType.WRITE_LOCK, 6, Duration.ofSeconds(60))
                .get();
        assertEquals(LockStatus.CANT_LOCK, cantLock);

        Thread.sleep(300); // replication delay

        // Owner can unlock
        LockStatus unlockStatus = client.setMode(Mode.BACKUP)
                .unlockObject(key, keyHint, 5)
                .get();
        assertEquals(LockStatus.OK, unlockStatus);
    }

    @Test
    @DisplayName("UnlockObject after list deleted returns NOT_FOUND")
    void testUnlockAfterListDeleted() throws ExecutionException, InterruptedException {
        String key = "unlockAfterListDeleted" + UUID.randomUUID();
        int ownerId = 13;

        // Create list
        KeyHintData keyHint = client.createList(key, Arrays.asList(Payload.of("data".getBytes(StandardCharsets.UTF_8))))
                .get();

        Thread.sleep(500); // replication wait

        // Write on BACKUP: lockObject
        LockStatus lockStatus = client.setMode(Mode.BACKUP)
                .lockObject(key, keyHint, LockType.WRITE_LOCK, ownerId, Duration.ofSeconds(60))
                .get();
        assertEquals(LockStatus.OK, lockStatus);

        Thread.sleep(300); // replication delay

        // Write on BACKUP: remove list
        Boolean removed = client.setMode(Mode.BACKUP)
                .remove(key, keyHint,ownerId)
                .get();
        assertTrue(removed);

        Thread.sleep(300); // replication delay

        // Unlock returns NOT_FOUND
        try {
            client.setMode(Mode.BACKUP)
                    .unlockObject(key, ownerId)
                    .get();
            fail("Expected NOT_FOUND");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail(e.getMessage());
        }
    }

    @Test
    @DisplayName("RemoveFromContainer removes element by value from list")
    void testRemoveFromContainer() throws ExecutionException, InterruptedException {
        String vecKey = "list_remove" + UUID.randomUUID();
        List<Payload> initialData = Arrays.asList(
                Payload.of("item1".getBytes(StandardCharsets.UTF_8)),
                Payload.of("item2".getBytes(StandardCharsets.UTF_8)),
                Payload.of("item3".getBytes(StandardCharsets.UTF_8))
        );

        // Create list
        KeyHintData hint = client.createList(vecKey, initialData)
                .get();
        assertNotNull(hint);

        Thread.sleep(500); // replication wait

        // Write on BACKUP: removeFromContainer
        Integer removed = client.setMode(Mode.BACKUP)
                .removeFromContainer(vecKey.getBytes(StandardCharsets.UTF_8), hint,
                        "item1".getBytes(StandardCharsets.UTF_8))
                .get();
        assertEquals(1, removed, "1 element should be removed");

        Thread.sleep(300); // replication delay

        // Verify on BACKUP: streamList returns 2 elements
        List<Payload> result = client.setMode(Mode.BACKUP)
                .streamList(vecKey, hint)
                .get();
        assertEquals(2, result.size(), "List must contain 2 elements");

        Thread.sleep(300); // replication delay

        // Verify on MASTER: streamList returns 2 elements
        List<Payload> masterResult = client.setMode(Mode.MASTER)
                .streamList(vecKey, hint)
                .get();
        assertEquals(2, masterResult.size(), "List must contain 2 elements on master");
    }

    @Test
    @DisplayName("RemoveFromContainer returns 0 if element does not exist")
    void testRemoveFromContainerNonExistent() throws ExecutionException, InterruptedException {
        String vecKey = "list_remove_nonexistent" + UUID.randomUUID();
        List<Payload> initialData = Arrays.asList(
                Payload.of("item1".getBytes(StandardCharsets.UTF_8)),
                Payload.of("item2".getBytes(StandardCharsets.UTF_8))
        );

        // Create list
        KeyHintData hint = client.createList(vecKey, initialData)
                .get();
        assertNotNull(hint);

        Thread.sleep(500); // replication wait

        // Write on BACKUP: removeFromContainer non-existent element
        Integer removed = client.setMode(Mode.BACKUP)
                .removeFromContainer(vecKey.getBytes(StandardCharsets.UTF_8), hint,
                        "item123".getBytes(StandardCharsets.UTF_8))
                .get();
        assertEquals(0, removed, "0 elements should be removed (element not found)");

        Thread.sleep(300); // replication delay

        // Verify on BACKUP: streamList returns 2 elements (unchanged)
        List<Payload> result = client.setMode(Mode.BACKUP)
                .streamList(vecKey, hint)
                .get();
        assertEquals(2, result.size());
    }
}