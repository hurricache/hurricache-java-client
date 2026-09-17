package com.hurricache.client.cluster.smart;

import com.hurricache.TestBaseCluster;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.OrderedPayload;
import com.hurricache.client.intf.Payload;
import com.hurricache.client.intf.Mode;
import com.hurricache.grpc.LockStatus;
import com.hurricache.grpc.LockType;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class OrderedSetOperationsTest extends TestBaseCluster {

    private static final int OWNER_CLIENT_ID = 100;
    private static final int INTRUDER_CLIENT_ID = 200;
    private static final long REPLICATION_DELAY_MS = 100;

    // ==================== Section 1: Ordered Set Creation ====================

    @Test
    @DisplayName("Create empty ordered set on master, verify replication to backup")
    void testCreateEmptyOrderedSet() throws ExecutionException, InterruptedException {
        String key = "emptyOrderedSet" + UUID.randomUUID();

        // Create empty ordered set
        KeyHintData keyHint = client.createOrderedSet(key, new ArrayList<>())
                .get();

        Thread.sleep(REPLICATION_DELAY_MS); // replication wait

        // Verify on BACKUP: streamOrderedSet returns empty list
        List<OrderedPayload> backupStream = client.setMode(Mode.BACKUP)
                .streamOrderedSet(key, keyHint)
                .get();
        assertNotNull(backupStream);
        assertTrue(backupStream.isEmpty(), "Expected empty ordered set on backup");

        // Verify on MASTER: getSize returns 0
        Integer masterSize = client.setMode(Mode.MASTER)
                .getSize(key, keyHint)
                .get();
        assertNotNull(masterSize);
        assertEquals(0, masterSize, "Expected size 0 on master");
    }

    @Test
    @DisplayName("Create ordered set with data on master, verify replication to backup")
    void testCreateOrderedSetWithData() throws ExecutionException, InterruptedException {
        String key = "orderedSetWithData" + UUID.randomUUID();
        List<OrderedPayload> initialData = List.of(
                OrderedPayload.of(1L, bytes("1")),
                OrderedPayload.of(2L, bytes("2")),
                OrderedPayload.of(3L, bytes("3"))
        );

        // Create ordered set
        KeyHintData keyHint = client.createOrderedSet(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS); // replication wait

        // Verify on MASTER: streamOrderedSet returns 3 elements
        List<OrderedPayload> masterStream = client.setMode(Mode.MASTER)
                .streamOrderedSet(key, keyHint)
                .get();
        assertNotNull(masterStream);
        assertEquals(3, masterStream.size());

        // Verify ordering (ascending by weight)
        assertEquals(1L, masterStream.get(0).getOrder());
        assertEquals(2L, masterStream.get(1).getOrder());
        assertEquals(3L, masterStream.get(2).getOrder());

        // Verify on BACKUP: streamOrderedSet returns 3 elements
        List<OrderedPayload> backupStream = client.setMode(Mode.BACKUP)
                .streamOrderedSet(key, keyHint)
                .get();
        assertNotNull(backupStream);
        assertEquals(3, backupStream.size());
        assertEquals(1L, backupStream.get(0).getOrder());
        assertEquals(2L, backupStream.get(1).getOrder());
        assertEquals(3L, backupStream.get(2).getOrder());

        // Write on BACKUP: addElementWithWeight
        client.setMode(Mode.BACKUP)
                .addElementWithWeight(key, keyHint, List.of(OrderedPayload.of(4L, bytes("4"))))
                .get();

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on BACKUP: streamOrderedSet returns 4 elements
        List<OrderedPayload> backupAfterWrite = client.setMode(Mode.BACKUP)
                .streamOrderedSet(key, keyHint)
                .get();
        assertEquals(4, backupAfterWrite.size());
        assertEquals(4L, backupAfterWrite.get(3).getOrder());

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on MASTER: streamOrderedSet returns 4 elements
        List<OrderedPayload> masterAfterWrite = client.setMode(Mode.MASTER)
                .streamOrderedSet(key, keyHint)
                .get();
        assertEquals(4, masterAfterWrite.size());
        assertEquals(4L, masterAfterWrite.get(3).getOrder());
    }

    @Test
    @DisplayName("Create large ordered set with chunking on master, verify 1500 elements on both nodes")
    void testCreateLargeOrderedSetWithChunking() throws ExecutionException, InterruptedException {
        String key = "largeOrderedSet" + UUID.randomUUID();
        int elementCount = 1500;

        List<OrderedPayload> payloads = new ArrayList<>(elementCount);
        for (int i = 0; i < elementCount; i++) {
            payloads.add(OrderedPayload.of(i, ("large_item_" + i + "_" + UUID.randomUUID()).getBytes(
                    StandardCharsets.UTF_8)));
        }

        // Create large ordered set
        KeyHintData keyHint = client.createOrderedSet(key, payloads)
                .get();

        Thread.sleep(1000); // replication wait for large object (~1000ms for 1500 elements)

        // Verify on MASTER: streamOrderedSet returns 1500 elements
        List<OrderedPayload> masterStream = client.setMode(Mode.MASTER)
                .streamOrderedSet(key, keyHint)
                .get();
        assertNotNull(masterStream);
        assertEquals(elementCount, masterStream.size(), "Expected " + elementCount + " elements on master");

        // Verify on BACKUP: streamOrderedSet returns 1500 elements
        List<OrderedPayload> backupStream = client.setMode(Mode.BACKUP)
                .streamOrderedSet(key, keyHint)
                .get();
        assertNotNull(backupStream);
        assertEquals(elementCount, backupStream.size(), "Expected " + elementCount + " elements on backup");

        // Verify integrity: first and last elements match
        assertEquals(0L, masterStream.get(0).getOrder(), "First element mismatch on master");
        assertEquals(elementCount - 1, masterStream.get(elementCount - 1).getOrder(), "Last element mismatch on master");
        assertEquals(0L, backupStream.get(0).getOrder(), "First element mismatch on backup");
        assertEquals(elementCount - 1, backupStream.get(elementCount - 1).getOrder(), "Last element mismatch on backup");
    }

    // ==================== Section 2: Stream Ordered Set Operations ====================

    @Test
    @DisplayName("Stream ordered set on master, verify replication and write on backup")
    void testStreamOrderedSet() throws ExecutionException, InterruptedException {
        String key = "streamOrderedSetTest" + UUID.randomUUID();
        List<OrderedPayload> initialData = List.of(
                OrderedPayload.of(1L, bytes("elem1")),
                OrderedPayload.of(2L, bytes("elem2")),
                OrderedPayload.of(3L, bytes("elem3"))
        );

        // Create ordered set
        KeyHintData keyHint = client.createOrderedSet(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS); // replication wait

        // Verify on MASTER: streamOrderedSet returns 3 elements
        List<OrderedPayload> masterStream = client.setMode(Mode.MASTER)
                .streamOrderedSet(key, keyHint)
                .get();
        assertNotNull(masterStream);
        assertEquals(3, masterStream.size());

        // Verify on BACKUP: streamOrderedSet returns 3 elements
        List<OrderedPayload> backupStream = client.setMode(Mode.BACKUP)
                .streamOrderedSet(key, keyHint)
                .get();
        assertNotNull(backupStream);
        assertEquals(3, backupStream.size());

        // Write on BACKUP: addElementWithWeight
        client.setMode(Mode.BACKUP)
                .addElementWithWeight(key, keyHint, List.of(OrderedPayload.of(4L, bytes("elem4"))))
                .get();

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on BACKUP: streamOrderedSet returns 4 elements
        List<OrderedPayload> backupAfterWrite = client.setMode(Mode.BACKUP)
                .streamOrderedSet(key, keyHint)
                .get();
        assertEquals(4, backupAfterWrite.size());

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on MASTER: streamOrderedSet returns 4 elements
        List<OrderedPayload> masterAfterWrite = client.setMode(Mode.MASTER)
                .streamOrderedSet(key, keyHint)
                .get();
        assertEquals(4, masterAfterWrite.size());

        // Write on MASTER: addElementWithWeight
        client.setMode(Mode.MASTER)
                .addElementWithWeight(key, keyHint, List.of(OrderedPayload.of(5L, bytes("elem5"))))
                .get();

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on MASTER: streamOrderedSet returns 5 elements
        List<OrderedPayload> masterAfterWrite2 = client.setMode(Mode.MASTER)
                .streamOrderedSet(key, keyHint)
                .get();
        assertEquals(5, masterAfterWrite2.size());

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on BACKUP: streamOrderedSet returns 5 elements
        List<OrderedPayload> backupAfterWrite2 = client.setMode(Mode.BACKUP)
                .streamOrderedSet(key, keyHint)
                .get();
        assertEquals(5, backupAfterWrite2.size());
    }

    // ==================== Section 3: Add Element With Weight Operations ====================

    @Test
    @DisplayName("addElementWithWeight on master, verify replication and write on backup")
    void testAddElementWithWeight() throws ExecutionException, InterruptedException {
        String key = "addElementWithWeightTest" + UUID.randomUUID();
        List<OrderedPayload> initialData = List.of(OrderedPayload.of(1L, bytes("1")));

        // Create ordered set
        KeyHintData keyHint = client.createOrderedSet(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS); // replication wait

        // Write on BACKUP: addElementWithWeight
        Integer added = client.setMode(Mode.BACKUP)
                .addElementWithWeight(key, keyHint, List.of(
                        OrderedPayload.of(2L, bytes("2")),
                        OrderedPayload.of(3L, bytes("3"))
                ))
                .get();
        assertNotNull(added);
        assertEquals(2, added);

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on BACKUP: streamOrderedSet returns 3 elements
        List<OrderedPayload> backupStream = client.setMode(Mode.BACKUP)
                .streamOrderedSet(key, keyHint)
                .get();
        assertEquals(3, backupStream.size());
        assertEquals(1L, backupStream.get(0).getOrder());
        assertEquals(2L, backupStream.get(1).getOrder());
        assertEquals(3L, backupStream.get(2).getOrder());

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on MASTER: streamOrderedSet returns 3 elements
        List<OrderedPayload> masterStream = client.setMode(Mode.MASTER)
                .streamOrderedSet(key, keyHint)
                .get();
        assertEquals(3, masterStream.size());

        // Write on MASTER: addElementWithWeight
        client.setMode(Mode.MASTER)
                .addElementWithWeight(key, keyHint, List.of(OrderedPayload.of(4L, bytes("4"))))
                .get();

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on MASTER: streamOrderedSet returns 4 elements
        List<OrderedPayload> masterAfterWrite = client.setMode(Mode.MASTER)
                .streamOrderedSet(key, keyHint)
                .get();
        assertEquals(4, masterAfterWrite.size());

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on BACKUP: streamOrderedSet returns 4 elements
        List<OrderedPayload> backupAfterWrite = client.setMode(Mode.BACKUP)
                .streamOrderedSet(key, keyHint)
                .get();
        assertEquals(4, backupAfterWrite.size());
    }

    @Test
    @DisplayName("addElementWithWeight allows duplicates (same keys with different weights)")
    void testAddElementWithWeightAllowsDuplicates() throws ExecutionException, InterruptedException {
        String key = "addElementWithWeightDup" + UUID.randomUUID();
        List<OrderedPayload> initialData = List.of(OrderedPayload.of(1L, bytes("1")));

        // Create ordered set
        KeyHintData keyHint = client.createOrderedSet(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS); // replication wait

        // Write on BACKUP: add duplicates
        Integer added = client.setMode(Mode.BACKUP)
                .addElementWithWeight(key, keyHint, List.of(
                        OrderedPayload.of(2L, bytes("1")),
                        OrderedPayload.of(3L, bytes("1"))
                ))
                .get();
        assertEquals(2, added, "2 elements should be added (duplicates allowed)");

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on BACKUP: streamOrderedSet returns 3 elements
        List<OrderedPayload> backupStream = client.setMode(Mode.BACKUP)
                .streamOrderedSet(key, keyHint)
                .get();
        assertEquals(3, backupStream.size());

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on MASTER: streamOrderedSet returns 3 elements
        List<OrderedPayload> masterStream = client.setMode(Mode.MASTER)
                .streamOrderedSet(key, keyHint)
                .get();
        assertEquals(3, masterStream.size());
    }

    @Test
    @DisplayName("addElementWithWeight with empty list returns 0")
    void testAddElementWithWeightEmptyList() throws ExecutionException, InterruptedException {
        String key = "addElementWithWeightEmpty" + UUID.randomUUID();
        List<OrderedPayload> initialData = List.of(OrderedPayload.of(1L, bytes("1")));

        // Create ordered set
        KeyHintData keyHint = client.createOrderedSet(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS); // replication wait

        // Verify on MASTER: streamOrderedSet returns 1 element
        List<OrderedPayload> masterStream = client.setMode(Mode.MASTER)
                .streamOrderedSet(key, keyHint)
                .get();
        assertEquals(1, masterStream.size());

        // Verify on BACKUP: streamOrderedSet returns 1 element
        List<OrderedPayload> backupStream = client.setMode(Mode.BACKUP)
                .streamOrderedSet(key, keyHint)
                .get();
        assertEquals(1, backupStream.size());

        // Write on BACKUP: addElementWithWeight with empty list
        Integer added = client.setMode(Mode.BACKUP)
                .addElementWithWeight(key, keyHint, new ArrayList<>())
                .get();
        assertEquals(0, added, "Adding an empty list must return 0");

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on BACKUP: streamOrderedSet still returns 1 element
        List<OrderedPayload> backupAfterWrite = client.setMode(Mode.BACKUP)
                .streamOrderedSet(key, keyHint)
                .get();
        assertEquals(1, backupAfterWrite.size());

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on MASTER: streamOrderedSet still returns 1 element (replication check)
        List<OrderedPayload> masterAfterWrite = client.setMode(Mode.MASTER)
                .streamOrderedSet(key, keyHint)
                .get();
        assertEquals(1, masterAfterWrite.size());
    }

    // ==================== Section 4: Stream Element In Range Ordered Operations ====================

    @Test
    @DisplayName("streamElementInRangeOrderedSet on master, verify replication and write on backup")
    void testStreamElementInRangeOrderedSet() throws ExecutionException, InterruptedException {
        String key = "streamRangeOrderedTest" + UUID.randomUUID();
        List<OrderedPayload> initialData = List.of(
                OrderedPayload.of(10L, bytes("1")),
                OrderedPayload.of(20L, bytes("2")),
                OrderedPayload.of(30L, bytes("3")),
                OrderedPayload.of(40L, bytes("4")),
                OrderedPayload.of(50L, bytes("5"))
        );

        // Create ordered set
        KeyHintData keyHint = client.createOrderedSet(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS); // replication wait

        // Verify on MASTER: streamElementInRangeOrderedSet returns 3 elements
        List<OrderedPayload> masterRange = client.setMode(Mode.MASTER)
                .streamElementInRangeOrderedSet(bytes(key), keyHint, 20L, 40L, false, 1, Duration.ofSeconds(30))
                .get();
        assertNotNull(masterRange);
        assertEquals(3, masterRange.size());

        // Verify on BACKUP: streamElementInRangeOrderedSet returns 3 elements
        List<OrderedPayload> backupRange = client.setMode(Mode.BACKUP)
                .streamElementInRangeOrderedSet(bytes(key), keyHint, 20L, 40L, false, 1, Duration.ofSeconds(30))
                .get();
        assertNotNull(backupRange);
        assertEquals(3, backupRange.size());

        // Write on BACKUP: addElementWithWeight
        client.setMode(Mode.BACKUP)
                .addElementWithWeight(key, keyHint, List.of(OrderedPayload.of(60L, bytes("6"))))
                .get();

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on BACKUP: streamElementInRangeOrderedSet returns 3 elements (range 20-40)
        List<OrderedPayload> backupRangeAfter = client.setMode(Mode.BACKUP)
                .streamElementInRangeOrderedSet(bytes(key), keyHint, 20L, 40L, false, 1, Duration.ofSeconds(30))
                .get();
        assertEquals(3, backupRangeAfter.size());

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on MASTER: streamElementInRangeOrderedSet returns 3 elements (range 20-40)
        List<OrderedPayload> masterRangeAfter = client.setMode(Mode.MASTER)
                .streamElementInRangeOrderedSet(bytes(key), keyHint, 20L, 40L, false, 1, Duration.ofSeconds(30))
                .get();
        assertEquals(3, masterRangeAfter.size());

        // Write on MASTER: addElementWithWeight
        client.setMode(Mode.MASTER)
                .addElementWithWeight(key, keyHint, List.of(OrderedPayload.of(70L, bytes("7"))))
                .get();

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on MASTER: streamElementInRangeOrderedSet still returns 3 elements (range 20-40)
        List<OrderedPayload> masterRangeAfter2 = client.setMode(Mode.MASTER)
                .streamElementInRangeOrderedSet(bytes(key), keyHint, 20L, 40L, false, 1, Duration.ofSeconds(30))
                .get();
        assertEquals(3, masterRangeAfter2.size());

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on BACKUP: streamElementInRangeOrderedSet still returns 3 elements
        List<OrderedPayload> backupRangeAfter2 = client.setMode(Mode.BACKUP)
                .streamElementInRangeOrderedSet(bytes(key), keyHint, 20L, 40L, false, 1, Duration.ofSeconds(30))
                .get();
        assertEquals(3, backupRangeAfter2.size());
    }

    @Test
    @DisplayName("streamElementInRangeOrderedSet with reverse=true returns elements in reverse order")
    void testStreamElementInRangeOrderedSetReverse() throws ExecutionException, InterruptedException {
        String key = "streamRangeOrderedReverse" + UUID.randomUUID();
        List<OrderedPayload> initialData = List.of(
                OrderedPayload.of(10L, bytes("1")),
                OrderedPayload.of(20L, bytes("2")),
                OrderedPayload.of(30L, bytes("3")),
                OrderedPayload.of(40L, bytes("4")),
                OrderedPayload.of(50L, bytes("5"))
        );

        // Create ordered set
        KeyHintData keyHint = client.createOrderedSet(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS); // replication wait

        // Verify on MASTER: reverse order works
        List<OrderedPayload> masterRange = client.setMode(Mode.MASTER)
                .streamElementInRangeOrderedSet(bytes(key), keyHint, 20L, 40L, true, 1, Duration.ofSeconds(30))
                .get();
        assertNotNull(masterRange);
        assertEquals(3, masterRange.size());
        assertEquals(40L, masterRange.get(0).getOrder());

        // Verify on BACKUP: reverse order works
        List<OrderedPayload> backupRange = client.setMode(Mode.BACKUP)
                .streamElementInRangeOrderedSet(bytes(key), keyHint, 20L, 40L, true, 1, Duration.ofSeconds(30))
                .get();
        assertNotNull(backupRange);
        assertEquals(3, backupRange.size());
        assertEquals(40L, backupRange.get(0).getOrder());

        // Write on BACKUP: addElementWithWeight
        client.setMode(Mode.BACKUP)
                .addElementWithWeight(key, keyHint, List.of(OrderedPayload.of(60L, bytes("6"))))
                .get();

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on BACKUP: reverse order returns 4 elements
        List<OrderedPayload> backupRangeAfter = client.setMode(Mode.BACKUP)
                .streamElementInRangeOrderedSet(bytes(key), keyHint, 20L, 40L, true, 1, Duration.ofSeconds(30))
                .get();
        assertEquals(3, backupRangeAfter.size()); // range 20-40 still only 3 elements

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on MASTER: reverse order returns 4 elements
        List<OrderedPayload> masterRangeAfter = client.setMode(Mode.MASTER)
                .streamElementInRangeOrderedSet(bytes(key), keyHint, 20L, 40L, true, 1, Duration.ofSeconds(30))
                .get();
        assertEquals(3, masterRangeAfter.size());

        // Write on MASTER: addElementWithWeight
        client.setMode(Mode.MASTER)
                .addElementWithWeight(key, keyHint, List.of(OrderedPayload.of(70L, bytes("7"))))
                .get();

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on MASTER: reverse order returns 4 elements
        List<OrderedPayload> masterRangeAfter2 = client.setMode(Mode.MASTER)
                .streamElementInRangeOrderedSet(bytes(key), keyHint, 20L, 40L, true, 1, Duration.ofSeconds(30))
                .get();
        assertEquals(3, masterRangeAfter2.size());

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on BACKUP: reverse order returns 4 elements
        List<OrderedPayload> backupRangeAfter2 = client.setMode(Mode.BACKUP)
                .streamElementInRangeOrderedSet(bytes(key), keyHint, 20L, 40L, true, 1, Duration.ofSeconds(30))
                .get();
        assertEquals(3, backupRangeAfter2.size());
    }

    @Test
    @DisplayName("streamElementInRangeOrderedSet returns empty list if no elements fall in range")
    void testStreamElementInRangeOrderedSetNoMatch() throws ExecutionException, InterruptedException {
        String key = "streamRangeOrderedNoMatch" + UUID.randomUUID();
        List<OrderedPayload> initialData = List.of(
                OrderedPayload.of(10L, bytes("1")),
                OrderedPayload.of(20L, bytes("2")),
                OrderedPayload.of(30L, bytes("3"))
        );

        // Create ordered set
        KeyHintData keyHint = client.createOrderedSet(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS); // replication wait

        // Verify on MASTER: streamElementInRangeOrderedSet with range 100-200 returns empty
        List<OrderedPayload> masterRange = client.setMode(Mode.MASTER)
                .streamElementInRangeOrderedSet(bytes(key), keyHint, 100L, 200L, false, 1, Duration.ofSeconds(30))
                .get();
        assertNotNull(masterRange);
        assertTrue(masterRange.isEmpty(), "List must be empty");

        // Verify on BACKUP: streamElementInRangeOrderedSet with range 100-200 returns empty
        List<OrderedPayload> backupRange = client.setMode(Mode.BACKUP)
                .streamElementInRangeOrderedSet(bytes(key), keyHint, 100L, 200L, false, 1, Duration.ofSeconds(30))
                .get();
        assertNotNull(backupRange);
        assertTrue(backupRange.isEmpty(), "List must be empty");

        // Write on BACKUP: addElementWithWeight
        client.setMode(Mode.BACKUP)
                .addElementWithWeight(key, keyHint, List.of(OrderedPayload.of(4L, bytes("4"))))
                .get();

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on BACKUP: range 100-200 still empty
        List<OrderedPayload> backupRangeAfter = client.setMode(Mode.BACKUP)
                .streamElementInRangeOrderedSet(bytes(key), keyHint, 100L, 200L, false, 1, Duration.ofSeconds(30))
                .get();
        assertTrue(backupRangeAfter.isEmpty(), "List must be empty");

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on MASTER: range 100-200 still empty
        List<OrderedPayload> masterRangeAfter = client.setMode(Mode.MASTER)
                .streamElementInRangeOrderedSet(bytes(key), keyHint, 100L, 200L, false, 1, Duration.ofSeconds(30))
                .get();
        assertTrue(masterRangeAfter.isEmpty());

        // Write on MASTER: addElementWithWeight
        client.setMode(Mode.MASTER)
                .addElementWithWeight(key, keyHint, List.of(OrderedPayload.of(5L, bytes("5"))))
                .get();

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on MASTER: range 100-200 still empty
        List<OrderedPayload> masterRangeAfter2 = client.setMode(Mode.MASTER)
                .streamElementInRangeOrderedSet(bytes(key), keyHint, 100L, 200L, false, 1, Duration.ofSeconds(30))
                .get();
        assertTrue(masterRangeAfter2.isEmpty());

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on BACKUP: range 100-200 still empty
        List<OrderedPayload> backupRangeAfter2 = client.setMode(Mode.BACKUP)
                .streamElementInRangeOrderedSet(bytes(key), keyHint, 100L, 200L, false, 1, Duration.ofSeconds(30))
                .get();
        assertTrue(backupRangeAfter2.isEmpty());
    }

    // ==================== Section 5: Remove From Container Operations ====================

    @Test
    @DisplayName("removeFromContainer on master, verify replication and write on backup")
    void testRemoveFromContainer() throws ExecutionException, InterruptedException {
        String key = "removeFromContainerTest" + UUID.randomUUID();
        List<OrderedPayload> initialData = List.of(
                OrderedPayload.of(1L, bytes("1")),
                OrderedPayload.of(2L, bytes("2")),
                OrderedPayload.of(3L, bytes("3"))
        );

        // Create ordered set
        KeyHintData keyHint = client.createOrderedSet(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS); // replication wait

        // Write on BACKUP: removeFromContainer
        Integer removed = client.setMode(Mode.BACKUP)
                .removeFromContainer(bytes(key), keyHint, OrderedPayload.of(1L, bytes("1")).getValue())
                .get();
        assertEquals(1, removed, "1 element should be removed");

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on BACKUP: streamOrderedSet returns 2 elements
        List<OrderedPayload> backupStream = client.setMode(Mode.BACKUP)
                .streamOrderedSet(key, keyHint)
                .get();
        assertEquals(2, backupStream.size());

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on MASTER: streamOrderedSet returns 2 elements
        List<OrderedPayload> masterStream = client.setMode(Mode.MASTER)
                .streamOrderedSet(key, keyHint)
                .get();
        assertEquals(2, masterStream.size());

        // Write on MASTER: removeFromContainer
        Integer removedMaster = client.setMode(Mode.MASTER)
                .removeFromContainer(bytes(key), keyHint, OrderedPayload.of(2L, bytes("2")).getValue())
                .get();
        assertEquals(1, removedMaster);

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on MASTER: streamOrderedSet returns 1 element
        List<OrderedPayload> masterAfterWrite = client.setMode(Mode.MASTER)
                .streamOrderedSet(key, keyHint)
                .get();
        assertEquals(1, masterAfterWrite.size());

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on BACKUP: streamOrderedSet returns 1 element
        List<OrderedPayload> backupAfterWrite = client.setMode(Mode.BACKUP)
                .streamOrderedSet(key, keyHint)
                .get();
        assertEquals(1, backupAfterWrite.size());
    }

    @Test
    @DisplayName("removeFromContainer removes multiple duplicates with same key")
    void testRemoveFromContainerMultipleDuplicates() throws ExecutionException, InterruptedException {
        String key = "removeFromContainerMulti" + UUID.randomUUID();
        List<OrderedPayload> initialData = List.of(
                OrderedPayload.of(1L, bytes("1")),
                OrderedPayload.of(2L, bytes("1")),
                OrderedPayload.of(3L, bytes("1")),
                OrderedPayload.of(4L, bytes("2"))
        );

        // Create ordered set
        KeyHintData keyHint = client.createOrderedSet(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS); // replication wait

        // Verify on MASTER: streamOrderedSet returns 4 elements
        List<OrderedPayload> masterStream = client.setMode(Mode.MASTER)
                .streamOrderedSet(key, keyHint)
                .get();
        assertEquals(4, masterStream.size());

        // Verify on BACKUP: streamOrderedSet returns 4 elements
        List<OrderedPayload> backupStream = client.setMode(Mode.BACKUP)
                .streamOrderedSet(key, keyHint)
                .get();
        assertEquals(4, backupStream.size());

        // Write on BACKUP: remove all "item1" (3 items)
        Integer removed = client.setMode(Mode.BACKUP)
                .removeFromContainer(bytes(key), keyHint, OrderedPayload.of(1L, bytes("1")).getValue())
                .get();
        assertEquals(3, removed, "3 elements with the same key must be removed");

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on BACKUP: streamOrderedSet returns 1 element
        List<OrderedPayload> backupAfterWrite = client.setMode(Mode.BACKUP)
                .streamOrderedSet(key, keyHint)
                .get();
        assertEquals(1, backupAfterWrite.size());

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on MASTER: streamOrderedSet returns 1 element
        List<OrderedPayload> masterAfterWrite = client.setMode(Mode.MASTER)
                .streamOrderedSet(key, keyHint)
                .get();
        assertEquals(1, masterAfterWrite.size());
    }

    @Test
    @DisplayName("removeFromContainer returns 0 if element does not exist")
    void testRemoveFromContainerNonExistent() throws ExecutionException, InterruptedException {
        String key = "removeFromContainerNonExist" + UUID.randomUUID();
        List<OrderedPayload> initialData = List.of(
                OrderedPayload.of(1L, bytes("1")),
                OrderedPayload.of(2L, bytes("2"))
        );

        // Create ordered set
        KeyHintData keyHint = client.createOrderedSet(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS); // replication wait

        // Verify on MASTER: streamOrderedSet returns 2 elements
        List<OrderedPayload> masterStream = client.setMode(Mode.MASTER)
                .streamOrderedSet(key, keyHint)
                .get();
        assertEquals(2, masterStream.size());

        // Verify on BACKUP: streamOrderedSet returns 2 elements
        List<OrderedPayload> backupStream = client.setMode(Mode.BACKUP)
                .streamOrderedSet(key, keyHint)
                .get();
        assertEquals(2, backupStream.size());

        // Write on BACKUP: removeFromContainer non-existent
        Integer removed = client.setMode(Mode.BACKUP)
                .removeFromContainer(bytes(key), keyHint, OrderedPayload.of(0L, bytes("nonexistent")).getValue())
                .get();
        assertEquals(0, removed, "0 elements should be removed (element not found)");

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on BACKUP: streamOrderedSet still returns 2 elements
        List<OrderedPayload> backupAfterWrite = client.setMode(Mode.BACKUP)
                .streamOrderedSet(key, keyHint)
                .get();
        assertEquals(2, backupAfterWrite.size());

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on MASTER: streamOrderedSet still returns 2 elements (replication check)
        List<OrderedPayload> masterAfterWrite = client.setMode(Mode.MASTER)
                .streamOrderedSet(key, keyHint)
                .get();
        assertEquals(2, masterAfterWrite.size());
    }

    // ==================== Section 6: Contains Container Key Operations ====================

    @Test
    @DisplayName("containsContainerKey on master, verify replication and write on backup")
    void testContainsContainerKey() throws ExecutionException, InterruptedException {
        String key = "containsContainerKeyTest" + UUID.randomUUID();
        List<OrderedPayload> initialData = List.of(
                OrderedPayload.of(1L, bytes("1")),
                OrderedPayload.of(2L, bytes("2"))
        );

        // Create ordered set
        KeyHintData keyHint = client.createOrderedSet(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS); // replication wait

        // Verify on MASTER: containsContainerKey for existing element
        Boolean masterExists = client.setMode(Mode.MASTER)
                .containsContainerKey(bytes(key), keyHint, OrderedPayload.of(1L, bytes("1")).getValue())
                .get();
        assertTrue(masterExists);

        // Verify on BACKUP: containsContainerKey for existing element
        Boolean backupExists = client.setMode(Mode.BACKUP)
                .containsContainerKey(bytes(key), keyHint, OrderedPayload.of(1L, bytes("1")).getValue())
                .get();
        assertTrue(backupExists);

        // Verify on BACKUP: containsContainerKey for non-existing element
        Boolean notExists = client.setMode(Mode.BACKUP)
                .containsContainerKey(bytes(key), keyHint, OrderedPayload.of(0L, bytes("nonexistent")).getValue())
                .get();
        assertFalse(notExists, "Element nonexistent must not exist");

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on MASTER: containsContainerKey for non-existing element
        Boolean masterNotExists = client.setMode(Mode.MASTER)
                .containsContainerKey(bytes(key), keyHint, OrderedPayload.of(0L, bytes("nonexistent")).getValue())
                .get();
        assertFalse(masterNotExists, "Element nonexistent must not exist");

        // Write on MASTER: addElementWithWeight
        client.setMode(Mode.MASTER)
                .addElementWithWeight(key, keyHint, List.of(OrderedPayload.of(3L, bytes("3"))))
                .get();

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on MASTER: containsContainerKey for new element
        Boolean newExists = client.setMode(Mode.MASTER)
                .containsContainerKey(bytes(key), keyHint, OrderedPayload.of(3L, bytes("3")).getValue())
                .get();
        assertTrue(newExists);

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on BACKUP: containsContainerKey for new element
        Boolean backupNewExists = client.setMode(Mode.BACKUP)
                .containsContainerKey(bytes(key), keyHint, OrderedPayload.of(3L, bytes("3")).getValue())
                .get();
        assertTrue(backupNewExists);

        // Write on BACKUP: addElementWithWeight
        client.setMode(Mode.BACKUP)
                .addElementWithWeight(key, keyHint, List.of(OrderedPayload.of(4L, bytes("4"))))
                .get();

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on BACKUP: containsContainerKey for new element
        Boolean backupNewExists2 = client.setMode(Mode.BACKUP)
                .containsContainerKey(bytes(key), keyHint, OrderedPayload.of(4L, bytes("4")).getValue())
                .get();
        assertTrue(backupNewExists2);

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on MASTER: containsContainerKey for new element
        Boolean masterNewExists2 = client.setMode(Mode.MASTER)
                .containsContainerKey(bytes(key), keyHint, OrderedPayload.of(4L, bytes("4")).getValue())
                .get();
        assertTrue(masterNewExists2);
    }

    // ==================== Section 7: Remove Element At Weight Range Operations ====================

    @Test
    @DisplayName("removeElementAtPosition on master, verify replication and write on backup")
    void testRemoveElementAtPosition() throws ExecutionException, InterruptedException {
        String key = "removeElementAtPositionTest" + UUID.randomUUID();
        List<OrderedPayload> initialData = List.of(
                OrderedPayload.of(10L, bytes("1")),
                OrderedPayload.of(20L, bytes("2")),
                OrderedPayload.of(30L, bytes("3")),
                OrderedPayload.of(40L, bytes("4")),
                OrderedPayload.of(50L, bytes("5"))
        );

        // Create ordered set
        KeyHintData keyHint = client.createOrderedSet(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS); // replication wait

        // Write on BACKUP: removeElementAtPosition(key, 20, 40)
        Boolean removed = client.setMode(Mode.BACKUP)
                .removeElementAtPosition(key, keyHint, 20, 40)
                .get();
        assertTrue(removed, "Elements must be removed");

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on BACKUP: streamOrderedSet returns 2 elements
        List<OrderedPayload> backupStream = client.setMode(Mode.BACKUP)
                .streamOrderedSet(key, keyHint)
                .get();
        assertEquals(2, backupStream.size());
        assertEquals(10L, backupStream.get(0).getOrder());
        assertEquals(50L, backupStream.get(1).getOrder());

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on MASTER: streamOrderedSet returns 2 elements
        List<OrderedPayload> masterStream = client.setMode(Mode.MASTER)
                .streamOrderedSet(key, keyHint)
                .get();
        assertEquals(2, masterStream.size());

        // Write on MASTER: removeElementAtPosition(key, 10, 10)
        Boolean rangeRemoved = client.setMode(Mode.MASTER)
                .removeElementAtPosition(key, keyHint, 10, 10)
                .get();
        assertTrue(rangeRemoved);

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on MASTER: streamOrderedSet returns 1 element
        List<OrderedPayload> masterAfterWrite = client.setMode(Mode.MASTER)
                .streamOrderedSet(key, keyHint)
                .get();
        assertEquals(1, masterAfterWrite.size());
        assertEquals(50L, masterAfterWrite.get(0).getOrder());

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on BACKUP: streamOrderedSet returns 1 element
        List<OrderedPayload> backupAfterWrite = client.setMode(Mode.BACKUP)
                .streamOrderedSet(key, keyHint)
                .get();
        assertEquals(1, backupAfterWrite.size());
    }

    @Test
    @DisplayName("removeElementAtPosition works when weight boundaries match")
    void testRemoveElementAtPositionSamePos() throws ExecutionException, InterruptedException {
        String key = "removeElementAtPositionSame" + UUID.randomUUID();
        List<OrderedPayload> initialData = List.of(
                OrderedPayload.of(10L, bytes("1")),
                OrderedPayload.of(20L, bytes("2")),
                OrderedPayload.of(30L, bytes("3"))
        );

        // Create ordered set
        KeyHintData keyHint = client.createOrderedSet(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS); // replication wait

        // Verify on MASTER: streamOrderedSet returns 3 elements
        List<OrderedPayload> masterStream = client.setMode(Mode.MASTER)
                .streamOrderedSet(key, keyHint)
                .get();
        assertEquals(3, masterStream.size());

        // Verify on BACKUP: streamOrderedSet returns 3 elements
        List<OrderedPayload> backupStream = client.setMode(Mode.BACKUP)
                .streamOrderedSet(key, keyHint)
                .get();
        assertEquals(3, backupStream.size());

        // Write on BACKUP: removeElementAtPosition(key, 20, 20)
        Boolean removed = client.setMode(Mode.BACKUP)
                .removeElementAtPosition(key, keyHint, 20, 20)
                .get();
        assertTrue(removed, "Element must be removed");

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on BACKUP: streamOrderedSet returns 2 elements
        List<OrderedPayload> backupAfterWrite = client.setMode(Mode.BACKUP)
                .streamOrderedSet(key, keyHint)
                .get();
        assertEquals(2, backupAfterWrite.size());
        assertEquals(10L, backupAfterWrite.get(0).getOrder());
        assertEquals(30L, backupAfterWrite.get(1).getOrder());

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on MASTER: streamOrderedSet returns 2 elements
        List<OrderedPayload> masterAfterWrite = client.setMode(Mode.MASTER)
                .streamOrderedSet(key, keyHint)
                .get();
        assertEquals(2, masterAfterWrite.size());
        assertEquals(10L, masterAfterWrite.get(0).getOrder());
        assertEquals(30L, masterAfterWrite.get(1).getOrder());

        // Write on MASTER: removeElementAtPosition(key, 10, 10)
        Boolean rangeRemoved = client.setMode(Mode.MASTER)
                .removeElementAtPosition(key, keyHint, 10, 10)
                .get();
        assertTrue(rangeRemoved);

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on MASTER: streamOrderedSet returns 1 element
        List<OrderedPayload> masterAfterWrite2 = client.setMode(Mode.MASTER)
                .streamOrderedSet(key, keyHint)
                .get();
        assertEquals(1, masterAfterWrite2.size());
        assertEquals(30L, masterAfterWrite2.get(0).getOrder());

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on BACKUP: streamOrderedSet returns 1 element
        List<OrderedPayload> backupAfterWrite2 = client.setMode(Mode.BACKUP)
                .streamOrderedSet(key, keyHint)
                .get();
        assertEquals(1, backupAfterWrite2.size());
        assertEquals(30L, backupAfterWrite2.get(0).getOrder());
    }

    // ==================== Section 8: Set TTL Operations ====================

    @Test
    @DisplayName("SetTtl on master, verify GetTtl on both nodes and write on backup")
    void testSetTtlAndGetTtl() throws ExecutionException, InterruptedException {
        String key = "setTtlOrderedSetTest" + UUID.randomUUID();

        // Create ordered set
        KeyHintData keyHint = client.createOrderedSet(key, List.of(OrderedPayload.of(1L, bytes("1"))))
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

    // ==================== Section 9: TTL Expiration Operations ====================

    @Test
    @DisplayName("TTL expiration: set on master, verify both nodes after TTL expires")
    void testTtlExpirationOnMaster() throws ExecutionException, InterruptedException {
        String key = "ttlExpirationOrderedSetMaster" + UUID.randomUUID();

        // Create ordered set
        KeyHintData keyHint = client.createOrderedSet(key,
                        List.of(OrderedPayload.of(1L, bytes("1"))))
                .get();

        Thread.sleep(REPLICATION_DELAY_MS); // replication wait

        // Verify on MASTER: streamOrderedSet returns 1 element
        List<OrderedPayload> masterStream = client.setMode(Mode.MASTER)
                .streamOrderedSet(key, keyHint)
                .get();
        assertNotNull(masterStream);
        assertEquals(1, masterStream.size());

        // Verify on BACKUP: streamOrderedSet returns 1 element
        List<OrderedPayload> backupStream = client.setMode(Mode.BACKUP)
                .streamOrderedSet(key, keyHint)
                .get();
        assertNotNull(backupStream);
        assertEquals(1, backupStream.size());

        // setTtl on MASTER: 2000ms
        Boolean ttlSet = client.setMode(Mode.MASTER)
                .setTtl(key, keyHint, 2000)
                .get();
        assertTrue(ttlSet);

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on MASTER: getTtl > 0
        Long ttlMaster = client.setMode(Mode.MASTER)
                .getTtl(key, keyHint)
                .get();
        assertNotNull(ttlMaster);
        assertTrue(ttlMaster > 0, "Expected TTL > 0 on master");

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on BACKUP: getTtl > 0 (replication check)
        Long ttlBackup = client.setMode(Mode.BACKUP)
                .getTtl(key, keyHint)
                .get();
        assertNotNull(ttlBackup);
        assertTrue(ttlBackup > 0, "Expected TTL > 0 on backup");

        // Wait for TTL to expire
        Thread.sleep(3000);

        // Verify on MASTER: existKey returns false after TTL expiry
        Boolean masterExists = client.setMode(Mode.MASTER)
                .existKey(key, keyHint)
                .get();
        assertFalse(masterExists, "Expected container to not exist on master after TTL expiry");

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on BACKUP: existKey returns false after TTL expiry
        Boolean backupExists = client.setMode(Mode.BACKUP)
                .existKey(key, keyHint)
                .get();
        assertFalse(backupExists, "Expected container to not exist on backup after TTL expiry");
    }

    @Test
    @DisplayName("TTL expiration: set on backup, verify both nodes after TTL expires")
    void testTtlExpirationOnBackup() throws ExecutionException, InterruptedException {
        String key = "ttlExpirationOrderedSetBackup" + UUID.randomUUID();

        // Create ordered set (always on master, KeyHint works on both nodes)
        KeyHintData keyHint = client.createOrderedSet(key,
                        List.of(OrderedPayload.of(1L, bytes("1"))))
                .get();

        Thread.sleep(REPLICATION_DELAY_MS); // replication wait

        // Verify on BACKUP: streamOrderedSet returns 1 element
        List<OrderedPayload> backupStream = client.setMode(Mode.BACKUP)
                .streamOrderedSet(key, keyHint)
                .get();
        assertNotNull(backupStream);
        assertEquals(1, backupStream.size());

        // Verify on MASTER: streamOrderedSet returns 1 element
        List<OrderedPayload> masterStream = client.setMode(Mode.MASTER)
                .streamOrderedSet(key, keyHint)
                .get();
        assertNotNull(masterStream);
        assertEquals(1, masterStream.size());

        // setTtl on BACKUP: 2000ms
        Boolean ttlSet = client.setMode(Mode.BACKUP)
                .setTtl(key, keyHint, 2000)
                .get();
        assertTrue(ttlSet);

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on BACKUP: getTtl > 0
        Long ttlBackup = client.setMode(Mode.BACKUP)
                .getTtl(key, keyHint)
                .get();
        assertNotNull(ttlBackup);
        assertTrue(ttlBackup > 0, "Expected TTL > 0 on backup");

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on MASTER: getTtl > 0 (replication check)
        Long ttlMaster = client.setMode(Mode.MASTER)
                .getTtl(key, keyHint)
                .get();
        assertNotNull(ttlMaster);
        assertTrue(ttlMaster > 0, "Expected TTL > 0 on master");

        // Wait for TTL to expire
        Thread.sleep(3000);

        // Verify on BACKUP: existKey returns false after TTL expiry
        Boolean backupExists = client.setMode(Mode.BACKUP)
                .existKey(key, keyHint)
                .get();
        assertFalse(backupExists, "Expected container to not exist on backup after TTL expiry");

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on MASTER: existKey returns false after TTL expiry
        Boolean masterExists = client.setMode(Mode.MASTER)
                .existKey(key, keyHint)
                .get();
        assertFalse(masterExists, "Expected container to not exist on master after TTL expiry");
    }

    // ==================== Section 10: Lock Operations ====================

    @Test
    @DisplayName("LockObject READ_LOCK: parallel reads OK, writes denied, unlock by owner only")
    void testLockObjectReadLock() throws ExecutionException, InterruptedException {
        String key = "readLockOrderedSetTest" + UUID.randomUUID();

        // Create ordered set
        KeyHintData keyHint = client.createOrderedSet(key, List.of(OrderedPayload.of(1L, bytes("1"))))
                .get();

        Thread.sleep(REPLICATION_DELAY_MS); // replication wait

        // Verify on MASTER: streamOrderedSet returns 1 element
        List<OrderedPayload> masterStream = client.setMode(Mode.MASTER)
                .streamOrderedSet(key, keyHint)
                .get();
        assertNotNull(masterStream);
        assertEquals(1, masterStream.size());

        // Verify on BACKUP: streamOrderedSet returns 1 element
        List<OrderedPayload> backupStream = client.setMode(Mode.BACKUP)
                .streamOrderedSet(key, keyHint)
                .get();
        assertNotNull(backupStream);
        assertEquals(1, backupStream.size());

        // Lock on BACKUP: READ_LOCK
        LockStatus lockStatus = client.setMode(Mode.BACKUP)
                .lockObject(key, keyHint, LockType.READ_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(60))
                .get();
        assertEquals(LockStatus.OK, lockStatus);

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on BACKUP: client 1 can read (streamOrderedSet)
        List<OrderedPayload> backupStreamOwner = client.setMode(Mode.BACKUP)
                .streamOrderedSet(key, keyHint, OWNER_CLIENT_ID)
                .get();
        assertNotNull(backupStreamOwner);

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on MASTER: client 1 can read (streamOrderedSet)
        List<OrderedPayload> masterStreamOwner = client.setMode(Mode.MASTER)
                .streamOrderedSet(key, keyHint, OWNER_CLIENT_ID)
                .get();
        assertNotNull(masterStreamOwner);

        // Client 2 can read (should succeed with READ_LOCK) on BACKUP
        List<OrderedPayload> backupStreamIntruder = client.setMode(Mode.BACKUP)
                .streamOrderedSet(key, keyHint, INTRUDER_CLIENT_ID)
                .get();
        assertNotNull(backupStreamIntruder);

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Client 2 can read on MASTER (READ_LOCK replicated)
        List<OrderedPayload> masterStreamIntruder = client.setMode(Mode.MASTER)
                .streamOrderedSet(key, keyHint, INTRUDER_CLIENT_ID)
                .get();
        assertNotNull(masterStreamIntruder);

        // Client 2 tries to write (should fail with PERMISSION_DENIED) on BACKUP
        try {
            client.setMode(Mode.BACKUP)
                    .addElementWithWeight(key, keyHint, List.of(OrderedPayload.of(2L, bytes("2"))), INTRUDER_CLIENT_ID)
                    .get();
            fail("Expected PERMISSION_DENIED for write with READ_LOCK");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.PERMISSION_DENIED, cause.getStatus().getCode());
        }

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Client 2 tries to write on MASTER (should fail with PERMISSION_DENIED)
        try {
            client.setMode(Mode.MASTER)
                    .addElementWithWeight(key, keyHint, List.of(OrderedPayload.of(3L, bytes("3"))), INTRUDER_CLIENT_ID)
                    .get();
            fail("Expected PERMISSION_DENIED for write with READ_LOCK on master");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.PERMISSION_DENIED, cause.getStatus().getCode());
        }

        // Unlock on BACKUP by owner (client 1)
        LockStatus unlockStatus = client.setMode(Mode.BACKUP)
                .unlockObject(key, keyHint, OWNER_CLIENT_ID)
                .get();
        assertEquals(LockStatus.OK, unlockStatus);

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify unlock on MASTER
        LockStatus unlockStatusMaster = client.setMode(Mode.MASTER)
                .unlockObject(key, keyHint, OWNER_CLIENT_ID)
                .get();
        assertEquals(LockStatus.OK, unlockStatusMaster);

        // Re-lock by owner to test CANT_UNLOCK
        LockStatus lockStatus2 = client.setMode(Mode.BACKUP)
                .lockObject(key, keyHint, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(60))
                .get();
        assertEquals(LockStatus.OK, lockStatus2);
        Thread.sleep(REPLICATION_DELAY_MS); // replication delay
        // Unlock by intruder on MASTER should fail
        LockStatus failedUnlock = client.setMode(Mode.MASTER)
                .unlockObject(key, keyHint, INTRUDER_CLIENT_ID)
                .get();
        assertEquals(LockStatus.CANT_UNLOCK, failedUnlock);

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify CANT_UNLOCK on BACKUP
        LockStatus failedUnlockBackup = client.setMode(Mode.BACKUP)
                .unlockObject(key, keyHint, INTRUDER_CLIENT_ID)
                .get();
        assertEquals(LockStatus.CANT_UNLOCK, failedUnlockBackup);

        // Unlock by owner on MASTER should succeed
        LockStatus successUnlock = client.setMode(Mode.MASTER)
                .unlockObject(key, keyHint, OWNER_CLIENT_ID)
                .get();
        assertEquals(LockStatus.OK, successUnlock);

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify unlock on BACKUP
        LockStatus successUnlockBackup = client.setMode(Mode.BACKUP)
                .unlockObject(key, keyHint, OWNER_CLIENT_ID)
                .get();
        assertEquals(LockStatus.OK, successUnlockBackup);
    }

    @Test
    @DisplayName("LockObject WRITE_LOCK: owner read/write OK, others denied, unlock by owner only")
    void testLockObjectWriteLock() throws ExecutionException, InterruptedException {
        String key = "writeLockOrderedSetTest" + UUID.randomUUID();

        // Create ordered set
        KeyHintData keyHint = client.createOrderedSet(key, List.of(OrderedPayload.of(1L, bytes("1"))))
                .get();

        Thread.sleep(REPLICATION_DELAY_MS); // replication wait

        // Verify on MASTER: streamOrderedSet returns 1 element
        List<OrderedPayload> masterStream = client.setMode(Mode.MASTER)
                .streamOrderedSet(key, keyHint)
                .get();
        assertNotNull(masterStream);
        assertEquals(1, masterStream.size());

        // Verify on BACKUP: streamOrderedSet returns 1 element
        List<OrderedPayload> backupStream = client.setMode(Mode.BACKUP)
                .streamOrderedSet(key, keyHint)
                .get();
        assertNotNull(backupStream);
        assertEquals(1, backupStream.size());

        // Lock on BACKUP: WRITE_LOCK
        LockStatus lockStatus = client.setMode(Mode.BACKUP)
                .lockObject(key, keyHint, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(60))
                .get();
        assertEquals(LockStatus.OK, lockStatus);

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on BACKUP: owner can write (addElementWithWeight)
        client.setMode(Mode.BACKUP)
                .addElementWithWeight(key, keyHint, List.of(OrderedPayload.of(2L, bytes("2"))), OWNER_CLIENT_ID)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on MASTER: owner can read (streamOrderedSet)
        List<OrderedPayload> masterStreamOwner = client.setMode(Mode.MASTER)
                .streamOrderedSet(key, keyHint, OWNER_CLIENT_ID)
                .get();
        assertNotNull(masterStreamOwner);
        assertEquals(2, masterStreamOwner.size());

        // Intruder tries to read (should fail with PERMISSION_DENIED) on BACKUP
        try {
            client.setMode(Mode.BACKUP)
                    .streamOrderedSet(key, keyHint, INTRUDER_CLIENT_ID)
                    .get();
            fail("Expected PERMISSION_DENIED for read with WRITE_LOCK");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.PERMISSION_DENIED, cause.getStatus().getCode());
        }

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Intruder tries to read on MASTER (should fail with PERMISSION_DENIED)
        try {
            client.setMode(Mode.MASTER)
                    .streamOrderedSet(key, keyHint, INTRUDER_CLIENT_ID)
                    .get();
            fail("Expected PERMISSION_DENIED for read with WRITE_LOCK on master");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.PERMISSION_DENIED, cause.getStatus().getCode());
        }

        // Intruder tries to write (should fail with PERMISSION_DENIED) on BACKUP
        try {
            client.setMode(Mode.BACKUP)
                    .addElementWithWeight(key, keyHint, List.of(OrderedPayload.of(3L, bytes("3"))), INTRUDER_CLIENT_ID)
                    .get();
            fail("Expected PERMISSION_DENIED for write with WRITE_LOCK");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.PERMISSION_DENIED, cause.getStatus().getCode());
        }

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Intruder tries to write on MASTER (should fail with PERMISSION_DENIED)
        try {
            client.setMode(Mode.MASTER)
                    .addElementWithWeight(key, keyHint, List.of(OrderedPayload.of(4L, bytes("4"))), INTRUDER_CLIENT_ID)
                    .get();
            fail("Expected PERMISSION_DENIED for write with WRITE_LOCK on master");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.PERMISSION_DENIED, cause.getStatus().getCode());
        }

        // Unlock on BACKUP by owner
        LockStatus unlockStatus = client.setMode(Mode.BACKUP)
                .unlockObject(key, keyHint, OWNER_CLIENT_ID)
                .get();
        assertEquals(LockStatus.OK, unlockStatus);

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify unlock on MASTER
        LockStatus unlockStatusMaster = client.setMode(Mode.MASTER)
                .unlockObject(key, keyHint, OWNER_CLIENT_ID)
                .get();
        assertEquals(LockStatus.OK, unlockStatusMaster);

        // Re-lock by owner to test CANT_UNLOCK
        LockStatus lockStatus2 = client.setMode(Mode.BACKUP)
                .lockObject(key, keyHint, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(60))
                .get();
        assertEquals(LockStatus.OK, lockStatus2);
        Thread.sleep(REPLICATION_DELAY_MS); // replication delay
        // Unlock by intruder on MASTER should fail
        LockStatus failedUnlock = client.setMode(Mode.MASTER)
                .unlockObject(key, keyHint, INTRUDER_CLIENT_ID)
                .get();
        assertEquals(LockStatus.CANT_UNLOCK, failedUnlock);

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify CANT_UNLOCK on BACKUP
        LockStatus failedUnlockBackup = client.setMode(Mode.BACKUP)
                .unlockObject(key, keyHint, INTRUDER_CLIENT_ID)
                .get();
        assertEquals(LockStatus.CANT_UNLOCK, failedUnlockBackup);

        // Unlock by owner on MASTER should succeed
        LockStatus successUnlock = client.setMode(Mode.MASTER)
                .unlockObject(key, keyHint, OWNER_CLIENT_ID)
                .get();
        assertEquals(LockStatus.OK, successUnlock);

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify unlock on BACKUP
        LockStatus successUnlockBackup = client.setMode(Mode.BACKUP)
                .unlockObject(key, keyHint, OWNER_CLIENT_ID)
                .get();
        assertEquals(LockStatus.OK, successUnlockBackup);


    }

    @Test
    @DisplayName("LockObject GLOBAL: only owner any operations, others denied, unlock by owner only")
    void testLockObjectGlobalLock() throws ExecutionException, InterruptedException {
        String key = "globalLockOrderedSetTest" + UUID.randomUUID();

        // Create ordered set
        KeyHintData keyHint = client.createOrderedSet(key, List.of(OrderedPayload.of(1L, bytes("1"))))
                .get();

        Thread.sleep(REPLICATION_DELAY_MS); // replication wait

        // Verify on MASTER: streamOrderedSet returns 1 element
        List<OrderedPayload> masterStream = client.setMode(Mode.MASTER)
                .streamOrderedSet(key, keyHint)
                .get();
        assertNotNull(masterStream);
        assertEquals(1, masterStream.size());

        // Verify on BACKUP: streamOrderedSet returns 1 element
        List<OrderedPayload> backupStream = client.setMode(Mode.BACKUP)
                .streamOrderedSet(key, keyHint)
                .get();
        assertNotNull(backupStream);
        assertEquals(1, backupStream.size());

        // Lock on BACKUP: GLOBAL
        LockStatus lockStatus = client.setMode(Mode.BACKUP)
                .lockObject(key, keyHint, LockType.GLOBAL, OWNER_CLIENT_ID, Duration.ofSeconds(60))
                .get();
        assertEquals(LockStatus.OK, lockStatus);

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on BACKUP: owner can read (streamOrderedSet)
        List<OrderedPayload> backupStreamOwner = client.setMode(Mode.BACKUP)
                .streamOrderedSet(key, keyHint, OWNER_CLIENT_ID)
                .get();
        assertNotNull(backupStreamOwner);

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on MASTER: owner can read (streamOrderedSet)
        List<OrderedPayload> masterStreamOwner = client.setMode(Mode.MASTER)
                .streamOrderedSet(key, keyHint, OWNER_CLIENT_ID)
                .get();
        assertNotNull(masterStreamOwner);

        // Intruder tries to read (should fail with PERMISSION_DENIED) on BACKUP
        try {
            client.setMode(Mode.BACKUP)
                    .streamOrderedSet(key, keyHint, INTRUDER_CLIENT_ID)
                    .get();
            fail("Expected PERMISSION_DENIED for read with GLOBAL_LOCK");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.PERMISSION_DENIED, cause.getStatus().getCode());
        }

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Intruder tries to read on MASTER (should fail with PERMISSION_DENIED)
        try {
            client.setMode(Mode.MASTER)
                    .streamOrderedSet(key, keyHint, INTRUDER_CLIENT_ID)
                    .get();
            fail("Expected PERMISSION_DENIED for read with GLOBAL_LOCK on master");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.PERMISSION_DENIED, cause.getStatus().getCode());
        }

        // Intruder tries to write (should fail with PERMISSION_DENIED) on BACKUP
        try {
            client.setMode(Mode.BACKUP)
                    .addElementWithWeight(key, keyHint, List.of(OrderedPayload.of(2L, bytes("2"))))
                    .get();
            fail("Expected PERMISSION_DENIED for write with GLOBAL_LOCK");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.PERMISSION_DENIED, cause.getStatus().getCode());
        }

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Intruder tries to write on MASTER (should fail with PERMISSION_DENIED)
        try {
            client.setMode(Mode.MASTER)
                    .addElementWithWeight(key, keyHint, List.of(OrderedPayload.of(3L, bytes("3"))))
                    .get();
            fail("Expected PERMISSION_DENIED for write with GLOBAL_LOCK on master");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.PERMISSION_DENIED, cause.getStatus().getCode());
        }

        // Unlock on BACKUP by owner (OWNER_CLIENT_ID)
        LockStatus unlockStatus = client.setMode(Mode.BACKUP)
                .unlockObject(key, keyHint, OWNER_CLIENT_ID)
                .get();
        assertEquals(LockStatus.OK, unlockStatus);

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify unlock on MASTER
        LockStatus unlockStatusMaster = client.setMode(Mode.MASTER)
                .unlockObject(key, keyHint, OWNER_CLIENT_ID)
                .get();
        assertEquals(LockStatus.OK, unlockStatusMaster);
        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Re-lock by owner to test CANT_UNLOCK
        LockStatus lockStatus2 = client.setMode(Mode.BACKUP)
                .lockObject(key, keyHint, LockType.GLOBAL, OWNER_CLIENT_ID, Duration.ofSeconds(60))
                .get();
        assertEquals(LockStatus.OK, lockStatus2);
        Thread.sleep(REPLICATION_DELAY_MS); // replication delay
        // Unlock by non-owner (INTRUDER_CLIENT_ID) on MASTER should fail
        LockStatus failedUnlock = client.setMode(Mode.MASTER)
                .unlockObject(key, keyHint, INTRUDER_CLIENT_ID)
                .get();
        assertEquals(LockStatus.CANT_UNLOCK, failedUnlock);

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify CANT_UNLOCK on BACKUP
        LockStatus failedUnlockBackup = client.setMode(Mode.BACKUP)
                .unlockObject(key, keyHint, INTRUDER_CLIENT_ID)
                .get();
        assertEquals(LockStatus.CANT_UNLOCK, failedUnlockBackup);

        // Unlock by owner (OWNER_CLIENT_ID) on MASTER should succeed
        LockStatus successUnlock = client.setMode(Mode.MASTER)
                .unlockObject(key, keyHint, OWNER_CLIENT_ID)
                .get();
        assertEquals(LockStatus.OK, successUnlock);

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify unlock on BACKUP
        LockStatus successUnlockBackup = client.setMode(Mode.BACKUP)
                .unlockObject(key, keyHint, OWNER_CLIENT_ID)
                .get();
        assertEquals(LockStatus.OK, successUnlockBackup);
    }

    @Test
    @DisplayName("UnlockObject: only lock owner can unlock, others get CANT_UNLOCK")
    void testUnlockObjectOwnerOnly() throws ExecutionException, InterruptedException {
        String key = "unlockOwnerOrderedSetTest" + UUID.randomUUID();

        // Create ordered set
        KeyHintData keyHint = client.createOrderedSet(key, List.of(OrderedPayload.of(1L, bytes("1"))))
                .get();

        Thread.sleep(REPLICATION_DELAY_MS); // replication wait

        // Verify on MASTER: streamOrderedSet returns 1 element
        List<OrderedPayload> masterStream = client.setMode(Mode.MASTER)
                .streamOrderedSet(key, keyHint)
                .get();
        assertNotNull(masterStream);
        assertEquals(1, masterStream.size());

        // Verify on BACKUP: streamOrderedSet returns 1 element
        List<OrderedPayload> backupStream = client.setMode(Mode.BACKUP)
                .streamOrderedSet(key, keyHint)
                .get();
        assertNotNull(backupStream);
        assertEquals(1, backupStream.size());

        // Lock by owner clientId=400 on BACKUP
        LockStatus lockStatus = client.setMode(Mode.BACKUP)
                .lockObject(key, keyHint, LockType.WRITE_LOCK, 400, Duration.ofSeconds(60))
                .get();
        assertEquals(LockStatus.OK, lockStatus);

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify lock replicated on MASTER
        LockStatus lockStatusMaster = client.setMode(Mode.MASTER)
                .lockObject(key, keyHint, LockType.WRITE_LOCK, 400, Duration.ofSeconds(60))
                .get();
        assertEquals(LockStatus.OK, lockStatusMaster);

        // Try unlock by non-owner (clientId=999) should fail with CANT_UNLOCK on BACKUP
        LockStatus failedUnlock = client.setMode(Mode.BACKUP)
                .unlockObject(key, keyHint, 999)
                .get();
        assertEquals(LockStatus.CANT_UNLOCK, failedUnlock);

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Try unlock by non-owner (clientId=999) on MASTER should also fail
        LockStatus failedUnlockMaster = client.setMode(Mode.MASTER)
                .unlockObject(key, keyHint, 999)
                .get();
        assertEquals(LockStatus.CANT_UNLOCK, failedUnlockMaster);

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on MASTER: lockObject by non-owner should still fail (lock still active)
        LockStatus cantLock = client.setMode(Mode.MASTER)
                .lockObject(key, keyHint, LockType.WRITE_LOCK, 999, Duration.ofSeconds(60))
                .get();
        assertEquals(LockStatus.CANT_LOCK, cantLock);

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on BACKUP: lockObject by non-owner should also fail
        LockStatus cantLockBackup = client.setMode(Mode.BACKUP)
                .lockObject(key, keyHint, LockType.WRITE_LOCK, 999, Duration.ofSeconds(60))
                .get();
        assertEquals(LockStatus.CANT_LOCK, cantLockBackup);

        // Unlock by owner (clientId=400) on BACKUP should succeed
        LockStatus successUnlock = client.setMode(Mode.BACKUP)
                .unlockObject(key, keyHint, 400)
                .get();
        assertEquals(LockStatus.OK, successUnlock);

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify unlock on MASTER
        LockStatus successUnlockMaster = client.setMode(Mode.MASTER)
                .unlockObject(key, keyHint, 400)
                .get();
        assertEquals(LockStatus.OK, successUnlockMaster);
    }

    @Test
    @DisplayName("Lock expiration: set short TTL lock on master, verify both nodes after lock expires")
    void testLockExpirationOnMaster() throws ExecutionException, InterruptedException {
        String key = "lockExpirationOrderedSetMaster" + UUID.randomUUID();

        // Create ordered set
        KeyHintData keyHint = client.createOrderedSet(key, List.of(OrderedPayload.of(1L, bytes("1"))))
                .get();

        Thread.sleep(REPLICATION_DELAY_MS); // replication wait

        // Verify on MASTER: streamOrderedSet returns 1 element
        List<OrderedPayload> masterStream = client.setMode(Mode.MASTER)
                .streamOrderedSet(key, keyHint)
                .get();
        assertNotNull(masterStream);
        assertEquals(1, masterStream.size());

        // Verify on BACKUP: streamOrderedSet returns 1 element
        List<OrderedPayload> backupStream = client.setMode(Mode.BACKUP)
                .streamOrderedSet(key, keyHint)
                .get();
        assertNotNull(backupStream);
        assertEquals(1, backupStream.size());

        // Lock on MASTER: WRITE_LOCK with 2s TTL
        LockStatus lockStatus = client.setMode(Mode.MASTER)
                .lockObject(key, keyHint, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(2))
                .get();
        assertEquals(LockStatus.OK, lockStatus);

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on MASTER: owner can write (addElementWithWeight)
        client.setMode(Mode.MASTER)
                .addElementWithWeight(key, keyHint, List.of(OrderedPayload.of(2L, bytes("2"))), OWNER_CLIENT_ID)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on BACKUP: streamOrderedSet shows lock is replicated
        List<OrderedPayload> backupStreamAfter = client.setMode(Mode.BACKUP)
                .streamOrderedSet(key, keyHint, OWNER_CLIENT_ID, Duration.ofSeconds(5))
                .get();
        assertEquals(2, backupStreamAfter.size());

        // Wait for lock TTL to expire (2s lock + buffer)
        Thread.sleep(3500);

        // Verify on MASTER: streamOrderedSet succeeds (lock expired, data accessible)
        List<OrderedPayload> masterStreamAfter = client.setMode(Mode.MASTER)
                .streamOrderedSet(key, keyHint, OWNER_CLIENT_ID)
                .get();
        assertNotNull(masterStreamAfter);

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on BACKUP: streamOrderedSet succeeds (lock expired, data replicated)
        List<OrderedPayload> backupStreamAfterLock = client.setMode(Mode.BACKUP)
                .streamOrderedSet(key, keyHint, OWNER_CLIENT_ID)
                .get();
        assertNotNull(backupStreamAfterLock);

        // Verify on BACKUP: client 2 can read/write (lock no longer active)
        List<OrderedPayload> backupStreamIntruder = client.setMode(Mode.BACKUP)
                .streamOrderedSet(key, keyHint, INTRUDER_CLIENT_ID)
                .get();
        assertNotNull(backupStreamIntruder);

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on MASTER: client 2 can read/write (lock no longer active)
        List<OrderedPayload> masterStreamIntruder = client.setMode(Mode.MASTER)
                .streamOrderedSet(key, keyHint, INTRUDER_CLIENT_ID)
                .get();
        assertNotNull(masterStreamIntruder);
    }

    @Test
    @DisplayName("Lock expiration: set short TTL lock on backup, verify both nodes after lock expires")
    void testLockExpirationOnBackup() throws ExecutionException, InterruptedException {
        String key = "lockExpirationOrderedSetBackup" + UUID.randomUUID();

        // Create ordered set on BACKUP
        KeyHintData keyHint = client.setMode(Mode.BACKUP)
                .createOrderedSet(key, List.of(OrderedPayload.of(1L, bytes("1"))))
                .get();

        Thread.sleep(REPLICATION_DELAY_MS); // replication wait

        // Verify on BACKUP: streamOrderedSet returns 1 element
        List<OrderedPayload> backupStream = client.setMode(Mode.BACKUP)
                .streamOrderedSet(key, keyHint)
                .get();
        assertNotNull(backupStream);
        assertEquals(1, backupStream.size());

        // Verify on MASTER: streamOrderedSet returns 1 element
        List<OrderedPayload> masterStream = client.setMode(Mode.MASTER)
                .streamOrderedSet(key, keyHint)
                .get();
        assertNotNull(masterStream);
        assertEquals(1, masterStream.size());

        // Lock on BACKUP: WRITE_LOCK with 2s TTL
        LockStatus lockStatus = client.setMode(Mode.BACKUP)
                .lockObject(key, keyHint, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(2))
                .get();
        assertEquals(LockStatus.OK, lockStatus);

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on BACKUP: owner can write (addElementWithWeight)
        client.setMode(Mode.BACKUP)
                .addElementWithWeight(key, keyHint, List.of(OrderedPayload.of(2L, bytes("2"))), OWNER_CLIENT_ID)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on MASTER: streamOrderedSet shows lock is replicated
        List<OrderedPayload> masterStreamAfter = client.setMode(Mode.MASTER)
                .streamOrderedSet(key, keyHint, OWNER_CLIENT_ID)
                .get();
        assertEquals(2, masterStreamAfter.size());

        // Wait for lock TTL to expire (2s lock + buffer)
        Thread.sleep(3500);

        // Verify on BACKUP: streamOrderedSet succeeds (lock expired, data accessible)
        List<OrderedPayload> backupStreamAfter = client.setMode(Mode.BACKUP)
                .streamOrderedSet(key, keyHint, OWNER_CLIENT_ID)
                .get();
        assertNotNull(backupStreamAfter);

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on MASTER: streamOrderedSet succeeds (lock expired, data replicated)
        List<OrderedPayload> masterStreamAfterLock = client.setMode(Mode.MASTER)
                .streamOrderedSet(key, keyHint, OWNER_CLIENT_ID)
                .get();
        assertNotNull(masterStreamAfterLock);

        // Verify on MASTER: client 2 can read/write (lock no longer active)
        List<OrderedPayload> masterStreamIntruder = client.setMode(Mode.MASTER)
                .streamOrderedSet(key, keyHint, INTRUDER_CLIENT_ID)
                .get();
        assertNotNull(masterStreamIntruder);

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on BACKUP: client 2 can read/write (lock no longer active)
        List<OrderedPayload> backupStreamIntruder = client.setMode(Mode.BACKUP)
                .streamOrderedSet(key, keyHint, INTRUDER_CLIENT_ID)
                .get();
        assertNotNull(backupStreamIntruder);

        assertNotNull(backupStreamIntruder);
    }

    // ==================== Section 11: Get Element By Weight Operations ====================

    @Test
    @DisplayName("getElementWithWeight on master, verify replication and write on backup")
    void testGetElementWithWeight() throws ExecutionException, InterruptedException {
        String key = "getElementWithWeightTest" + UUID.randomUUID();
        List<OrderedPayload> initialData = List.of(
                OrderedPayload.of(10L, bytes("1")),
                OrderedPayload.of(20L, bytes("2")),
                OrderedPayload.of(30L, bytes("3")),
                OrderedPayload.of(40L, bytes("4")),
                OrderedPayload.of(50L, bytes("5"))
        );

        // Create ordered set
        KeyHintData keyHint = client.createOrderedSet(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS); // replication wait

        // Read on BACKUP: getElementWithWeight(key, 30)
        Payload result = client.setMode(Mode.BACKUP)
                .getElementWithWeight(key, keyHint, 30)
                .get();
        assertNotNull(result);
        assertEquals("3", new String(result.getValue(), StandardCharsets.UTF_8));

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on BACKUP: getElementWithWeight(key, 10)
        Payload result10 = client.setMode(Mode.BACKUP)
                .getElementWithWeight(key, keyHint, 10)
                .get();
        assertNotNull(result10);
        assertEquals("1", new String(result10.getValue(), StandardCharsets.UTF_8));

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on MASTER: getElementWithWeight(key, 30)
        Payload masterResult = client.setMode(Mode.MASTER)
                .getElementWithWeight(key, keyHint, 30)
                .get();
        assertNotNull(masterResult);
        assertEquals("3", new String(masterResult.getValue(), StandardCharsets.UTF_8));

        // Write on MASTER: addElementWithWeight
        client.setMode(Mode.MASTER)
                .addElementWithWeight(key, keyHint, List.of(OrderedPayload.of(60L, bytes("6"))))
                .get();

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on MASTER: getElementWithWeight(key, 60)
        Payload masterNew = client.setMode(Mode.MASTER)
                .getElementWithWeight(key, keyHint, 60)
                .get();
        assertNotNull(masterNew);
        assertEquals("6", new String(masterNew.getValue(), StandardCharsets.UTF_8));

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on BACKUP: getElementWithWeight(key, 60)
        Payload backupNew = client.setMode(Mode.BACKUP)
                .getElementWithWeight(key, keyHint, 60)
                .get();
        assertNotNull(backupNew);
        assertEquals("6", new String(backupNew.getValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("getElementWithWeight returns any matching element if multiple have the same weight")
    void testGetElementWithWeightMultipleSameWeight() throws ExecutionException, InterruptedException {
        String key = "getElementWithWeightMulti" + UUID.randomUUID();
        List<OrderedPayload> initialData = List.of(
                OrderedPayload.of(20L, bytes("1")),
                OrderedPayload.of(20L, bytes("2")),
                OrderedPayload.of(20L, bytes("3")),
                OrderedPayload.of(40L, bytes("4"))
        );

        // Create ordered set
        KeyHintData keyHint = client.createOrderedSet(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS); // replication wait

        // Verify on MASTER: streamOrderedSet returns 4 elements
        List<OrderedPayload> masterStream = client.setMode(Mode.MASTER)
                .streamOrderedSet(key, keyHint)
                .get();
        assertNotNull(masterStream);
        assertEquals(4, masterStream.size());

        // Verify on BACKUP: streamOrderedSet returns 4 elements
        List<OrderedPayload> backupStream = client.setMode(Mode.BACKUP)
                .streamOrderedSet(key, keyHint)
                .get();
        assertNotNull(backupStream);
        assertEquals(4, backupStream.size());

        // Read on BACKUP: getElementWithWeight(key, 20)
        Payload result = client.setMode(Mode.BACKUP)
                .getElementWithWeight(key, keyHint, 20)
                .get();
        assertNotNull(result);
        String value = new String(result.getValue(), StandardCharsets.UTF_8);
        assertTrue(value.equals("1") || value.equals("2") || value.equals("3"));

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on MASTER: also returns one of the items
        Payload masterResult = client.setMode(Mode.MASTER)
                .getElementWithWeight(key, keyHint, 20)
                .get();
        assertNotNull(masterResult);
    }

    @Test
    @DisplayName("getElementAtPosition throws NOT_FOUND if weight does not exist")
    void testGetElementAtPositionNotFound() throws ExecutionException, InterruptedException {
        String key = "getElementAtPositionNotFound" + UUID.randomUUID();
        List<OrderedPayload> initialData = List.of(
                OrderedPayload.of(10L, bytes("1")),
                OrderedPayload.of(20L, bytes("2")),
                OrderedPayload.of(30L, bytes("3"))
        );

        // Create ordered set
        KeyHintData keyHint = client.createOrderedSet(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS); // replication wait

        // Verify on MASTER: streamOrderedSet returns 3 elements
        List<OrderedPayload> masterStream = client.setMode(Mode.MASTER)
                .streamOrderedSet(key, keyHint)
                .get();
        assertNotNull(masterStream);
        assertEquals(3, masterStream.size());

        // Verify on BACKUP: streamOrderedSet returns 3 elements
        List<OrderedPayload> backupStream = client.setMode(Mode.BACKUP)
                .streamOrderedSet(key, keyHint)
                .get();
        assertNotNull(backupStream);
        assertEquals(3, backupStream.size());

        // Read on BACKUP: getElementAtPosition(key, 100) should fail
        try {
            client.setMode(Mode.BACKUP)
                    .getElementAtPosition(key, keyHint, 100)
                    .get();
            fail("getElementAtPosition must throw NOT_FOUND for non-existent weight");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on MASTER: also fails
        try {
            client.setMode(Mode.MASTER)
                    .getElementAtPosition(key, keyHint, 100)
                    .get();
            fail("getElementAtPosition must throw NOT_FOUND for non-existent weight");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    @Test
    @DisplayName("getAndRemoveElementWithWeight on master, verify replication and write on backup")
    void testGetAndRemoveElementWithWeight() throws ExecutionException, InterruptedException {
        String key = "getAndRemoveElementWithWeightTest" + UUID.randomUUID();
        List<OrderedPayload> initialData = List.of(
                OrderedPayload.of(10L, bytes("1")),
                OrderedPayload.of(20L, bytes("2")),
                OrderedPayload.of(30L, bytes("3"))
        );

        // Create ordered set
        KeyHintData keyHint = client.createOrderedSet(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS); // replication wait

        // Read on BACKUP: getAndRemoveElementWithWeight(key, 20)
        Payload removed = client.setMode(Mode.BACKUP)
                .getAndRemoveElementWithWeight(key, keyHint, 20)
                .get();
        assertNotNull(removed);
        assertEquals("2", new String(removed.getValue(), StandardCharsets.UTF_8));

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on BACKUP: streamOrderedSet returns 2 elements
        List<OrderedPayload> backupStream = client.setMode(Mode.BACKUP)
                .streamOrderedSet(key, keyHint)
                .get();
        assertEquals(2, backupStream.size());

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on MASTER: streamOrderedSet returns 2 elements
        List<OrderedPayload> masterStream = client.setMode(Mode.MASTER)
                .streamOrderedSet(key, keyHint)
                .get();
        assertEquals(2, masterStream.size());

        // Read on MASTER: getAndRemoveElementWithWeight(key, 10)
        Payload removedMaster = client.setMode(Mode.MASTER)
                .getAndRemoveElementWithWeight(key, keyHint, 10)
                .get();
        assertNotNull(removedMaster);
        assertEquals("1", new String(removedMaster.getValue(), StandardCharsets.UTF_8));

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on MASTER: streamOrderedSet returns 1 element
        List<OrderedPayload> masterAfterWrite = client.setMode(Mode.MASTER)
                .streamOrderedSet(key, keyHint)
                .get();
        assertEquals(1, masterAfterWrite.size());

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on BACKUP: streamOrderedSet returns 1 element
        List<OrderedPayload> backupAfterWrite = client.setMode(Mode.BACKUP)
                .streamOrderedSet(key, keyHint)
                .get();
        assertEquals(1, backupAfterWrite.size());
    }

    @Test
    @DisplayName("getAndRemoveElementAtPosition throws NOT_FOUND if weight does not exist")
    void testGetAndRemoveElementAtPositionNotFound() throws ExecutionException, InterruptedException {
        String key = "getAndRemoveElementAtPositionNotFound" + UUID.randomUUID();
        List<OrderedPayload> initialData = List.of(
                OrderedPayload.of(10L, bytes("1")),
                OrderedPayload.of(20L, bytes("2"))
        );

        // Create ordered set
        KeyHintData keyHint = client.createOrderedSet(key, initialData)
                .get();

        Thread.sleep(REPLICATION_DELAY_MS); // replication wait

        // Verify on MASTER: streamOrderedSet returns 2 elements
        List<OrderedPayload> masterStream = client.setMode(Mode.MASTER)
                .streamOrderedSet(key, keyHint)
                .get();
        assertNotNull(masterStream);
        assertEquals(2, masterStream.size());

        // Verify on BACKUP: streamOrderedSet returns 2 elements
        List<OrderedPayload> backupStream = client.setMode(Mode.BACKUP)
                .streamOrderedSet(key, keyHint)
                .get();
        assertNotNull(backupStream);
        assertEquals(2, backupStream.size());

        // Read on BACKUP: getAndRemoveElementAtPosition(key, 100) should fail
        try {
            client.setMode(Mode.BACKUP)
                    .getAndRemoveElementAtPosition(key, keyHint, 100)
                    .get();
            fail("getAndRemoveElementAtPosition must throw NOT_FOUND for non-existent weight");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }

        Thread.sleep(REPLICATION_DELAY_MS); // replication delay

        // Verify on MASTER: also fails
        try {
            client.setMode(Mode.MASTER)
                    .getAndRemoveElementAtPosition(key, keyHint, 100)
                    .get();
            fail("getAndRemoveElementAtPosition must throw NOT_FOUND for non-existent weight");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

}