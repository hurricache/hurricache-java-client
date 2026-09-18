package com.hurricache.client.cluster.smart;

import com.hurricache.TestBaseCluster;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.OrderedPayload;
import com.hurricache.client.intf.Payload;
import com.hurricache.client.intf.Mode;
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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class OrderedSetOperationsTest extends TestBaseCluster {

    // ==================== Section 1: Ordered Set Creation ====================

    @Test
    @DisplayName("Create empty ordered set on master, verify replication to backup")
    void testCreateEmptyOrderedSet() throws ExecutionException, InterruptedException {
        String key = "emptyOrderedSet" + UUID.randomUUID();

        // Create empty ordered set
        KeyHintData keyHint = client.createOrderedSet(key, new ArrayList<>())
                .get();

        Thread.sleep(500); // replication wait

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
                OrderedPayload.of(1L, "item1".getBytes(StandardCharsets.UTF_8)),
                OrderedPayload.of(2L, "item2".getBytes(StandardCharsets.UTF_8)),
                OrderedPayload.of(3L, "item3".getBytes(StandardCharsets.UTF_8))
        );

        // Create ordered set
        KeyHintData keyHint = client.createOrderedSet(key, initialData)
                .get();

        Thread.sleep(500); // replication wait

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
                .addElementWithWeight(key, keyHint, List.of(OrderedPayload.of(4L, "item4".getBytes(StandardCharsets.UTF_8))))
                .get();

        Thread.sleep(300); // replication delay

        // Verify on BACKUP: streamOrderedSet returns 4 elements
        List<OrderedPayload> backupAfterWrite = client.setMode(Mode.BACKUP)
                .streamOrderedSet(key, keyHint)
                .get();
        assertEquals(4, backupAfterWrite.size());
        assertEquals(4L, backupAfterWrite.get(3).getOrder());

        Thread.sleep(300); // replication delay

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
            payloads.add(OrderedPayload.of((long) i, ("large_item_" + i + "_" + UUID.randomUUID()).getBytes(StandardCharsets.UTF_8)));
        }

        // Create large ordered set
        KeyHintData keyHint = client.createOrderedSet(key, payloads)
                .get();

        Thread.sleep(500); // replication wait

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
        assertEquals((long)(elementCount - 1), masterStream.get(elementCount - 1).getOrder(), "Last element mismatch on master");
        assertEquals(0L, backupStream.get(0).getOrder(), "First element mismatch on backup");
        assertEquals((long)(elementCount - 1), backupStream.get(elementCount - 1).getOrder(), "Last element mismatch on backup");
    }

    // ==================== Section 2: Stream Ordered Set Operations ====================

    @Test
    @DisplayName("Stream ordered set on master, verify replication and write on backup")
    void testStreamOrderedSet() throws ExecutionException, InterruptedException {
        String key = "streamOrderedSetTest" + UUID.randomUUID();
        List<OrderedPayload> initialData = List.of(
                OrderedPayload.of(1L, "elem1".getBytes(StandardCharsets.UTF_8)),
                OrderedPayload.of(2L, "elem2".getBytes(StandardCharsets.UTF_8)),
                OrderedPayload.of(3L, "elem3".getBytes(StandardCharsets.UTF_8))
        );

        // Create ordered set
        KeyHintData keyHint = client.createOrderedSet(key, initialData)
                .get();

        Thread.sleep(500); // replication wait

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
                .addElementWithWeight(key, keyHint, List.of(OrderedPayload.of(4L, "elem4".getBytes(StandardCharsets.UTF_8))))
                .get();

        Thread.sleep(300); // replication delay

        // Verify on BACKUP: streamOrderedSet returns 4 elements
        List<OrderedPayload> backupAfterWrite = client.setMode(Mode.BACKUP)
                .streamOrderedSet(key, keyHint)
                .get();
        assertEquals(4, backupAfterWrite.size());

        Thread.sleep(300); // replication delay

        // Verify on MASTER: streamOrderedSet returns 4 elements
        List<OrderedPayload> masterAfterWrite = client.setMode(Mode.MASTER)
                .streamOrderedSet(key, keyHint)
                .get();
        assertEquals(4, masterAfterWrite.size());
    }

    // ==================== Section 3: Add Element With Weight Operations ====================

    @Test
    @DisplayName("addElementWithWeight on master, verify replication and write on backup")
    void testAddElementWithWeight() throws ExecutionException, InterruptedException {
        String key = "addElementWithWeightTest" + UUID.randomUUID();
        List<OrderedPayload> initialData = List.of(OrderedPayload.of(1L, "item1".getBytes(StandardCharsets.UTF_8)));

        // Create ordered set
        KeyHintData keyHint = client.createOrderedSet(key, initialData)
                .get();

        Thread.sleep(500); // replication wait

        // Write on BACKUP: addElementWithWeight
        Integer added = client.setMode(Mode.BACKUP)
                .addElementWithWeight(key, keyHint, List.of(
                        OrderedPayload.of(2L, "item2".getBytes(StandardCharsets.UTF_8)),
                        OrderedPayload.of(3L, "item3".getBytes(StandardCharsets.UTF_8))
                ))
                .get();
        assertNotNull(added);
        assertEquals(2, added);

        Thread.sleep(300); // replication delay

        // Verify on BACKUP: streamOrderedSet returns 3 elements
        List<OrderedPayload> backupStream = client.setMode(Mode.BACKUP)
                .streamOrderedSet(key, keyHint)
                .get();
        assertEquals(3, backupStream.size());
        assertEquals(1L, backupStream.get(0).getOrder());
        assertEquals(2L, backupStream.get(1).getOrder());
        assertEquals(3L, backupStream.get(2).getOrder());

        Thread.sleep(300); // replication delay

        // Verify on MASTER: streamOrderedSet returns 3 elements
        List<OrderedPayload> masterStream = client.setMode(Mode.MASTER)
                .streamOrderedSet(key, keyHint)
                .get();
        assertEquals(3, masterStream.size());

        // Write on MASTER: addElementWithWeight
        client.setMode(Mode.MASTER)
                .addElementWithWeight(key, keyHint, List.of(OrderedPayload.of(4L, "item4".getBytes(StandardCharsets.UTF_8))))
                .get();

        Thread.sleep(300); // replication delay

        // Verify on MASTER: streamOrderedSet returns 4 elements
        List<OrderedPayload> masterAfterWrite = client.setMode(Mode.MASTER)
                .streamOrderedSet(key, keyHint)
                .get();
        assertEquals(4, masterAfterWrite.size());

        Thread.sleep(300); // replication delay

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
        List<OrderedPayload> initialData = List.of(OrderedPayload.of(1L, "item1".getBytes(StandardCharsets.UTF_8)));

        // Create ordered set
        KeyHintData keyHint = client.createOrderedSet(key, initialData)
                .get();

        Thread.sleep(500); // replication wait

        // Write on BACKUP: add duplicates
        Integer added = client.setMode(Mode.BACKUP)
                .addElementWithWeight(key, keyHint, List.of(
                        OrderedPayload.of(2L, "item1".getBytes(StandardCharsets.UTF_8)),
                        OrderedPayload.of(3L, "item1".getBytes(StandardCharsets.UTF_8))
                ))
                .get();
        assertEquals(2, added, "2 elements should be added (duplicates allowed)");

        Thread.sleep(300); // replication delay

        // Verify on BACKUP: streamOrderedSet returns 3 elements
        List<OrderedPayload> backupStream = client.setMode(Mode.BACKUP)
                .streamOrderedSet(key, keyHint)
                .get();
        assertEquals(3, backupStream.size());

        Thread.sleep(300); // replication delay

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
        List<OrderedPayload> initialData = List.of(OrderedPayload.of(1L, "item1".getBytes(StandardCharsets.UTF_8)));

        // Create ordered set
        KeyHintData keyHint = client.createOrderedSet(key, initialData)
                .get();

        Thread.sleep(500); // replication wait

        // Write on BACKUP: addElementWithWeight with empty list
        Integer added = client.setMode(Mode.BACKUP)
                .addElementWithWeight(key, keyHint, new ArrayList<>())
                .get();
        assertEquals(0, added, "Adding an empty list must return 0");

        Thread.sleep(300); // replication delay

        // Verify on BACKUP: streamOrderedSet still returns 1 element
        List<OrderedPayload> backupStream = client.setMode(Mode.BACKUP)
                .streamOrderedSet(key, keyHint)
                .get();
        assertEquals(1, backupStream.size());
    }

    // ==================== Section 4: Stream Element In Range Ordered Operations ====================

    @Test
    @DisplayName("streamElementInRangeOrderedSet on master, verify replication and write on backup")
    void testStreamElementInRangeOrderedSet() throws ExecutionException, InterruptedException {
        String key = "streamRangeOrderedTest" + UUID.randomUUID();
        List<OrderedPayload> initialData = List.of(
                OrderedPayload.of(10L, "item1".getBytes(StandardCharsets.UTF_8)),
                OrderedPayload.of(20L, "item2".getBytes(StandardCharsets.UTF_8)),
                OrderedPayload.of(30L, "item3".getBytes(StandardCharsets.UTF_8)),
                OrderedPayload.of(40L, "item4".getBytes(StandardCharsets.UTF_8)),
                OrderedPayload.of(50L, "item5".getBytes(StandardCharsets.UTF_8))
        );

        // Create ordered set
        KeyHintData keyHint = client.createOrderedSet(key, initialData)
                .get();

        Thread.sleep(500); // replication wait

        // Write on BACKUP: streamElementInRangeOrderedSet(key, 20, 40)
        List<OrderedPayload> rangeData = client.setMode(Mode.BACKUP)
                .streamElementInRangeOrderedSet(key.getBytes(StandardCharsets.UTF_8), keyHint, 20L, 40L, false, 1, Duration.ofSeconds(30))
                .get();
        assertNotNull(rangeData);
        assertEquals(3, rangeData.size());
        assertEquals(20L, rangeData.get(0).getOrder());
        assertEquals(30L, rangeData.get(1).getOrder());
        assertEquals(40L, rangeData.get(2).getOrder());

        Thread.sleep(300); // replication delay

        // Verify on MASTER: streamElementInRangeOrderedSet returns 3 elements
        List<OrderedPayload> masterRange = client.setMode(Mode.MASTER)
                .streamElementInRangeOrderedSet(key.getBytes(StandardCharsets.UTF_8), keyHint, 20L, 40L, false, 1, Duration.ofSeconds(30))
                .get();
        assertNotNull(masterRange);
        assertEquals(3, masterRange.size());

        // Write on MASTER: addElementWithWeight
        client.setMode(Mode.MASTER)
                .addElementWithWeight(key, keyHint, List.of(OrderedPayload.of(60L, "item6".getBytes(StandardCharsets.UTF_8))))
                .get();

        Thread.sleep(300); // replication delay

        // Verify on MASTER: streamElementInRangeOrderedSet still returns 3 elements (range 20-40)
        List<OrderedPayload> masterRangeAfter = client.setMode(Mode.MASTER)
                .streamElementInRangeOrderedSet(key.getBytes(StandardCharsets.UTF_8), keyHint, 20L, 40L, false, 1, Duration.ofSeconds(30))
                .get();
        assertEquals(3, masterRangeAfter.size());

        Thread.sleep(300); // replication delay

        // Verify on BACKUP: streamElementInRangeOrderedSet still returns 3 elements
        List<OrderedPayload> backupRangeAfter = client.setMode(Mode.BACKUP)
                .streamElementInRangeOrderedSet(key.getBytes(StandardCharsets.UTF_8), keyHint, 20L, 40L, false, 1, Duration.ofSeconds(30))
                .get();
        assertEquals(3, backupRangeAfter.size());
    }

    @Test
    @DisplayName("streamElementInRangeOrderedSet with reverse=true returns elements in reverse order")
    void testStreamElementInRangeOrderedSetReverse() throws ExecutionException, InterruptedException {
        String key = "streamRangeOrderedReverse" + UUID.randomUUID();
        List<OrderedPayload> initialData = List.of(
                OrderedPayload.of(10L, "item1".getBytes(StandardCharsets.UTF_8)),
                OrderedPayload.of(20L, "item2".getBytes(StandardCharsets.UTF_8)),
                OrderedPayload.of(30L, "item3".getBytes(StandardCharsets.UTF_8)),
                OrderedPayload.of(40L, "item4".getBytes(StandardCharsets.UTF_8)),
                OrderedPayload.of(50L, "item5".getBytes(StandardCharsets.UTF_8))
        );

        // Create ordered set
        KeyHintData keyHint = client.createOrderedSet(key, initialData)
                .get();

        Thread.sleep(500); // replication wait

        // Write on BACKUP: streamElementInRangeOrderedSet with reverse
        List<OrderedPayload> rangeData = client.setMode(Mode.BACKUP)
                .streamElementInRangeOrderedSet(key.getBytes(StandardCharsets.UTF_8), keyHint, 20L, 40L, true, 1, Duration.ofSeconds(30))
                .get();
        assertNotNull(rangeData);
        assertEquals(3, rangeData.size());
        assertEquals(40L, rangeData.get(0).getOrder());
        assertEquals(30L, rangeData.get(1).getOrder());
        assertEquals(20L, rangeData.get(2).getOrder());

        Thread.sleep(300); // replication delay

        // Verify on MASTER: reverse order also works
        List<OrderedPayload> masterRange = client.setMode(Mode.MASTER)
                .streamElementInRangeOrderedSet(key.getBytes(StandardCharsets.UTF_8), keyHint, 20L, 40L, true, 1, Duration.ofSeconds(30))
                .get();
        assertEquals(3, masterRange.size());
        assertEquals(40L, masterRange.get(0).getOrder());
    }

    @Test
    @DisplayName("streamElementInRangeOrderedSet returns empty list if no elements fall in range")
    void testStreamElementInRangeOrderedSetNoMatch() throws ExecutionException, InterruptedException {
        String key = "streamRangeOrderedNoMatch" + UUID.randomUUID();
        List<OrderedPayload> initialData = List.of(
                OrderedPayload.of(10L, "item1".getBytes(StandardCharsets.UTF_8)),
                OrderedPayload.of(20L, "item2".getBytes(StandardCharsets.UTF_8)),
                OrderedPayload.of(30L, "item3".getBytes(StandardCharsets.UTF_8))
        );

        // Create ordered set
        KeyHintData keyHint = client.createOrderedSet(key, initialData)
                .get();

        Thread.sleep(500); // replication wait

        // Write on BACKUP: streamElementInRangeOrderedSet with range 100-200
        List<OrderedPayload> rangeData = client.setMode(Mode.BACKUP)
                .streamElementInRangeOrderedSet(key.getBytes(StandardCharsets.UTF_8), keyHint, 100L, 200L, false, 1, Duration.ofSeconds(30))
                .get();
        assertNotNull(rangeData);
        assertTrue(rangeData.isEmpty(), "List must be empty");

        Thread.sleep(300); // replication delay

        // Verify on MASTER: also empty
        List<OrderedPayload> masterRange = client.setMode(Mode.MASTER)
                .streamElementInRangeOrderedSet(key.getBytes(StandardCharsets.UTF_8), keyHint, 100L, 200L, false, 1, Duration.ofSeconds(30))
                .get();
        assertTrue(masterRange.isEmpty());
    }

    // ==================== Section 5: Remove From Container Operations ====================

    @Test
    @DisplayName("removeFromContainer on master, verify replication and write on backup")
    void testRemoveFromContainer() throws ExecutionException, InterruptedException {
        String key = "removeFromContainerTest" + UUID.randomUUID();
        List<OrderedPayload> initialData = List.of(
                OrderedPayload.of(1L, "item1".getBytes(StandardCharsets.UTF_8)),
                OrderedPayload.of(2L, "item2".getBytes(StandardCharsets.UTF_8)),
                OrderedPayload.of(3L, "item3".getBytes(StandardCharsets.UTF_8))
        );

        // Create ordered set
        KeyHintData keyHint = client.createOrderedSet(key, initialData)
                .get();

        Thread.sleep(500); // replication wait

        // Write on BACKUP: removeFromContainer
        Integer removed = client.setMode(Mode.BACKUP)
                .removeFromContainer(key.getBytes(StandardCharsets.UTF_8), keyHint, OrderedPayload.of(1L, "item1".getBytes(StandardCharsets.UTF_8)).getValue())
                .get();
        assertEquals(1, removed, "1 element should be removed");

        Thread.sleep(300); // replication delay

        // Verify on BACKUP: streamOrderedSet returns 2 elements
        List<OrderedPayload> backupStream = client.setMode(Mode.BACKUP)
                .streamOrderedSet(key, keyHint)
                .get();
        assertEquals(2, backupStream.size());

        Thread.sleep(300); // replication delay

        // Verify on MASTER: streamOrderedSet returns 2 elements
        List<OrderedPayload> masterStream = client.setMode(Mode.MASTER)
                .streamOrderedSet(key, keyHint)
                .get();
        assertEquals(2, masterStream.size());

        // Write on MASTER: removeFromContainer
        Integer removedMaster = client.setMode(Mode.MASTER)
                .removeFromContainer(key.getBytes(StandardCharsets.UTF_8), keyHint, OrderedPayload.of(2L, "item2".getBytes(StandardCharsets.UTF_8)).getValue())
                .get();
        assertEquals(1, removedMaster);

        Thread.sleep(300); // replication delay

        // Verify on MASTER: streamOrderedSet returns 1 element
        List<OrderedPayload> masterAfterWrite = client.setMode(Mode.MASTER)
                .streamOrderedSet(key, keyHint)
                .get();
        assertEquals(1, masterAfterWrite.size());

        Thread.sleep(300); // replication delay

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
                OrderedPayload.of(1L, "item1".getBytes(StandardCharsets.UTF_8)),
                OrderedPayload.of(2L, "item1".getBytes(StandardCharsets.UTF_8)),
                OrderedPayload.of(3L, "item1".getBytes(StandardCharsets.UTF_8)),
                OrderedPayload.of(4L, "item2".getBytes(StandardCharsets.UTF_8))
        );

        // Create ordered set
        KeyHintData keyHint = client.createOrderedSet(key, initialData)
                .get();

        Thread.sleep(500); // replication wait

        // Write on BACKUP: remove all "item1" (3 items)
        Integer removed = client.setMode(Mode.BACKUP)
                .removeFromContainer(key.getBytes(StandardCharsets.UTF_8), keyHint, OrderedPayload.of(1L, "item1".getBytes(StandardCharsets.UTF_8)).getValue())
                .get();
        assertEquals(3, removed, "3 elements with the same key must be removed");

        Thread.sleep(300); // replication delay

        // Verify on BACKUP: streamOrderedSet returns 1 element
        List<OrderedPayload> backupStream = client.setMode(Mode.BACKUP)
                .streamOrderedSet(key, keyHint)
                .get();
        assertEquals(1, backupStream.size());

        Thread.sleep(300); // replication delay

        // Verify on MASTER: streamOrderedSet returns 1 element
        List<OrderedPayload> masterStream = client.setMode(Mode.MASTER)
                .streamOrderedSet(key, keyHint)
                .get();
        assertEquals(1, masterStream.size());
    }

    @Test
    @DisplayName("removeFromContainer returns 0 if element does not exist")
    void testRemoveFromContainerNonExistent() throws ExecutionException, InterruptedException {
        String key = "removeFromContainerNonExist" + UUID.randomUUID();
        List<OrderedPayload> initialData = List.of(
                OrderedPayload.of(1L, "item1".getBytes(StandardCharsets.UTF_8)),
                OrderedPayload.of(2L, "item2".getBytes(StandardCharsets.UTF_8))
        );

        // Create ordered set
        KeyHintData keyHint = client.createOrderedSet(key, initialData)
                .get();

        Thread.sleep(500); // replication wait

        // Write on BACKUP: removeFromContainer non-existent
        Integer removed = client.setMode(Mode.BACKUP)
                .removeFromContainer(key.getBytes(StandardCharsets.UTF_8), keyHint, OrderedPayload.of(0L, "nonexistent".getBytes(StandardCharsets.UTF_8)).getValue())
                .get();
        assertEquals(0, removed, "0 elements should be removed (element not found)");

        Thread.sleep(300); // replication delay

        // Verify on BACKUP: streamOrderedSet still returns 2 elements
        List<OrderedPayload> backupStream = client.setMode(Mode.BACKUP)
                .streamOrderedSet(key, keyHint)
                .get();
        assertEquals(2, backupStream.size());
    }

    // ==================== Section 6: Contains Container Key Operations ====================

    @Test
    @DisplayName("containsContainerKey on master, verify replication and write on backup")
    void testContainsContainerKey() throws ExecutionException, InterruptedException {
        String key = "containsContainerKeyTest" + UUID.randomUUID();
        List<OrderedPayload> initialData = List.of(
                OrderedPayload.of(1L, "item1".getBytes(StandardCharsets.UTF_8)),
                OrderedPayload.of(2L, "item2".getBytes(StandardCharsets.UTF_8))
        );

        // Create ordered set
        KeyHintData keyHint = client.createOrderedSet(key, initialData)
                .get();

        Thread.sleep(500); // replication wait

        // Write on BACKUP: containsContainerKey for existing element
        Boolean exists = client.setMode(Mode.BACKUP)
                .containsContainerKey(key.getBytes(StandardCharsets.UTF_8), keyHint, OrderedPayload.of(1L, "item1".getBytes(StandardCharsets.UTF_8)).getValue())
                .get();
        assertTrue(exists, "Element item1 must exist");

        Thread.sleep(300); // replication delay

        // Verify on BACKUP: containsContainerKey for non-existing element
        Boolean notExists = client.setMode(Mode.BACKUP)
                .containsContainerKey(key.getBytes(StandardCharsets.UTF_8), keyHint, OrderedPayload.of(0L, "nonexistent".getBytes(StandardCharsets.UTF_8)).getValue())
                .get();
        assertTrue(!notExists, "Element nonexistent must not exist");

        Thread.sleep(300); // replication delay

        // Verify on MASTER: containsContainerKey for existing element
        Boolean masterExists = client.setMode(Mode.MASTER)
                .containsContainerKey(key.getBytes(StandardCharsets.UTF_8), keyHint, OrderedPayload.of(1L, "item1".getBytes(StandardCharsets.UTF_8)).getValue())
                .get();
        assertTrue(masterExists);

        // Write on MASTER: addElementWithWeight
        client.setMode(Mode.MASTER)
                .addElementWithWeight(key, keyHint, List.of(OrderedPayload.of(3L, "item3".getBytes(StandardCharsets.UTF_8))))
                .get();

        Thread.sleep(300); // replication delay

        // Verify on MASTER: containsContainerKey for new element
        Boolean newExists = client.setMode(Mode.MASTER)
                .containsContainerKey(key.getBytes(StandardCharsets.UTF_8), keyHint, OrderedPayload.of(3L, "item3".getBytes(StandardCharsets.UTF_8)).getValue())
                .get();
        assertTrue(newExists);

        Thread.sleep(300); // replication delay

        // Verify on BACKUP: containsContainerKey for new element
        Boolean backupNewExists = client.setMode(Mode.BACKUP)
                .containsContainerKey(key.getBytes(StandardCharsets.UTF_8), keyHint, OrderedPayload.of(3L, "item3".getBytes(StandardCharsets.UTF_8)).getValue())
                .get();
        assertTrue(backupNewExists);
    }

    // ==================== Section 7: Remove Element At Weight Range Operations ====================

    @Test
    @DisplayName("removeElementAtPosition on master, verify replication and write on backup")
    void testRemoveElementAtPosition() throws ExecutionException, InterruptedException {
        String key = "removeElementAtPositionTest" + UUID.randomUUID();
        List<OrderedPayload> initialData = List.of(
                OrderedPayload.of(10L, "item1".getBytes(StandardCharsets.UTF_8)),
                OrderedPayload.of(20L, "item2".getBytes(StandardCharsets.UTF_8)),
                OrderedPayload.of(30L, "item3".getBytes(StandardCharsets.UTF_8)),
                OrderedPayload.of(40L, "item4".getBytes(StandardCharsets.UTF_8)),
                OrderedPayload.of(50L, "item5".getBytes(StandardCharsets.UTF_8))
        );

        // Create ordered set
        KeyHintData keyHint = client.createOrderedSet(key, initialData)
                .get();

        Thread.sleep(500); // replication wait

        // Write on BACKUP: removeElementAtPosition(key, 20, 40)
        Boolean removed = client.setMode(Mode.BACKUP)
                .removeElementAtPosition(key, keyHint, 20, 40)
                .get();
        assertTrue(removed, "Elements must be removed");

        Thread.sleep(300); // replication delay

        // Verify on BACKUP: streamOrderedSet returns 2 elements
        List<OrderedPayload> backupStream = client.setMode(Mode.BACKUP)
                .streamOrderedSet(key, keyHint)
                .get();
        assertEquals(2, backupStream.size());
        assertEquals(10L, backupStream.get(0).getOrder());
        assertEquals(50L, backupStream.get(1).getOrder());

        Thread.sleep(300); // replication delay

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

        Thread.sleep(300); // replication delay

        // Verify on MASTER: streamOrderedSet returns 1 element
        List<OrderedPayload> masterAfterWrite = client.setMode(Mode.MASTER)
                .streamOrderedSet(key, keyHint)
                .get();
        assertEquals(1, masterAfterWrite.size());
        assertEquals(50L, masterAfterWrite.get(0).getOrder());

        Thread.sleep(300); // replication delay

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
                OrderedPayload.of(10L, "item1".getBytes(StandardCharsets.UTF_8)),
                OrderedPayload.of(20L, "item2".getBytes(StandardCharsets.UTF_8)),
                OrderedPayload.of(30L, "item3".getBytes(StandardCharsets.UTF_8))
        );

        // Create ordered set
        KeyHintData keyHint = client.createOrderedSet(key, initialData)
                .get();

        Thread.sleep(500); // replication wait

        // Write on BACKUP: removeElementAtPosition(key, 20, 20)
        Boolean removed = client.setMode(Mode.BACKUP)
                .removeElementAtPosition(key, keyHint, 20, 20)
                .get();
        assertTrue(removed, "Element must be removed");

        Thread.sleep(300); // replication delay

        // Verify on BACKUP: streamOrderedSet returns 2 elements
        List<OrderedPayload> backupStream = client.setMode(Mode.BACKUP)
                .streamOrderedSet(key, keyHint)
                .get();
        assertEquals(2, backupStream.size());
        assertEquals(10L, backupStream.get(0).getOrder());
        assertEquals(30L, backupStream.get(1).getOrder());
    }

    // ==================== Section 8: Set TTL Operations ====================

    @Test
    @DisplayName("SetTtl on master, verify GetTtl on both nodes and write on backup")
    void testSetTtlAndGetTtl() throws ExecutionException, InterruptedException {
        String key = "setTtlOrderedSetTest" + UUID.randomUUID();

        // Create ordered set
        KeyHintData keyHint = client.createOrderedSet(key, List.of(OrderedPayload.of(1L, "item1".getBytes(StandardCharsets.UTF_8))))
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

    // ==================== Section 9: TTL Expiration Operations ====================

    @Test
    @DisplayName("TTL expiration: set on master, verify both nodes after TTL expires")
    void testTtlExpirationOnMaster() throws ExecutionException, InterruptedException {
        String key = "ttlExpirationOrderedSetMaster" + UUID.randomUUID();

        // Create ordered set with 2s TTL
        KeyHintData keyHint = client.createOrderedSet(key,
                        List.of(OrderedPayload.of(1L, "item1".getBytes(StandardCharsets.UTF_8))))
                .get();
        client.setTtl(key, keyHint, 2000).get();

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

        // Verify on MASTER: streamOrderedSet throws NOT_FOUND
        try {
            client.setMode(Mode.MASTER)
                    .streamOrderedSet(key, keyHint)
                    .get();
            fail("Expected NOT_FOUND on master after TTL expiry");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }

        Thread.sleep(300); // replication delay

        // Verify on BACKUP: streamOrderedSet throws NOT_FOUND
        try {
            client.setMode(Mode.BACKUP)
                    .streamOrderedSet(key, keyHint)
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
        String key = "ttlExpirationOrderedSetBackup" + UUID.randomUUID();

        // Create ordered set on BACKUP with 2s TTL
        KeyHintData keyHint = client.setMode(Mode.BACKUP)
                .createOrderedSet(key,
                        List.of(OrderedPayload.of(1L, "item1".getBytes(StandardCharsets.UTF_8))))
                .get();
        client.setMode(Mode.BACKUP).setTtl(key, keyHint, 2000).get();

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

        // Verify on BACKUP: streamOrderedSet throws NOT_FOUND
        try {
            client.setMode(Mode.BACKUP)
                    .streamOrderedSet(key, keyHint)
                    .get();
            fail("Expected NOT_FOUND on backup after TTL expiry");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }

        Thread.sleep(300); // replication delay

        // Verify on MASTER: streamOrderedSet throws NOT_FOUND
        try {
            client.setMode(Mode.MASTER)
                    .streamOrderedSet(key, keyHint)
                    .get();
            fail("Expected NOT_FOUND on master after TTL expiry");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    // ==================== Section 10: Lock Operations ====================

    @Test
    @DisplayName("LockObject READ_LOCK: parallel reads OK, writes denied, unlock by owner only")
    void testLockObjectReadLock() throws ExecutionException, InterruptedException {
        String key = "readLockOrderedSetTest" + UUID.randomUUID();

        // Create ordered set
        KeyHintData keyHint = client.createOrderedSet(key, List.of(OrderedPayload.of(1L, "item1".getBytes(StandardCharsets.UTF_8))))
                .get();

        Thread.sleep(500); // replication wait

        // Write on BACKUP: lockObject READ_LOCK
        LockStatus lockStatus = client.setMode(Mode.BACKUP)
                .lockObject(key, keyHint, LockType.READ_LOCK, 1, Duration.ofSeconds(60))
                .get();
        assertEquals(LockStatus.OK, lockStatus);

        Thread.sleep(300); // replication delay

        // Verify on BACKUP: client 1 can read (streamOrderedSet)
        List<OrderedPayload> backupStream = client.setMode(Mode.BACKUP)
                .streamOrderedSet(key, keyHint, 1)
                .get();
        assertNotNull(backupStream);

        Thread.sleep(300); // replication delay

        // Verify on MASTER: client 1 can read (streamOrderedSet)
        List<OrderedPayload> masterStream = client.setMode(Mode.MASTER)
                .streamOrderedSet(key, keyHint, 1)
                .get();
        assertNotNull(masterStream);

        // Write on BACKUP: client 2 can read (should succeed with READ_LOCK)
        List<OrderedPayload> backupStream2 = client.setMode(Mode.BACKUP)
                .streamOrderedSet(key, keyHint, 2)
                .get();
        assertNotNull(backupStream2);

        // Write on BACKUP: client 2 tries to write (should fail with PERMISSION_DENIED)
        try {
            client.setMode(Mode.BACKUP)
                    .addElementWithWeight(key, keyHint, List.of(OrderedPayload.of(2L, "item2".getBytes(StandardCharsets.UTF_8))))
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
        String key = "writeLockOrderedSetTest" + UUID.randomUUID();

        // Create ordered set
        KeyHintData keyHint = client.createOrderedSet(key, List.of(OrderedPayload.of(1L, "item1".getBytes(StandardCharsets.UTF_8))))
                .get();

        Thread.sleep(500); // replication wait

        // Write on BACKUP: lockObject WRITE_LOCK
        LockStatus lockStatus = client.setMode(Mode.BACKUP)
                .lockObject(key, keyHint, LockType.WRITE_LOCK, 1, Duration.ofSeconds(60))
                .get();
        assertEquals(LockStatus.OK, lockStatus);

        Thread.sleep(300); // replication delay

        // Verify on BACKUP: client 1 can write (addElementWithWeight)
        client.setMode(Mode.BACKUP)
                .addElementWithWeight(key, keyHint, List.of(OrderedPayload.of(2L, "item2".getBytes(StandardCharsets.UTF_8))))
                .get();

        Thread.sleep(300); // replication delay

        // Verify on MASTER: client 1 can read (streamOrderedSet)
        List<OrderedPayload> masterStream = client.setMode(Mode.MASTER)
                .streamOrderedSet(key, keyHint, 1)
                .get();
        assertNotNull(masterStream);

        // Write on BACKUP: client 2 tries to read (should fail with PERMISSION_DENIED)
        try {
            client.setMode(Mode.BACKUP)
                    .streamOrderedSet(key, keyHint, 2)
                    .get();
            fail("Expected PERMISSION_DENIED for read with WRITE_LOCK");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.PERMISSION_DENIED, cause.getStatus().getCode());
        }

        // Write on BACKUP: client 2 tries to write (should fail with PERMISSION_DENIED)
        try {
            client.setMode(Mode.BACKUP)
                    .addElementWithWeight(key, keyHint, List.of(OrderedPayload.of(3L, "item3".getBytes(StandardCharsets.UTF_8))))
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
        String key = "globalLockOrderedSetTest" + UUID.randomUUID();

        // Create ordered set
        KeyHintData keyHint = client.createOrderedSet(key, List.of(OrderedPayload.of(1L, "item1".getBytes(StandardCharsets.UTF_8))))
                .get();

        Thread.sleep(500); // replication wait

        // Write on BACKUP: lockObject GLOBAL
        LockStatus lockStatus = client.setMode(Mode.BACKUP)
                .lockObject(key, keyHint, LockType.GLOBAL, 1, Duration.ofSeconds(60))
                .get();
        assertEquals(LockStatus.OK, lockStatus);

        Thread.sleep(300); // replication delay

        // Verify on BACKUP: client 1 can read (streamOrderedSet)
        List<OrderedPayload> backupStream = client.setMode(Mode.BACKUP)
                .streamOrderedSet(key, keyHint, 1)
                .get();
        assertNotNull(backupStream);

        Thread.sleep(300); // replication delay

        // Verify on MASTER: client 1 can read (streamOrderedSet)
        List<OrderedPayload> masterStream = client.setMode(Mode.MASTER)
                .streamOrderedSet(key, keyHint, 1)
                .get();
        assertNotNull(masterStream);

        // Write on BACKUP: client 2 tries to read (should fail with PERMISSION_DENIED)
        try {
            client.setMode(Mode.BACKUP)
                    .streamOrderedSet(key, keyHint, 2)
                    .get();
            fail("Expected PERMISSION_DENIED for read with GLOBAL_LOCK");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.PERMISSION_DENIED, cause.getStatus().getCode());
        }

        // Write on BACKUP: client 2 tries to write (should fail with PERMISSION_DENIED)
        try {
            client.setMode(Mode.BACKUP)
                    .addElementWithWeight(key, keyHint, List.of(OrderedPayload.of(2L, "item2".getBytes(StandardCharsets.UTF_8))))
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
        String key = "unlockOwnerOrderedSetTest" + UUID.randomUUID();

        // Create ordered set
        KeyHintData keyHint = client.createOrderedSet(key, List.of(OrderedPayload.of(1L, "item1".getBytes(StandardCharsets.UTF_8))))
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
        String key = "lockExpirationOrderedSetMaster" + UUID.randomUUID();

        // Create ordered set
        KeyHintData keyHint = client.createOrderedSet(key, List.of(OrderedPayload.of(1L, "item1".getBytes(StandardCharsets.UTF_8))))
                .get();

        Thread.sleep(500); // replication wait

        // Write on MASTER: lockObject with 2s TTL
        LockStatus lockStatus = client.setMode(Mode.MASTER)
                .lockObject(key, keyHint, LockType.WRITE_LOCK, 1, Duration.ofSeconds(2))
                .get();
        assertEquals(LockStatus.OK, lockStatus);

        Thread.sleep(300); // replication delay

        // Verify on MASTER: client 1 can write (addElementWithWeight)
        client.setMode(Mode.MASTER)
                .addElementWithWeight(key, keyHint, List.of(OrderedPayload.of(2L, "item2".getBytes(StandardCharsets.UTF_8))))
                .get();

        Thread.sleep(300); // replication delay

        // Verify on BACKUP: streamOrderedSet shows lock is replicated (clientId=1)
        List<OrderedPayload> backupStream = client.setMode(Mode.BACKUP)
                .streamOrderedSet(key.getBytes(StandardCharsets.UTF_8), keyHint, 1, Duration.ofSeconds(5))
                .get();
        assertEquals(2, backupStream.size());

        // Wait for lock TTL to expire (2s lock + buffer)
        Thread.sleep(3500);

        // Verify on MASTER: streamOrderedSet succeeds (lock expired, data accessible)
        List<OrderedPayload> masterStream = client.setMode(Mode.MASTER)
                .streamOrderedSet(key, keyHint, 1)
                .get();
        assertNotNull(masterStream);

        Thread.sleep(300); // replication delay

        // Verify on BACKUP: streamOrderedSet succeeds (lock expired, data replicated)
        List<OrderedPayload> backupStreamAfter = client.setMode(Mode.BACKUP)
                .streamOrderedSet(key, keyHint)
                .get();
        assertNotNull(backupStreamAfter);

        // Verify on BACKUP: client 2 can read/write (lock no longer active)
        List<OrderedPayload> backupStream2 = client.setMode(Mode.BACKUP)
                .streamOrderedSet(key, keyHint, 2)
                .get();
        assertNotNull(backupStream2);
    }

    @Test
    @DisplayName("Lock expiration: set short TTL lock on backup, verify both nodes after lock expires")
    void testLockExpirationOnBackup() throws ExecutionException, InterruptedException {
        String key = "lockExpirationOrderedSetBackup" + UUID.randomUUID();

        // Create ordered set on BACKUP
        KeyHintData keyHint = client.setMode(Mode.BACKUP)
                .createOrderedSet(key.getBytes(StandardCharsets.UTF_8), List.of(OrderedPayload.of(1L, "item1".getBytes(StandardCharsets.UTF_8))))
                .get();

        Thread.sleep(500); // replication wait

        // Write on BACKUP: lockObject with 2s TTL
        LockStatus lockStatus = client.setMode(Mode.BACKUP)
                .lockObject(key, keyHint, LockType.WRITE_LOCK, 1, Duration.ofSeconds(2))
                .get();
        assertEquals(LockStatus.OK, lockStatus);

        Thread.sleep(300); // replication delay

        // Verify on BACKUP: client 1 can write (addElementWithWeight)
        client.setMode(Mode.BACKUP)
                .addElementWithWeight(key, keyHint, List.of(OrderedPayload.of(2L, "item2".getBytes(StandardCharsets.UTF_8))))
                .get();

        Thread.sleep(300); // replication delay

        // Verify on MASTER: streamOrderedSet shows lock is replicated (clientId=1)
        List<OrderedPayload> masterStream = client.setMode(Mode.MASTER)
                .streamOrderedSet(key.getBytes(StandardCharsets.UTF_8), keyHint, 1, Duration.ofSeconds(5))
                .get();
        assertEquals(2, masterStream.size());

        // Wait for lock TTL to expire (2s lock + buffer)
        Thread.sleep(3500);

        // Verify on BACKUP: streamOrderedSet succeeds (lock expired, data accessible)
        List<OrderedPayload> backupStream = client.setMode(Mode.BACKUP)
                .streamOrderedSet(key, keyHint, 1)
                .get();
        assertNotNull(backupStream);

        Thread.sleep(300); // replication delay

        // Verify on MASTER: streamOrderedSet succeeds (lock expired, data replicated)
        List<OrderedPayload> masterStreamAfter = client.setMode(Mode.MASTER)
                .streamOrderedSet(key, keyHint, 1)
                .get();
        assertNotNull(masterStreamAfter);

        // Verify on MASTER: client 2 can read/write (lock no longer active)
        List<OrderedPayload> masterStream2 = client.setMode(Mode.MASTER)
                .streamOrderedSet(key, keyHint, 2)
                .get();
        assertNotNull(masterStream2);
    }

    // ==================== Section 11: Get Element By Weight Operations ====================

    @Test
    @DisplayName("getElementWithWeight on master, verify replication and write on backup")
    void testGetElementWithWeight() throws ExecutionException, InterruptedException {
        String key = "getElementWithWeightTest" + UUID.randomUUID();
        List<OrderedPayload> initialData = List.of(
                OrderedPayload.of(10L, "item1".getBytes(StandardCharsets.UTF_8)),
                OrderedPayload.of(20L, "item2".getBytes(StandardCharsets.UTF_8)),
                OrderedPayload.of(30L, "item3".getBytes(StandardCharsets.UTF_8)),
                OrderedPayload.of(40L, "item4".getBytes(StandardCharsets.UTF_8)),
                OrderedPayload.of(50L, "item5".getBytes(StandardCharsets.UTF_8))
        );

        // Create ordered set
        KeyHintData keyHint = client.createOrderedSet(key, initialData)
                .get();

        Thread.sleep(500); // replication wait

        // Write on BACKUP: getElementWithWeight(key, 30)
        Payload result = client.setMode(Mode.BACKUP)
                .getElementWithWeight(key, keyHint, 30)
                .get();
        assertNotNull(result);
        assertEquals("item3", new String(result.getValue(), StandardCharsets.UTF_8));

        Thread.sleep(300); // replication delay

        // Verify on BACKUP: getElementWithWeight(key, 10)
        Payload result10 = client.setMode(Mode.BACKUP)
                .getElementWithWeight(key, keyHint, 10)
                .get();
        assertNotNull(result10);
        assertEquals("item1", new String(result10.getValue(), StandardCharsets.UTF_8));

        Thread.sleep(300); // replication delay

        // Verify on MASTER: getElementWithWeight(key, 30)
        Payload masterResult = client.setMode(Mode.MASTER)
                .getElementWithWeight(key, keyHint, 30)
                .get();
        assertNotNull(masterResult);
        assertEquals("item3", new String(masterResult.getValue(), StandardCharsets.UTF_8));

        // Write on MASTER: addElementWithWeight
        client.setMode(Mode.MASTER)
                .addElementWithWeight(key, keyHint, List.of(OrderedPayload.of(60L, "item6".getBytes(StandardCharsets.UTF_8))))
                .get();

        Thread.sleep(300); // replication delay

        // Verify on MASTER: getElementWithWeight(key, 60)
        Payload masterNew = client.setMode(Mode.MASTER)
                .getElementWithWeight(key, keyHint, 60)
                .get();
        assertNotNull(masterNew);
        assertEquals("item6", new String(masterNew.getValue(), StandardCharsets.UTF_8));

        Thread.sleep(300); // replication delay

        // Verify on BACKUP: getElementWithWeight(key, 60)
        Payload backupNew = client.setMode(Mode.BACKUP)
                .getElementWithWeight(key, keyHint, 60)
                .get();
        assertNotNull(backupNew);
        assertEquals("item6", new String(backupNew.getValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("getElementWithWeight returns any matching element if multiple have the same weight")
    void testGetElementWithWeightMultipleSameWeight() throws ExecutionException, InterruptedException {
        String key = "getElementWithWeightMulti" + UUID.randomUUID();
        List<OrderedPayload> initialData = List.of(
                OrderedPayload.of(20L, "item1".getBytes(StandardCharsets.UTF_8)),
                OrderedPayload.of(20L, "item2".getBytes(StandardCharsets.UTF_8)),
                OrderedPayload.of(20L, "item3".getBytes(StandardCharsets.UTF_8)),
                OrderedPayload.of(40L, "item4".getBytes(StandardCharsets.UTF_8))
        );

        // Create ordered set
        KeyHintData keyHint = client.createOrderedSet(key, initialData)
                .get();

        Thread.sleep(500); // replication wait

        // Write on BACKUP: getElementWithWeight(key, 20)
        Payload result = client.setMode(Mode.BACKUP)
                .getElementWithWeight(key, keyHint, 20)
                .get();
        assertNotNull(result);
        String value = new String(result.getValue(), StandardCharsets.UTF_8);
        assertTrue(value.equals("item1") || value.equals("item2") || value.equals("item3"));

        Thread.sleep(300); // replication delay

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
                OrderedPayload.of(10L, "item1".getBytes(StandardCharsets.UTF_8)),
                OrderedPayload.of(20L, "item2".getBytes(StandardCharsets.UTF_8)),
                OrderedPayload.of(30L, "item3".getBytes(StandardCharsets.UTF_8))
        );

        // Create ordered set
        KeyHintData keyHint = client.createOrderedSet(key, initialData)
                .get();

        Thread.sleep(500); // replication wait

        // Write on BACKUP: getElementAtPosition(key, 100) should fail
        try {
            client.setMode(Mode.BACKUP)
                    .getElementAtPosition(key, keyHint, 100)
                    .get();
            fail("getElementAtPosition must throw NOT_FOUND for non-existent weight");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }

        Thread.sleep(300); // replication delay

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
                OrderedPayload.of(10L, "item1".getBytes(StandardCharsets.UTF_8)),
                OrderedPayload.of(20L, "item2".getBytes(StandardCharsets.UTF_8)),
                OrderedPayload.of(30L, "item3".getBytes(StandardCharsets.UTF_8))
        );

        // Create ordered set
        KeyHintData keyHint = client.createOrderedSet(key, initialData)
                .get();

        Thread.sleep(500); // replication wait

        // Write on BACKUP: getAndRemoveElementWithWeight(key, 20)
        Payload removed = client.setMode(Mode.BACKUP)
                .getAndRemoveElementWithWeight(key, keyHint, 20)
                .get();
        assertNotNull(removed);
        assertEquals("item2", new String(removed.getValue(), StandardCharsets.UTF_8));

        Thread.sleep(300); // replication delay

        // Verify on BACKUP: streamOrderedSet returns 2 elements
        List<OrderedPayload> backupStream = client.setMode(Mode.BACKUP)
                .streamOrderedSet(key, keyHint)
                .get();
        assertEquals(2, backupStream.size());

        Thread.sleep(300); // replication delay

        // Verify on MASTER: streamOrderedSet returns 2 elements
        List<OrderedPayload> masterStream = client.setMode(Mode.MASTER)
                .streamOrderedSet(key, keyHint)
                .get();
        assertEquals(2, masterStream.size());

        // Write on MASTER: getAndRemoveElementWithWeight(key, 10)
        Payload removedMaster = client.setMode(Mode.MASTER)
                .getAndRemoveElementWithWeight(key, keyHint, 10)
                .get();
        assertNotNull(removedMaster);
        assertEquals("item1", new String(removedMaster.getValue(), StandardCharsets.UTF_8));

        Thread.sleep(300); // replication delay

        // Verify on MASTER: streamOrderedSet returns 1 element
        List<OrderedPayload> masterAfterWrite = client.setMode(Mode.MASTER)
                .streamOrderedSet(key, keyHint)
                .get();
        assertEquals(1, masterAfterWrite.size());

        Thread.sleep(300); // replication delay

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
                OrderedPayload.of(10L, "item1".getBytes(StandardCharsets.UTF_8)),
                OrderedPayload.of(20L, "item2".getBytes(StandardCharsets.UTF_8))
        );

        // Create ordered set
        KeyHintData keyHint = client.createOrderedSet(key, initialData)
                .get();

        Thread.sleep(500); // replication wait

        // Write on BACKUP: getAndRemoveElementAtPosition(key, 100) should fail
        try {
            client.setMode(Mode.BACKUP)
                    .getAndRemoveElementAtPosition(key, keyHint, 100)
                    .get();
            fail("getAndRemoveElementAtPosition must throw NOT_FOUND for non-existent weight");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }

        Thread.sleep(300); // replication delay

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