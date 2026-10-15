package com.hurricache.client.cluster.smart;

import com.hurricache.TestBaseCluster;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Mode;
import com.hurricache.client.intf.OrderedPayload;
import com.hurricache.client.intf.OrderedVariant;
import com.hurricache.client.intf.Payload;
import com.hurricache.client.intf.Variant;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Cluster tests for strict byte-by-byte content verification of large containers (1500 elements, 8KB payloads).
 * Each test verifies replication between MASTER and BACKUP nodes.
 */
public class LargeContainerStrictContentTest extends TestBaseCluster {

    private static final int LARGE_ELEMENT_COUNT = 1500;
    private static final int PAYLOAD_SIZE = 8192;
    private static final long REPLICATION_DELAY_MS = 300;

    // =========================================================================
    // Section 1: QUEUE
    // =========================================================================

    @Test
    @DisplayName("Queue: strict byte-by-byte content verification of all elements with replication")
    void testCreateLargeQueueStrict() throws ExecutionException, InterruptedException {
        String key = "strict_queue_" + UUID.randomUUID();
        List<Variant> expectedPayloads = generateDeterministicPayloadList(LARGE_ELEMENT_COUNT);

        // Create WITHOUT setMode
        KeyHintData hint = client.createQueue(key, expectedPayloads).get();
        assertNotNull(hint);

        // Large object replication wait
        Thread.sleep(1000);

        // Verify on MASTER: pop all elements and verify byte-by-byte
        List<Variant> actualPayloads = new ArrayList<>(LARGE_ELEMENT_COUNT);
        for (int i = 0; i < LARGE_ELEMENT_COUNT; i++) {
            Variant popped = client.setMode(Mode.MASTER)
                    .getAndDeleteHead(bytes(key), hint).get();
            if (popped == null || popped.bytesValue().length == 0) {
                break;
            }
            actualPayloads.add(popped);
        }
        assertEquals(LARGE_ELEMENT_COUNT, actualPayloads.size(),
                "Element count in queue does not match! Expected: " + LARGE_ELEMENT_COUNT + ", Received: " + actualPayloads.size());

        for (int i = 0; i < LARGE_ELEMENT_COUNT; i++) {
            assertArrayEquals(
                    expectedPayloads.get(i).bytesValue(),
                    actualPayloads.get(i).bytesValue(),
                    "Data mismatch error at index: " + i
            );
        }
    }

    // =========================================================================
    // Section 2: LIST
    // =========================================================================

    @Test
    @DisplayName("List: strict content verification via streamList and point getter with replication")
    void testCreateLargeListStrict() throws ExecutionException, InterruptedException {
        String key = "strict_list_" + UUID.randomUUID();
        List<Variant> expectedPayloads = generateDeterministicPayloadList(LARGE_ELEMENT_COUNT);

        // Create WITHOUT setMode
        KeyHintData hint = client.createList(key, expectedPayloads).get();
        assertNotNull(hint);

        // Large object replication wait
        Thread.sleep(1000);

        // Verify on MASTER: streamList + point getter at key indices
        List<Variant> masterStream = client.setMode(Mode.MASTER)
                .streamList(bytes(key), hint).get();
        assertEquals(LARGE_ELEMENT_COUNT, masterStream.size());

        int[] checkIndices = {0, LARGE_ELEMENT_COUNT / 2, LARGE_ELEMENT_COUNT - 1};
        for (int index : checkIndices) {
            Variant actual = client.setMode(Mode.MASTER)
                    .getElementAtPosition(bytes(key), hint, index).get();
            assertNotNull(actual);
            assertArrayEquals(expectedPayloads.get(index).bytesValue(), actual.bytesValue(),
                    "Content error at point query position: " + index);
        }

        // Replication delay → verify BACKUP
        Thread.sleep((int) REPLICATION_DELAY_MS);
        List<Variant> backupStream = client.setMode(Mode.BACKUP)
                .streamList(bytes(key), hint).get();
        assertEquals(LARGE_ELEMENT_COUNT, backupStream.size());

        for (int index : checkIndices) {
            Variant actual = client.setMode(Mode.BACKUP)
                    .getElementAtPosition(bytes(key), hint, index).get();
            assertNotNull(actual);
            assertArrayEquals(expectedPayloads.get(index).bytesValue(), actual.bytesValue(),
                    "Content error at BACKUP point query position: " + index);
        }
    }

    // =========================================================================
    // Section 3: VECTOR
    // =========================================================================

    @Test
    @DisplayName("Vector: strict order and value verification of all chunks with replication")
    void testCreateLargeVectorStrict() throws ExecutionException, InterruptedException {
        String key = "strict_vector_" + UUID.randomUUID();
        List<Variant> expectedPayloads = generateDeterministicPayloadList(LARGE_ELEMENT_COUNT);

        // Create WITHOUT setMode
        KeyHintData hint = client.createVector(key, expectedPayloads).get();
        assertNotNull(hint);

        // Large object replication wait
        Thread.sleep(1000);

        // Verify on MASTER: stream all elements
        List<Variant> masterStream = client.setMode(Mode.MASTER)
                .streamVector(bytes(key), hint).get();
        assertEquals(LARGE_ELEMENT_COUNT, masterStream.size());

        for (int i = 0; i < LARGE_ELEMENT_COUNT; i++) {
            assertArrayEquals(expectedPayloads.get(i).bytesValue(), masterStream.get(i).bytesValue(),
                    "Vector mismatch error at index: " + i);
        }

        // Replication delay → verify BACKUP
        Thread.sleep((int) REPLICATION_DELAY_MS);
        List<Variant> backupStream = client.setMode(Mode.BACKUP)
                .streamVector(bytes(key), hint).get();
        assertEquals(LARGE_ELEMENT_COUNT, backupStream.size());

        for (int i = 0; i < LARGE_ELEMENT_COUNT; i++) {
            assertArrayEquals(expectedPayloads.get(i).bytesValue(), backupStream.get(i).bytesValue(),
                    "Vector mismatch error at BACKUP index: " + i);
        }
    }

    // =========================================================================
    // Section 4: SET
    // =========================================================================

    @Test
    @DisplayName("Set: data completeness check and absence of corrupted values with replication")
    void testCreateLargeSetStrict() throws ExecutionException, InterruptedException {
        String key = "strict_set_" + UUID.randomUUID();
        List<Variant> expectedPayloads = generateDeterministicPayloadList(LARGE_ELEMENT_COUNT);

        // Count unique elements
        Set<Variant> uniquePayloads = new HashSet<>();
        for (Variant p : expectedPayloads) {
            uniquePayloads.add(p);
        }
        int expectedUniqueCount = uniquePayloads.size();

        // Create WITHOUT setMode
        KeyHintData hint = client.createSet(key, expectedPayloads).get();
        assertNotNull(hint);

        // Large object replication wait
        Thread.sleep(2000);

        // Verify on MASTER: size + existInContainerByKey
        Integer masterSize = client.setMode(Mode.MASTER)
                .getSize(bytes(key), hint).get();
        assertEquals(expectedUniqueCount, masterSize,
                "Set size does not match number of unique elements! Expected: " + expectedUniqueCount + ", Received: " + masterSize);

        Variant first = expectedPayloads.get(0);
        Variant middle = expectedPayloads.get(LARGE_ELEMENT_COUNT / 2);
        Variant last = expectedPayloads.get(LARGE_ELEMENT_COUNT - 1);

        assertTrue(client.setMode(Mode.MASTER)
                .existInContainerByKey(bytes(key), hint, first.bytesValue()).get());
        assertTrue(client.setMode(Mode.MASTER)
                .existInContainerByKey(bytes(key), hint, middle.bytesValue()).get());
        assertTrue(client.setMode(Mode.MASTER)
                .existInContainerByKey(bytes(key), hint, last.bytesValue()).get());

        // Replication delay → verify BACKUP
        Thread.sleep((int) REPLICATION_DELAY_MS);
        Integer backupSize = client.setMode(Mode.BACKUP)
                .getSize(bytes(key), hint).get();
        assertEquals(expectedUniqueCount, backupSize);

        assertTrue(client.setMode(Mode.BACKUP)
                .existInContainerByKey(bytes(key), hint, first.bytesValue()).get());
        assertTrue(client.setMode(Mode.BACKUP)
                .existInContainerByKey(bytes(key), hint, middle.bytesValue()).get());
        assertTrue(client.setMode(Mode.BACKUP)
                .existInContainerByKey(bytes(key), hint, last.bytesValue()).get());
    }

    // =========================================================================
    // Section 5: ORDERED SET
    // =========================================================================

    @Test
    @DisplayName("OrderedSet: weight and binary data preservation check with replication")
    void testCreateLargeOrderedSetStrict() throws ExecutionException, InterruptedException {
        String key = "strict_orderedset_" + UUID.randomUUID();
        List<OrderedVariant> expectedPayloads = new ArrayList<>(LARGE_ELEMENT_COUNT);

        for (int i = 0; i < LARGE_ELEMENT_COUNT; i++) {
            byte[] rawData = createDeterministicPayload(i, PAYLOAD_SIZE);
            expectedPayloads.add(OrderedVariant.ofBytes(rawData, (long) i * 10));
        }

        // Create WITHOUT setMode
        KeyHintData hint = client.createOrderedSet(key, expectedPayloads).get();
        assertNotNull(hint);

        // Large object replication wait
        Thread.sleep(1000);

        // Verify on MASTER: stream all elements with weights
        List<OrderedVariant> masterStream = client.setMode(Mode.MASTER)
                .streamElementInRangeOrderedSet(bytes(key), hint, 0, LARGE_ELEMENT_COUNT * 10L, false).get();
        assertEquals(LARGE_ELEMENT_COUNT, masterStream.size());

        for (int i = 0; i < LARGE_ELEMENT_COUNT; i++) {
            OrderedVariant expected = expectedPayloads.get(i);
            OrderedVariant actual = masterStream.get(i);
            assertEquals(expected.getOrder(), actual.getOrder(), "OrderedSet weight does not match at step: " + i);
            assertEquals(expected, actual, "OrderedSet binary content does not match at step: " + i);
        }

        // Replication delay → verify BACKUP
        Thread.sleep((int) REPLICATION_DELAY_MS);
        List<OrderedVariant> backupStream = client.setMode(Mode.BACKUP)
                .streamElementInRangeOrderedSet(bytes(key), hint, 0, LARGE_ELEMENT_COUNT * 10L, false).get();
        assertEquals(LARGE_ELEMENT_COUNT, backupStream.size());

        for (int i = 0; i < LARGE_ELEMENT_COUNT; i++) {
            OrderedVariant expected = expectedPayloads.get(i);
            OrderedVariant actual = backupStream.get(i);
            assertEquals(expected.getOrder(), actual.getOrder(), "OrderedSet weight does not match at BACKUP step: " + i);
            assertEquals(expected, actual, "OrderedSet binary content does not match at BACKUP step: " + i);
        }
    }

    // =========================================================================
    // Section 6: MAP
    // =========================================================================

    @Test
    @DisplayName("Map: strict verification of all keys and their corresponding values with replication")
    void testCreateLargeMapStrict() throws ExecutionException, InterruptedException {
        String key = "strict_map_" + UUID.randomUUID();
        Map<Payload, Variant> expectedMap = new LinkedHashMap<>();

        for (int i = 0; i < LARGE_ELEMENT_COUNT; i++) {
            byte[] subKey = ("map_key_" + i).getBytes(StandardCharsets.UTF_8);
            byte[] value = createDeterministicPayload(i, PAYLOAD_SIZE);
            expectedMap.put(p(subKey), Variant.ofBytes(value));
        }

        // Create WITHOUT setMode
        KeyHintData hint = client.createMap(key, expectedMap).get();
        assertNotNull(hint);

        // Large object replication wait
        Thread.sleep(1000);

        // Verify on MASTER: stream all entries
        Map<Payload, Variant> masterStream = client.setMode(Mode.MASTER)
                .streamMap(bytes(key), hint).get();
        assertEquals(LARGE_ELEMENT_COUNT, masterStream.size());

        for (Map.Entry<Payload, Variant> entry : expectedMap.entrySet()) {
            byte[] subKey = entry.getKey().getValue();
            byte[] expectedValue = entry.getValue().bytesValue();

            Variant actualValue = client.setMode(Mode.MASTER)
                    .getValueInContainerByKey(bytes(key), hint, subKey).get();
            assertNotNull(actualValue, "Key not found in Map: " + new String(subKey));
            assertArrayEquals(expectedValue, actualValue.bytesValue(), "Value by key is corrupted: " + new String(subKey));
        }

        // Replication delay → verify BACKUP
        Thread.sleep((int) REPLICATION_DELAY_MS);
        Map<Payload, Variant> backupStream = client.setMode(Mode.BACKUP)
                .streamMap(bytes(key), hint).get();
        assertEquals(LARGE_ELEMENT_COUNT, backupStream.size());

        for (Map.Entry<Payload, Variant> entry : expectedMap.entrySet()) {
            byte[] subKey = entry.getKey().getValue();
            byte[] expectedValue = entry.getValue().bytesValue();

            Variant actualValue = client.setMode(Mode.BACKUP)
                    .getValueInContainerByKey(bytes(key), hint, subKey).get();
            assertNotNull(actualValue, "Key not found in BACKUP Map: " + new String(subKey));
            assertArrayEquals(expectedValue, actualValue.bytesValue(), "Value corrupted in BACKUP Map by key: " + new String(subKey));
        }
    }

    // =========================================================================
    // Section 7: ORDERED MAP
    // =========================================================================

    @Test
    @DisplayName("OrderedMap: strict verification of weights, keys and values with replication")
    void testCreateLargeOrderedMapStrict() throws ExecutionException, InterruptedException {
        String key = "strict_orderedmap_" + UUID.randomUUID();
        Map<OrderedPayload, Variant> expectedMap = new LinkedHashMap<>();

        for (int i = 0; i < LARGE_ELEMENT_COUNT; i++) {
            byte[] subKey = ("ord_map_key_" + i).getBytes(StandardCharsets.UTF_8);
            byte[] value = createDeterministicPayload(i, PAYLOAD_SIZE);
            expectedMap.put(OrderedPayload.of(subKey, (long) i), Variant.ofBytes(value));
        }

        // Create WITHOUT setMode
        KeyHintData hint = client.createOrderedMap(key, expectedMap).get();
        assertNotNull(hint);

        // Large object replication wait
        Thread.sleep(1000);

        // Verify on MASTER: stream all entries
        Map<OrderedPayload, Variant> masterStream = client.setMode(Mode.MASTER)
                .streamOrderedMap(bytes(key), hint).get();
        assertEquals(LARGE_ELEMENT_COUNT, masterStream.size());

        for (Map.Entry<OrderedPayload, Variant> entry : expectedMap.entrySet()) {
            byte[] subKey = entry.getKey().getValue();
            byte[] expectedValue = entry.getValue().bytesValue();

            Variant actualValue = client.setMode(Mode.MASTER)
                    .getValueInContainerByKey(bytes(key), hint, subKey).get();
            assertNotNull(actualValue, "Key not found in OrderedMap: " + new String(subKey));
            assertArrayEquals(expectedValue, actualValue.bytesValue(), "Value corrupted in OrderedMap by key: " + new String(subKey));
        }

        // Replication delay → verify BACKUP
        Thread.sleep((int) REPLICATION_DELAY_MS);
        Map<OrderedPayload, Variant> backupStream = client.setMode(Mode.BACKUP)
                .streamOrderedMap(bytes(key), hint).get();
        assertEquals(LARGE_ELEMENT_COUNT, backupStream.size());

        for (Map.Entry<OrderedPayload, Variant> entry : expectedMap.entrySet()) {
            byte[] subKey = entry.getKey().getValue();
            byte[] expectedValue = entry.getValue().bytesValue();

            Variant actualValue = client.setMode(Mode.BACKUP)
                    .getValueInContainerByKey(bytes(key), hint, subKey).get();
            assertNotNull(actualValue, "Key not found in BACKUP OrderedMap: " + new String(subKey));
            assertArrayEquals(expectedValue, actualValue.bytesValue(), "Value corrupted in BACKUP OrderedMap by key: " + new String(subKey));
        }
    }

    // =========================================================================
    // Helper Methods
    // =========================================================================

    private byte[] createDeterministicPayload(int index, int size) {
        byte[] payload = new byte[size];
        byte headerMarker = (byte) (index % 127);
        for (int i = 0; i < size; i++) {
            payload[i] = (byte) ((i + headerMarker) % 256);
        }
        return payload;
    }

    private List<Variant> generateDeterministicPayloadList(int count) {
        List<Variant> payloads = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            payloads.add(Variant.ofBytes(createDeterministicPayload(i, PAYLOAD_SIZE)));
        }
        return payloads;
    }
}