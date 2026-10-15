package com.hurricache.client.standalone.simple;

import com.hurricache.TestBase;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.OrderedPayload;
import com.hurricache.client.intf.Payload;
import com.hurricache.client.intf.Variant;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LargeContainerStrictContentTest extends TestBase {

    private static final int LARGE_ELEMENT_COUNT = 1500;
    private static final int PAYLOAD_SIZE = 8192;
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Test
    @DisplayName("Queue: strict byte-by-byte content verification of elements")
    void testCreateLargeQueueStrict() throws ExecutionException, InterruptedException {
        byte[] key = generateRandomKey();
        List<Variant> expectedPayloads = generateDeterministicPayloadList(LARGE_ELEMENT_COUNT);

        CompletableFuture<KeyHintData> future = client.createQueue(key, null, expectedPayloads, getTestTtl(), 0, TIMEOUT);
        KeyHintData hint = future.get();
        assertNotNull(hint);
        Thread.sleep(1000);
        List<Variant> actualPayloads = new ArrayList<>(LARGE_ELEMENT_COUNT);
        for (int i = 0; i < LARGE_ELEMENT_COUNT; i++) {
            Variant popped = client.getAndDeleteHead(key, hint, 0, TIMEOUT).get();
            if (popped == null) {
                break;
            }
            actualPayloads.add(popped);
        }
        assertEquals(LARGE_ELEMENT_COUNT, actualPayloads.size(),
                     String.format("Element count in queue does not match! Expected: %d, Received: %d",
                                   LARGE_ELEMENT_COUNT, actualPayloads.size()));

        for (int i = 0; i < LARGE_ELEMENT_COUNT; i++) {
            assertArrayEquals(
                    expectedPayloads.get(i).bytesValue(),
                    actualPayloads.get(i).bytesValue(),
                    "Data mismatch error at index: " + i
            );
        }
    }

    @Test
    @DisplayName("List: strict content verification via streamList and point getter")
    void testCreateLargeListStrict() throws ExecutionException, InterruptedException {
        byte[] key = generateRandomKey();
        List<Variant> expectedPayloads = generateDeterministicPayloadList(LARGE_ELEMENT_COUNT);

        CompletableFuture<KeyHintData> future = client.createList(key, null, expectedPayloads, getTestTtl(), 0, TIMEOUT);
        KeyHintData hint = future.get();
        assertNotNull(hint);
        Thread.sleep(1000);
        Integer size = client.getSize(key, hint, 0, TIMEOUT).get();
        assertEquals(LARGE_ELEMENT_COUNT, size);

        int[] checkIndices = {0, LARGE_ELEMENT_COUNT / 2, LARGE_ELEMENT_COUNT - 1};
        for (int index : checkIndices) {
            Variant actual = client.getElementAtPosition(key, hint, index, 0, TIMEOUT).get();
            assertNotNull(actual);
            assertArrayEquals(expectedPayloads.get(index).bytesValue(), actual.bytesValue(),
                              "Content error at point query position: " + index);
        }
    }

    @Test
    @DisplayName("Vector: strict order and value verification of all chunks")
    void testCreateLargeVectorStrict() throws ExecutionException, InterruptedException {
        byte[] key = generateRandomKey();
        List<Variant> expectedPayloads = generateDeterministicPayloadList(LARGE_ELEMENT_COUNT);

        CompletableFuture<KeyHintData> future = client.createVector(key, null, expectedPayloads, getTestTtl(), 0, TIMEOUT);
        KeyHintData hint = future.get();
        assertNotNull(hint);
        Thread.sleep(1000);
        List<Variant> actualPayloads = client.streamVector(key, hint, 0, TIMEOUT).get();
        assertEquals(LARGE_ELEMENT_COUNT, actualPayloads.size());

        for (int i = 0; i < LARGE_ELEMENT_COUNT; i++) {
            assertArrayEquals(expectedPayloads.get(i).bytesValue(), actualPayloads.get(i).bytesValue(),
                              "Vector mismatch error at index: " + i);
        }
    }

    @Test
    @DisplayName("Set: data completeness check and absence of corrupted values")
    void testCreateLargeSetStrict() throws ExecutionException, InterruptedException {
        byte[] key = generateRandomKey();
         List<Variant> expectedPayloads = generateDeterministicPayloadList(LARGE_ELEMENT_COUNT);

        Set<Object> uniquePayloads = new HashSet<>();
        for (Variant p : expectedPayloads) {
            uniquePayloads.add(new Object() {
                private final byte[] val = p.bytesValue();

                @Override
                public boolean equals(Object o) {
                    if (this == o) return true;
                    if (o == null) return false;
                    try {
                        var field = o.getClass().getDeclaredField("val");
                        field.setAccessible(true);
                        return Arrays.equals(this.val, (byte[]) field.get(o));
                    } catch (Exception e) {
                        return false;
                    }
                }

                @Override
                public int hashCode() {
                    return Arrays.hashCode(val);
                }
            });
        }
        int expectedUniqueCount = uniquePayloads.size();

        CompletableFuture<KeyHintData> future = client.createSet(key, null, expectedPayloads, getTestTtl(), 0, TIMEOUT);
        KeyHintData hint = future.get();
        assertNotNull(hint);
        Thread.sleep(1000);

        Integer size = client.getSize(key, hint, 0, TIMEOUT).get();
        assertEquals(expectedUniqueCount, size,
                     String.format("Set size does not match number of unique elements! Expected: %d, Received: %d",
                                   expectedUniqueCount, size));

        Variant first = expectedPayloads.get(0);
        Variant middle = expectedPayloads.get(LARGE_ELEMENT_COUNT / 2);
        Variant last = expectedPayloads.get(LARGE_ELEMENT_COUNT - 1);

        assertTrue(client.existInContainerByKey(key, hint, first.bytesValue(), 0, TIMEOUT).get());
        assertTrue(client.existInContainerByKey(key, hint, middle.bytesValue(), 0, TIMEOUT).get());
        assertTrue(client.existInContainerByKey(key, hint, last.bytesValue(), 0, TIMEOUT).get());
    }

    @Test
    @DisplayName("OrderedSet: weight and binary data preservation check")
    void testCreateLargeOrderedSetStrict() throws ExecutionException, InterruptedException {
        byte[] key = generateRandomKey();
        List<OrderedPayload> expectedPayloads = new ArrayList<>(LARGE_ELEMENT_COUNT);

        for (int i = 0; i < LARGE_ELEMENT_COUNT; i++) {
            byte[] rawData = createDeterministicPayload(i, PAYLOAD_SIZE);
            expectedPayloads.add(new OrderedPayload(rawData, (long) i * 10));
        }

        CompletableFuture<KeyHintData> future = client.createOrderedSet(key, null, expectedPayloads, getTestTtl(), 0, null);
        KeyHintData hint = future.get();
        assertNotNull(hint);
        Thread.sleep(1000);
        List<OrderedPayload> streamed = client.streamElementInRangeOrderedSet(key, hint, 0, LARGE_ELEMENT_COUNT * 10L, false, 0, TIMEOUT).get();
        assertEquals(LARGE_ELEMENT_COUNT, streamed.size());

        for (int i = 0; i < LARGE_ELEMENT_COUNT; i++) {
            OrderedPayload expected = expectedPayloads.get(i);
            OrderedPayload actual = streamed.get(i);

            assertEquals(expected.getOrder(), actual.getOrder(), "OrderedSet weight does not match at step: " + i);
            assertArrayEquals(expected.getValue(), actual.getValue(), "OrderedSet binary content does not match at step: " + i);
        }
    }

    @Test
    @DisplayName("Map: strict verification of all keys and their corresponding values")
    void testCreateLargeMapStrict() throws ExecutionException, InterruptedException {
        byte[] key = generateRandomKey();
        Map<Payload, Variant> expectedMap = new LinkedHashMap<>();

        for (int i = 0; i < LARGE_ELEMENT_COUNT; i++) {
            byte[] subKey = ("map_key_" + i).getBytes(StandardCharsets.UTF_8);
            byte[] value = createDeterministicPayload(i, PAYLOAD_SIZE);
            expectedMap.put(p(subKey), Variant.ofBytes(value));
        }

        CompletableFuture<KeyHintData> future = client.createMap(key, null, expectedMap, getTestTtl(), 0, TIMEOUT);
        KeyHintData hint = future.get();
        assertNotNull(hint);

        Map<Payload, Variant> actualMap = client.streamMap(key, hint, 0, TIMEOUT).get();
        assertEquals(LARGE_ELEMENT_COUNT, actualMap.size());

        for (Map.Entry<Payload, Variant> entry : expectedMap.entrySet()) {
            byte[] subKey = entry.getKey().getValue();
            byte[] expectedValue = entry.getValue().bytesValue();

            Variant actualValue = client.getValueInContainerByKey(key, hint, subKey, 0, TIMEOUT).get();
            assertNotNull(actualValue, "Key not found in Map: " + new String(subKey));
            assertArrayEquals(expectedValue, actualValue.bytesValue(), "Value by key is corrupted: " + new String(subKey));
        }
    }

    @Test
    @DisplayName("OrderedMap: strict verification of weights, keys and values")
    void testCreateLargeOrderedMapStrict() throws ExecutionException, InterruptedException {
        byte[] key = generateRandomKey();
        Map<OrderedPayload, Variant> expectedMap = new LinkedHashMap<>();

        for (int i = 0; i < LARGE_ELEMENT_COUNT; i++) {
            byte[] subKey = ("ord_map_key_" + i).getBytes(StandardCharsets.UTF_8);
            byte[] value = createDeterministicPayload(i, PAYLOAD_SIZE);
            expectedMap.put(OrderedPayload.of(subKey, (long) i), Variant.ofBytes(value));
        }

        CompletableFuture<KeyHintData> future = client.createOrderedMap(key, null, expectedMap, getTestTtl(), 0, TIMEOUT);
        KeyHintData hint = future.get();
        assertNotNull(hint);
        Thread.sleep(1000);
        Map<OrderedPayload, Variant> actualMap = client.streamOrderedMap(key, hint, 0, null).get();
        assertEquals(LARGE_ELEMENT_COUNT, actualMap.size());

        for (Map.Entry<OrderedPayload, Variant> entry : expectedMap.entrySet()) {
            byte[] subKey = entry.getKey().getValue();
            byte[] expectedValue = entry.getValue().bytesValue();

            Variant actualValue = client.getValueInContainerByKey(key, hint, subKey, 0, TIMEOUT).get();
            assertNotNull(actualValue, "Key not found in OrderedMap: " + new String(subKey));
            assertArrayEquals(expectedValue, actualValue.bytesValue(), "Value corrupted in OrderedMap by key: " + new String(subKey));
        }
    }

    private byte[] generateRandomKey() {
        return ("strict_container_key_" + UUID.randomUUID()).getBytes(StandardCharsets.UTF_8);
    }

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