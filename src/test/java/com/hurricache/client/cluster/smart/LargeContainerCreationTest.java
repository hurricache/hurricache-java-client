package com.hurricache.client.cluster.smart;

import com.hurricache.TestBaseCluster;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.OrderedPayload;
import com.hurricache.client.intf.Payload;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

public class LargeContainerCreationTest extends TestBaseCluster {

    private static final int LARGE_ELEMENT_COUNT = 1500;
    private static final int PAYLOAD_SIZE = 8192; // 8 KB на элемент (~12 MB суммарно для проверки сплиттинга чанков)
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Test
    @DisplayName("Создание большой Queue с автоматическим разбиением на чанки")
    void testCreateLargeQueue() throws ExecutionException, InterruptedException {
        byte[] key = generateRandomKey();
        List<Payload> payloads = generateLargePayloadList(LARGE_ELEMENT_COUNT);

        CompletableFuture<KeyHintData> future = client.createQueue(key, null, payloads, getTestTtl(), 0, TIMEOUT);
        KeyHintData hint = future.get();

        assertNotNull(hint);
        Thread.sleep(500);

        // Вычитываем все чанки из очереди
        List<Payload> actualPayloads = new ArrayList<>(LARGE_ELEMENT_COUNT);
        for (int i = 0; i < LARGE_ELEMENT_COUNT; i++) {
            Payload popped = client.getAndRemoveFront(key, hint, 0, TIMEOUT).get();
            if (popped == null) {
                break;
            }
            actualPayloads.add(popped);
        }

        // Проверяем размер и целостность через вычищенные элементы
        assertNotNull(actualPayloads, "Стрим вернул null");
        assertEquals(LARGE_ELEMENT_COUNT, actualPayloads.size(),
                     String.format("Ожидали %d элементов в очереди, но вычитано: %d", LARGE_ELEMENT_COUNT, actualPayloads.size()));

        // Проверяем границы элементов
        assertArrayEquals(payloads.get(0).getValue(), actualPayloads.get(0).getValue(), "Первый элемент не совпадает!");
        assertArrayEquals(payloads.get(LARGE_ELEMENT_COUNT - 1).getValue(),
                          actualPayloads.get(LARGE_ELEMENT_COUNT - 1).getValue(), "Последний элемент не совпадает!");
    }

    @Test
    @DisplayName("Создание большого List с автоматическим разбиением на чанки")
    void testCreateLargeList() throws ExecutionException, InterruptedException {
        byte[] key = generateRandomKey();
        List<Payload> payloads = generateLargePayloadList(LARGE_ELEMENT_COUNT);

        CompletableFuture<KeyHintData> future = client.createList(key, null, payloads, getTestTtl(), 0, TIMEOUT);
        KeyHintData hint = future.get();

        assertNotNull(hint);

        Integer size = client.getSize(key, hint, 0, TIMEOUT).get();
        assertEquals(LARGE_ELEMENT_COUNT, size);
    }

    @Test
    @DisplayName("Создание большого Vector с автоматическим разбиением на чанки")
    void testCreateLargeVector() throws ExecutionException, InterruptedException {
        byte[] key = generateRandomKey();
        List<Payload> payloads = generateLargePayloadList(LARGE_ELEMENT_COUNT);

        CompletableFuture<KeyHintData> future = client.createVector(key, null, payloads, getTestTtl(), 0, TIMEOUT);
        KeyHintData hint = future.get();

        assertNotNull(hint);

        List<Payload> streamed = client.streamVector(key, hint, 0, TIMEOUT).get();
        assertEquals(LARGE_ELEMENT_COUNT, streamed.size());
    }

    @Test
    @DisplayName("Создание большого Set с автоматическим разбиением на чанки")
    void testCreateLargeSet() throws ExecutionException, InterruptedException {
        byte[] key = generateRandomKey();
        List<Payload> payloads = generateLargePayloadList(LARGE_ELEMENT_COUNT);

        CompletableFuture<KeyHintData> future = client.createSet(key, null, payloads, getTestTtl(), 0, TIMEOUT);
        KeyHintData hint = future.get();

        assertNotNull(hint);
        Thread.sleep(500);
        Integer size = client.getSize(key, hint, 0, TIMEOUT).get();
        assertEquals(LARGE_ELEMENT_COUNT, size);
    }

    @Test
    @DisplayName("Создание большого OrderedSet с автоматическим разбиением на чанки")
    void testCreateLargeOrderedSet() throws ExecutionException, InterruptedException {
        byte[] key = generateRandomKey();
        List<OrderedPayload> payloads = new ArrayList<>(LARGE_ELEMENT_COUNT);

        for (int i = 0; i < LARGE_ELEMENT_COUNT; i++) {
            byte[] rawData = createLargePayload(PAYLOAD_SIZE);
            payloads.add(new OrderedPayload(rawData, (long) i));
        }

        CompletableFuture<KeyHintData> future = client.createOrderedSet(key, payloads, getTestTtl(), 0, TIMEOUT);
        KeyHintData hint = future.get();

        assertNotNull(hint);
        Thread.sleep(500);
        Integer size = client.getSize(key, hint, 0, TIMEOUT).get();
        assertEquals(LARGE_ELEMENT_COUNT, size);
    }

    @Test
    @DisplayName("Создание большой Map с автоматическим разбиением на чанки")
    void testCreateLargeMap() throws ExecutionException, InterruptedException {
        byte[] key = generateRandomKey();
        Map<Payload, Payload> map = new LinkedHashMap<>();

        for (int i = 0; i < LARGE_ELEMENT_COUNT; i++) {
            byte[] subKey = ("sub_key_" + i).getBytes(StandardCharsets.UTF_8);
            byte[] value = createLargePayload(PAYLOAD_SIZE);
            map.put(Payload.of(subKey), Payload.of(value));
        }

        CompletableFuture<KeyHintData> future = client.createMap(key, map, getTestTtl(), 0, TIMEOUT);
        KeyHintData hint = future.get();

        assertNotNull(hint);

        Map<Payload, Payload> streamedMap = client.streamMap(key, hint, 0, TIMEOUT).get();
        assertEquals(LARGE_ELEMENT_COUNT, streamedMap.size());
    }

    @Test
    @DisplayName("Создание большой OrderedMap с автоматическим разбиением на чанки")
    void testCreateLargeOrderedMap() throws ExecutionException, InterruptedException {
        byte[] key = generateRandomKey();
        Map<OrderedPayload, Payload> orderedMap = new LinkedHashMap<>();

        for (int i = 0; i < LARGE_ELEMENT_COUNT; i++) {
            byte[] subKey = ("ord_key_" + i).getBytes(StandardCharsets.UTF_8);
            byte[] value = createLargePayload(PAYLOAD_SIZE);
            orderedMap.put(OrderedPayload.of(subKey, (long) i), Payload.of(value));
        }

        CompletableFuture<KeyHintData> future = client.createOrderedMap(key, orderedMap, getTestTtl(), 0, TIMEOUT);
        KeyHintData hint = future.get();

        assertNotNull(hint);

        Map<OrderedPayload, Payload> streamedMap = client.streamOrderedMap(key, hint, 0, TIMEOUT).get();
        assertEquals(LARGE_ELEMENT_COUNT, streamedMap.size());
    }

    // =========================================================================
    // Вспомогательные методы
    // =========================================================================

    private byte[] generateRandomKey() {
        return ("container_key_" + UUID.randomUUID()).getBytes(StandardCharsets.UTF_8);
    }

    private List<Payload> generateLargePayloadList(int count) {
        List<Payload> payloads = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            payloads.add(Payload.of(createLargePayload(PAYLOAD_SIZE)));
        }
        return payloads;
    }
}