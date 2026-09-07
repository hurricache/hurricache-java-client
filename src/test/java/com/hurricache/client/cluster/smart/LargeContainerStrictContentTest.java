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

public class LargeContainerStrictContentTest extends TestBaseCluster {

    private static final int LARGE_ELEMENT_COUNT = 1500;
    private static final int PAYLOAD_SIZE = 8192; // 8 KB на элемент (~12 MB суммарно для гарантированного сплиттинга)
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Test
    @DisplayName("Queue: строгая проверка побайтового содержимого элементов")
    void testCreateLargeQueueStrict() throws ExecutionException, InterruptedException {
        byte[] key = generateRandomKey();
        List<Payload> expectedPayloads = generateDeterministicPayloadList(LARGE_ELEMENT_COUNT);

        CompletableFuture<KeyHintData> future = client.createQueue(key, null, expectedPayloads, getTestTtl(), 0, TIMEOUT);
        KeyHintData hint = future.get();
        assertNotNull(hint);
        Thread.sleep(1000);
        List<Payload> actualPayloads = new ArrayList<>(LARGE_ELEMENT_COUNT);
        for (int i = 0; i < LARGE_ELEMENT_COUNT; i++) {
            Payload popped = client.getAndRemoveFront(key, hint, 0, TIMEOUT).get();
            if (popped == null) {
                break;
            }
            actualPayloads.add(popped);
        }
        // 1. Проверяем точный размер вычитанного списка
        assertEquals(LARGE_ELEMENT_COUNT, actualPayloads.size(),
                     String.format("Количество элементов в очереди не совпадает! Ожидалось: %d, Получено: %d",
                                   LARGE_ELEMENT_COUNT, actualPayloads.size()));

        // 2. Побайтовая проверка содержимого каждого элемента
        for (int i = 0; i < LARGE_ELEMENT_COUNT; i++) {
            assertArrayEquals(
                    expectedPayloads.get(i).getValue(),
                    actualPayloads.get(i).getValue(),
                    "Ошибка несовпадения данных на индексе: " + i
            );
        }
    }

    @Test
    @DisplayName("List: строгая проверка содержимого через streamList и точечный геттер")
    void testCreateLargeListStrict() throws ExecutionException, InterruptedException {
        byte[] key = generateRandomKey();
        List<Payload> expectedPayloads = generateDeterministicPayloadList(LARGE_ELEMENT_COUNT);

        CompletableFuture<KeyHintData> future = client.createList(key, null, expectedPayloads, getTestTtl(), 0, TIMEOUT);
        KeyHintData hint = future.get();
        assertNotNull(hint);

        Integer size = client.getSize(key, hint, 0, TIMEOUT).get();
        assertEquals(LARGE_ELEMENT_COUNT, size);

        // Проверяем выборочные точки: начало (первый чанк), середина (промежуточный чанк), конец (последний чанк)
        int[] checkIndices = {0, LARGE_ELEMENT_COUNT / 2, LARGE_ELEMENT_COUNT - 1};
        for (int index : checkIndices) {
            Payload actual = client.getElementAtPosition(key, hint, index, 0, TIMEOUT).get();
            assertNotNull(actual);
            assertArrayEquals(expectedPayloads.get(index).getValue(), actual.getValue(),
                              "Ошибка содержимого при точечном запросе позиции: " + index);
        }
    }

    @Test
    @DisplayName("Vector: строгая проверка порядка и значений всех чанков")
    void testCreateLargeVectorStrict() throws ExecutionException, InterruptedException {
        byte[] key = generateRandomKey();
        List<Payload> expectedPayloads = generateDeterministicPayloadList(LARGE_ELEMENT_COUNT);

        CompletableFuture<KeyHintData> future = client.createVector(key, null, expectedPayloads, getTestTtl(), 0, TIMEOUT);
        KeyHintData hint = future.get();
        assertNotNull(hint);

        List<Payload> actualPayloads = client.streamVector(key, hint, 0, TIMEOUT).get();
        assertEquals(LARGE_ELEMENT_COUNT, actualPayloads.size());

        for (int i = 0; i < LARGE_ELEMENT_COUNT; i++) {
            assertArrayEquals(expectedPayloads.get(i).getValue(), actualPayloads.get(i).getValue(),
                              "Ошибка несовпадения вектора на индексе: " + i);
        }
    }

    @Test
    @DisplayName("Set: проверка полноты данных и отсутствия битых значений")
    void testCreateLargeSetStrict() throws ExecutionException, InterruptedException {
        byte[] key = generateRandomKey();
        List<Payload> expectedPayloads = generateDeterministicPayloadList(LARGE_ELEMENT_COUNT);

        CompletableFuture<KeyHintData> future = client.createSet(key, null, expectedPayloads, getTestTtl(), 0, TIMEOUT);
        KeyHintData hint = future.get();
        assertNotNull(hint);
        Thread.sleep(1000);
        Integer size = client.getSize(key, hint, 0, TIMEOUT).get();
        assertEquals(LARGE_ELEMENT_COUNT, size);

        // Для Set проверяем точечные элементы из разных чанков
        Payload first = expectedPayloads.get(0);
        Payload middle = expectedPayloads.get(LARGE_ELEMENT_COUNT / 2);
        Payload last = expectedPayloads.get(LARGE_ELEMENT_COUNT - 1);

        assertTrue(client.containsContainerKey(key, hint, first.getValue(), 0, TIMEOUT).get());
        assertTrue(client.containsContainerKey(key, hint, middle.getValue(), 0, TIMEOUT).get());
        assertTrue(client.containsContainerKey(key, hint, last.getValue(), 0, TIMEOUT).get());
    }

    @Test
    @DisplayName("OrderedSet: проверка сохранения весов и бинарных данных")
    void testCreateLargeOrderedSetStrict() throws ExecutionException, InterruptedException {
        byte[] key = generateRandomKey();
        List<OrderedPayload> expectedPayloads = new ArrayList<>(LARGE_ELEMENT_COUNT);

        for (int i = 0; i < LARGE_ELEMENT_COUNT; i++) {
            byte[] rawData = createDeterministicPayload(i, PAYLOAD_SIZE);
            expectedPayloads.add(new OrderedPayload(rawData, (long) i * 10)); // Вес = i * 10
        }

        CompletableFuture<KeyHintData> future = client.createOrderedSet(key, expectedPayloads, getTestTtl(), 0, null);
        KeyHintData hint = future.get();
        assertNotNull(hint);
        Thread.sleep(1000);
        List<OrderedPayload> streamed = client.streamElementInRangeOrderedSet(key, hint, 0, LARGE_ELEMENT_COUNT * 10L, false, 0, TIMEOUT).get();
        assertEquals(LARGE_ELEMENT_COUNT, streamed.size());

        for (int i = 0; i < LARGE_ELEMENT_COUNT; i++) {
            OrderedPayload expected = expectedPayloads.get(i);
            OrderedPayload actual = streamed.get(i);

            assertEquals(expected.getOrder(), actual.getOrder(), "Вес OrderedSet не совпадает на шаге: " + i);
            assertArrayEquals(expected.getValue(), actual.getValue(), "Бинарный контент OrderedSet не совпадает на шаге: " + i);
        }
    }

    @Test
    @DisplayName("Map: строгая проверка всех ключей и соответствующих им значений")
    void testCreateLargeMapStrict() throws ExecutionException, InterruptedException {
        byte[] key = generateRandomKey();
        Map<Payload, Payload> expectedMap = new LinkedHashMap<>();

        for (int i = 0; i < LARGE_ELEMENT_COUNT; i++) {
            byte[] subKey = ("map_key_" + i).getBytes(StandardCharsets.UTF_8);
            byte[] value = createDeterministicPayload(i, PAYLOAD_SIZE);
            expectedMap.put(Payload.of(subKey), Payload.of(value));
        }

        CompletableFuture<KeyHintData> future = client.createMap(key, expectedMap, getTestTtl(), 0, TIMEOUT);
        KeyHintData hint = future.get();
        assertNotNull(hint);

        Map<Payload, Payload> actualMap = client.streamMap(key, hint, 0, TIMEOUT).get();
        assertEquals(LARGE_ELEMENT_COUNT, actualMap.size());

        // Строгая валидация каждой пары K/V
        for (Map.Entry<Payload, Payload> entry : expectedMap.entrySet()) {
            byte[] subKey = entry.getKey().getValue();
            byte[] expectedValue = entry.getValue().getValue();

            byte[] actualValue = client.getContainerValue(key, hint, subKey, 0, TIMEOUT).get();
            assertNotNull(actualValue, "Ключ не найден в Map: " + new String(subKey));
            assertArrayEquals(expectedValue, actualValue, "Значение по ключу искажено: " + new String(subKey));
        }
    }

    @Test
    @DisplayName("OrderedMap: строгая проверка весов, ключей и значений")
    void testCreateLargeOrderedMapStrict() throws ExecutionException, InterruptedException {
        byte[] key = generateRandomKey();
        Map<OrderedPayload, Payload> expectedMap = new LinkedHashMap<>();

        for (int i = 0; i < LARGE_ELEMENT_COUNT; i++) {
            byte[] subKey = ("ord_map_key_" + i).getBytes(StandardCharsets.UTF_8);
            byte[] value = createDeterministicPayload(i, PAYLOAD_SIZE);
            expectedMap.put(OrderedPayload.of(subKey, (long) i), Payload.of(value));
        }

        CompletableFuture<KeyHintData> future = client.createOrderedMap(key, expectedMap, getTestTtl(), 0, TIMEOUT);
        KeyHintData hint = future.get();
        assertNotNull(hint);

        Map<OrderedPayload, Payload> actualMap = client.streamOrderedMap(key, hint, 0, TIMEOUT).get();
        assertEquals(LARGE_ELEMENT_COUNT, actualMap.size());

        for (Map.Entry<OrderedPayload, Payload> entry : expectedMap.entrySet()) {
            byte[] subKey = entry.getKey().getValue();
            byte[] expectedValue = entry.getValue().getValue();

            byte[] actualValue = client.getContainerValue(key, hint, subKey, 0, TIMEOUT).get();
            assertNotNull(actualValue, "Ключ не найден в OrderedMap: " + new String(subKey));
            assertArrayEquals(expectedValue, actualValue, "Значение искажено в OrderedMap по ключу: " + new String(subKey));
        }
    }

    // =========================================================================
    // Вспомогательные генераторы детерминированных данных для проверок
    // =========================================================================

    private byte[] generateRandomKey() {
        return ("strict_container_key_" + UUID.randomUUID()).getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Создает пэйлоад со строгой привязкой байт к индексу элемента,
     * чтобы выявить любые смещения, перестановки или повреждения данных в чанках.
     */
    private byte[] createDeterministicPayload(int index, int size) {
        byte[] payload = new byte[size];
        byte headerMarker = (byte) (index % 127);
        for (int i = 0; i < size; i++) {
            payload[i] = (byte) ((i + headerMarker) % 256);
        }
        return payload;
    }

    private List<Payload> generateDeterministicPayloadList(int count) {
        List<Payload> payloads = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            payloads.add(Payload.of(createDeterministicPayload(i, PAYLOAD_SIZE)));
        }
        return payloads;
    }
}