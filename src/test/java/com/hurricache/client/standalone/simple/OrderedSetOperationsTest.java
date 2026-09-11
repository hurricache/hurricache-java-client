package com.hurricache.client.standalone.simple;

import com.hurricache.TestBase;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.OrderedPayload;
import com.hurricache.client.intf.Payload;
import com.hurricache.grpc.ContainerType;
import com.hurricache.grpc.LockStatus;
import com.hurricache.grpc.LockType;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
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

/**
 * Тесты для операций с Ordered Set (sorted set с весами).
 * Покрывает все методы, поддерживаемые контейнером Ordered Set.
 */
public class OrderedSetOperationsTest extends TestBase {

    private static final Duration TEST_TTL = Duration.ofSeconds(60);
    private static final int DEFAULT_CLIENT_ID = 1;
    private static final int SECONDARY_CLIENT_ID = 2;
    private static final int OWNER_CLIENT_ID = 100;
    private static final int INTRUDER_CLIENT_ID = 200;

    private String baseKey;

    @BeforeEach
    void setUp() {
        baseKey = "orderedset_test_" + UUID.randomUUID();
    }

    private byte[] bytes(String val) {
        return val.getBytes(StandardCharsets.UTF_8);
    }

    private OrderedPayload op(Long order, String val) {
        return OrderedPayload.of(order, val.getBytes(StandardCharsets.UTF_8));
    }

    private OrderedPayload op(byte[] value, Long order) {
        return OrderedPayload.of(value, order);
    }

    private String str(Payload payload) {
        return new String(payload.getValue(), StandardCharsets.UTF_8);
    }

    // =========================================================================
    // 1. CREATE ORDERED SET OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("Создание пустого Ordered Set")
    void testCreateEmptyOrderedSet() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_empty";
        
        KeyHintData hint = client.createOrderedSet(setKey, new ArrayList<>()).get();
        assertNotNull(hint, "KeyHint должен быть создан");
        
        Thread.sleep(500);
        
        List<OrderedPayload> result = client.streamOrderedSet(setKey, hint).get();
        assertNotNull(result);
        assertEquals(0, result.size(), "Пустой ordered set должен вернуть пустой список");
    }

    @Test
    @DisplayName("Создание Ordered Set с начальными данными")
    void testCreateOrderedSetWithInitialData() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_initial";
        List<OrderedPayload> initialData = List.of(
            op(1L, "item1"),
            op(2L, "item2"),
            op(3L, "item3")
        );
        
        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint, "KeyHint должен быть создан");
        
        Thread.sleep(500);
        
        List<OrderedPayload> result = client.streamOrderedSet(setKey, hint).get();
        assertNotNull(result);
        assertEquals(3, result.size(), "Ordered set должен содержать 3 элемента");
        
        List<String> resultStrings = result.stream()
            .map(this::str)
            .toList();
        assertTrue(resultStrings.contains("item1"));
        assertTrue(resultStrings.contains("item2"));
        assertTrue(resultStrings.contains("item3"));
        
        // Проверяем порядок (по возрастанию веса)
        assertEquals(1L, result.get(0).getOrder());
        assertEquals(2L, result.get(1).getOrder());
        assertEquals(3L, result.get(2).getOrder());
    }

    @Test
    @DisplayName("Создание большого Ordered Set с автоматическим разбиением на чанки (чанкинг)")
    void testCreateLargeOrderedSetWithChunking() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_large";
        int elementCount = 1500;
        List<OrderedPayload> payloads = new ArrayList<>();
        
        for (int i = 0; i < elementCount; i++) {
            payloads.add(op((long) i, "large_item_" + i + "_" + UUID.randomUUID()));
        }
        
        KeyHintData hint = client.createOrderedSet(setKey, payloads).get();
        assertNotNull(hint, "KeyHint должен быть создан");
        
        Thread.sleep(500);
        
        List<OrderedPayload> result = client.streamOrderedSet(setKey, hint).get();
        assertNotNull(result);
        assertEquals(elementCount, result.size(), "Ordered set должен содержать " + elementCount + " элементов");
    }

    // =========================================================================
    // 2. STREAM ORDERED SET OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("streamOrderedSet пустого ordered set возвращает пустой ответ")
    void testStreamOrderedSetEmptySet() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_stream_empty";
        
        KeyHintData hint = client.createOrderedSet(setKey, new ArrayList<>()).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        List<OrderedPayload> result = client.streamOrderedSet(setKey, hint).get();
        assertNotNull(result);
        assertEquals(0, result.size(), "streamOrderedSet пустого ordered set должен вернуть пустой список");
    }

    @Test
    @DisplayName("streamOrderedSet возвращает все содержимое контейнера")
    void testStreamOrderedSetReturnsAllContent() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_stream_all";
        List<OrderedPayload> initialData = List.of(
            op(1L, "elem1"),
            op(2L, "elem2"),
            op(3L, "elem3"),
            op(4L, "elem4"),
            op(5L, "elem5")
        );
        
        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        List<OrderedPayload> result = client.streamOrderedSet(setKey, hint).get();
        assertNotNull(result);
        assertEquals(5, result.size(), "streamOrderedSet должен вернуть все 5 элементов");
    }

    // =========================================================================
    // 3. ADD ELEMENT WITH WEIGHT OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("addElementOrdered добавляет элементы в ordered set c весом и возвращает количество добавленных элементов")
    void testaddElementOrdered() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_add_weight";
        List<OrderedPayload> initialData = List.of(op(1L, "item1"));
        
        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        // Добавляем 2 новых элемента с весами
        List<OrderedPayload> newElements = List.of(op(2L, "item2"), op(3L, "item3"));
        Integer added = client.addElementWithWeight(setKey, hint, newElements).get();
        assertEquals(2, added, "Должно быть добавлено 2 элемента");
        
        Thread.sleep(500);
        
        List<OrderedPayload> result = client.streamOrderedSet(setKey, hint).get();
        assertEquals(3, result.size(), "Ordered set должен содержать 3 элемента");
        
        // Проверяем порядок
        assertEquals(1L, result.get(0).getOrder());
        assertEquals(2L, result.get(1).getOrder());
        assertEquals(3L, result.get(2).getOrder());
    }

    @Test
    @DisplayName("addElementOrdered позволяет дубликаты (одинаковые ключи с разными весами)")
    void testaddElementOrderedAllowsDuplicates() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_add_weight_dup";
        List<OrderedPayload> initialData = List.of(op(1L, "item1"));
        
        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        // Добавляем элемент с тем же ключом но другим весом (дубликат)
        List<OrderedPayload> duplicateElements = List.of(op(2L, "item1"), op(3L, "item1"));
        Integer added = client.addElementWithWeight(setKey, hint, duplicateElements).get();
        assertEquals(2, added, "Должно быть добавлено 2 элемента (дубликаты разрешены)");
        
        Thread.sleep(500);
        
        List<OrderedPayload> result = client.streamOrderedSet(setKey, hint).get();
        assertEquals(3, result.size(), "Ordered set должен содержать 3 элемента");
    }

    @Test
    @DisplayName("addElementOrdered с пустым списком возвращает 0")
    void testaddElementOrderedEmptyList() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_add_weight_empty";
        List<OrderedPayload> initialData = List.of(op(1L, "item1"));
        
        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        Integer added = client.addElementWithWeight(setKey, hint, new ArrayList<>()).get();
        assertEquals(0, added, "Добавление пустого списка должно вернуть 0");
    }

    // =========================================================================
    // 4. STREAM ELEMENT IN RANGE ORDERED OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("streamElementInRangeOrdered возвращает список OrderedPayload согласно startWeight endWeight включительно")
    void testStreamElementInRangeOrdered() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_range";
        List<OrderedPayload> initialData = List.of(
            op(10L, "item1"),
            op(20L, "item2"),
            op(30L, "item3"),
            op(40L, "item4"),
            op(50L, "item5")
        );
        
        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        // Запрашиваем элементы с весом от 20 до 40 включительно
        List<OrderedPayload> result = client.streamElementInRangeOrderedSet(setKey.getBytes(StandardCharsets.UTF_8), hint, 20L, 40L, false, DEFAULT_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertNotNull(result);
        assertEquals(3, result.size(), "Должно быть 3 элемента с весом от 20 до 40");
        
        // Проверяем, что это правильные элементы
        assertEquals(20L, result.get(0).getOrder());
        assertEquals(30L, result.get(1).getOrder());
        assertEquals(40L, result.get(2).getOrder());
    }

    @Test
    @DisplayName("streamElementInRangeOrdered с reverse=true возвращает элементы в обратном порядке")
    void testStreamElementInRangeOrderedReverse() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_range_reverse";
        List<OrderedPayload> initialData = List.of(
            op(10L, "item1"),
            op(20L, "item2"),
            op(30L, "item3"),
            op(40L, "item4"),
            op(50L, "item5")
        );
        
        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        // Запрашиваем элементы с весом от 20 до 40 в обратном порядке
        List<OrderedPayload> result = client.streamElementInRangeOrderedSet(setKey.getBytes(StandardCharsets.UTF_8), hint, 20L, 40L, true, DEFAULT_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertNotNull(result);
        assertEquals(3, result.size(), "Должно быть 3 элемента");
        
        // Проверяем обратный порядок
        assertEquals(40L, result.get(0).getOrder());
        assertEquals(30L, result.get(1).getOrder());
        assertEquals(20L, result.get(2).getOrder());
    }

    @Test
    @DisplayName("streamElementInRangeOrdered возвращает пустой список если нет элементов в диапазоне")
    void testStreamElementInRangeOrderedNoMatch() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_range_nomatch";
        List<OrderedPayload> initialData = List.of(
            op(10L, "item1"),
            op(20L, "item2"),
            op(30L, "item3")
        );
        
        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        // Запрашиваем элементы с весом от 100 до 200 (нет таких элементов)
        List<OrderedPayload> result = client.streamElementInRangeOrderedSet(setKey.getBytes(StandardCharsets.UTF_8), hint, 100L, 200L, false, DEFAULT_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertNotNull(result);
        assertEquals(0, result.size(), "Должен быть пустой список");
    }

    // =========================================================================
    // 5. REMOVE FROM CONTAINER OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("removeFromContainer удаляет элемент из ordered set и возвращает количество удаленных элементов")
    void testRemoveFromContainer() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_remove";
        List<OrderedPayload> initialData = List.of(op(1L, "item1"), op(2L, "item2"), op(3L, "item3"));
        
        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        Integer removed = client.removeFromContainer(setKey.getBytes(StandardCharsets.UTF_8), hint, op(1L, "item1").getValue()).get();
        assertEquals(1, removed, "Должен быть удален 1 элемент");
        
        Thread.sleep(500);
        
        List<OrderedPayload> result = client.streamOrderedSet(setKey, hint).get();
        assertEquals(2, result.size(), "Ordered set должен содержать 2 элемента");
    }

    @Test
    @DisplayName("removeFromContainer может вернуть больше 1 если в контейнере есть одинаковые ключи но с разным весом")
    void testRemoveFromContainerMultipleDuplicates() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_remove_multi";
        // Добавляем одинаковый ключ с разными весами
        List<OrderedPayload> initialData = List.of(
            op(1L, "item1"),
            op(2L, "item1"),
            op(3L, "item1"),
            op(4L, "item2")
        );
        
        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        // Удаляем все элементы с ключом "item1" (их 3)
        Integer removed = client.removeFromContainer(setKey.getBytes(StandardCharsets.UTF_8), hint, op(1L, "item1").getValue()).get();
        assertEquals(3, removed, "Должно быть удалено 3 элемента с одинаковым ключом");
        
        Thread.sleep(500);
        
        List<OrderedPayload> result = client.streamOrderedSet(setKey, hint).get();
        assertEquals(1, result.size(), "Ordered set должен содержать 1 элемент");
        assertEquals("item2", str(result.get(0)));
    }

    @Test
    @DisplayName("removeFromContainer возвращает 0 если элемента нет")
    void testRemoveFromContainerNonExistent() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_remove_nonexist";
        List<OrderedPayload> initialData = List.of(op(1L, "item1"), op(2L, "item2"));
        
        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        Integer removed = client.removeFromContainer(setKey.getBytes(StandardCharsets.UTF_8), hint, op(0L, "nonexistent").getValue()).get();
        assertEquals(0, removed, "Должно быть удалено 0 элементов (элемент не найден)");
    }

    // =========================================================================
    // 6. CONTAINS CONTAINER KEY OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("containsContainerKey проверяет наличие элемента в контейнере")
    void testContainsContainerKey() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_contains";
        List<OrderedPayload> initialData = List.of(op(1L, "item1"), op(2L, "item2"));
        
        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        // Проверяем существующий элемент
        Boolean exists = client.containsContainerKey(setKey.getBytes(StandardCharsets.UTF_8), hint, op(1L, "item1").getValue()).get();
        assertTrue(exists, "Элемент item1 должен существовать");
        
        // Проверяем несуществующий элемент
        Boolean notExists = client.containsContainerKey(setKey.getBytes(StandardCharsets.UTF_8), hint, op(0L, "nonexistent").getValue()).get();
        Assertions.assertFalse(notExists, "Элемент nonexistent не должен существовать");
    }

    // =========================================================================
    // 7. REMOVE ELEMENT AT POSITION OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("removeElementAtPosition удаляет элементы с указанным весом pos начальное значение веса endPos конечное")
    void testRemoveElementAtPosition() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_remove_pos";
        List<OrderedPayload> initialData = List.of(
            op(10L, "item1"),
            op(20L, "item2"),
            op(30L, "item3"),
            op(40L, "item4"),
            op(50L, "item5")
        );
        
        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        // Удаляем элементы с весом от 20 до 40 включительно
        Boolean removed = client.removeElementAtPosition(setKey, hint, 20, 40).get();
        assertTrue(removed, "Элементы должны быть удалены");
        
        Thread.sleep(500);
        
        List<OrderedPayload> result = client.streamOrderedSet(setKey, hint).get();
        assertEquals(2, result.size(), "Ordered set должен содержать 2 элемента");
        assertEquals(10L, result.get(0).getOrder());
        assertEquals(50L, result.get(1).getOrder());
    }

    @Test
    @DisplayName("removeElementAtPosition: pos и endPos могут совпадать")
    void testRemoveElementAtPositionSamePos() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_remove_pos_same";
        List<OrderedPayload> initialData = List.of(
            op(10L, "item1"),
            op(20L, "item2"),
            op(30L, "item3")
        );
        
        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        // Удаляем элемент с весом 20 (pos = endPos = 20)
        Boolean removed = client.removeElementAtPosition(setKey, hint, 20, 20).get();
        assertTrue(removed, "Элемент должен быть удален");
        
        Thread.sleep(500);
        
        List<OrderedPayload> result = client.streamOrderedSet(setKey, hint).get();
        assertEquals(2, result.size(), "Ordered set должен содержать 2 элемента");
        assertEquals(10L, result.get(0).getOrder());
        assertEquals(30L, result.get(1).getOrder());
    }

    // =========================================================================
    // 8. ADD ELEMENT ORDERED OPERATIONS (без веса)
    // =========================================================================

    @Test
    @DisplayName("addElementOrdered добавляет элементы в ordered set c весом и возвращает количество добавленных элементов")
    void testAddElementOrdered() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_add_ordered";
        List<OrderedPayload> initialData = List.of(op(1L, "item1"));
        
        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        // Добавляем 2 новых элемента с весами
        List<OrderedPayload> newElements = List.of(op(2L, "item2"), op(3L, "item3"));
        Integer added = client.addElementWithWeight(setKey.getBytes(StandardCharsets.UTF_8), hint, newElements, DEFAULT_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(2, added, "Должно быть добавлено 2 элемента");
        
        Thread.sleep(500);
        
        List<OrderedPayload> result = client.streamOrderedSet(setKey, hint).get();
        assertEquals(3, result.size(), "Ordered set должен содержать 3 элемента");
    }

    @Test
    @DisplayName("addElementOrdered позволяет дубликаты (одинаковые ключи с разными весами)")
    void testAddElementOrderedAllowsDuplicates() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_add_ordered_dup";
        List<OrderedPayload> initialData = List.of(op(1L, "item1"));
        
        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        // Добавляем элемент с тем же ключом но другим весом (дубликат)
        List<OrderedPayload> duplicateElements = List.of(op(2L, "item1"), op(3L, "item1"));
        Integer added = client.addElementWithWeight(setKey.getBytes(StandardCharsets.UTF_8), hint, duplicateElements, DEFAULT_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(2, added, "Должно быть добавлено 2 элемента (дубликаты разрешены)");
    }

    // =========================================================================
    // 9. SET TTL OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("setTtl устанавливает TTL на ordered set")
    void testSetTtl() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_ttl_set";
        List<OrderedPayload> initialData = List.of(op(1L, "item1"));
        
        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        Boolean setTtlResult = client.setTtl(setKey, hint, 100).get();
        assertTrue(setTtlResult, "TTL должен быть успешно установлен");
    }

    @Test
    @DisplayName("getTtl получает TTL ordered set")
    void testGetTtl() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_ttl_get";
        List<OrderedPayload> initialData = List.of(op(1L, "item1"));
        
        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        // Устанавливаем TTL
        client.setTtl(setKey, hint, 100).get();
        
        Thread.sleep(500);
        assertNotFound(client.getTtl(setKey, hint));
    }

    // =========================================================================
    // 10. TTL EXPIRATION OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("После истечения TTL ordered set должен удалиться")
    void testTtlExpiration() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_ttl_expire";
        List<OrderedPayload> initialData = List.of(op(1L, "item1"));
        
        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        // Устанавливаем TTL = 1 секунда
        client.setTtl(setKey, hint, 1).get();
        
        Thread.sleep(1500);
        
        // Проверяем, что ordered set удален - streamOrderedSet должен вернуть пустой список или ошибку
        assertNotFound(client.streamOrderedSet(setKey, hint));
    }

    // =========================================================================
    // 11. LOCKING OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("READ_LOCK: несколько клиентов могут читать параллельно")
    void testReadLockParallelReads() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_read_lock";
        List<OrderedPayload> initialData = List.of(op(1L, "item1"));
        
        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        // Первый клиент берет READ_LOCK
        LockStatus lock1 = client.lockObject(setKey, LockType.READ_LOCK, DEFAULT_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock1, "Первый клиент должен получить READ_LOCK");
        
        // Второй клиент также может взять READ_LOCK
        LockStatus lock2 = client.lockObject(setKey, LockType.READ_LOCK, SECONDARY_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock2, "Второй клиент должен получить READ_LOCK");
        
        // Оба клиента могут читать
        List<OrderedPayload> result1 = client.streamOrderedSet(setKey, hint, DEFAULT_CLIENT_ID).get();
        assertNotNull(result1);
        assertEquals(1, result1.size());
        
        List<OrderedPayload> result2 = client.streamOrderedSet(setKey, hint, SECONDARY_CLIENT_ID).get();
        assertNotNull(result2);
        assertEquals(1, result2.size());
        
        // Освобождаем блокировки
        client.unlockObject(setKey, DEFAULT_CLIENT_ID).get();
        client.unlockObject(setKey, SECONDARY_CLIENT_ID).get();
    }

    @Test
    @DisplayName("WRITE_LOCK: только владелец может читать и писать, другие не могут ничего")
    void testWriteLockExclusiveAccess() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_write_lock";
        List<OrderedPayload> initialData = List.of(op(1L, "item1"));
        
        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        // Владелец берет WRITE_LOCK
        LockStatus lock = client.lockObject(setKey, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock, "Владелец должен получить WRITE_LOCK");
        
        // Владелец может читать
        List<OrderedPayload> readResult = client.streamOrderedSet(setKey, hint, OWNER_CLIENT_ID).get();
        assertNotNull(readResult);
        assertEquals(1, readResult.size());
        
        // Владелец может писать (добавлять элементы)
        Integer added = client.addElementWithWeight(setKey.getBytes(StandardCharsets.UTF_8), hint, List.of(op(2L, "item2")), OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(1, added, "Владелец должен добавить элемент");
        
        // Другой клиент не может читать
        try {
            client.streamOrderedSet(setKey, hint, INTRUDER_CLIENT_ID).get();
            fail("Интриган не должен иметь доступа к чтению при WRITE_LOCK");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.PERMISSION_DENIED, cause.getStatus().getCode(), "Должна быть ошибка PERMISSION_DENIED");
        }
        
        // Другой клиент не может писать
        try {
            client.addElementWithWeight(setKey.getBytes(StandardCharsets.UTF_8), hint, List.of(op(3L, "item3")), INTRUDER_CLIENT_ID, Duration.ofSeconds(30)).get();
            fail("Интриган не должен иметь доступа к записи при WRITE_LOCK");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.PERMISSION_DENIED, cause.getStatus().getCode(), "Должна быть ошибка PERMISSION_DENIED");
        }
        
        // Освобождаем блокировку
        client.unlockObject(setKey, OWNER_CLIENT_ID).get();
    }

    @Test
    @DisplayName("GLOBAL: только владелец делает любые операции, все остальные блокируются")
    void testGlobalLockExclusiveAccess() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_global_lock";
        List<OrderedPayload> initialData = List.of(op(1L, "item1"));
        
        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        // Владелец берет GLOBAL LOCK
        LockStatus lock = client.lockObject(setKey, LockType.GLOBAL, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock, "Владелец должен получить GLOBAL LOCK");
        
        // Владелец может читать
        List<OrderedPayload> readResult = client.streamOrderedSet(setKey, hint, OWNER_CLIENT_ID).get();
        assertNotNull(readResult);
        assertEquals(1, readResult.size());
        
        // Владелец может писать
        Integer added = client.addElementWithWeight(setKey.getBytes(StandardCharsets.UTF_8), hint, List.of(op(2L, "item2")), OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(1, added, "Владелец должен добавить элемент");
        
        // Другой клиент не может читать
        try {
            client.streamOrderedSet(setKey, hint, INTRUDER_CLIENT_ID).get();
            fail("Интриган не должен иметь доступа к чтению при GLOBAL LOCK");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.PERMISSION_DENIED, cause.getStatus().getCode(), "Должна быть ошибка PERMISSION_DENIED");
        }
        
        // Другой клиент не может писать
        try {
            client.addElementWithWeight(setKey.getBytes(StandardCharsets.UTF_8), hint, List.of(op(3L, "item3")), INTRUDER_CLIENT_ID, Duration.ofSeconds(30)).get();
            fail("Интриган не должен иметь доступа к записи при GLOBAL LOCK");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.PERMISSION_DENIED, cause.getStatus().getCode(), "Должна быть ошибка PERMISSION_DENIED");
        }

        assertDenied(client.unlockObject(setKey, INTRUDER_CLIENT_ID));
        
        // Освобождаем блокировку владельцем
        client.unlockObject(setKey, OWNER_CLIENT_ID).get();
    }

    @Test
    @DisplayName("unlockObject: снять блокировку может только владелец")
    void testUnlockByOwnerOnly() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_unlock_owner";
        List<OrderedPayload> initialData = List.of(op(1L, "item1"));
        
        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        // Владелец берет WRITE_LOCK
        LockStatus lock = client.lockObject(setKey, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock, "Владелец должен получить WRITE_LOCK");
        
        // Интриган пытается снять блокировку
        assertDenied(client.unlockObject(setKey, INTRUDER_CLIENT_ID));
        
        // Владелец снимает блокировку
        LockStatus validUnlock = client.unlockObject(setKey, OWNER_CLIENT_ID).get();
        assertEquals(LockStatus.OK, validUnlock, "Владелец должен снять блокировку");
    }

    // =========================================================================
    // 12. METHODS NOT SUPPORTED BY ORDERED SET (should return error)
    // =========================================================================

    // =========================================================================
    // 14. GET ELEMENT AT POSITION OPERATIONS (для ordered set)
    // =========================================================================

    @Test
    @DisplayName("getElementAtPosition возвращает элемент с указанным весом")
    void testGetElementAtPosition() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_get_pos";
        List<OrderedPayload> initialData = List.of(
            op(10L, "item1"),
            op(20L, "item2"),
            op(30L, "item3"),
            op(40L, "item4"),
            op(50L, "item5")
        );
        
        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        // Получаем элемент с весом 30
        Payload result = client.getElementWithWeight(setKey, hint, 30).get();
        assertNotNull(result);
        System.out.println("getElementAtPosition: " + str(result));
        assertEquals("item3", str(result));
    }

    @Test
    @DisplayName("getElementAtPosition возвращает один из элементов если их несколько с одинаковым весом")
    void testGetElementAtPositionMultipleSameWeight() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_get_pos_multi";
        // Добавляем несколько элементов с одинаковым весом
        List<OrderedPayload> initialData = List.of(
            op(20L, "item1"),
            op(20L, "item2"),
            op(20L, "item3"),
            op(40L, "item4")
        );
        
        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        // Получаем элемент с весом 20 (может вернуть любой из 3-х)
        Payload result = client.getElementWithWeight(setKey, hint, 20).get();
        System.out.println("getElementAtPosition: " + str(result));
        assertNotNull(result);
        // Проверяем, что это один из ожидаемых элементов
        assertTrue(str(result).equals("item1") || str(result).equals("item2") || str(result).equals("item3"));
    }

    @Test
    @DisplayName("getElementAtPosition возвращает NOT_FOUND если веса нет")
    void testGetElementAtPositionNotFound() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_get_pos_notfound";
        List<OrderedPayload> initialData = List.of(
            op(10L, "item1"),
            op(20L, "item2"),
            op(30L, "item3")
        );
        
        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        // Пытаемся получить элемент с несуществующим весом
        try {
            client.getElementAtPosition(setKey, hint, 100).get();
            fail("getElementAtPosition должен вызвать ошибку NOT_FOUND для несуществующего веса");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode(), "Должна быть ошибка NOT_FOUND");
        }
    }

    @Test
    @DisplayName("getAndRemoveElementAtPosition возвращает и удаляет элемент с указанным весом")
    void testGetAndRemoveElementAtPosition() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_get_remove_pos";
        List<OrderedPayload> initialData = List.of(
            op(10L, "item1"),
            op(20L, "item2"),
            op(30L, "item3")
        );
        
        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        // Получаем и удаляем элемент с весом 20
        Payload result = client.getAndRemoveElementWithWeight(setKey, hint, 20).get();
        System.out.println("getElementAtPosition: " + str(result));
        assertNotNull(result);
        assertEquals("item2", str(result));
        
        Thread.sleep(500);
        
        // Проверяем, что элемент удален
        List<OrderedPayload> remaining = client.streamOrderedSet(setKey, hint).get();
        assertEquals(2, remaining.size(), "Ordered set должен содержать 2 элемента");
    }

    @Test
    @DisplayName("getAndRemoveElementAtPosition возвращает NOT_FOUND если веса нет")
    void testGetAndRemoveElementAtPositionNotFound() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_get_remove_pos_notfound";
        List<OrderedPayload> initialData = List.of(
            op(10L, "item1"),
            op(20L, "item2")
        );
        
        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        // Пытаемся получить и удалить элемент с несуществующим весом
        try {
            client.getAndRemoveElementAtPosition(setKey, hint, 100).get();
            fail("getAndRemoveElementAtPosition должен вызвать ошибку NOT_FOUND для несуществующего веса");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode(), "Должна быть ошибка NOT_FOUND");
        }
    }

    // =========================================================================
    // 15. ADD ELEMENT TO POSITION OPERATIONS (для ordered set)
    // =========================================================================

    @Test
    @DisplayName("addElementToPosition добавляет элемент с весом pos")
    void testAddElementToPosition() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_add_pos";
        List<OrderedPayload> initialData = List.of(
            op(10L, "item1"),
            op(30L, "item3")
        );
        
        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        // Добавляем элемент с весом 20
        Integer added = client.addElementWithWeight(setKey, hint, List.of(OrderedPayload.of(20,bytes("item2")))).get();
        assertEquals(1, added, "Должен быть добавлен 1 элемент");
        
        Thread.sleep(500);
        
        List<OrderedPayload> result = client.streamOrderedSet(setKey, hint).get();
        assertEquals(3, result.size(), "Ordered set должен содержать 3 элемента");
        assertEquals(10L, result.get(0).getOrder());
        assertEquals(20L, result.get(1).getOrder());
        assertEquals(30L, result.get(2).getOrder());
    }

    @Test
    @DisplayName("addElementToPosition добавляет элемент с весом pos в пустой ordered set")
    void testAddElementToPositionEmptySet() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_add_pos_empty";
        
        KeyHintData hint = client.createOrderedSet(setKey, new ArrayList<>()).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        // Добавляем элемент с весом 10
        Integer added = client.addElementWithWeight(setKey, hint, List.of(OrderedPayload.of(10,bytes("item1")))).get();
        assertEquals(1, added, "Должен быть добавлен 1 элемент");
        
        Thread.sleep(500);
        
        List<OrderedPayload> result = client.streamOrderedSet(setKey, hint).get();
        assertEquals(1, result.size(), "Ordered set должен содержать 1 элемент");
        assertEquals(10L, result.get(0).getOrder());
    }

    @Test
    @DisplayName("Методы, не применимые к ordered set, должны вызывать ошибку")
    void testUnsupportedMethodsForOrderedSet() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_unsupported";
        List<OrderedPayload> initialData = List.of(op(1L, "item1"));
        
        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        // getHead - не применим к ordered set
        try {
            client.getHead(setKey, hint).get();
            fail("getHead должен вызвать ошибку для ordered set");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.INTERNAL, cause.getStatus().getCode(), "Должна быть ошибка INTERNAL");
        }
        
        // getFront - не применим к ordered set
        try {
            client.getHead(setKey, hint).get();
            fail("getFront должен вызвать ошибку для ordered set");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.INTERNAL, cause.getStatus().getCode(), "Должна быть ошибка INTERNAL");
        }
        
        // getTail - не применим к ordered set
        try {
            client.getTail(setKey, hint).get();
            fail("getTail должен вызвать ошибку для ordered set");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.INTERNAL, cause.getStatus().getCode(), "Должна быть ошибка INTERNAL");
        }
        

        
        // streamElementInRangeUnordered - не применим к ordered set
        try {
            client.streamElementInRangeUnordered(setKey.getBytes(StandardCharsets.UTF_8), hint, ContainerType.ORDERED_SET, 0, 10, DEFAULT_CLIENT_ID, Duration.ofSeconds(30)).get();
            fail("streamElementInRangeUnordered должен вызвать ошибку для ordered set");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.INVALID_ARGUMENT, cause.getStatus().getCode(), "Должна быть ошибка INVALID_ARGUMENT");
        }
        catch (IllegalArgumentException e) {
            assertEquals("Unsupported container type for stream operation: ORDERED_SET",e.getMessage());
        }
    }

    // =========================================================================
    // 13. ADDITIONAL EDGE CASES
    // =========================================================================

    @Test
    @DisplayName("addElementOrdered с пустым списком возвращает 0")
    void testaddElementOrderedEmptyListEdgeCase() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_add_weight_empty_edge";
        List<OrderedPayload> initialData = List.of(op(1L, "item1"));
        
        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        Integer added = client.addElementWithWeight(setKey, hint, new ArrayList<>()).get();
        assertEquals(0, added, "Добавление пустого списка должно вернуть 0");
    }

    @Test
    @DisplayName("removeFromContainer с несуществующим элементом возвращает 0")
    void testRemoveFromContainerNonExistentElement() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_remove_nonexist_elem";
        List<OrderedPayload> initialData = List.of(op(1L, "item1"));
        
        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        Integer removed = client.removeFromContainer(setKey.getBytes(StandardCharsets.UTF_8), hint, op(0L, "nonexistent").getValue()).get();
        assertEquals(0, removed, "Удаление несуществующего элемента должно вернуть 0");
    }

    @Test
    @DisplayName("containsContainerKey с несуществующим элементом возвращает false")
    void testContainsContainerKeyNonExistent() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_contains_nonexist";
        List<OrderedPayload> initialData = List.of(op(1L, "item1"));
        
        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        Boolean exists = client.containsContainerKey(setKey.getBytes(StandardCharsets.UTF_8), hint, op(0L, "nonexistent").getValue()).get();
        Assertions.assertFalse(exists);
    }

    @Test
    @DisplayName("streamOrderedSet с несуществующим ordered set возвращает ошибку")
    void testStreamOrderedSetNonExistent() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_stream_nonexist";
        
        // Создаем KeyHint для несуществующего ordered set
        KeyHintData hint = KeyHintData.of(1, 1);
        
        try {
            client.streamOrderedSet(setKey, hint).get();
            fail("streamOrderedSet для несуществующего ordered set должен вызвать ошибку");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode(), "Должна быть ошибка NOT_FOUND");
        }
    }

    @Test
    @DisplayName("TTL expiration: ordered set удаляется после истечения TTL")
    void testTtlExpirationComplete() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_ttl_expire_complete";
        List<OrderedPayload> initialData = List.of(op(1L, "item1"));
        
        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        // Устанавливаем TTL = 1 секунда
        client.setTtl(setKey, hint, 1).get();
        
        Thread.sleep(1500);
        
        // Проверяем, что ordered set удален - getSize должен вернуть ошибку или 0
        try {
            Integer size = client.getSize(setKey, hint).get();
            // Если размер 0, значит контейнер пуст (удален)
            assertTrue(size == 0 || size == null, "После истечения TTL размер должен быть 0");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode(), "Должна быть ошибка NOT_FOUND");
        }
    }

    @Test
    @DisplayName("Блокировка с истекшим TTL: unlock возвращает OK")
    void testUnlockOnExpiredLock() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_unlock_expired";
        List<OrderedPayload> initialData = List.of(op(1L, "item1"));
        
        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        // Берем блокировку с TTL = 1 секунда
        LockStatus lock = client.lockObject(setKey, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(1)).get();
        assertEquals(LockStatus.OK, lock, "Должна быть получена блокировка");
        
        Thread.sleep(1500);
        
        // Пытаемся снять истекшую блокировку
        LockStatus unlock = client.unlockObject(setKey, OWNER_CLIENT_ID).get();
        assertEquals(LockStatus.OK, unlock, "Снятие истекшей блокировки должно вернуть OK");
    }

    @Test
    @DisplayName("streamElementInRangeOrdered: endWeight всегда включается")
    void testStreamElementInRangeOrderedEndWeightIncluded() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_range_end_included";
        List<OrderedPayload> initialData = List.of(
            op(10L, "item1"),
            op(20L, "item2"),
            op(30L, "item3")
        );
        
        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        // Запрашиваем элементы с весом от 10 до 30 (30 должен быть включен)
        List<OrderedPayload> result = client.streamElementInRangeOrderedSet(setKey.getBytes(StandardCharsets.UTF_8), hint, 10L, 30L, false, DEFAULT_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertNotNull(result);
        assertEquals(3, result.size(), "Должно быть 3 элемента, endWeight 30 должен быть включен");
        assertEquals(30L, result.get(2).getOrder());
    }

    @Test
    @DisplayName("streamElementInRangeOrdered: startWeight всегда включается")
    void testStreamElementInRangeOrderedStartWeightIncluded() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_range_start_included";
        List<OrderedPayload> initialData = List.of(
            op(10L, "item1"),
            op(20L, "item2"),
            op(30L, "item3")
        );
        
        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        // Запрашиваем элементы с весом от 10 до 30 (10 должен быть включен)
        List<OrderedPayload> result = client.streamElementInRangeOrderedSet(setKey.getBytes(StandardCharsets.UTF_8), hint, 10L, 30L, false, DEFAULT_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertNotNull(result);
        assertEquals(3, result.size(), "Должно быть 3 элемента, startWeight 10 должен быть включен");
        assertEquals(10L, result.get(0).getOrder());
    }

    @Test
    @DisplayName("addElementOrdered: добавление элементов с одинаковыми весами")
    void testaddElementOrderedSameWeight() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_add_weight_same_weight";
        List<OrderedPayload> initialData = List.of(op(1L, "item1"));
        
        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        // Добавляем элементы с тем же весом (допустимо)
        List<OrderedPayload> newElements = List.of(op(1L, "item2"), op(1L, "item3"));
        Integer added = client.addElementWithWeight(setKey, hint, newElements).get();
        assertEquals(2, added, "Должно быть добавлено 2 элемента");
        
        Thread.sleep(500);
        
        List<OrderedPayload> result = client.streamOrderedSet(setKey, hint).get();
        assertEquals(3, result.size(), "Ordered set должен содержать 3 элемента");
    }

    @Test
    @DisplayName("removeElementAtPosition: удаление диапазона с endPos включительно")
    void testRemoveElementAtPositionEndPosIncluded() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_remove_pos_end_included";
        List<OrderedPayload> initialData = List.of(
            op(10L, "item1"),
            op(20L, "item2"),
            op(30L, "item3"),
            op(40L, "item4")
        );
        
        KeyHintData hint = client.createOrderedSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        // Удаляем элементы с весом от 20 до 30 (30 должен быть удален)
        Boolean removed = client.removeElementAtPosition(setKey, hint, 20, 30).get();
        assertTrue(removed, "Элементы должны быть удалены");
        
        Thread.sleep(500);
        
        List<OrderedPayload> result = client.streamOrderedSet(setKey, hint).get();
        assertEquals(2, result.size(), "Ordered set должен содержать 2 элемента");
        assertEquals(10L, result.get(0).getOrder());
        assertEquals(40L, result.get(1).getOrder());
    }
}