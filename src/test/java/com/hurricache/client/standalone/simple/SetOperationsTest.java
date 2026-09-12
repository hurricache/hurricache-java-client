package com.hurricache.client.standalone.simple;

import com.hurricache.TestBase;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Payload;
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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Тесты для операций с Set (unordered set / hashset).
 * Покрывает все методы, поддерживаемые контейнером Set.
 */
public class SetOperationsTest extends TestBase {

    private static final Duration TEST_TTL = Duration.ofSeconds(60);
    private static final int DEFAULT_CLIENT_ID = 1;
    private static final int SECONDARY_CLIENT_ID = 2;
    private static final int OWNER_CLIENT_ID = 100;
    private static final int INTRUDER_CLIENT_ID = 200;

    private String baseKey;

    @BeforeEach
    void setUp() {
        baseKey = "set_test_" + UUID.randomUUID();
    }



    private Payload p(String val) {
        return Payload.of(val.getBytes(StandardCharsets.UTF_8));
    }

    private String str(Payload payload) {
        return new String(payload.getValue(), StandardCharsets.UTF_8);
    }

    // =========================================================================
    // 1. CREATE SET OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("Создание пустого Set")
    void testCreateEmptySet() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_empty";
        
        KeyHintData hint = client.createSet(setKey, new ArrayList<>()).get();
        assertNotNull(hint, "KeyHint должен быть создан");
        
        Thread.sleep(500);
        
        List<Payload> result = client.streamSet(setKey, hint).get();
        assertNotNull(result);
        assertEquals(0, result.size(), "Пустой set должен вернуть пустой список");
    }

    @Test
    @DisplayName("Создание Set с начальными данными")
    void testCreateSetWithInitialData() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_initial";
        List<Payload> initialData = List.of(
            p("item1"),
            p("item2"),
            p("item3")
        );
        
        KeyHintData hint = client.createSet(setKey, initialData).get();
        assertNotNull(hint, "KeyHint должен быть создан");
        
        Thread.sleep(500);
        
        List<Payload> result = client.streamSet(setKey, hint).get();
        assertNotNull(result);
        assertEquals(3, result.size(), "Set должен содержать 3 элемента");
        
        List<String> resultStrings = result.stream()
            .map(this::str)
            .toList();
        assertTrue(resultStrings.contains("item1"));
        assertTrue(resultStrings.contains("item2"));
        assertTrue(resultStrings.contains("item3"));
    }

    @Test
    @DisplayName("Создание большого Set с автоматическим разбиением на чанки (чанкинг)")
    void testCreateLargeSetWithChunking() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_large";
        int elementCount = 1500;
        List<Payload> payloads = new ArrayList<>();
        
        for (int i = 0; i < elementCount; i++) {
            payloads.add(p("large_item_" + i + "_" + UUID.randomUUID()));
        }
        
        KeyHintData hint = client.createSet(setKey, payloads).get();
        assertNotNull(hint, "KeyHint должен быть создан");
        
        Thread.sleep(500);
        
        List<Payload> result = client.streamSet(setKey, hint).get();
        assertNotNull(result);
        assertEquals(elementCount, result.size(), "Set должен содержать " + elementCount + " элементов");
    }

    // =========================================================================
    // 2. STREAM SET OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("streamSet пустого set возвращает пустой ответ")
    void testStreamSetEmptySet() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_stream_empty";
        
        KeyHintData hint = client.createSet(setKey, new ArrayList<>()).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        List<Payload> result = client.streamSet(setKey, hint).get();
        assertNotNull(result);
        assertEquals(0, result.size(), "streamSet пустого set должен вернуть пустой список");
    }

    @Test
    @DisplayName("streamSet возвращает все содержимое контейнера")
    void testStreamSetReturnsAllContent() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_stream_all";
        List<Payload> initialData = List.of(
            p("elem1"),
            p("elem2"),
            p("elem3"),
            p("elem4"),
            p("elem5")
        );
        
        KeyHintData hint = client.createSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        List<Payload> result = client.streamSet(setKey, hint).get();
        assertNotNull(result);
        assertEquals(5, result.size(), "streamSet должен вернуть все 5 элементов");
    }

    // =========================================================================
    // 3. ADD ELEMENT OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("addElement добавляет элементы в set и возвращает количество добавленных уникальных элементов")
    void testAddElement() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_add";
        List<Payload> initialData = List.of(p("item1"));
        
        KeyHintData hint = client.createSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        // Добавляем 2 новых элемента
        List<Payload> newElements = List.of(p("item2"), p("item3"));
        Integer added = client.addElementUnordered(setKey, newElements).get();
        assertEquals(2, added, "Должно быть добавлено 2 уникальных элемента");
        
        Thread.sleep(500);
        
        Integer size = client.getSize(setKey, hint).get();
        assertEquals(3, size, "Set должен содержать 3 элемента");
    }

    @Test
    @DisplayName("addElement не добавляет дубликаты и возвращает 0 для дубликатов")
    void testAddElementNoDuplicates() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_no_dup";
        List<Payload> initialData = List.of(p("item1"));
        
        KeyHintData hint = client.createSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        // Пытаемся добавить дубликат
        List<Payload> duplicateElements = List.of(p("item1"), p("item2"));
        Integer added = client.addElementUnordered(setKey, duplicateElements).get();
        assertEquals(1, added, "Должен быть добавлен только 1 новый элемент (item2), item1 - дубликат");
        
        Thread.sleep(500);
        
        Integer size = client.getSize(setKey, hint).get();
        assertEquals(2, size, "Set должен содержать 2 элемента");
    }

    // =========================================================================
    // 4. REMOVE FROM CONTAINER OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("removeFromContainer удаляет элемент из set и возвращает 1")
    void testRemoveFromContainer() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_remove";
        List<Payload> initialData = List.of(p("item1"), p("item2"), p("item3"));
        
        KeyHintData hint = client.createSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        Integer removed = client.removeFromContainer(setKey.getBytes(StandardCharsets.UTF_8), hint, p("item1").getValue()).get();
        assertEquals(1, removed, "Должен быть удален 1 элемент");
        
        Thread.sleep(500);
        
        Integer size = client.getSize(setKey, hint).get();
        assertEquals(2, size, "Set должен содержать 2 элемента");
    }

    @Test
    @DisplayName("removeFromContainer возвращает 0 если элемента нет")
    void testRemoveFromContainerNonExistent() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_remove_nonexist";
        List<Payload> initialData = List.of(p("item1"), p("item2"));
        
        KeyHintData hint = client.createSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        Integer removed = client.removeFromContainer(setKey.getBytes(StandardCharsets.UTF_8), hint, p("nonexistent").getValue()).get();
        assertEquals(0, removed, "Должно быть удалено 0 элементов (элемент не найден)");
        
        Thread.sleep(500);
        
        Integer size = client.getSize(setKey, hint).get();
        assertEquals(2, size, "Set должен содержать 2 элемента");
    }

    // =========================================================================
    // 5. CONTAINS CONTAINER KEY OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("containsContainerKey проверяет наличие элемента в контейнере")
    void testContainsContainerKey() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_contains";
        List<Payload> initialData = List.of(p("item1"), p("item2"));
        
        KeyHintData hint = client.createSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        // Проверяем существующий элемент
        Boolean exists = client.containsContainerKey(setKey.getBytes(StandardCharsets.UTF_8), hint, p("item1").getValue()).get();
        assertTrue(exists, "Элемент item1 должен существовать");
        
        // Проверяем несуществующий элемент
        Boolean notExists = client.containsContainerKey(setKey.getBytes(StandardCharsets.UTF_8), hint, p("nonexistent").getValue()).get();
        Assertions.assertFalse(notExists, "Элемент nonexistent не должен существовать");
    }

    // =========================================================================
    // 6. SET TTL OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("setTtl устанавливает TTL на set")
    void testSetTtl() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_ttl_set";
        List<Payload> initialData = List.of(p("item1"));
        
        KeyHintData hint = client.createSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        Boolean setTtlResult = client.setTtl(setKey, hint, 100).get();
        assertTrue(setTtlResult, "TTL должен быть успешно установлен");
    }

    @Test
    @DisplayName("getTtl получает TTL set")
    void testGetTtl() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_ttl_get";
        List<Payload> initialData = List.of(p("item1"));
        
        KeyHintData hint = client.createSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        // Устанавливаем TTL
        client.setTtl(setKey, hint, 100).get();
        
        Thread.sleep(500);
        assertNotFound(client.getTtl(setKey, hint));

    }

    // =========================================================================
    // 7. TTL EXPIRATION OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("После истечения TTL set должен удалиться")
    void testTtlExpiration() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_ttl_expire";
        List<Payload> initialData = List.of(p("item1"));
        
        KeyHintData hint = client.createSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        // Устанавливаем TTL = 1 секунда
        client.setTtl(setKey, hint, 1).get();
        
        Thread.sleep(1500);
        
        // Проверяем, что set удален - streamSet должен вернуть пустой список или ошибку
        assertNotFound(client.streamSet(setKey, hint));

        // После истечения TTL контейнер удаляется, streamSet может вернуть пустой список
        // или вызвать ошибку в зависимости от реализации сервера
    }

    // =========================================================================
    // 8. LOCKING OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("READ_LOCK: несколько клиентов могут читать параллельно")
    void testReadLockParallelReads() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_read_lock";
        List<Payload> initialData = List.of(p("item1"));
        
        KeyHintData hint = client.createSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        // Первый клиент берет READ_LOCK
        LockStatus lock1 = client.lockObject(setKey, LockType.READ_LOCK, DEFAULT_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock1, "Первый клиент должен получить READ_LOCK");
        
        // Второй клиент также может взять READ_LOCK
        LockStatus lock2 = client.lockObject(setKey, LockType.READ_LOCK, SECONDARY_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock2, "Второй клиент должен получить READ_LOCK");
        
        // Оба клиента могут читать
        List<Payload> result1 = client.streamSet(setKey, hint, DEFAULT_CLIENT_ID).get();
        assertNotNull(result1);
        assertEquals(1, result1.size());
        
        List<Payload> result2 = client.streamSet(setKey, hint, SECONDARY_CLIENT_ID).get();
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
        List<Payload> initialData = List.of(p("item1"));
        
        KeyHintData hint = client.createSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        // Владелец берет WRITE_LOCK
        LockStatus lock = client.lockObject(setKey, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock, "Владелец должен получить WRITE_LOCK");
        
        // Владелец может читать
        List<Payload> readResult = client.streamSet(setKey, hint, OWNER_CLIENT_ID).get();
        assertNotNull(readResult);
        assertEquals(1, readResult.size());
        
        // Владелец может писать (добавлять элементы)
        Integer added = client.addElementUnordered(setKey.getBytes(StandardCharsets.UTF_8), hint, List.of(p("item2")), OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(1, added, "Владелец должен добавить элемент");
        
        // Другой клиент не может читать
        try {
            client.streamSet(setKey, hint, INTRUDER_CLIENT_ID).get();
            fail("Интриган не должен иметь доступа к чтению при WRITE_LOCK");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.PERMISSION_DENIED, cause.getStatus().getCode(), "Должна быть ошибка PERMISSION_DENIED");
        }
        
        // Другой клиент не может писать
        try {
            client.addElementUnordered(setKey.getBytes(StandardCharsets.UTF_8), hint, List.of(p("item3")), INTRUDER_CLIENT_ID, Duration.ofSeconds(30)).get();
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
        List<Payload> initialData = List.of(p("item1"));
        
        KeyHintData hint = client.createSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        // Владелец берет GLOBAL LOCK
        LockStatus lock = client.lockObject(setKey, LockType.GLOBAL, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock, "Владелец должен получить GLOBAL LOCK");
        
        // Владелец может читать
        List<Payload> readResult = client.streamSet(setKey, hint, OWNER_CLIENT_ID).get();
        assertNotNull(readResult);
        assertEquals(1, readResult.size());
        
        // Владелец может писать
        Integer added = client.addElementUnordered(setKey.getBytes(StandardCharsets.UTF_8), hint, List.of(p("item2")), OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(1, added, "Владелец должен добавить элемент");
        
        // Другой клиент не может читать
        try {
            client.streamSet(setKey, hint, INTRUDER_CLIENT_ID).get();
            fail("Интриган не должен иметь доступа к чтению при GLOBAL LOCK");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.PERMISSION_DENIED, cause.getStatus().getCode(), "Должна быть ошибка PERMISSION_DENIED");
        }
        
        // Другой клиент не может писать
        try {
            client.addElementUnordered(setKey.getBytes(StandardCharsets.UTF_8), hint, List.of(p("item3")), INTRUDER_CLIENT_ID, Duration.ofSeconds(30)).get();
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
        List<Payload> initialData = List.of(p("item1"));
        
        KeyHintData hint = client.createSet(setKey, initialData).get();
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
    // 9. METHODS NOT SUPPORTED BY SET (should return error)
    // =========================================================================

    @Test
    @DisplayName("Методы, не применимые к set, должны вызывать ошибку")
    void testUnsupportedMethodsForSet() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_unsupported";
        List<Payload> initialData = List.of(p("item1"));
        
        KeyHintData hint = client.createSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        // getElementAtPosition - не применим к set
        try {
            client.getElementAtPosition(setKey, hint, 0).get();
            fail("getElementAtPosition должен вызвать ошибку для set");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.INTERNAL, cause.getStatus().getCode(), "Должна быть ошибка INTERNAL");
        }
        
        // getAndRemoveElementAtPosition - не применим к set
        try {
            client.getAndRemoveElementAtPosition(setKey, hint, 0).get();
            fail("getAndRemoveElementAtPosition должен вызвать ошибку для set");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.INTERNAL, cause.getStatus().getCode(), "Должна быть ошибка INTERNAL");
        }
        
        // addElementToPosition - не применим к set
//        try {
//            client.addElementToPosition(setKey, hint, List.of(p("item2")), 0).get();
//            fail("addElementToPosition должен вызвать ошибку для set");
//        } catch (ExecutionException e) {
//            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
//            assertEquals(Status.Code.INTERNAL, cause.getStatus().getCode(), "Должна быть ошибка INTERNAL");
//        }
        
        // removeElementAtPosition - не применим к set
        try {
            client.removeElementAtPosition(setKey, hint, 0, 0).get();
            fail("removeElementAtPosition должен вызвать ошибку для set");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.INTERNAL, cause.getStatus().getCode(), "Должна быть ошибка INTERNAL");
        }
        
        // getHead - не применим к set
        try {
            client.getHead(setKey, hint).get();
            fail("getHead должен вызвать ошибку для set");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.INTERNAL, cause.getStatus().getCode(), "Должна быть ошибка INTERNAL");
        }
        
        // getFront - не применим к set
        try {
            client.getHead(setKey, hint).get();
            fail("getFront должен вызвать ошибку для set");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.INTERNAL, cause.getStatus().getCode(), "Должна быть ошибка INTERNAL");
        }
        
        // getTail - не применим к set
        try {
            client.getTail(setKey, hint).get();
            fail("getTail должен вызвать ошибку для set");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.INTERNAL, cause.getStatus().getCode(), "Должна быть ошибка INTERNAL");
        }
        
        // streamList - не применим к set
//        try {
//            client.streamList(setKey, hint).get();
//            fail("streamList должен вызвать ошибку для set");
//        } catch (ExecutionException e) {
//            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
//            assertEquals(Status.Code.INTERNAL, cause.getStatus().getCode(), "Должна быть ошибка INTERNAL");
//        }
        
        // streamVector - не применим к set
//        try {
//            client.streamVector(setKey, hint).get();
//            fail("streamVector должен вызвать ошибку для set");
//        } catch (ExecutionException e) {
//            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
//            assertEquals(Status.Code.INTERNAL, cause.getStatus().getCode(), "Должна быть ошибка INTERNAL");
//        }
        
        // streamElementInRangeOrderedSet - не применим к unordered set
        try {
            client.streamElementInRangeOrderedSet(setKey.getBytes(StandardCharsets.UTF_8), hint, 0L, 10L, false, 0, Duration.ofSeconds(30)).get();
            fail("streamElementInRangeOrderedSet должен вызвать ошибку для unordered set");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.INVALID_ARGUMENT, cause.getStatus().getCode(), "Должна быть ошибка INTERNAL");
        }
    }

    // =========================================================================
    // 10. ADDITIONAL EDGE CASES
    // =========================================================================

    @Test
    @DisplayName("addElement с пустым списком возвращает 0")
    void testAddElementEmptyList() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_add_empty";
        List<Payload> initialData = List.of(p("item1"));
        
        KeyHintData hint = client.createSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        Integer added = client.addElementUnordered(setKey, new ArrayList<>()).get();
        assertEquals(0, added, "Добавление пустого списка должно вернуть 0");
    }

    @Test
    @DisplayName("removeFromContainer с несуществующим элементом возвращает 0")
    void testRemoveFromContainerNonExistentElement() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_remove_nonexist_elem";
        List<Payload> initialData = List.of(p("item1"));
        
        KeyHintData hint = client.createSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        Integer removed = client.removeFromContainer(setKey.getBytes(StandardCharsets.UTF_8), hint, p("nonexistent").getValue()).get();
        assertEquals(0, removed, "Удаление несуществующего элемента должно вернуть 0");
    }

    @Test
    @DisplayName("containsContainerKey с несуществующим элементом возвращает false")
    void testContainsContainerKeyNonExistent() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_contains_nonexist";
        List<Payload> initialData = List.of(p("item1"));
        
        KeyHintData hint = client.createSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        Boolean exists = client.containsContainerKey(setKey.getBytes(StandardCharsets.UTF_8), hint, p("nonexistent").getValue()).get();
        Assertions.assertFalse(exists);
    }

    @Test
    @DisplayName("streamSet с несуществующим set возвращает ошибку")
    void testStreamSetNonExistent() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_stream_nonexist";
        
        // Создаем KeyHint для несуществующего set
        KeyHintData hint = KeyHintData.of(1, 1);
        
        try {
            client.streamSet(setKey, hint).get();
            fail("streamSet для несуществующего set должен вызвать ошибку");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode(), "Должна быть ошибка NOT_FOUND");
        }
    }

    @Test
    @DisplayName("TTL expiration: set удаляется после истечения TTL")
    void testTtlExpirationComplete() throws ExecutionException, InterruptedException {
        String setKey = baseKey + "_ttl_expire_complete";
        List<Payload> initialData = List.of(p("item1"));
        
        KeyHintData hint = client.createSet(setKey, initialData).get();
        assertNotNull(hint);
        
        Thread.sleep(500);
        
        // Устанавливаем TTL = 1 секунда
        client.setTtl(setKey, hint, 1).get();
        
        Thread.sleep(1500);
        
        // Проверяем, что set удален - getSize должен вернуть ошибку или 0
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
        List<Payload> initialData = List.of(p("item1"));
        
        KeyHintData hint = client.createSet(setKey, initialData).get();
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
}