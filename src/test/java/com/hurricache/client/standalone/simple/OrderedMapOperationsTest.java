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
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Comprehensive tests for OrderedMap container operations.
 */
public class OrderedMapOperationsTest extends TestBase {

    private static final Duration TEST_TIMEOUT = Duration.ofSeconds(2);
    private static final int DEFAULT_CLIENT_ID = 101;
    private static final int OWNER_CLIENT_ID = 100;
    private static final int INTRUDER_CLIENT_ID = 200;

    private String baseKey;

    @BeforeEach
    void setUp() {
        baseKey = "orderedmap_test_" + UUID.randomUUID();
    }

    private byte[] bytes(String val) {
        return val.getBytes(StandardCharsets.UTF_8);
    }

    private Payload p(String val) {
        return Payload.of(val.getBytes(StandardCharsets.UTF_8));
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
    // 12. GET CONTAINER VALUE + UPDATE CONTAINER VALUE OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("getContainerValue возвращает значение по ключу")
    void testGetContainerValue() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_get_cv";
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1"),
                op(2L, "k2"), p("v2")
        );
        client.createOrderedMap(mapKey, initialData).get();

        byte[] value = client.getContainerValue(bytes(mapKey), null, bytes("k1")).get();
        assertNotNull(value);
        assertEquals("v1", new String(value, StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("getContainerValue для несуществующего ключа возвращает NOT_FOUND")
    void testGetContainerValueNotFound() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_get_cv_nf";
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1")
        );
        client.createOrderedMap(mapKey, initialData).get();

        ExecutionException ex = Assertions.assertThrows(
                ExecutionException.class,
                () -> client.getContainerValue(bytes(mapKey), null, bytes("nonexistent")).get()
        );
        Assertions.assertInstanceOf(StatusRuntimeException.class, ex.getCause());
        assertEquals(Status.Code.NOT_FOUND, ((StatusRuntimeException) ex.getCause()).getStatus().getCode());
    }

    @Test
    @DisplayName("updateContainerValue обновляет существующий ключ")
    void testUpdateContainerValue() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_update_cv";
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1"),
                op(2L, "k2"), p("v2")
        );
        client.createOrderedMap(mapKey, initialData).get();

        byte[] oldValue = client.updateContainerValue(bytes(mapKey), null, bytes("k1"), bytes("new_v1")).get();
        assertNotNull(oldValue);
        assertEquals("v1", new String(oldValue, StandardCharsets.UTF_8), "Возвращено старое значение");

        byte[] newValue = client.getContainerValue(bytes(mapKey), null, bytes("k1")).get();
        assertEquals("new_v1", new String(newValue, StandardCharsets.UTF_8), "Новое значение сохранено");
    }

    @Test
    @DisplayName("updateContainerValue для несуществующего ключа возвращает NOT_FOUND")
    void testUpdateContainerValueNotFound() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_update_cv_nf";
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1")
        );
        client.createOrderedMap(mapKey, initialData).get();

        ExecutionException ex = Assertions.assertThrows(
                ExecutionException.class,
                () -> client.updateContainerValue(bytes(mapKey), null, bytes("nonexistent"), bytes("new_value")).get()
        );
        Assertions.assertInstanceOf(StatusRuntimeException.class, ex.getCause());
        assertEquals(Status.Code.NOT_FOUND, ((StatusRuntimeException) ex.getCause()).getStatus().getCode());
    }

    // =========================================================================
    // 11. GET SIZE OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("getSize для пустого ordered map возвращает 0")
    void testGetSizeEmptyMap() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_size_empty";
        client.createOrderedMap(mapKey, Map.of()).get();

        Integer size = client.getSize(mapKey, null).get();
        assertEquals(0, size, "Пустой ordered map должен иметь размер 0");
    }

    @Test
    @DisplayName("getSize для ordered map с элементами возвращает корректное количество")
    void testGetSizeWithElements() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_size_with";
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1"),
                op(2L, "k2"), p("v2"),
                op(3L, "k3"), p("v3")
        );
        client.createOrderedMap(mapKey, initialData).get();

        Integer size = client.getSize(mapKey, null).get();
        assertEquals(3, size, "OrderedMap должен содержать 3 элемента");
    }

    // =========================================================================
    // 13. TTL OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("setTtl устанавливает TTL на ordered map")
    void testSetTtl() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_ttl_set";
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1")
        );
        client.createOrderedMap(mapKey, initialData).get();

        Boolean setResult = client.setTtl(bytes(mapKey), null, 100, DEFAULT_CLIENT_ID, TEST_TIMEOUT).get();
        assertTrue(setResult, "TTL должен быть установлен");

        Long ttl = client.getTtl(bytes(mapKey), null, DEFAULT_CLIENT_ID, TEST_TIMEOUT).get();
        assertNotNull(ttl);
        assertTrue(ttl > 0, "TTL должен быть положительным");
    }

    @Test
    @DisplayName("getTtl возвращает актуальное TTL значение")
    void testGetTtl() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_ttl_get";
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1")
        );
        client.createOrderedMap(mapKey, initialData).get();

        // Устанавливаем TTL 5000 мс
        client.setTtl(bytes(mapKey), null, 5000, DEFAULT_CLIENT_ID, TEST_TIMEOUT).get();

        Long ttl = client.getTtl(bytes(mapKey), null, DEFAULT_CLIENT_ID, TEST_TIMEOUT).get();
        assertNotNull(ttl);
        assertTrue(ttl > 0 && ttl <= 5000, "TTL должно быть в диапазоне");
    }

    @Test
    @DisplayName("TTL истечение: после истечения контейнер недоступен")
    void testTtlExpiration() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_ttl_expire";
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1")
        );
        client.createOrderedMap(mapKey, initialData).get();

        // Устанавливаем TTL 100 мс
        client.setTtl(bytes(mapKey), null, 100, DEFAULT_CLIENT_ID, TEST_TIMEOUT).get();

        // Ждём истечения
        Thread.sleep(200);

        // Контейнер должен быть недоступен
        try {
            client.streamOrderedMap(mapKey).get();
            fail("streamOrderedMap истёкшего контейнера должен вызвать ошибку");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    // =========================================================================
    // 14. LOCK OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("lockObject получает WRITE_LOCK для ordered map")
    void testWriteLockOrderedMap() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_write_lock";
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1")
        );
        client.createOrderedMap(mapKey, initialData).get();

        LockStatus lock = client.lockObject(mapKey, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock, "WRITE_LOCK должен быть получен");

        // Владелец может выполнять операции
        Map<OrderedPayload, Payload> result = client.streamOrderedMap(bytes(mapKey), null, OWNER_CLIENT_ID, TEST_TIMEOUT).get();
        assertEquals(1, result.size());

        // Разблокировка
        LockStatus unlock = client.unlockObject(bytes(mapKey), null, OWNER_CLIENT_ID).get();
        assertEquals(LockStatus.OK, unlock);
    }

    @Test
    @DisplayName("lockObject получает READ_LOCK для ordered map")
    void testReadLockOrderedMap() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_read_lock";
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1")
        );
        client.createOrderedMap(mapKey, initialData).get();

        LockStatus lock = client.lockObject(mapKey, LockType.READ_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock, "READ_LOCK должен быть получен");

        // Владелец может читать
        Map<OrderedPayload, Payload> result = client.streamOrderedMap(bytes(mapKey), null, OWNER_CLIENT_ID, TEST_TIMEOUT).get();
        assertEquals(1, result.size());

        // Разблокировка
        LockStatus unlock = client.unlockObject(bytes(mapKey), null, OWNER_CLIENT_ID).get();
        assertEquals(LockStatus.OK, unlock);
    }

    @Test
    @DisplayName("INTRUDER не может получить WRITE_LOCK когда есть WRITE_LOCK")
    void testWriteLockIntruderDenied() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_write_lock_denied";
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1")
        );
        client.createOrderedMap(mapKey, initialData).get();

        // Владелец получает WRITE_LOCK
        LockStatus lock = client.lockObject(mapKey, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        // Интриган не может получить WRITE_LOCK — получает CANT_UNLOCK
        LockStatus intruderLock = client.lockObject(mapKey, LockType.WRITE_LOCK, INTRUDER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertNotEquals(LockStatus.OK, intruderLock, "Интриган не должен получить блокировку");
    }

    @Test
    @DisplayName("INTRUDER не может разблокировать когда есть WRITE_LOCK")
    void testIntruderCannotUnlock() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_intruder_unlock";
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1")
        );
        client.createOrderedMap(mapKey, initialData).get();

        // Владелец получает WRITE_LOCK
        client.lockObject(mapKey, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();

        // Интриган не может разблокировать
        LockStatus unlock = client.unlockObject(mapKey, INTRUDER_CLIENT_ID).get();
        assertEquals(LockStatus.CANT_UNLOCK, unlock, "Интриган не может разблокировать");

        // Владелец разблокирует
        LockStatus unlockOwner = client.unlockObject(bytes(mapKey), null, OWNER_CLIENT_ID).get();
        assertEquals(LockStatus.OK, unlockOwner);
    }

    @Test
    @DisplayName("Два READ_LOCK одновременно на один контейнер")
    void testMultipleReadLocks() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_multi_read";
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1")
        );
        client.createOrderedMap(mapKey, initialData).get();

        // Первый клиент получает READ_LOCK
        LockStatus lock1 = client.lockObject(mapKey, LockType.READ_LOCK, DEFAULT_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock1);

        // Второй клиент также может получить READ_LOCK
        LockStatus lock2 = client.lockObject(mapKey, LockType.READ_LOCK, INTRUDER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock2);

        // Оба разблокируют
        client.unlockObject(mapKey, DEFAULT_CLIENT_ID).get();
        client.unlockObject(mapKey, INTRUDER_CLIENT_ID).get();
    }

    // =========================================================================
    // 15. LOCK EXPIRATION
    // =========================================================================

    @Test
    @DisplayName("После истечения WRITE_LOCK интриган может получить блокировку")
    void testWriteLockExpiration() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_write_lock_exp";
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1")
        );
        client.createOrderedMap(mapKey, initialData).get();

        // Владелец получает WRITE_LOCK на 2 секунды
        LockStatus lock = client.lockObject(mapKey, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(2)).get();
        assertEquals(LockStatus.OK, lock);

        // Ждём истечения
        Thread.sleep(3000);

        // Теперь интриган может получить WRITE_LOCK
        LockStatus intruderLock = client.lockObject(mapKey, LockType.WRITE_LOCK, INTRUDER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, intruderLock);

        // Разблокирует
        client.unlockObject(mapKey, INTRUDER_CLIENT_ID).get();
    }

    @Test
    @DisplayName("После истечения READ_LOCK интриган может получить WRITE_LOCK")
    void testReadLockExpirationToWrite() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_read_lock_exp";
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1")
        );
        client.createOrderedMap(mapKey, initialData).get();

        // Владелец получает READ_LOCK на 2 секунды
        LockStatus lock = client.lockObject(mapKey, LockType.READ_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(2)).get();
        assertEquals(LockStatus.OK, lock);

        // Ждём истечения
        Thread.sleep(3000);

        // Теперь интриган может получить WRITE_LOCK
        LockStatus intruderLock = client.lockObject(mapKey, LockType.WRITE_LOCK, INTRUDER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, intruderLock);

        // Разблокирует
        client.unlockObject(mapKey, INTRUDER_CLIENT_ID).get();
    }

    @Test
    @DisplayName("После истечения блокировки контейнер доступен")
    void testLockExpirationAccess() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_lock_exp_access";
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1")
        );
        client.createOrderedMap(mapKey, initialData).get();

        // Владелец получает WRITE_LOCK на 1 секунду
        client.lockObject(mapKey, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(1)).get();

        // Ждём истечения
        Thread.sleep(2000);

        // Интриган может читать
        Map<OrderedPayload, Payload> result = client.streamOrderedMap(bytes(mapKey), null, INTRUDER_CLIENT_ID, TEST_TIMEOUT).get();
        assertEquals(1, result.size());
    }

    // =========================================================================
    // 16. UNSUPPORTED METHODS FOR ORDERED MAP
    // =========================================================================

    @Test
    @DisplayName("Методы не применимые к ORDERED MAP должны вызывать ошибку")
    void testUnsupportedMethodsForOrderedMap() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_unsupported";
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1")
        );
        client.createOrderedMap(mapKey, initialData).get();

        // getElementAtPosition — не применим к ORDERED MAP
        try {
            client.getElementAtPosition(mapKey, null, 0).get();
            fail("getElementAtPosition должен вызвать ошибку для ORDERED MAP");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.INTERNAL, cause.getStatus().getCode());
        }

        // getAndRemoveElementAtPosition — не применим к ORDERED MAP
        try {
            client.getAndRemoveElementAtPosition(mapKey, null, 0).get();
            fail("getAndRemoveElementAtPosition должен вызвать ошибку для ORDERED MAP");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }

        // getHead — не применим к ORDERED MAP
        try {
            client.getHead(mapKey, null).get();
            fail("getHead должен вызвать ошибку для ORDERED MAP");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.INTERNAL, cause.getStatus().getCode());
        }

        // getTail — не применим к ORDERED MAP
        try {
            client.getTail(mapKey, null).get();
            fail("getTail должен вызвать ошибку для ORDERED MAP");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.INTERNAL, cause.getStatus().getCode());
        }
    }

    // =========================================================================
    // 10. REMOVE CONTAINER OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("remove удаляет существующий контейнер")
    void testRemoveExistingContainer() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_remove_cont";
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1")
        );
        client.createOrderedMap(mapKey, initialData).get();

        Boolean removed = client.remove(bytes(mapKey), null, DEFAULT_CLIENT_ID, TEST_TIMEOUT).get();
        assertTrue(removed, "Контейнер должен быть удалён");

        // Проверяем что контейнер действительно удалён
        try {
            client.streamOrderedMap(mapKey).get();
            fail("streamOrderedMap удалённого контейнера должен вызвать ошибку");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    @Test
    @DisplayName("remove несуществующего контейнера возвращает NOT_FOUND")
    void testRemoveNonExistentContainer() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_remove_nonexist";

        ExecutionException ex = Assertions.assertThrows(
                ExecutionException.class,
                () -> client.remove(bytes(mapKey), null, DEFAULT_CLIENT_ID, TEST_TIMEOUT).get()
        );
        Assertions.assertInstanceOf(StatusRuntimeException.class, ex.getCause());
        assertEquals(Status.Code.NOT_FOUND, ((StatusRuntimeException) ex.getCause()).getStatus().getCode());
    }

    // =========================================================================
    // 9. GET AND REMOVE CONTAINER VALUE OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("getAndRemoveContainerValue извлекает и удаляет элемент")
    void testGetAndRemoveContainerValue() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_get_remove";
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1"),
                op(2L, "k2"), p("v2")
        );
        client.createOrderedMap(mapKey, initialData).get();

        byte[] removedValue = client.getAndRemoveContainerValue(bytes(mapKey), null, bytes("k1")).get();
        assertNotNull(removedValue);
        assertEquals("v1", new String(removedValue, StandardCharsets.UTF_8));

        Map<OrderedPayload, Payload> result = client.streamOrderedMap(mapKey).get();
        assertEquals(1, result.size(), "Остался 1 элемент");
        assertFalse(result.entrySet().stream().anyMatch(e -> "k1".equals(new String(e.getKey().getValue(), StandardCharsets.UTF_8))));
    }

    @Test
    @DisplayName("getAndRemoveContainerValue для несуществующего ключа возвращает null")
    void testGetAndRemoveContainerValueNotFound() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_get_remove_nf";
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1")
        );
        client.createOrderedMap(mapKey, initialData).get();

        byte[] removedValue = client.getAndRemoveContainerValue(bytes(mapKey), null, bytes("nonexistent")).get();
        assertNotNull(removedValue, "Для несуществующего ключа должен вернуться пустой массив");
        assertEquals(0, removedValue.length, "Пустой массив байтов");
    }

    // =========================================================================
    // 8. STREAM ELEMENT IN RANGE ORDERED MAP OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("streamElementInRangeOrderedMap возвращает элементы в диапазоне весов")
    void testStreamElementInRangeOrderedMap() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_range";
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(10L, "k1"), p("v1"),
                op(20L, "k2"), p("v2"),
                op(30L, "k3"), p("v3"),
                op(40L, "k4"), p("v4"),
                op(50L, "k5"), p("v5")
        );
        client.createOrderedMap(mapKey, initialData).get();

        // Запрашиваем элементы с весами от 20 до 40 включительно
        Map<OrderedPayload, Payload> result = client.streamElementInRangeOrderedMap(bytes(mapKey), null, 20L, 40L, false, DEFAULT_CLIENT_ID, TEST_TIMEOUT).get();
        assertNotNull(result);
        assertEquals(3, result.size(), "Должно быть 3 элемента с весами от 20 до 40");
        assertTrue(result.entrySet().stream().anyMatch(e -> 20L == e.getKey().getOrder()));
        assertTrue(result.entrySet().stream().anyMatch(e -> 30L == e.getKey().getOrder()));
        assertTrue(result.entrySet().stream().anyMatch(e -> 40L == e.getKey().getOrder()));
    }

    @Test
    @DisplayName("streamElementInRangeOrderedMap reverse=true возвращает в обратном порядке")
    void testStreamElementInRangeOrderedMapReverse() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_range_rev";
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(10L, "k1"), p("v1"),
                op(20L, "k2"), p("v2"),
                op(30L, "k3"), p("v3"),
                op(40L, "k4"), p("v4"),
                op(50L, "k5"), p("v5")
        );
        client.createOrderedMap(mapKey, initialData).get();

        // Запрашиваем элементы с весами от 20 до 40 в обратном порядке
        Map<OrderedPayload, Payload> result = client.streamElementInRangeOrderedMap(bytes(mapKey), null, 20L, 40L, true, DEFAULT_CLIENT_ID, TEST_TIMEOUT).get();
        assertNotNull(result);
        assertEquals(3, result.size(), "Должно быть 3 элемента");
        assertTrue(result.entrySet().stream().anyMatch(e -> 40L == e.getKey().getOrder()));
        assertTrue(result.entrySet().stream().anyMatch(e -> 30L == e.getKey().getOrder()));
        assertTrue(result.entrySet().stream().anyMatch(e -> 20L == e.getKey().getOrder()));
    }

    @Test
    @DisplayName("streamElementInRangeOrderedMap вне диапазона возвращает пустой ответ")
    void testStreamElementInRangeOrderedMapNoMatch() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_range_nomatch";
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(10L, "k1"), p("v1"),
                op(20L, "k2"), p("v2"),
                op(30L, "k3"), p("v3")
        );
        client.createOrderedMap(mapKey, initialData).get();

        // Запрашиваем элементы с весами от 100 до 200 (не существует)
        Map<OrderedPayload, Payload> result = client.streamElementInRangeOrderedMap(bytes(mapKey), null, 100L, 200L, false, DEFAULT_CLIENT_ID, TEST_TIMEOUT).get();
        assertNotNull(result);
        assertEquals(0, result.size(), "Список должен быть пустым");
    }

    // =========================================================================
    // 7. REMOVE ELEMENT AT POSITION (WEIGHT RANGE)
    // =========================================================================

    @Test
    @DisplayName("removeElementAtPosition удаляет элементы в диапазоне весов")
    void testRemoveElementAtPosition() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_remove_pos";
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(10L, "k1"), p("v1"),
                op(20L, "k2"), p("v2"),
                op(30L, "k3"), p("v3"),
                op(40L, "k4"), p("v4"),
                op(50L, "k5"), p("v5")
        );
        client.createOrderedMap(mapKey, initialData).get();

        // Удаление элементов с весами от 20 до 29 (только k2 с весом 20)
        Boolean removed = client.removeElementAtPosition(bytes(mapKey), null, 20, 29).get();
        assertTrue(removed, "Элементы должны быть удалены");

        Map<OrderedPayload, Payload> result = client.streamOrderedMap(mapKey).get();
        assertEquals(4, result.size(), "Осталось 4 элемента");
        assertTrue(result.entrySet().stream().anyMatch(e -> 10L == e.getKey().getOrder()));
        assertTrue(result.entrySet().stream().anyMatch(e -> 30L == e.getKey().getOrder()));
        assertTrue(result.entrySet().stream().anyMatch(e -> 40L == e.getKey().getOrder()));
        assertTrue(result.entrySet().stream().anyMatch(e -> 50L == e.getKey().getOrder()));
    }

    @Test
    @DisplayName("removeElementAtPosition включает endPos в диапазон (хвост включён)")
    void testRemoveElementAtPositionEndIncluded() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_remove_pos_end";
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(10L, "k1"), p("v1"),
                op(20L, "k2"), p("v2"),
                op(30L, "k3"), p("v3"),
                op(40L, "k4"), p("v4")
        );
        client.createOrderedMap(mapKey, initialData).get();

        // Удаление элементов с весами от 20 до 29 (только k2 с весом 20)
        Boolean removed = client.removeElementAtPosition(bytes(mapKey), null, 20, 30).get();
        assertTrue(removed, "Элементы должны быть удалены");

        Map<OrderedPayload, Payload> result = client.streamOrderedMap(mapKey).get();
        assertEquals(2, result.size(), "Осталось 3 элемента");
        assertTrue(result.entrySet().stream().anyMatch(e -> 10L == e.getKey().getOrder()));
        assertTrue(result.entrySet().stream().anyMatch(e -> 40L == e.getKey().getOrder()));
    }

    @Test
    @DisplayName("removeElementAtPosition работает когда границы совпадают")
    void testRemoveElementAtPositionSamePos() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_remove_pos_same";
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(10L, "k1"), p("v1"),
                op(20L, "k2"), p("v2"),
                op(30L, "k3"), p("v3")
        );
        client.createOrderedMap(mapKey, initialData).get();

        // Удаление элемента с весом 20 (minWeight = maxWeight = 20)
        Boolean removed = client.removeElementAtPosition(bytes(mapKey), null, 20, 20).get();
        assertTrue(removed, "Элемент должен быть удалён");

        Map<OrderedPayload, Payload> result = client.streamOrderedMap(mapKey).get();
        assertEquals(2, result.size(), "Осталось 2 элемента");
        assertTrue(result.entrySet().stream().anyMatch(e -> 10L == e.getKey().getOrder()));
        assertTrue(result.entrySet().stream().anyMatch(e -> 30L == e.getKey().getOrder()));
    }

    // =========================================================================
    // 6. CONTAINS CONTAINER KEY OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("containsContainerKey проверяет существующий ключ")
    void testContainsContainerKeyExists() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_contains_exist";
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1"),
                op(2L, "k2"), p("v2")
        );
        client.createOrderedMap(mapKey, initialData).get();

        Boolean exists = client.containsContainerKey(bytes(mapKey), null, bytes("k1")).get();
        assertTrue(exists, "Ключ k1 должен существовать");
    }

    @Test
    @DisplayName("containsContainerKey проверяет несуществующий ключ")
    void testContainsContainerKeyNotExists() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_contains_notexist";
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1")
        );
        client.createOrderedMap(mapKey, initialData).get();

        Boolean notExists = client.containsContainerKey(bytes(mapKey), null, bytes("nonexistent")).get();
        assertFalse(notExists, "Ключ nonexistent не должен существовать");
    }

    // =========================================================================
    // 5. REMOVE FROM CONTAINER WITH CONTAINERTYPE OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("removeFromContainer с ContainerType.ORDERED_MAP удаляет по ключу и значению")
    void testRemoveFromContainerWithType() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_remove_type";
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1"),
                op(2L, "k2"), p("v2")
        );
        client.createOrderedMap(mapKey, initialData).get();

        List<Payload> keys = List.of(p("k1"));
        List<Payload> values = List.of(p("v1"));

        Integer removed = client.removeFromContainer(bytes(mapKey), null, ContainerType.ORDERED_MAP, keys, values).get();
        assertEquals(1, removed, "Должен быть удалён 1 элемент");

        Map<OrderedPayload, Payload> result = client.streamOrderedMap(mapKey).get();
        assertEquals(1, result.size());
        assertFalse(result.entrySet().stream().anyMatch(e -> "k1".equals(new String(e.getKey().getValue(), StandardCharsets.UTF_8))));
    }

    @Test
    @DisplayName("removeFromContainer с ContainerType.ORDERED_MAP удаляет по ключу игнорируя значение")
    void testRemoveFromContainerWithTypeIgnoreValue() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_remove_type_ignore";
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1")
        );
        client.createOrderedMap(mapKey, initialData).get();

        // Даже с неверным значением, удаление по ключу работает
        List<Payload> keys = List.of(p("k1"));
        List<Payload> values = List.of(p("wrong_value"));

        Integer removed = client.removeFromContainer(bytes(mapKey), null, ContainerType.ORDERED_MAP, keys, values).get();
        assertEquals(1, removed, "Элемент удалён по ключу (значение игнорируется)");

        Map<OrderedPayload, Payload> result = client.streamOrderedMap(mapKey).get();
        assertEquals(0, result.size(), "OrderedMap пуст после удаления");
    }

    // =========================================================================
    // 4. REMOVE FROM CONTAINER BY KEY OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("removeFromContainer удаляет элемент по ключу и возвращает количество")
    void testRemoveFromContainerByKey() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_remove_key";
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1"),
                op(2L, "k2"), p("v2"),
                op(3L, "k3"), p("v3")
        );
        client.createOrderedMap(mapKey, initialData).get();

        Integer removed = client.removeFromContainer(bytes(mapKey), null, bytes("k2")).get();
        assertEquals(1, removed, "Должен быть удалён 1 элемент");

        Map<OrderedPayload, Payload> result = client.streamOrderedMap(mapKey).get();
        assertEquals(2, result.size(), "Осталось 2 элемента");
        assertFalse(result.entrySet().stream().anyMatch(e -> "k2".equals(new String(e.getKey().getValue(), StandardCharsets.UTF_8))));
    }

    @Test
    @DisplayName("removeFromContainer возвращает 0 для несуществующего ключа")
    void testRemoveFromContainerByKeyNotFound() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_remove_key_nf";
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1")
        );
        client.createOrderedMap(mapKey, initialData).get();

        Integer removed = client.removeFromContainer(bytes(mapKey), null, bytes("nonexistent")).get();
        assertEquals(0, removed, "Ничего не удалено (элемент не найден)");
    }

    @Test
    @DisplayName("removeFromContainer из пустого контейнера возвращает 0")
    void testRemoveFromContainerFromEmptyMap() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_remove_empty";
        client.createOrderedMap(mapKey, Map.of()).get();

        Integer removed = client.removeFromContainer(bytes(mapKey), null, bytes("k1")).get();
        assertEquals(0, removed, "Ничего не удалено из пустого контейнера");
    }

    // =========================================================================
    // 3. ADD ELEMENT ORDERED MAP OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("addElementOrderedMap добавляет элементы в OrderedMap и возвращает количество")
    void testAddElementOrderedMap() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_add";
        client.createOrderedMap(mapKey, Map.of()).get();

        List<OrderedPayload> keys = List.of(op(1L, "k1"), op(2L, "k2"), op(3L, "k3"));
        List<Payload> values = List.of(p("v1"), p("v2"), p("v3"));

        Integer added = client.addElementOrderedMap(bytes(mapKey), null, keys, values, DEFAULT_CLIENT_ID, TEST_TIMEOUT).get();
        assertEquals(3, added, "Должно быть добавлено 3 элемента");

        Map<OrderedPayload, Payload> result = client.streamOrderedMap(mapKey).get();
        assertEquals(3, result.size());
        assertTrue(result.entrySet().stream().anyMatch(e -> Long.valueOf(1L).equals(e.getKey().getOrder()) && "v1".equals(str(e.getValue()))));
        assertTrue(result.entrySet().stream().anyMatch(e -> Long.valueOf(2L).equals(e.getKey().getOrder()) && "v2".equals(str(e.getValue()))));
        assertTrue(result.entrySet().stream().anyMatch(e -> Long.valueOf(3L).equals(e.getKey().getOrder()) && "v3".equals(str(e.getValue()))));
    }

    @Test
    @DisplayName("addElementOrderedMap допускает дубликаты (одинаковый key с разными весами)")
    void testAddElementOrderedMapDuplicates() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_add_dup";
        client.createOrderedMap(mapKey, Map.of()).get();

        // Добавляем элементы с одинаковым key но разными весами
        List<OrderedPayload> keys = List.of(op(1L, "same_key"), op(2L, "same_key"), op(3L, "same_key"));
        List<Payload> values = List.of(p("v1"), p("v2"), p("v3"));

        Integer added = client.addElementOrderedMap(bytes(mapKey), null, keys, values, DEFAULT_CLIENT_ID, TEST_TIMEOUT).get();
        assertEquals(3, added, "Должно быть добавлено 3 элемента (дубликаты разрешены)");

        Map<OrderedPayload, Payload> result = client.streamOrderedMap(mapKey).get();
        assertEquals(3, result.size(), "Всего 3 элемента с одинаковым key");
    }

    @Test
    @DisplayName("addElementOrderedMap с пустым списком возвращает 0")
    void testAddElementOrderedMapEmptyList() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_add_empty";
        client.createOrderedMap(mapKey, Map.of()).get();

        Integer added = client.addElementOrderedMap(bytes(mapKey), null, List.of(), List.of(), DEFAULT_CLIENT_ID, TEST_TIMEOUT).get();
        assertEquals(0, added, "Добавление пустого списка должно вернуть 0");

        Map<OrderedPayload, Payload> result = client.streamOrderedMap(mapKey).get();
        assertEquals(0, result.size());
    }

    // =========================================================================
    // 2. STREAM ORDERED MAP OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("streamOrderedMap пустого OrderedMap возвращает пустой ответ")
    void testStreamOrderedMapEmpty() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_stream_empty";

        KeyHintData hint = client.createOrderedMap(mapKey, Map.of()).get();
        assertNotNull(hint);

        Map<OrderedPayload, Payload> result = client.streamOrderedMap(mapKey, hint).get();
        assertNotNull(result);
        assertEquals(0, result.size(), "streamOrderedMap пустого ordered map должен вернуть пустую карту");
    }

    @Test
    @DisplayName("streamOrderedMap возвращает все содержимое контейнера")
    void testStreamOrderedMapWithData() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_stream_data";
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "elem1"), p("val1"),
                op(2L, "elem2"), p("val2"),
                op(3L, "elem3"), p("val3"),
                op(4L, "elem4"), p("val4"),
                op(5L, "elem5"), p("val5")
        );

        KeyHintData hint = client.createOrderedMap(mapKey, initialData).get();
        assertNotNull(hint);

        Map<OrderedPayload, Payload> result = client.streamOrderedMap(mapKey, hint).get();
        assertNotNull(result);
        assertEquals(5, result.size(), "streamOrderedMap должен вернуть все 5 элементов");
    }

    @Test
    @DisplayName("streamOrderedMap с явным clientId")
    void testStreamOrderedMapWithClientId() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_stream_cid";
        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1")
        );

        KeyHintData hint = client.createOrderedMap(mapKey, initialData).get();
        assertNotNull(hint);

        Map<OrderedPayload, Payload> result = client.streamOrderedMap(bytes(mapKey), hint, DEFAULT_CLIENT_ID, TEST_TIMEOUT).get();
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.entrySet().stream().anyMatch(e -> Long.valueOf(1L).equals(e.getKey().getOrder()) && "v1".equals(str(e.getValue()))));
    }

    // =========================================================================
    // 1. CREATE ORDERED MAP OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("Создание пустого OrderedMap")
    void testCreateEmptyOrderedMap() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_empty";

        KeyHintData hint = client.createOrderedMap(mapKey, Map.of()).get();
        assertNotNull(hint, "KeyHint должен быть создан");

        Map<OrderedPayload, Payload> result = client.streamOrderedMap(mapKey, hint).get();
        assertNotNull(result);
        assertEquals(0, result.size(), "Пустой ordered map должен вернуть пустую карту");
    }

    @Test
    @DisplayName("Создание OrderedMap с начальными данными")
    void testCreateOrderedMapWithInitialData() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_initial";

        Map<OrderedPayload, Payload> initialData = Map.of(
                op(1L, "k1"), p("v1"),
                op(2L, "k2"), p("v2"),
                op(3L, "k3"), p("v3")
        );

        KeyHintData hint = client.createOrderedMap(mapKey, initialData).get();
        assertNotNull(hint, "KeyHint должен быть создан");

        Map<OrderedPayload, Payload> result = client.streamOrderedMap(mapKey, hint).get();
        assertNotNull(result);
        assertEquals(3, result.size(), "OrderedMap должен содержать 3 элемента");
        assertTrue(result.entrySet().stream().anyMatch(e -> Long.valueOf(1L).equals(e.getKey().getOrder()) && "v1".equals(str(e.getValue()))));
        assertTrue(result.entrySet().stream().anyMatch(e -> Long.valueOf(2L).equals(e.getKey().getOrder()) && "v2".equals(str(e.getValue()))));
        assertTrue(result.entrySet().stream().anyMatch(e -> Long.valueOf(3L).equals(e.getKey().getOrder()) && "v3".equals(str(e.getValue()))));
    }

    @Test
    @DisplayName("Создание большого OrderedMap с автоматическим разбиением на чанки (чанкинг)")
    void testCreateOrderedMapWithLargeDataChunking() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_large";
        int elementCount = 1500;
        Map<OrderedPayload, Payload> largeData = new java.util.LinkedHashMap<>();

        for (int i = 0; i < elementCount; i++) {
            largeData.put(op((long) i, "key_" + i), p("value_" + i + "_" + UUID.randomUUID()));
        }

        KeyHintData hint = client.createOrderedMap(mapKey, largeData).get();
        assertNotNull(hint, "KeyHint должен быть создан");

        Map<OrderedPayload, Payload> result = client.streamOrderedMap(mapKey, hint).get();
        assertNotNull(result);
        assertEquals(elementCount, result.size(), "OrderedMap должен содержать " + elementCount + " элементов");
    }
}