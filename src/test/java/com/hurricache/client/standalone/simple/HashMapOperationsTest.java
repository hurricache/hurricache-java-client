package com.hurricache.client.standalone.simple;

import com.hurricache.TestBase;
import com.hurricache.client.intf.KeyHintData;
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
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Comprehensive tests for HashedMap (MAP container) operations.
 */
public class HashMapOperationsTest extends TestBase {

    private static final Duration TEST_TIMEOUT = Duration.ofSeconds(2);
    private static final int DEFAULT_CLIENT_ID = 101;
    private static final int OWNER_CLIENT_ID = 100;
    private static final int INTRUDER_CLIENT_ID = 200;

    private String baseKey;

    @BeforeEach
    void setUp() {
        baseKey = "hashmap_test_" + UUID.randomUUID();
    }


    private Payload p(String val) {
        return Payload.of(val.getBytes(StandardCharsets.UTF_8));
    }

    private String str(Payload payload) {
        return new String(payload.getValue(), StandardCharsets.UTF_8);
    }

    // =========================================================================
    // 14. UNSUPPORTED METHODS FOR MAP
    // =========================================================================

    @Test
    @DisplayName("Методы не применимые к MAP должны вызывать ошибку")
    void testUnsupportedMethodsForMap() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_unsupported";
        Map<Payload, Payload> initialData = Map.of(
                p("k1"), p("v1")
        );
        client.createMap(mapKey, initialData).get();

        // getElementAtPosition — не применим к MAP
        try {
            client.getElementAtPosition(mapKey, null, 0).get();
            fail("getElementAtPosition должен вызвать ошибку для MAP");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.INTERNAL, cause.getStatus().getCode());
        }

        // getAndRemoveElementAtPosition — не применим к MAP
        try {
            client.getAndRemoveElementAtPosition(mapKey, null, 0).get();
            fail("getAndRemoveElementAtPosition должен вызвать ошибку для MAP");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.INTERNAL, cause.getStatus().getCode());
        }

        // getHead — не применим к MAP
        try {
            client.getHead(mapKey, null).get();
            fail("getHead должен вызвать ошибку для MAP");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.INTERNAL, cause.getStatus().getCode());
        }

        // getTail — не применим к MAP
        try {
            client.getTail(mapKey, null).get();
            fail("getTail должен вызвать ошибку для MAP");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.INTERNAL, cause.getStatus().getCode());
        }

        // addElementToPosition — не применим к MAP (пропускаем, т.к. метод может поддерживаться)
        // try {
        //     client.addElementToPosition(mapKey, null, List.of(p("v1")), 0).get();
        //     fail("addElementToPosition должен вызвать ошибку для MAP");
        // } catch (ExecutionException e) {
        //     StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
        //     assertEquals(Status.Code.INTERNAL, cause.getStatus().getCode());
        // }
    }

    // =========================================================================
    // 13.1. LOCK EXPIRATION OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("READ_LOCK на 2 сек: после истечения другой клиент может читать")
    void testReadLockExpiration() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_read_lock_exp";
        Map<Payload, Payload> initialData = Map.of(
                p("k1"), p("v1")
        );
        client.createMap(mapKey, initialData).get();

        // Берём READ_LOCK на 2 секунды
        LockStatus lock = client.lockObject(mapKey, LockType.READ_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(2)).get();
        assertEquals(LockStatus.OK, lock);

        // Ждём истечения
        Thread.sleep(3000);

        // Теперь другой клиент может читать
        byte[] val = client.getContainerValue(bytes(mapKey), null, p("k1").getValue(), INTRUDER_CLIENT_ID, TEST_TIMEOUT).get();
        assertNotNull(val);
        assertEquals("v1", new String(val, StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("WRITE_LOCK на 2 сек: после истечения другой клиент может получить WRITE_LOCK")
    void testWriteLockExpiration() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_write_lock_exp";
        Map<Payload, Payload> initialData = Map.of(
                p("k1"), p("v1")
        );
        client.createMap(mapKey, initialData).get();

        // Берём WRITE_LOCK на 2 секунды
        LockStatus lock = client.lockObject(mapKey, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(2)).get();
        assertEquals(LockStatus.OK, lock);

        // Ждём истечения
        Thread.sleep(3000);

        // Теперь интриган может получить WRITE_LOCK
        LockStatus newLock = client.lockObject(mapKey, LockType.WRITE_LOCK, INTRUDER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, newLock);

        client.unlockObject(mapKey, INTRUDER_CLIENT_ID).get();
    }

    @Test
    @DisplayName("GLOBAL на 2 сек: после истечения другой клиент может получить WRITE_LOCK")
    void testGlobalLockExpiration() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_global_lock_exp";
        Map<Payload, Payload> initialData = Map.of(
                p("k1"), p("v1")
        );
        client.createMap(mapKey, initialData).get();

        // Берём GLOBAL LOCK на 2 секунды
        LockStatus lock = client.lockObject(mapKey, LockType.GLOBAL, OWNER_CLIENT_ID, Duration.ofSeconds(2)).get();
        assertEquals(LockStatus.OK, lock);

        // Ждём истечения
        Thread.sleep(3000);

        // Теперь интриган может получить WRITE_LOCK
        LockStatus newLock = client.lockObject(mapKey, LockType.WRITE_LOCK, INTRUDER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, newLock);

        client.unlockObject(mapKey, INTRUDER_CLIENT_ID).get();
    }

    @Test
    @DisplayName("Любой lock на 2 сек: после истечения новый lock OK для другого клиента")
    void testLockExpirationAllowsNewLock() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_lock_exp_any";
        Map<Payload, Payload> initialData = Map.of(
                p("k1"), p("v1")
        );
        client.createMap(mapKey, initialData).get();

        // Берём READ_LOCK на 2 секунды
        LockStatus lock = client.lockObject(mapKey, LockType.READ_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(2)).get();
        assertEquals(LockStatus.OK, lock);

        // Ждём истечения
        Thread.sleep(3000);

        // Теперь интриган может получить WRITE_LOCK
        LockStatus newLock = client.lockObject(mapKey, LockType.WRITE_LOCK, INTRUDER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, newLock);

        client.unlockObject(mapKey, INTRUDER_CLIENT_ID).get();
    }

    // =========================================================================
    // 13. LOCKING OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("READ_LOCK: несколько клиентов могут читать параллельно")
    void testReadLockParallelReads() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_read_lock";
        Map<Payload, Payload> initialData = Map.of(
                p("k1"), p("v1")
        );
        client.createMap(mapKey, initialData).get();

        LockStatus lock1 = client.lockObject(mapKey, LockType.READ_LOCK, DEFAULT_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock1, "Первый клиент должен получить READ_LOCK");

        LockStatus lock2 = client.lockObject(mapKey, LockType.READ_LOCK, INTRUDER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock2, "Второй клиент должен получить READ_LOCK");

        // Оба могут читать
        byte[] val1 = client.getContainerValue(bytes(mapKey), null, p("k1").getValue(), DEFAULT_CLIENT_ID, TEST_TIMEOUT).get();
        assertNotNull(val1);

        byte[] val2 = client.getContainerValue(bytes(mapKey), null, p("k1").getValue(), INTRUDER_CLIENT_ID, TEST_TIMEOUT).get();
        assertNotNull(val2);

        // Освобождаем блокировки
        client.unlockObject(mapKey, DEFAULT_CLIENT_ID).get();
        client.unlockObject(mapKey, INTRUDER_CLIENT_ID).get();
    }

    @Test
    @DisplayName("WRITE_LOCK: только владелец читает и пишет, другие заблокированы")
    void testWriteLockExclusiveAccess() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_write_lock";
        Map<Payload, Payload> initialData = Map.of(
                p("k1"), p("v1")
        );
        client.createMap(mapKey, initialData).get();

        LockStatus lock = client.lockObject(mapKey, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock, "Владелец должен получить WRITE_LOCK");

        // Владелец может читать
        byte[] val = client.getContainerValue(bytes(mapKey), null, p("k1").getValue(), OWNER_CLIENT_ID, TEST_TIMEOUT).get();
        assertNotNull(val);

        // Владелец может добавлять элементы
        List<Payload> keys = List.of(p("k2"));
        List<Payload> values = List.of(p("v2"));
        Integer added = client.addElementHashMap(bytes(mapKey), null, keys, values, OWNER_CLIENT_ID, TEST_TIMEOUT).get();
        assertEquals(1, added);

        // Интриган не может читать
        assertDenied(client.getContainerValue(bytes(mapKey), null, p("k1").getValue(), INTRUDER_CLIENT_ID, TEST_TIMEOUT));

        // Интриган не может добавлять
        List<Payload> intruderKeys = List.of(p("k3"));
        List<Payload> intruderValues = List.of(p("v3"));
        assertDenied(client.addElementHashMap(bytes(mapKey), null, intruderKeys, intruderValues, INTRUDER_CLIENT_ID, TEST_TIMEOUT));

        // Освобождаем блокировку
        client.unlockObject(mapKey, OWNER_CLIENT_ID).get();
    }

    @Test
    @DisplayName("GLOBAL: только владелец делает любые операции, все остальные заблокированы")
    void testGlobalLockExclusiveAccess() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_global_lock";
        Map<Payload, Payload> initialData = Map.of(
                p("k1"), p("v1")
        );
        client.createMap(mapKey, initialData).get();

        LockStatus lock = client.lockObject(mapKey, LockType.GLOBAL, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock, "Владелец должен получить GLOBAL LOCK");

        // Владелец может читать
        byte[] val = client.getContainerValue(bytes(mapKey), null, p("k1").getValue(), OWNER_CLIENT_ID, TEST_TIMEOUT).get();
        assertNotNull(val);

        // Интриган не может читать
        assertDenied(client.getContainerValue(bytes(mapKey), null, p("k1").getValue(), INTRUDER_CLIENT_ID, TEST_TIMEOUT));

        // Интриган не может добавлять
        List<Payload> intruderKeys = List.of(p("k2"));
        List<Payload> intruderValues = List.of(p("v2"));
        assertDenied(client.addElementHashMap(bytes(mapKey), null, intruderKeys, intruderValues, INTRUDER_CLIENT_ID, TEST_TIMEOUT));

        // Интриган не может разблокировать
        LockStatus unlockStatus = client.unlockObject(mapKey, INTRUDER_CLIENT_ID).get();
        assertEquals(LockStatus.CANT_UNLOCK, unlockStatus);

        // Освобождаем блокировку владельцем
        client.unlockObject(mapKey, OWNER_CLIENT_ID).get();
    }

    @Test
    @DisplayName("unlockObject: только владелец может разблокировать")
    void testUnlockByOwnerOnly() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_unlock_owner";
        Map<Payload, Payload> initialData = Map.of(
                p("k1"), p("v1")
        );
        client.createMap(mapKey, initialData).get();

        LockStatus lock = client.lockObject(mapKey, LockType.WRITE_LOCK, OWNER_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        // Интриган пытается разблокировать
        LockStatus status = client.unlockObject(mapKey, INTRUDER_CLIENT_ID).get();
        assertEquals(LockStatus.CANT_UNLOCK, status);

        // Владелец разблокирует
        LockStatus validUnlock = client.unlockObject(mapKey, OWNER_CLIENT_ID).get();
        assertEquals(LockStatus.OK, validUnlock);
    }

    @Test
    @DisplayName("READ_LOCK: чтение OK, но запись запрещена")
    void testReadLockBlocksWrites() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_read_lock_writes";
        Map<Payload, Payload> initialData = Map.of(
                p("k1"), p("v1")
        );
        client.createMap(mapKey, initialData).get();

        LockStatus lock = client.lockObject(mapKey, LockType.READ_LOCK, DEFAULT_CLIENT_ID, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, lock);

        // Чтение под READ_LOCK OK
        byte[] val = client.getContainerValue(bytes(mapKey), null, p("k1").getValue(), DEFAULT_CLIENT_ID, TEST_TIMEOUT).get();
        assertNotNull(val);

        // Запись запрещена
        List<Payload> keys = List.of(p("k2"));
        List<Payload> values = List.of(p("v2"));
        assertDenied(client.addElementHashMap(bytes(mapKey), null, keys, values, DEFAULT_CLIENT_ID, TEST_TIMEOUT));

        client.unlockObject(mapKey, DEFAULT_CLIENT_ID).get();
    }

    // =========================================================================
    // 12. TTL OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("setTtl устанавливает TTL на контейнер")
    void testSetTtl() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_ttl_set";
        Map<Payload, Payload> initialData = Map.of(
                p("k1"), p("v1")
        );
        client.createMap(mapKey, initialData).get();

        Boolean setResult = client.setTtl(bytes(mapKey), null, 100, DEFAULT_CLIENT_ID, TEST_TIMEOUT).get();
        assertTrue(setResult, "TTL должен быть успешно установлен");
    }

    @Test
    @DisplayName("getTtl получает TTL контейнера")
    void testGetTtl() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_ttl_get";
        Map<Payload, Payload> initialData = Map.of(
                p("k1"), p("v1")
        );
        client.createMap(mapKey, initialData).get();

        client.setTtl(bytes(mapKey), null, 100, DEFAULT_CLIENT_ID, TEST_TIMEOUT).get();

        Long ttl = client.getTtl(bytes(mapKey), null, DEFAULT_CLIENT_ID, TEST_TIMEOUT).get();
        assertNotNull(ttl);
        assertTrue(ttl > 0, "TTL должен быть больше 0");
    }

    @Test
    @DisplayName("После истечения TTL getContainerValue возвращает NOT_FOUND")
    void testTtlExpiration() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_ttl_expire";
        Map<Payload, Payload> initialData = Map.of(
                p("k1"), p("v1")
        );
        client.createMap(mapKey, initialData).get();

        // Устанавливаем TTL = 1 секунда
        client.setTtl(bytes(mapKey), null, 1, DEFAULT_CLIENT_ID, TEST_TIMEOUT).get();

        // Ждём истечения TTL
        Thread.sleep(1500);

        // Проверяем что контейнер удалён — getContainerValue вернёт NOT_FOUND
        assertNotFound(client.getContainerValue(bytes(mapKey), null, p("k1").getValue()));
    }

    // =========================================================================
    // 11. GET AND REMOVE CONTAINER VALUE OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("getAndRemoveContainerValue извлекает и удаляет элемент")
    void testGetAndRemoveContainerValue() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_get_remove";
        Map<Payload, Payload> initialData = Map.of(
                p("k1"), p("v1"),
                p("k2"), p("v2")
        );
        client.createMap(mapKey, initialData).get();

        byte[] removedValue = client.getAndRemoveContainerValue(bytes(mapKey), null, p("k1").getValue()).get();
        assertNotNull(removedValue);
        assertEquals("v1", new String(removedValue, StandardCharsets.UTF_8));

        Map<Payload, Payload> result = client.streamMap(mapKey).get();
        assertEquals(1, result.size(), "Остался 1 элемент");
        assertFalse(result.containsKey(p("k1")));
    }

    @Test
    @DisplayName("getAndRemoveContainerValue для несуществующего ключа возвращает NOT_FOUND")
    void testGetAndRemoveContainerValueNotFound() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_get_remove_nf";
        Map<Payload, Payload> initialData = Map.of(
                p("k1"), p("v1")
        );
        client.createMap(mapKey, initialData).get();

        ExecutionException ex = Assertions.assertThrows(
                ExecutionException.class,
                () -> client.getAndRemoveContainerValue(bytes(mapKey), null, p("nonexistent").getValue()).get()
        );
        Assertions.assertInstanceOf(StatusRuntimeException.class, ex.getCause());
        assertEquals(Status.Code.NOT_FOUND, ((StatusRuntimeException) ex.getCause()).getStatus().getCode());
    }

    // =========================================================================
    // 10. GET AND UPDATE CONTAINER VALUE OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("getContainerValue получает значение по ключу")
    void testGetContainerValue() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_get_val";
        Map<Payload, Payload> initialData = Map.of(
                p("k1"), p("v1"),
                p("k2"), p("v2")
        );
        client.createMap(mapKey, initialData).get();

        byte[] value = client.getContainerValue(bytes(mapKey), null, p("k1").getValue()).get();
        assertNotNull(value);
        assertEquals("v1", new String(value, StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("getContainerValue для несуществующего ключа возвращает NOT_FOUND")
    void testGetContainerValueNotFound() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_get_val_nf";
        Map<Payload, Payload> initialData = Map.of(
                p("k1"), p("v1")
        );
        client.createMap(mapKey, initialData).get();

        ExecutionException ex = Assertions.assertThrows(
                ExecutionException.class,
                () -> client.getContainerValue(bytes(mapKey), null, p("nonexistent").getValue()).get()
        );
        Assertions.assertInstanceOf(StatusRuntimeException.class, ex.getCause());
        assertEquals(Status.Code.NOT_FOUND, ((StatusRuntimeException) ex.getCause()).getStatus().getCode());
    }

    @Test
    @DisplayName("updateContainerValue обновляет значение и возвращает старое")
    void testUpdateContainerValue() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_update";
        Map<Payload, Payload> initialData = Map.of(
                p("k1"), p("old_value")
        );
        client.createMap(mapKey, initialData).get();

        byte[] elemKey = bytes("k1");
        byte[] oldValue = client.updateContainerValue(bytes(mapKey), null, elemKey, p("new_value").getValue()).get();
        assertNotNull(oldValue, "oldValue не должен быть null");
        assertEquals("old_value", new String(oldValue, StandardCharsets.UTF_8));

        byte[] newValue = client.getContainerValue(bytes(mapKey), null, elemKey).get();
        assertEquals("new_value", new String(newValue, StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("updateContainerValue для несуществующего ключа возвращает NOT_FOUND")
    void testUpdateContainerValueNotFound() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_update_nf";
        Map<Payload, Payload> initialData = Map.of(
                p("k1"), p("v1")
        );
        client.createMap(mapKey, initialData).get();

        ExecutionException ex = Assertions.assertThrows(
                ExecutionException.class,
                () -> client.updateContainerValue(bytes(mapKey), null, p("nonexistent").getValue(), p("new").getValue()).get()
        );
        Assertions.assertInstanceOf(StatusRuntimeException.class, ex.getCause());
        assertEquals(Status.Code.NOT_FOUND, ((StatusRuntimeException) ex.getCause()).getStatus().getCode());
    }

    // =========================================================================
    // 9. GET SIZE OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("getSize для пустого map возвращает 0")
    void testGetSizeEmptyMap() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_size_empty";
        client.createMap(mapKey, Map.of()).get();

        Integer size = client.getSize(mapKey, null).get();
        assertEquals(0, size, "Пустой map должен иметь размер 0");
    }

    @Test
    @DisplayName("getSize для map с элементами возвращает корректное количество")
    void testGetSizeWithElements() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_size_with";
        Map<Payload, Payload> initialData = Map.of(
                p("k1"), p("v1"),
                p("k2"), p("v2"),
                p("k3"), p("v3")
        );
        client.createMap(mapKey, initialData).get();

        Integer size = client.getSize(mapKey, null).get();
        assertEquals(3, size, "Map должен содержать 3 элемента");
    }

    // =========================================================================
    // 8. REMOVE CONTAINER OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("remove удаляет существующий контейнер")
    void testRemoveExistingContainer() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_remove_cont";
        Map<Payload, Payload> initialData = Map.of(
                p("k1"), p("v1")
        );
        client.createMap(mapKey, initialData).get();

        Boolean removed = client.remove(bytes(mapKey), null, DEFAULT_CLIENT_ID, TEST_TIMEOUT).get();
        assertTrue(removed, "Контейнер должен быть удалён");

        // Проверяем что контейнер действительно удалён
        try {
            client.streamMap(mapKey).get();
            fail("streamMap удалённого контейнера должен вызвать ошибку");
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
    // 7. REMOVE ELEMENT AT POSITION (NOT SUPPORTED FOR MAP)
    // =========================================================================

    @Test
    @DisplayName("removeElementAtPosition не поддерживается для MAP — возвращает ошибку")
    void testRemoveElementAtPositionNotSupported() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_remove_pos";
        Map<Payload, Payload> initialData = Map.of(
                p("k1"), p("v1")
        );
        client.createMap(mapKey, initialData).get();

        try {
            client.removeElementAtPosition(bytes(mapKey), null, 0, 0).get();
            fail("removeElementAtPosition должен вызвать ошибку для MAP");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.INTERNAL, cause.getStatus().getCode(), "Должна быть ошибка INTERNAL");
        }
    }

    // =========================================================================
    // 6. CONTAINS CONTAINER KEY OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("containsContainerKey проверяет существующий ключ")
    void testContainsContainerKeyExists() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_contains_exist";
        Map<Payload, Payload> initialData = Map.of(
                p("k1"), p("v1"),
                p("k2"), p("v2")
        );
        client.createMap(mapKey, initialData).get();

        Boolean exists = client.containsContainerKey(bytes(mapKey), null, p("k1").getValue()).get();
        assertTrue(exists, "Ключ k1 должен существовать");
    }

    @Test
    @DisplayName("containsContainerKey проверяет несуществующий ключ")
    void testContainsContainerKeyNotExists() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_contains_notexist";
        Map<Payload, Payload> initialData = Map.of(
                p("k1"), p("v1")
        );
        client.createMap(mapKey, initialData).get();

        Boolean notExists = client.containsContainerKey(bytes(mapKey), null, p("nonexistent").getValue()).get();
        assertFalse(notExists, "Ключ nonexistent не должен существовать");
    }

    // =========================================================================
    // 5. REMOVE FROM CONTAINER WITH CONTAINERTYPE OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("removeFromContainer с ContainerType.MAP удаляет по ключу и значению")
    void testRemoveFromContainerWithType() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_remove_type";
        Map<Payload, Payload> initialData = Map.of(
                p("k1"), p("v1"),
                p("k2"), p("v2")
        );
        client.createMap(mapKey, initialData).get();

        List<Payload> keys = List.of(Payload.of(p("k1").getValue()));
        List<Payload> values = List.of(Payload.of(p("v1").getValue()));

        Integer removed = client.removeFromContainer(bytes(mapKey), null, ContainerType.MAP, keys, values).get();
        assertEquals(1, removed, "Должен быть удалён 1 элемент");

        Map<Payload, Payload> result = client.streamMap(mapKey).get();
        assertEquals(1, result.size());
        assertFalse(result.containsKey(p("k1")));
    }

    @Test
    @DisplayName("removeFromContainer с ContainerType.MAP удаляет по ключу даже если значение не совпадает")
    void testRemoveFromContainerWithTypeByWrongValue() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_remove_type";
        Map<Payload, Payload> initialData = Map.of(
                p("k1"), p("v1"),
                p("k2"), p("v2")
        );
        client.createMap(mapKey, initialData).get();

        List<Payload> keys = List.of(Payload.of(p("k1").getValue()));
        List<Payload> values = List.of(Payload.of(p("wrong_value").getValue()));

        Integer removed = client.removeFromContainer(bytes(mapKey), null, ContainerType.MAP, keys, values).get();
        assertEquals(1, removed, "Элемент удалён по ключу");

        Map<Payload, Payload> result = client.streamMap(mapKey).get();
        assertEquals(1, result.size(), "Остался 1 элемент");
        assertFalse(result.entrySet().stream().anyMatch(e -> "k1".equals(str(e.getKey()))));
    }

    // =========================================================================
    // 4. REMOVE FROM CONTAINER BY KEY OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("removeFromContainer удаляет элемент по ключу и возвращает 1")
    void testRemoveFromContainerByKey() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_remove_key";
        Map<Payload, Payload> initialData = Map.of(
                p("k1"), p("v1"),
                p("k2"), p("v2"),
                p("k3"), p("v3")
        );
        client.createMap(mapKey, initialData).get();

        Integer removed = client.removeFromContainer(bytes(mapKey), null, p("k2").getValue()).get();
        assertEquals(1, removed, "Должен быть удалён 1 элемент");

        Map<Payload, Payload> result = client.streamMap(mapKey).get();
        assertEquals(2, result.size(), "Осталось 2 элемента");
        assertFalse(result.containsKey(p("k2")));
    }

    @Test
    @DisplayName("removeFromContainer возвращает 0 для несуществующего ключа")
    void testRemoveFromContainerByKeyNotFound() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_remove_key_nf";
        Map<Payload, Payload> initialData = Map.of(
                p("k1"), p("v1")
        );
        client.createMap(mapKey, initialData).get();

        Integer removed = client.removeFromContainer(bytes(mapKey), null, p("nonexistent").getValue()).get();
        assertEquals(0, removed, "Ничего не удалено (элемент не найден)");
    }

    @Test
    @DisplayName("removeFromContainer из пустого контейнера возвращает 0")
    void testRemoveFromContainerFromEmptyMap() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_remove_empty";
        client.createMap(mapKey, Map.of()).get();

        Integer removed = client.removeFromContainer(bytes(mapKey), null, p("k1").getValue()).get();
        assertEquals(0, removed, "Ничего не удалено из пустого контейнера");
    }

    // =========================================================================
    // 3. ADD ELEMENT HASHMAP OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("addElementHashMap добавляет элементы и возвращает количество")
    void testAddElementHashMap() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_add";
        client.createMap(mapKey, Map.of()).get();

        List<Payload> keys = List.of(p("k1"), p("k2"), p("k3"));
        List<Payload> values = List.of(p("v1"), p("v2"), p("v3"));

        Integer added = client.addElementHashMap(bytes(mapKey), null, keys, values, DEFAULT_CLIENT_ID, TEST_TIMEOUT).get();
        assertEquals(3, added, "Должно быть добавлено 3 элемента");

        Map<Payload, Payload> result = client.streamMap(mapKey).get();
        assertEquals(3, result.size());
        assertTrue(result.entrySet().stream().anyMatch(e -> "k1".equals(str(e.getKey())) && "v1".equals(str(e.getValue()))));
        assertTrue(result.entrySet().stream().anyMatch(e -> "k2".equals(str(e.getKey())) && "v2".equals(str(e.getValue()))));
        assertTrue(result.entrySet().stream().anyMatch(e -> "k3".equals(str(e.getKey())) && "v3".equals(str(e.getValue()))));
    }

    @Test
    @DisplayName("addElementHashMap не допускает дубликаты ключей")
    void testAddElementHashMapNoDuplicates() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_add_nodup";
        client.createMap(mapKey, Map.of()).get();

        // Добавляем уникальные ключи
        List<Payload> keys1 = List.of(p("k1"), p("k2"));
        List<Payload> values1 = List.of(p("v1"), p("v2"));
        Integer added1 = client.addElementHashMap(bytes(mapKey), null, keys1, values1, DEFAULT_CLIENT_ID, TEST_TIMEOUT).get();
        assertEquals(2, added1);

        // Пытаемся добавить дубликат k1 — должен вернуть 0
        List<Payload> keys2 = List.of(p("k1"));
        List<Payload> values2 = List.of(p("v1_new"));
        Integer added2 = client.addElementHashMap(bytes(mapKey), null, keys2, values2, DEFAULT_CLIENT_ID, TEST_TIMEOUT).get();
        assertEquals(0, added2, "Дубликат ключа не должен добавиться");

        Map<Payload, Payload> result = client.streamMap(mapKey).get();
        assertEquals(2, result.size(), "Всего должно быть 2 элемента");
    }

    @Test
    @DisplayName("addElementHashMap с пустым списком возвращает 0")
    void testAddElementHashMapEmptyList() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_add_empty";
        client.createMap(mapKey, Map.of()).get();

        Integer added = client.addElementHashMap(bytes(mapKey), null, List.of(), List.of(), DEFAULT_CLIENT_ID, TEST_TIMEOUT).get();
        assertEquals(0, added, "Добавление пустого списка должно вернуть 0");

        Map<Payload, Payload> result = client.streamMap(mapKey).get();
        assertEquals(0, result.size());
    }

    // =========================================================================
    // 2. STREAM MAP OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("streamMap пустого HashedMap возвращает пустой ответ")
    void testStreamMapEmpty() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_stream_empty";

        KeyHintData hint = client.createMap(mapKey, Map.of()).get();
        assertNotNull(hint);

        Map<Payload, Payload> result = client.streamMap(mapKey, hint).get();
        assertNotNull(result);
        assertEquals(0, result.size(), "streamMap пустого map должен вернуть пустую карту");
    }

    @Test
    @DisplayName("streamMap возвращает все содержимое контейнера")
    void testStreamMapWithData() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_stream_data";
        Map<Payload, Payload> initialData = Map.of(
                p("elem1"), p("val1"),
                p("elem2"), p("val2"),
                p("elem3"), p("val3"),
                p("elem4"), p("val4"),
                p("elem5"), p("val5")
        );

        KeyHintData hint = client.createMap(mapKey, initialData).get();
        assertNotNull(hint);

        Map<Payload, Payload> result = client.streamMap(mapKey, hint).get();
        assertNotNull(result);
        assertEquals(5, result.size(), "streamMap должен вернуть все 5 элементов");
    }

    @Test
    @DisplayName("streamMap с явным clientId")
    void testStreamMapWithClientId() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_stream_cid";
        Map<Payload, Payload> initialData = Map.of(
                p("k1"), p("v1")
        );

        KeyHintData hint = client.createMap(mapKey, initialData).get();
        assertNotNull(hint);

        Map<Payload, Payload> result = client.streamMap(bytes(mapKey), hint, DEFAULT_CLIENT_ID, TEST_TIMEOUT).get();
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.entrySet().stream().anyMatch(e -> "k1".equals(str(e.getKey())) && "v1".equals(str(e.getValue()))));
    }

    // =========================================================================
    // 1. CREATE MAP OPERATIONS
    // =========================================================================

    @Test
    @DisplayName("Создание пустого HashedMap")
    void testCreateEmptyMap() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_empty";

        KeyHintData hint = client.createMap(mapKey, Map.of()).get();
        assertNotNull(hint, "KeyHint должен быть создан");

        Map<Payload, Payload> result = client.streamMap(mapKey, hint).get();
        assertNotNull(result);
        assertEquals(0, result.size(), "Пустой map должен вернуть пустую карту");
    }

    @Test
    @DisplayName("Создание HashedMap с начальными данными")
    void testCreateMapWithInitialData() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_initial";

        Map<Payload, Payload> initialData = Map.of(
                p("k1"), p("v1"),
                p("k2"), p("v2"),
                p("k3"), p("v3")
        );

        KeyHintData hint = client.createMap(mapKey, initialData).get();
        assertNotNull(hint, "KeyHint должен быть создан");

        Map<Payload, Payload> result = client.streamMap(mapKey, hint).get();
        assertNotNull(result);
        assertEquals(3, result.size(), "Map должен содержать 3 элемента");
        // Проверяем через stream т.к. streamMap возвращает новые Payload объекты
        assertTrue(result.entrySet().stream().anyMatch(e -> "k1".equals(str(e.getKey())) && "v1".equals(str(e.getValue()))));
        assertTrue(result.entrySet().stream().anyMatch(e -> "k2".equals(str(e.getKey())) && "v2".equals(str(e.getValue()))));
        assertTrue(result.entrySet().stream().anyMatch(e -> "k3".equals(str(e.getKey())) && "v3".equals(str(e.getValue()))));
    }

    @Test
    @DisplayName("Создание большого HashedMap с автоматическим разбиением на чанки (чанкинг)")
    void testCreateMapWithLargeDataChunking() throws ExecutionException, InterruptedException {
        String mapKey = baseKey + "_large";
        int elementCount = 1500;
        Map<Payload, Payload> largeData = new java.util.HashMap<>();

        for (int i = 0; i < elementCount; i++) {
            largeData.put(p("key_" + i), p("value_" + i + "_" + UUID.randomUUID()));
        }

        KeyHintData hint = client.createMap(mapKey, largeData).get();
        assertNotNull(hint, "KeyHint должен быть создан");

        Map<Payload, Payload> result = client.streamMap(mapKey, hint).get();
        assertNotNull(result);
        assertEquals(elementCount, result.size(), "Map должен содержать " + elementCount + " элементов");
    }
}