package com.hurricache.client.cluster.smart;

import com.hurricache.TestBaseCluster;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Mode;
import com.hurricache.client.intf.Payload;
import com.hurricache.grpc.LockStatus;
import com.hurricache.grpc.LockType;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

// Статический импорт стандартных утверждений JUnit 5
import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class HurriCacheClientMissingTests extends TestBaseCluster {

    private final int DEFAULT_CLIENT_ID = 42;
    private final int SECONDARY_CLIENT_ID = 99;

    @BeforeAll
    void setUp() {
        // Инициализация вашего клиента
        // client = new HurriCacheClient(...);
    }

    // =========================================================================
    // 1. НЕГАТИВНЫЕ СЦЕНАРИИ И ОБРАБОТКА ОШИБОК
    // =========================================================================

    @Test
    @DisplayName("TTL: Проверка истечения времени жизни ключа")
    void testTtlExpiration() throws Exception {
        String key = "ttl:test:key"+ UUID.randomUUID();
        byte[] payload = "temp_data".getBytes();

        client.createKeyValue(key, payload, DEFAULT_CLIENT_ID).get();
        Thread.sleep(500);
        Boolean ttlSet = client.setTtl(key, null, 100).get();
        assertTrue(ttlSet, "TTL должен быть успешно установлен");

        Long remainingTtl = client.getTtl(key, null).get();
        assertNotNull(remainingTtl);
        assertTrue(remainingTtl > 0L, "Остаток TTL должен быть больше 0");

        // Ждем истечения TTL
        Thread.sleep(150);
        assertThrows(ExecutionException.class, () -> {
            client.getValue(key, null, DEFAULT_CLIENT_ID).get();
        });
    }

    @Test
    @DisplayName("Non-existent Key: Чтение и удаление несуществующего ключа")
    void testNonExistentKeyOperations() throws Exception {
        String missingKey = "key:does:not:exist:" + UUID.randomUUID();


        assertThrows(ExecutionException.class, () -> {
            client.getValue(missingKey, null, DEFAULT_CLIENT_ID).get();
        });

        assertThrows(ExecutionException.class, () -> {
            client.remove(missingKey, null, DEFAULT_CLIENT_ID).get();
        });

    }

    @Test
    @DisplayName("Type Mismatch: Запрос операции со списком для обычного KV ключа")
    void testTypeMismatchErrorHandling() throws Exception {
        String key = "kv:for:mismatch"+ UUID.randomUUID();
        client.createKeyValue(key, "just_string".getBytes(), DEFAULT_CLIENT_ID).get();
        Thread.sleep(500);
        assertThrows(ExecutionException.class, () -> {
            client.getHead(key, null).get();
        }, "Запрос операции списка для KV-ключа должен завершаться ошибкой");
    }

    @Test
    @DisplayName("Bounds: Доступ к элементам коллекции по некорректному индексу")
    void testOutOfBoundsPosition() throws Exception {
        String key = "list:bounds:test"+ UUID.randomUUID();
        List<Payload> initial = List.of(Payload.of("elem1".getBytes()));
        KeyHintData hint = client.createList(key, initial, DEFAULT_CLIENT_ID).get();

        assertThrows(ExecutionException.class, () -> {
            client.getElementAtPosition(key, hint, 100).get();
        });

        assertThrows(ExecutionException.class, () -> {
            client.getElementAtPosition(key, hint, -1).get();
        });
    }

    // =========================================================================
    // 2. БЛОКИРОВКИ И КОНКУРЕНТНЫЙ ДОСТУП
    // =========================================================================

    @Test
    @DisplayName("Locks: Совместимость нескольких READ_LOCK от разных клиентов")
    void testMultipleReadLocksAllowed() throws Exception {
        String key = "lock:shared:read"+ UUID.randomUUID();
        client.createKeyValue(key, "data".getBytes(), DEFAULT_CLIENT_ID).get();
        Thread.sleep(500);
        LockStatus lock1 = client.lockObject(key, LockType.READ_LOCK, DEFAULT_CLIENT_ID, Duration.ofSeconds(2)).get();
        LockStatus lock2 = client.lockObject(key, LockType.READ_LOCK, SECONDARY_CLIENT_ID, Duration.ofSeconds(2)).get();

        assertEquals(LockStatus.OK, lock1);
        assertEquals(LockStatus.OK, lock2);

        client.unlockObject(key, DEFAULT_CLIENT_ID).get();
        client.unlockObject(key, SECONDARY_CLIENT_ID).get();
    }

    @Test
    @DisplayName("Locks: Отклонение WRITE_LOCK при активном READ_LOCK")
    void testWriteLockRejectedWhenReadLocked() throws Exception {
        String key = "lock:exclusive:write"+ UUID.randomUUID();
        client.createKeyValue(key, "data".getBytes(), DEFAULT_CLIENT_ID).get();
        Thread.sleep(500);
        client.lockObject(key, LockType.READ_LOCK, DEFAULT_CLIENT_ID, Duration.ofMinutes(5)).get();
        Thread.sleep(500);
        LockStatus writeLockStatus = client.lockObject(
                key, LockType.WRITE_LOCK, SECONDARY_CLIENT_ID, Duration.ofMillis(200)
        ).get();
        Thread.sleep(500);
        assertEquals(LockStatus.CANT_LOCK, writeLockStatus);
        Thread.sleep(500);
        client.unlockObject(key, DEFAULT_CLIENT_ID).get();
    }

    @Test
    @DisplayName("Locks: Запрет анлока чужим clientId")
    void testUnlockByWrongClientFails() throws Exception {
        String key = "lock:wrong:owner"+ UUID.randomUUID();
        client.createKeyValue(key, "data".getBytes(), DEFAULT_CLIENT_ID).get();
        Thread.sleep(500);
        client.lockObject(key, LockType.WRITE_LOCK, DEFAULT_CLIENT_ID, Duration.ofMinutes(5)).get();
        Thread.sleep(500);
        LockStatus unlockStatus = client.unlockObject(key, SECONDARY_CLIENT_ID).get();
        assertEquals(LockStatus.CANT_UNLOCK, unlockStatus);
        Thread.sleep(500);
        LockStatus validUnlock = client.unlockObject(key, DEFAULT_CLIENT_ID).get();
        assertEquals(LockStatus.OK, validUnlock);
    }

    // =========================================================================
    // 3. МНОГОПОТОЧНОСТЬ И НАГРУЗКА (CONCURRENCY)
    // =========================================================================

    @Test
    @DisplayName("Concurrency: Параллельное увеличение атомика из 10 потоков")
    void testConcurrentAtomicIncrements() throws Exception {
        String key = "atomic:concurrent:counter"+ UUID.randomUUID();
        KeyHintData hint = client.atomicCreate(key, 0L).get();
        Thread.sleep(500);
        int threads = 10;
        int incrementsPerThread = 50;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);

        for (int i = 0; i < threads; i++) {
            executor.submit(() -> {
                try {
                    for (int j = 0; j < incrementsPerThread; j++) {
                        client.atomicAdd(key, hint, 1L).get();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            });
        }

        boolean finished = latch.await(10, TimeUnit.SECONDS);
        assertTrue(finished, "Все потоки должны успешно завершиться до таймаута");

        long finalValue = client.atomicAdd(key, hint, 0L).get();
        assertEquals((long) threads * incrementsPerThread, finalValue);

        executor.shutdown();
    }

    @Test
    @DisplayName("Concurrency: Одновременный Push и Pop в очередь")
    void testConcurrentQueuePushPop() throws Exception {
        String key = "queue:concurrent:test"+ UUID.randomUUID();
        KeyHintData hint = client.createQueue(key, new ArrayList<>()).get();
        Thread.sleep(500);
        int itemCount = 100;
        ExecutorService executor = Executors.newFixedThreadPool(2);
        AtomicInteger poppedCount = new AtomicInteger(0);

        CompletableFuture<Void> producer = CompletableFuture.runAsync(() -> {
            for (int i = 0; i < itemCount; i++) {
                try {
                    client.addElementToTail(key, hint, List.of(Payload.of(("val-" + i).getBytes()))).get();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }, executor);

        CompletableFuture<Void> consumer = CompletableFuture.runAsync(() -> {
            int emptyReads = 0;
            while (poppedCount.get() < itemCount && emptyReads < 50) {
                try {
                    Payload item = client.getAndRemoveFront(key, hint).get();
                    if (item != null) {
                        poppedCount.incrementAndGet();
                        emptyReads = 0;
                    } else {
                        emptyReads++;
                        Thread.sleep(10);
                    }
                } catch (Exception e) {
                    // Игнорируем промежуточные отсутствия
                }
            }
        }, executor);

        CompletableFuture.allOf(producer, consumer).get(10, TimeUnit.SECONDS);
        assertEquals(itemCount, poppedCount.get());

        executor.shutdown();
    }

    // =========================================================================
    // 4. РЕЖИМЫ (MASTER / BACKUP)
    // =========================================================================

    @Test
    @DisplayName("Mode: Отклонение операций записи при Mode.BACKUP")
    void testWriteOperationFailsInBackupMode() throws Exception {
        String key = "mode:backup:write:test"  + UUID.randomUUID();
        client.createKeyValue(key, "initial".getBytes(), DEFAULT_CLIENT_ID).get();
        Thread.sleep(500);
        client.setMode(Mode.BACKUP);

        byte[] val = client.getValue(key, null, DEFAULT_CLIENT_ID).get();
        assertNotNull(val);

        byte[] bytes = client.updateKeyValue(key, null, "new_val".getBytes(), DEFAULT_CLIENT_ID).get();
        assertArrayEquals(val, bytes);

        client.setMode(Mode.MASTER);
    }

    // =========================================================================
    // 5. ГРАНИЧНЫЕ ЗНАЧЕНИЯ И СНЕЙПШОТЫ ДАННЫХ (EDGE CASES)
    // =========================================================================

    @Test
    @DisplayName("Edge Case: Создание и работа с пустым Payload (byte[0])")
    void testEmptyPayloadHandling() throws Exception {
        String key = "edge:empty:payload"+ UUID.randomUUID();
        byte[] emptyBuffer = new byte[0];

        client.createKeyValue(key, emptyBuffer, DEFAULT_CLIENT_ID).get();
        Thread.sleep(500);
        byte[] retrieved = client.getValue(key, null, DEFAULT_CLIENT_ID).get();

        assertNotNull(retrieved);
        assertEquals(0, retrieved.length);
    }

    @Test
    @DisplayName("Edge Case: Большой размер объекта (1 МБ)")
    void testLargePayloadHandling() throws Exception {
        String key = "edge:large:payload"+ UUID.randomUUID();
        byte[] largeBuffer = new byte[1024 * 1024]; // 1MB
        Arrays.fill(largeBuffer, (byte) 0xAB);

        client.createKeyValue(key, largeBuffer, DEFAULT_CLIENT_ID).get();
        Thread.sleep(500);
        byte[] retrieved = client.getValue(key, null, DEFAULT_CLIENT_ID).get();

        assertNotNull(retrieved);
        assertArrayEquals(largeBuffer, retrieved);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "key with spaces",
            "key:with:colons",
            "ключ_на_кириллице",
            "key\nwith\nnewlines",
            "special_chars_!@#$%^&*()_+"
    })
    @DisplayName("Edge Case: Специальные символы и UTF-8 в названии ключей")
    void testSpecialCharacterKeys(String specialKey) throws Exception {
        byte[] value = ("test_data"+ UUID.randomUUID()).getBytes();
        client.createKeyValue(specialKey, value, DEFAULT_CLIENT_ID).get();
        Thread.sleep(500);
        byte[] retrieved = client.getValue(specialKey, null, DEFAULT_CLIENT_ID).get();
        assertArrayEquals(value, retrieved);

        client.remove(specialKey, null, DEFAULT_CLIENT_ID).get();
    }
}