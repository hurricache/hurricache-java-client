package com.hurricache.client.cluster.smart;

import com.hurricache.TestBaseCluster;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Mode;
import com.hurricache.client.intf.OrderedPayload;
import com.hurricache.client.intf.Payload;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class ContainerMapOperationsTest extends TestBaseCluster {

    private static final Duration TEST_TIMEOUT = Duration.ofSeconds(2);
    private static final int CLIENT_ID = 101;

    @Test
    void testMapContainerOperationsCreateOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String mapKey = "mapContainerKey" + UUID.randomUUID();
        byte[] keyBytes = mapKey.getBytes(StandardCharsets.UTF_8);
        byte[] elemKey = "field1".getBytes(StandardCharsets.UTF_8);
        byte[] elemValue = "val1".getBytes(StandardCharsets.UTF_8);
        byte[] updatedValue = "val1_updated".getBytes(StandardCharsets.UTF_8);

        // 1. Создаем Map на MASTER с начальным элементом
        KeyHintData keyHint = client.setMode(Mode.MASTER)
                .createMap(mapKey, Map.of(Payload.of(elemKey), Payload.of(elemValue)))
                .get();

        // Ожидаем репликации в кластере
        Thread.sleep(500);

        // 2. Проверяем наличие ключа и значение на BACKUP
        Boolean containsKey = client.setMode(Mode.BACKUP)
                .containsContainerKey(keyBytes, keyHint, elemKey)
                .get();
        Assertions.assertTrue(containsKey);

        byte[] fetchedVal = client.setMode(Mode.BACKUP)
                .getContainerValue(keyBytes, keyHint, elemKey)
                .get();
        Assertions.assertNotNull(fetchedVal);
        Assertions.assertEquals("val1", new String(fetchedVal, StandardCharsets.UTF_8));

        // 3. Обновляем значение на BACKUP и проверяем полученное предыдущее/актуальное значение
        client.setMode(Mode.BACKUP)
                .updateContainerValue(keyBytes, keyHint, elemKey, updatedValue)
                .get();

        Thread.sleep(500);
        byte[] updatedValFetched = client.setMode(Mode.MASTER)
                .getContainerValue(keyBytes, keyHint, elemKey)
                .get();
        Assertions.assertNotNull(updatedValFetched);
        Assertions.assertEquals("val1_updated", new String(updatedValFetched, StandardCharsets.UTF_8));

        // 4. Проверяем getAndRemoveContainerValue на BACKUP
        byte[] removedValue = client.setMode(Mode.BACKUP)
                .getAndRemoveContainerValue(keyBytes, keyHint, elemKey)
                .get();
        Assertions.assertNotNull(removedValue);
        Assertions.assertEquals("val1_updated", new String(removedValue, StandardCharsets.UTF_8));

        // 5. Убеждаемся, что элемент удален
        Thread.sleep(500);

        Boolean b = client.setMode(Mode.MASTER).containsContainerKey(keyBytes, keyHint, elemKey).get();
        Assertions.assertFalse(b);
    }

    @Test
    void testMapContainerOperationsCreateOnBackupValidateOnMaster() throws ExecutionException, InterruptedException {
        String mapKey = "mapContainerKey" + UUID.randomUUID();
        byte[] keyBytes = mapKey.getBytes(StandardCharsets.UTF_8);
        byte[] elemKey = "fieldA".getBytes(StandardCharsets.UTF_8);
        byte[] elemValue = "valA".getBytes(StandardCharsets.UTF_8);

        // 1. Создаем Map на BACKUP
        KeyHintData keyHint = client.setMode(Mode.BACKUP)
                .createMap(mapKey, Map.of(Payload.of(elemKey), Payload.of(elemValue)))
                .get();

        Thread.sleep(500);

        // 2. Добавляем/Обновляем значение на MASTER
        byte[] newVal = "valA_new".getBytes(StandardCharsets.UTF_8);
        client.setMode(Mode.MASTER)
                .updateContainerValue(keyBytes, keyHint, elemKey, newVal)
                .get();

        // 3. Удаляем элемент через removeFromContainer на MASTER
        Integer removedCount = client.setMode(Mode.MASTER)
                .removeFromContainer(keyBytes, keyHint, elemKey)
                .get();
        Assertions.assertEquals(1, removedCount);

        // 4. Проверяем отсутствие ключа на BACKUP
        Thread.sleep(1000);

        Boolean b = client.setMode(Mode.BACKUP).containsContainerKey(keyBytes, keyHint, elemKey).get();
        Assertions.assertFalse(b);

        ExecutionException ex1 = Assertions.assertThrows(
                ExecutionException.class,
                () -> client.setMode(Mode.BACKUP)
                        .getContainerValue(keyBytes, keyHint, elemKey)
                        .get()
        );
        Assertions.assertInstanceOf(StatusRuntimeException.class, ex1.getCause());
        Assertions.assertEquals(Status.Code.NOT_FOUND, ((StatusRuntimeException) ex1.getCause()).getStatus().getCode());
    }

    // =========================================================================
    // Расширение: Батчевое добавление и проверка репликации HashMap / OrderedMap
    // =========================================================================

    @Test
    @DisplayName("Батчевое добавление элементов в HashMap с проверкой репликации на BACKUP")
    void testAddElementHashMapBatchReplication() throws ExecutionException, InterruptedException {
        String mapKey = "batchHashMapKey_" + UUID.randomUUID();
        byte[] keyBytes = mapKey.getBytes(StandardCharsets.UTF_8);

        // 1. Создаем пустой контейнер на MASTER
        KeyHintData keyHint = client.setMode(Mode.MASTER)
                .createMap(mapKey, Map.of())
                .get();

        List<Payload> keys = List.of(
                Payload.of("k1".getBytes(StandardCharsets.UTF_8)),
                Payload.of("k2".getBytes(StandardCharsets.UTF_8))
        );
        List<Payload> values = List.of(
                Payload.of("v1".getBytes(StandardCharsets.UTF_8)),
                Payload.of("v2".getBytes(StandardCharsets.UTF_8))
        );
        Thread.sleep(500);
        // 2. Вызываем addElementHashMap на MASTER
        Integer addedCount = client.setMode(Mode.MASTER)
                .addElementHashMap(keyBytes, keyHint, keys, values, CLIENT_ID, TEST_TIMEOUT)
                .get();

        Assertions.assertEquals(2, addedCount);
        Thread.sleep(500); // Ожидаем репликацию на BACKUP

        // 3. Валидируем с BACKUP узла
        byte[] val1 = client.setMode(Mode.BACKUP)
                .getContainerValue(keyBytes, keyHint, "k1".getBytes(StandardCharsets.UTF_8))
                .get();
        byte[] val2 = client.setMode(Mode.BACKUP)
                .getContainerValue(keyBytes, keyHint, "k2".getBytes(StandardCharsets.UTF_8))
                .get();

        Assertions.assertEquals("v1", new String(val1, StandardCharsets.UTF_8));
        Assertions.assertEquals("v2", new String(val2, StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("Батчевое добавление элементов в OrderedMap с проверкой порядка")
    void testAddElementOrderedMapBatchReplication() throws ExecutionException, InterruptedException {
        String mapKey = "batchOrderedMapKey_" + UUID.randomUUID();
        byte[] keyBytes = mapKey.getBytes(StandardCharsets.UTF_8);

        KeyHintData keyHint = client.setMode(Mode.MASTER)
                .createOrderedMap(mapKey, Map.of())
                .get();

        List<OrderedPayload> orderedKeys = List.of(
                OrderedPayload.of("score_100".getBytes(StandardCharsets.UTF_8), 100L),
                OrderedPayload.of("score_50".getBytes(StandardCharsets.UTF_8), 50L)
        );
        List<Payload> values = List.of(
                Payload.of("userMax".getBytes(StandardCharsets.UTF_8)),
                Payload.of("userMid".getBytes(StandardCharsets.UTF_8))
        );
        Thread.sleep(500);
        // 1. Вставляем элементы через addElementOrderedMap на BACKUP (проверяем прямую запись в бэкап)
        Integer addedCount = client.setMode(Mode.BACKUP)
                .addElementOrderedMap(keyBytes, keyHint, orderedKeys, values, CLIENT_ID, TEST_TIMEOUT)
                .get();

        Assertions.assertEquals(2, addedCount);
        Thread.sleep(500);

        // 2. Вычитываем с MASTER
        byte[] userMaxVal = client.setMode(Mode.MASTER)
                .getContainerValue(keyBytes, keyHint, "score_100".getBytes(StandardCharsets.UTF_8))
                .get();

        Assertions.assertNotNull(userMaxVal);
        Assertions.assertEquals("userMax", new String(userMaxVal, StandardCharsets.UTF_8));
    }
}