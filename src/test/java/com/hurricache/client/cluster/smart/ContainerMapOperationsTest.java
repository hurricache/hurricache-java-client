package com.hurricache.client.cluster.smart;

import com.hurricache.TestBaseCluster;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Mode;
import com.hurricache.client.intf.Payload;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class ContainerMapOperationsTest extends TestBaseCluster {

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
        ExecutionException ex = Assertions.assertThrows(
                ExecutionException.class,
                () -> client.setMode(Mode.MASTER)
                        .containsContainerKey(keyBytes, keyHint, elemKey)
                        .get()
        );
        Assertions.assertInstanceOf(StatusRuntimeException.class, ex.getCause());
        Assertions.assertEquals(Status.Code.NOT_FOUND, ((StatusRuntimeException) ex.getCause()).getStatus().getCode());

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

        Thread.sleep(500);

        ExecutionException ex = Assertions.assertThrows(
                ExecutionException.class,
                () -> client.setMode(Mode.BACKUP)
                        .containsContainerKey(keyBytes, keyHint, elemKey)
                        .get()
        );
        Assertions.assertInstanceOf(StatusRuntimeException.class, ex.getCause());
        Assertions.assertEquals(Status.Code.NOT_FOUND, ((StatusRuntimeException) ex.getCause()).getStatus().getCode());

        ExecutionException ex1 = Assertions.assertThrows(
                ExecutionException.class,
                () -> client.setMode(Mode.BACKUP)
                        .getContainerValue(keyBytes, keyHint, elemKey)
                        .get()
        );
        Assertions.assertInstanceOf(StatusRuntimeException.class, ex1.getCause());
        Assertions.assertEquals(Status.Code.NOT_FOUND, ((StatusRuntimeException) ex1.getCause()).getStatus().getCode());
    }
}