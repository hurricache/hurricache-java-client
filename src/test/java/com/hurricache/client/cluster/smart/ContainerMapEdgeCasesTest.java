package com.hurricache.client.cluster.smart;

import com.hurricache.TestBaseCluster;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Mode;
import com.hurricache.client.intf.Payload;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class ContainerMapEdgeCasesTest extends TestBaseCluster {

    @Test
    @DisplayName("Проверка запроса элемента из несуществующего контейнера")
    void testGetFromNonExistentContainer() {
        String nonExistentKey = "nonExistentMap_" + UUID.randomUUID();
        byte[] keyBytes = nonExistentKey.getBytes(StandardCharsets.UTF_8);
        byte[] elemKey = "field".getBytes(StandardCharsets.UTF_8);

        // containsContainerKey -> NOT_FOUND
        ExecutionException ex1 = Assertions.assertThrows(
                ExecutionException.class,
                () -> client.setMode(Mode.MASTER)
                        .containsContainerKey(keyBytes, null, elemKey)
                        .get()
        );
        Assertions.assertInstanceOf(StatusRuntimeException.class, ex1.getCause());
        Assertions.assertEquals(Status.Code.NOT_FOUND, ((StatusRuntimeException) ex1.getCause()).getStatus().getCode());

        // getContainerValue -> NOT_FOUND
        ExecutionException ex2 = Assertions.assertThrows(
                ExecutionException.class,
                () -> client.setMode(Mode.MASTER)
                        .getContainerValue(keyBytes, null, elemKey)
                        .get()
        );
        Assertions.assertInstanceOf(StatusRuntimeException.class, ex2.getCause());
        Assertions.assertEquals(Status.Code.NOT_FOUND, ((StatusRuntimeException) ex2.getCause()).getStatus().getCode());
    }

    @Test
    @DisplayName("Проверка вызова getAndRemoveContainerValue для несуществующего ключа элемента")
    void testGetAndRemoveNonExistentElementKey() throws ExecutionException, InterruptedException {
        String mapKey = "mapForMissingElement_" + UUID.randomUUID();
        byte[] keyBytes = mapKey.getBytes(StandardCharsets.UTF_8);
        byte[] existingElemKey = "existing".getBytes(StandardCharsets.UTF_8);
        byte[] missingElemKey = "missing".getBytes(StandardCharsets.UTF_8);

        KeyHintData keyHint = client.setMode(Mode.MASTER)
                .createMap(mapKey, Map.of(Payload.of(existingElemKey), Payload.of("val".getBytes(StandardCharsets.UTF_8))))
                .get();

        Thread.sleep(500);

        // Попытка забрать несуществующий ключ должна завершаться с NOT_FOUND
        ExecutionException ex = Assertions.assertThrows(
                ExecutionException.class,
                () -> client.setMode(Mode.BACKUP)
                        .getAndRemoveContainerValue(keyBytes, keyHint, missingElemKey)
                        .get()
        );
        Assertions.assertInstanceOf(StatusRuntimeException.class, ex.getCause());
        Assertions.assertEquals(Status.Code.NOT_FOUND, ((StatusRuntimeException) ex.getCause()).getStatus().getCode());

        // Убеждаемся, что существующий элемент остался нетронутым
        Boolean containsExisting = client.setMode(Mode.MASTER)
                .containsContainerKey(keyBytes, keyHint, existingElemKey)
                .get();
        Assertions.assertTrue(containsExisting);
    }

    @Test
    @DisplayName("Проверка removeFromContainer при попытке удалить несуществующий ключ элемента")
    void testRemoveNonExistentElementReturnsZero() throws ExecutionException, InterruptedException {
        String mapKey = "mapZeroRemove_" + UUID.randomUUID();
        byte[] keyBytes = mapKey.getBytes(StandardCharsets.UTF_8);
        byte[] elemKey = "key1".getBytes(StandardCharsets.UTF_8);
        byte[] nonExistentKey = "key2".getBytes(StandardCharsets.UTF_8);

        KeyHintData keyHint = client.setMode(Mode.MASTER)
                .createMap(mapKey, Map.of(Payload.of(elemKey), Payload.of("v1".getBytes(StandardCharsets.UTF_8))))
                .get();

        Thread.sleep(500);

        // Попытка удалить несуществующий элемент должна выбросить NOT_FOUND
        ExecutionException ex = Assertions.assertThrows(
                ExecutionException.class,
                () -> client.setMode(Mode.BACKUP)
                        .removeFromContainer(keyBytes, keyHint, nonExistentKey)
                        .get()
        );
        Assertions.assertInstanceOf(StatusRuntimeException.class, ex.getCause());
        Assertions.assertEquals(Status.Code.NOT_FOUND, ((StatusRuntimeException) ex.getCause()).getStatus().getCode());
    }

    @Test
    @DisplayName("Проверка updateContainerValue для существующего элемента в Map")
    void testUpdateContainerValueUpdatesExistingElement() throws ExecutionException, InterruptedException {
        String mapKey = "mapUpdate_" + UUID.randomUUID();
        byte[] keyBytes = mapKey.getBytes(StandardCharsets.UTF_8);
        byte[] elemKey = "existingField".getBytes(StandardCharsets.UTF_8);
        byte[] initialValue = "oldValue".getBytes(StandardCharsets.UTF_8);
        byte[] updatedValue = "newValue".getBytes(StandardCharsets.UTF_8);

        // 1. Создаем Map с уже существующим полем "existingField"
        KeyHintData keyHint = client.setMode(Mode.MASTER)
                .createMap(keyBytes, Map.of(Payload.of(elemKey), Payload.of(initialValue)))
                .get();

        Thread.sleep(500);

        // 2. Выполняем чистый UPDATE существующего поля
        client.setMode(Mode.MASTER)
                .updateContainerValue(keyBytes, keyHint, elemKey, updatedValue)
                .get();

        Thread.sleep(500);

        // 3. Вычитываем и проверяем, что значение действительно изменилось
        byte[] fetchedValue = client.setMode(Mode.BACKUP)
                .getContainerValue(keyBytes, keyHint, elemKey)
                .get();

        Assertions.assertNotNull(fetchedValue);
        Assertions.assertEquals("newValue", new String(fetchedValue, StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("Проверка updateContainerValue возвращает NOT_FOUND для несуществующего элемента")
    void testUpdateContainerValueFailsOnMissingElement() throws ExecutionException, InterruptedException {
        String mapKey = "mapUpdateMissing_" + UUID.randomUUID();
        byte[] keyBytes = mapKey.getBytes(StandardCharsets.UTF_8);
        byte[] missingElemKey = "nonExistingField".getBytes(StandardCharsets.UTF_8);
        byte[] newValue = "someValue".getBytes(StandardCharsets.UTF_8);

        // Создаем пустую Map
        KeyHintData keyHint = client.setMode(Mode.MASTER)
                .createMap(mapKey, Map.of())
                .get();

        Thread.sleep(500);

        // Попытка обновить несуществующий ключ должна выбросить исключение (Status.NOT_FOUND из gRPC)
        ExecutionException exception = Assertions.assertThrows(ExecutionException.class, () -> client.setMode(Mode.MASTER)
                .updateContainerValue(keyBytes, keyHint, missingElemKey, newValue)
                .get());

        Assertions.assertTrue(exception.getMessage().contains("NOT_FOUND"));
    }

    @Test
    @DisplayName("Проверка полных сигнатур методов (с передачей явных clientId и timeout)")
    void testExplicitClientIdAndTimeoutSignatures() throws ExecutionException, InterruptedException {
        String mapKey = "mapExplicitSig_" + UUID.randomUUID();
        byte[] keyBytes = mapKey.getBytes(StandardCharsets.UTF_8);
        byte[] elemKey = "k1".getBytes(StandardCharsets.UTF_8);
        byte[] elemValue = "v1".getBytes(StandardCharsets.UTF_8);
        int customClientId = 99;
        Duration customTimeout = Duration.ofSeconds(3);

        KeyHintData keyHint = client.setMode(Mode.MASTER)
                .createMap(mapKey, Map.of(Payload.of(elemKey), Payload.of(elemValue)))
                .get();

        Thread.sleep(500);

        Boolean contains = client.setMode(Mode.BACKUP)
                .containsContainerKey(keyBytes, keyHint, elemKey, customClientId, customTimeout)
                .get();
        Assertions.assertTrue(contains);

        byte[] val = client.setMode(Mode.BACKUP)
                .getContainerValue(keyBytes, keyHint, elemKey, customClientId, customTimeout)
                .get();
        Assertions.assertNotNull(val);
        Assertions.assertEquals("v1", new String(val, StandardCharsets.UTF_8));

        byte[] updatedVal = "v2".getBytes(StandardCharsets.UTF_8);
        client.setMode(Mode.BACKUP)
                .updateContainerValue(keyBytes, keyHint, elemKey, updatedVal, customClientId, customTimeout)
                .get();
        Thread.sleep(500);
        byte[] removedVal = client.setMode(Mode.MASTER)
                .getAndRemoveContainerValue(keyBytes, keyHint, elemKey, customClientId, customTimeout)
                .get();
        Assertions.assertEquals("v2", new String(removedVal, StandardCharsets.UTF_8));

        // Повторный вызов removeFromContainer для уже удалённого элемента бросит NOT_FOUND
        ExecutionException ex = Assertions.assertThrows(
                ExecutionException.class,
                () -> client.setMode(Mode.MASTER)
                        .removeFromContainer(keyBytes, keyHint, elemKey, customClientId, customTimeout)
                        .get()
        );
        Assertions.assertInstanceOf(StatusRuntimeException.class, ex.getCause());
        Assertions.assertEquals(Status.Code.NOT_FOUND, ((StatusRuntimeException) ex.getCause()).getStatus().getCode());
    }
}