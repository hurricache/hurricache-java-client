package com.hurricache.client.standalone.simple;

import com.hurricache.TestBase;
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

public class ContainerMapOperationsTest extends TestBase {

    private static final Duration TEST_TIMEOUT = Duration.ofSeconds(2);
    private static final int CLIENT_ID = 101;

    @Test
    void testMapContainerOperations() throws ExecutionException, InterruptedException {
        String mapKey = "mapContainerKey" + UUID.randomUUID();
        byte[] keyBytes = mapKey.getBytes(StandardCharsets.UTF_8);
        byte[] elemKey = "field1".getBytes(StandardCharsets.UTF_8);
        byte[] elemValue = "val1".getBytes(StandardCharsets.UTF_8);
        byte[] updatedValue = "val1_updated".getBytes(StandardCharsets.UTF_8);

        Assertions.assertNotNull(client.createMap(mapKey, Map.of(Payload.of(elemKey), Payload.of(elemValue))));

        Thread.sleep(500);

        Boolean containsKey = client.containsContainerKey(keyBytes, null, elemKey).get();
        Assertions.assertTrue(containsKey);

        byte[] fetchedVal = client.getContainerValue(keyBytes, null, elemKey).get();
        Assertions.assertNotNull(fetchedVal);
        Assertions.assertEquals("val1", new String(fetchedVal, StandardCharsets.UTF_8));

        client.updateContainerValue(keyBytes, null, elemKey, updatedValue).get();

        Thread.sleep(500);
        byte[] updatedValFetched = client.getContainerValue(keyBytes, null, elemKey).get();
        Assertions.assertNotNull(updatedValFetched);
        Assertions.assertEquals("val1_updated", new String(updatedValFetched, StandardCharsets.UTF_8));

        byte[] removedValue = client.getAndRemoveContainerValue(keyBytes, null, elemKey).get();
        Assertions.assertNotNull(removedValue);
        Assertions.assertEquals("val1_updated", new String(removedValue, StandardCharsets.UTF_8));

        Thread.sleep(500);

        Boolean b = client.containsContainerKey(keyBytes, null, elemKey).get();
        Assertions.assertFalse(b);
    }

    @Test
    void testMapContainerOperationsSimple() throws ExecutionException, InterruptedException {
        String mapKey = "mapContainerKey" + UUID.randomUUID();
        byte[] keyBytes = mapKey.getBytes(StandardCharsets.UTF_8);
        byte[] elemKey = "fieldA".getBytes(StandardCharsets.UTF_8);
        byte[] elemValue = "valA".getBytes(StandardCharsets.UTF_8);

        Assertions.assertNotNull(client.createMap(mapKey, Map.of(Payload.of(elemKey), Payload.of(elemValue))));

        Thread.sleep(500);

        byte[] newVal = "valA_new".getBytes(StandardCharsets.UTF_8);
        client.updateContainerValue(keyBytes, null, elemKey, newVal).get();

        Integer removedCount = client.removeFromContainer(keyBytes, null, elemKey).get();
        Assertions.assertEquals(1, removedCount);

        Thread.sleep(1000);

        Boolean b = client.containsContainerKey(keyBytes, null, elemKey).get();
        Assertions.assertFalse(b);

        ExecutionException ex1 = Assertions.assertThrows(
                ExecutionException.class,
                () -> client.getContainerValue(keyBytes, null, elemKey).get()
        );
        Assertions.assertInstanceOf(StatusRuntimeException.class, ex1.getCause());
        Assertions.assertEquals(Status.Code.NOT_FOUND, ((StatusRuntimeException) ex1.getCause()).getStatus().getCode());
    }

    @Test
    @DisplayName("Батчевое добавление элементов в HashMap с проверкой репликации на BACKUP")
    void testAddElementHashMapBatchReplication() throws ExecutionException, InterruptedException {
        String mapKey = "batchHashMapKey_" + UUID.randomUUID();
        byte[] keyBytes = mapKey.getBytes(StandardCharsets.UTF_8);

        Assertions.assertNotNull(client.createMap(mapKey, Map.of()));

        List<Payload> keys = List.of(
                Payload.of("k1".getBytes(StandardCharsets.UTF_8)),
                Payload.of("k2".getBytes(StandardCharsets.UTF_8))
        );
        List<Payload> values = List.of(
                Payload.of("v1".getBytes(StandardCharsets.UTF_8)),
                Payload.of("v2".getBytes(StandardCharsets.UTF_8))
        );
        Thread.sleep(500);

        Integer addedCount = client.addElementHashMap(keyBytes, null, keys, values, CLIENT_ID, TEST_TIMEOUT).get();

        Assertions.assertEquals(2, addedCount);
        Thread.sleep(500);

        byte[] val1 = client.getContainerValue(keyBytes, null, "k1".getBytes(StandardCharsets.UTF_8)).get();
        byte[] val2 = client.getContainerValue(keyBytes, null, "k2".getBytes(StandardCharsets.UTF_8)).get();

        Assertions.assertEquals("v1", new String(val1, StandardCharsets.UTF_8));
        Assertions.assertEquals("v2", new String(val2, StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("Батчевое добавление элементов в OrderedMap с проверкой порядка")
    void testAddElementOrderedMapBatchReplication() throws ExecutionException, InterruptedException {
        String mapKey = "batchOrderedMapKey_" + UUID.randomUUID();
        byte[] keyBytes = mapKey.getBytes(StandardCharsets.UTF_8);

        Assertions.assertNotNull(client.createOrderedMap(mapKey, Map.of()));

        List<OrderedPayload> orderedKeys = List.of(
                OrderedPayload.of("score_100".getBytes(StandardCharsets.UTF_8), 100L),
                OrderedPayload.of("score_50".getBytes(StandardCharsets.UTF_8), 50L)
        );
        List<Payload> values = List.of(
                Payload.of("userMax".getBytes(StandardCharsets.UTF_8)),
                Payload.of("userMid".getBytes(StandardCharsets.UTF_8))
        );
        Thread.sleep(500);

        Integer addedCount = client.addElementOrderedMap(keyBytes, null, orderedKeys, values, CLIENT_ID, TEST_TIMEOUT).get();

        Assertions.assertEquals(2, addedCount);
        Thread.sleep(500);

        byte[] userMaxVal = client.getContainerValue(keyBytes, null, "score_100".getBytes(StandardCharsets.UTF_8)).get();

        Assertions.assertNotNull(userMaxVal);
        Assertions.assertEquals("userMax", new String(userMaxVal, StandardCharsets.UTF_8));
    }
}