package com.hurricache.client.cluster.smart;

import com.hurricache.TestBaseCluster;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Mode;
import com.hurricache.client.intf.OrderedPayload;
import com.hurricache.client.intf.Payload;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class MapAndSetCollectionsTest extends TestBaseCluster {

    private Payload p(String val) {
        return Payload.of(val.getBytes(StandardCharsets.UTF_8));
    }

    private byte[] bytes(String val) {
        return val.getBytes(StandardCharsets.UTF_8);
    }

    private String str(Payload payload) {
        return new String(payload.getValue(), StandardCharsets.UTF_8);
    }

    // =========================================================================
    // UNORDERED MAP TESTS
    // =========================================================================

    @Test
    void testUnorderedMapCreateOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String mapKey = "testMap" + UUID.randomUUID();

        // 1. Create Map on Master
        Map<Payload, Payload> initialData = Map.of(
                p("k1"), p("v1"),
                p("k2"), p("v2")
        );
        KeyHintData keyHint = client.setMode(Mode.MASTER)
                .createMap(mapKey, initialData)
                .get();

        Thread.sleep(500); // Allow replication

        // 2. Read Map on Backup
        Map<Payload, Payload> resultMap = client.setMode(Mode.BACKUP)
                .streamMap(mapKey, keyHint)
                .get();

        Assertions.assertNotNull(resultMap);
        Assertions.assertEquals(2, resultMap.size());

        // 3. Get Size on Backup
        Integer size = client.setMode(Mode.BACKUP).getSize(mapKey, keyHint).get();
        Assertions.assertEquals(2, size);

        // Check contents
        Assertions.assertTrue(resultMap.entrySet().stream()
                                      .anyMatch(e -> "k1".equals(str(e.getKey())) && "v1".equals(str(e.getValue()))));


    }

    @Test
    void testUnorderedMapCreateOnBackupValidateOnMaster() throws ExecutionException, InterruptedException {
        String mapKey = "testMap" + UUID.randomUUID();

        // 1. Create Map on Backup
        KeyHintData keyHint = client.setMode(Mode.BACKUP)
                .createMap(mapKey, Map.of(p("user1"), p("active")))
                .get();

        Thread.sleep(500);

        // 2. Validate on Master
        Map<Payload, Payload> resultMap = client.setMode(Mode.MASTER)
                .streamMap(mapKey, keyHint)
                .get();

        Assertions.assertEquals(1, resultMap.size());
        Assertions.assertTrue(resultMap.entrySet().stream()
                                      .anyMatch(e -> "user1".equals(str(e.getKey())) && "active".equals(str(e.getValue()))));
    }

    // =========================================================================
    // ORDERED MAP TESTS
    // =========================================================================

    @Test
    void testOrderedMapCreateOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String orderedMapKey = "testOrderedMap" + UUID.randomUUID();

        // OrderedPayload constructed via OrderedPayload.of(Long order, byte[] value)
        OrderedPayload op1 = OrderedPayload.of(100L, bytes("key1"));
        OrderedPayload op2 = OrderedPayload.of(200L, bytes("key2"));

        KeyHintData keyHint = client.setMode(Mode.MASTER)
                .createOrderedMap(orderedMapKey, Map.of(op1, p("val1"), op2, p("val2")))
                .get();

        Thread.sleep(500);

        // Stream OrderedMap from Backup
        Map<OrderedPayload, Payload> resultMap = client.setMode(Mode.BACKUP)
                .streamOrderedMap(orderedMapKey, keyHint)
                .get();

        Assertions.assertNotNull(resultMap);
        Assertions.assertEquals(2, resultMap.size());

        Integer size = client.setMode(Mode.BACKUP).getSize(orderedMapKey, keyHint).get();
        Assertions.assertEquals(2, size);
    }

    // =========================================================================
    // UNORDERED SET TESTS
    // =========================================================================

    @Test
    void testUnorderedSetCreateOnMasterMutateOnBackup() throws ExecutionException, InterruptedException {
        String setKey = "testSet" + UUID.randomUUID();

        // 1. Create Set on Master with initial values
        KeyHintData keyHint = client.setMode(Mode.MASTER)
                .createSet(setKey, List.of(p("item1"), p("item2")))
                .get();

        Thread.sleep(500);

        // 2. Add elements (sadd) on Backup
        Integer added = client.setMode(Mode.BACKUP)
                .addElementUnordered(setKey, keyHint, List.of(p("item3")))
                .get();
        Assertions.assertTrue(added == 1);
        Thread.sleep(500);
        // 3. Verify Size on Master
        Integer size = client.setMode(Mode.MASTER).getSize(setKey, keyHint).get();
        Assertions.assertEquals(3, size);
        Thread.sleep(500);
        // 4. Remove element (srem) on Backup
        Integer removedCount = client.setMode(Mode.BACKUP)
                .removeFromContainer(client.serializeKey(setKey), keyHint, p("item1").getValue())
                .get();
        Assertions.assertEquals(1, removedCount);
        Thread.sleep(500);

        // 5. Verify Size after removal
        Integer finalSize = client.setMode(Mode.MASTER).getSize(setKey, keyHint).get();
        Assertions.assertEquals(2, finalSize);
    }

    @Test
    void testUnorderedSetCreateOnBackupMutateOnMaster() throws ExecutionException, InterruptedException {
        String setKey = "testSet" + UUID.randomUUID();

        // Create on Backup
        KeyHintData keyHint = client.setMode(Mode.BACKUP)
                .createSet(setKey, List.of(p("elem1")))
                .get();

        Thread.sleep(500);

        // Add on Master
        client.setMode(Mode.MASTER)
                .addElementUnordered(setKey, keyHint, List.of(p("elem2")))
                .get();

        Integer size = client.setMode(Mode.MASTER).getSize(setKey, keyHint).get();
        Assertions.assertEquals(2, size);
    }

    // =========================================================================
    // ORDERED SET TESTS
    // =========================================================================

    @Test
    void testOrderedSetRangeStreamingAndWeights() throws ExecutionException, InterruptedException {
        String zsetKey = "testOrderedSet" + UUID.randomUUID();

        // 1. Create OrderedSet with initial weighted payloads on Master
        OrderedPayload op1 = OrderedPayload.of(100L, bytes("player1"));
        OrderedPayload op2 = OrderedPayload.of(200L, bytes("player2"));

        KeyHintData keyHint = client.setMode(Mode.MASTER)
                .createOrderedSet(zsetKey, List.of(op1, op2))
                .get();

        Thread.sleep(500);

        // 2. Add more elements with weights on Backup
        OrderedPayload op3 = OrderedPayload.of(150L, bytes("player3"));
        Integer added = client.setMode(Mode.BACKUP)
                .addElementWithWeight(zsetKey, keyHint, List.of(op3))
                .get();
        Assertions.assertTrue(added > 0);
        Thread.sleep(500);
        // 3. Range Stream elements by weight [100..180] on Master -> Should return player1 (100) and player3 (150)
        List<OrderedPayload> rangeResults = client.setMode(Mode.MASTER)
                .streamElementInRangeOrderedSet(client.serializeKey(zsetKey), keyHint, 100, 180, false, client.getDefaultClientId(), client.getDefaultTimeout())
                .get();

        Assertions.assertNotNull(rangeResults);
        Assertions.assertEquals(2, rangeResults.size());

        List<String> names = rangeResults.stream()
                .map(this::str)
                .toList();

        Assertions.assertTrue(names.contains("player1"));
        Assertions.assertTrue(names.contains("player3"));
        Assertions.assertFalse(names.contains("player2")); // 200 is out of range [100..180]
    }

    @Test
    void testOrderedSetReverseRangeStreaming() throws ExecutionException, InterruptedException {
        String zsetKey = "testOrderedSetRev" + UUID.randomUUID();

        KeyHintData keyHint = client.setMode(Mode.BACKUP)
                .createOrderedSet(zsetKey, List.of(
                        OrderedPayload.of(10L, bytes("a")),
                        OrderedPayload.of(20L, bytes("b")),
                        OrderedPayload.of(30L, bytes("c"))
                ))
                .get();

        Thread.sleep(500);

        // Reverse range query on Master
        List<OrderedPayload> reverseResults = client.setMode(Mode.MASTER)
                .streamElementInRangeOrderedSet(client.serializeKey(zsetKey), keyHint, 10, 30, true, client.getDefaultClientId(), client.getDefaultTimeout())
                .get();

        Assertions.assertNotNull(reverseResults);
        Assertions.assertEquals(3, reverseResults.size());
        Assertions.assertArrayEquals(bytes("c"), reverseResults.get(0).getValue());
        Assertions.assertArrayEquals(bytes("b"), reverseResults.get(1).getValue());
        Assertions.assertArrayEquals(bytes("a"), reverseResults.get(2).getValue());
    }

    @Test
    void testOrderedSetNonReverseRangeStreaming() throws ExecutionException, InterruptedException {
        String zsetKey = "testOrderedSetRev" + UUID.randomUUID();

        KeyHintData keyHint = client.setMode(Mode.BACKUP)
                .createOrderedSet(zsetKey, List.of(
                        OrderedPayload.of(10L, bytes("a")),
                        OrderedPayload.of(20L, bytes("b")),
                        OrderedPayload.of(30L, bytes("c"))
                ))
                .get();

        Thread.sleep(500);

        // Reverse range query on Master
        List<OrderedPayload> reverseResults = client.setMode(Mode.MASTER)
                .streamElementInRangeOrderedSet(client.serializeKey(zsetKey), keyHint, 10, 30, false, client.getDefaultClientId(), client.getDefaultTimeout())
                .get();

        Assertions.assertNotNull(reverseResults);
        Assertions.assertEquals(3, reverseResults.size());

        Assertions.assertArrayEquals(bytes("a"), reverseResults.get(0).getValue());
        Assertions.assertArrayEquals(bytes("b"), reverseResults.get(1).getValue());
        Assertions.assertArrayEquals(bytes("c"), reverseResults.get(2).getValue());
    }

    // =========================================================================
    // ORDERED MAP RANGE & REVERSE STREAMING TESTS
    // =========================================================================

//    @Test
//    void testOrderedMapRangeStreaming() throws ExecutionException, InterruptedException {
//        String mapKey = "testOrderedMapRange" + UUID.randomUUID();
//
//        OrderedPayload k1 = OrderedPayload.of(10L, bytes("k1"));
//        OrderedPayload k2 = OrderedPayload.of(20L, bytes("k2"));
//        OrderedPayload k3 = OrderedPayload.of(30L, bytes("k3"));
//
//        Map<OrderedPayload, Payload> initialData = Map.of(
//                k1, p("v1"),
//                k2, p("v2"),
//                k3, p("v3")
//        );
//
//        KeyHintData keyHint = client.setMode(Mode.BACKUP)
//                .createOrderedMap(mapKey, initialData)
//                .get();
//
//        Thread.sleep(500);
//
//        // Прямой стриминг диапазонов весов [10..30]
//        Map<OrderedPayload, Payload> rangeResults = client.setMode(Mode.MASTER)
//                .streamElementInRangeOrderedMap(
//                        client.serializeKey(mapKey),
//                        keyHint,
//                        10,
//                        30,
//                        false,
//                        client.getDefaultClientId(),
//                        client.getDefaultTimeout()
//                )
//                .get();
//
//        Assertions.assertNotNull(rangeResults);
//        Assertions.assertEquals(3, rangeResults.size());
//
//        // Проверка корректности маппинга ключей и значений
//        Assertions.assertEquals("v1", str(rangeResults.get(k1)));
//        Assertions.assertEquals("v2", str(rangeResults.get(k2)));
//        Assertions.assertEquals("v3", str(rangeResults.get(k3)));
//    }
//
//    @Test
//    void testOrderedMapReverseRangeStreaming() throws ExecutionException, InterruptedException {
//        String mapKey = "testOrderedMapRev" + UUID.randomUUID();
//
//        OrderedPayload k1 = OrderedPayload.of(10L, bytes("a"));
//        OrderedPayload k2 = OrderedPayload.of(20L, bytes("b"));
//        OrderedPayload k3 = OrderedPayload.of(30L, bytes("c"));
//
//        Map<OrderedPayload, Payload> initialData = Map.of(
//                k1, p("val_a"),
//                k2, p("val_b"),
//                k3, p("val_c")
//        );
//
//        KeyHintData keyHint = client.setMode(Mode.BACKUP)
//                .createOrderedMap(mapKey, initialData)
//                .get();
//
//        Thread.sleep(500);
//
//        // Реверсивный стриминг по диапазону весов [10..30]
//        Map<OrderedPayload, Payload> reverseResults = client.setMode(Mode.MASTER)
//                .streamElementInRangeOrderedMap(
//                        client.serializeKey(mapKey),
//                        keyHint,
//                        10,
//                        30,
//                        true,
//                        client.getDefaultClientId(),
//                        client.getDefaultTimeout()
//                )
//                .get();
//
//        Assertions.assertNotNull(reverseResults);
//        Assertions.assertEquals(3, reverseResults.size());
//
//        // Проверяем порядок ключей в итераторе (должен быть c -> b -> a)
//        List<OrderedPayload> keysInOrder = List.copyOf(reverseResults.keySet());
//        Assertions.assertArrayEquals(bytes("c"), keysInOrder.get(0).getValue());
//        Assertions.assertArrayEquals(bytes("b"), keysInOrder.get(1).getValue());
//        Assertions.assertArrayEquals(bytes("a"), keysInOrder.get(2).getValue());
//
//        // Проверяем значения
//        Assertions.assertEquals("val_c", str(reverseResults.get(keysInOrder.get(0))));
//        Assertions.assertEquals("val_b", str(reverseResults.get(keysInOrder.get(1))));
//        Assertions.assertEquals("val_a", str(reverseResults.get(keysInOrder.get(2))));
//    }
//
//    @Test
//    void testOrderedMapRangeSubSetStreaming() throws ExecutionException, InterruptedException {
//        String mapKey = "testOrderedMapSubSet" + UUID.randomUUID();
//
//        OrderedPayload k1 = OrderedPayload.of(100L, bytes("user1"));
//        OrderedPayload k2 = OrderedPayload.of(200L, bytes("user2"));
//        OrderedPayload k3 = OrderedPayload.of(300L, bytes("user3"));
//
//        KeyHintData keyHint = client.setMode(Mode.MASTER)
//                .createOrderedMap(mapKey, Map.of(k1, p("score100"), k2, p("score200"), k3, p("score300")))
//                .get();
//
//        Thread.sleep(500);
//
//        // Стриминг поддиапазона весов [150..350] на BACKUP с reverse = true (должны вернуться user3 и user2)
//        Map<OrderedPayload, Payload> subSetResults = client.setMode(Mode.BACKUP)
//                .streamElementInRange(
//                        client.serializeKey(mapKey),
//                        keyHint,
//                        150,
//                        350,
//                        true,
//                        client.getDefaultClientId(),
//                        client.getDefaultTimeout()
//                )
//                .get();
//
//        Assertions.assertNotNull(subSetResults);
//        Assertions.assertEquals(2, subSetResults.size());
//
//        List<OrderedPayload> keys = List.copyOf(subSetResults.keySet());
//        Assertions.assertArrayEquals(bytes("user3"), keys.get(0).getValue());
//        Assertions.assertArrayEquals(bytes("user2"), keys.get(1).getValue());
//    }
}