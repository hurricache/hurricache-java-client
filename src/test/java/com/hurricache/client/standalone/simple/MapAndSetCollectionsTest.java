package com.hurricache.client.standalone.simple;

import com.hurricache.TestBase;
import com.hurricache.client.intf.OrderedPayload;
import com.hurricache.client.intf.Payload;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class MapAndSetCollectionsTest extends TestBase {

    private Payload p(String val) {
        return Payload.of(val.getBytes(StandardCharsets.UTF_8));
    }

    private byte[] bytes(String val) {
        return val.getBytes(StandardCharsets.UTF_8);
    }

    private String str(Payload payload) {
        return new String(payload.getValue(), StandardCharsets.UTF_8);
    }

    @Test
    void testUnorderedMap() throws ExecutionException, InterruptedException {
        String mapKey = "testMap" + UUID.randomUUID();

        Map<Payload, Payload> initialData = Map.of(
                p("k1"), p("v1"),
                p("k2"), p("v2")
        );
        Assertions.assertNotNull(client.createMap(mapKey, initialData).get());

        Thread.sleep(500);

        Map<Payload, Payload> resultMap = client.streamMap(mapKey).get();

        Assertions.assertNotNull(resultMap);
        Assertions.assertEquals(2, resultMap.size());

        Assertions.assertTrue(resultMap.entrySet().stream()
                                      .anyMatch(e -> "k1".equals(str(e.getKey())) && "v1".equals(str(e.getValue()))));
    }

    @Test
    void testUnorderedMapSimple() throws ExecutionException, InterruptedException {
        String mapKey = "testMap" + UUID.randomUUID();

        Assertions.assertNotNull(client.createMap(mapKey, Map.of(p("user1"), p("active"))).get());

        Thread.sleep(500);

        Map<Payload, Payload> resultMap = client.streamMap(mapKey).get();

        Assertions.assertEquals(1, resultMap.size());
        Assertions.assertTrue(resultMap.entrySet().stream()
                                      .anyMatch(e -> "user1".equals(str(e.getKey())) && "active".equals(str(e.getValue()))));
    }

    @Test
    void testOrderedMap() throws ExecutionException, InterruptedException {
        String orderedMapKey = "testOrderedMap" + UUID.randomUUID();

        OrderedPayload op1 = OrderedPayload.of(100L, bytes("key1"));
        OrderedPayload op2 = OrderedPayload.of(200L, bytes("key2"));

        Assertions.assertNotNull(client.createOrderedMap(orderedMapKey, Map.of(op1, p("val1"), op2, p("val2"))).get());

        Thread.sleep(500);

        Map<OrderedPayload, Payload> resultMap = client.streamOrderedMap(orderedMapKey).get();

        Assertions.assertNotNull(resultMap);
        Assertions.assertEquals(2, resultMap.size());

        Integer size = client.getSize(orderedMapKey).get();
        Assertions.assertEquals(2, size);
    }

    @Test
    void testUnorderedSet() throws ExecutionException, InterruptedException {
        String setKey = "testSet" + UUID.randomUUID();

        Assertions.assertNotNull(client.createSet(setKey, List.of(p("item1"), p("item2"))).get());

        Thread.sleep(500);

        Integer added = client.addElementUnordered(setKey, List.of(p("item3"))).get();
        Assertions.assertTrue(added == 1);
        Thread.sleep(500);

        Integer size = client.getSize(setKey).get();
        Assertions.assertEquals(3, size);
        Thread.sleep(500);

        Integer removedCount = client.removeFromContainer(setKey.getBytes(StandardCharsets.UTF_8), null, p("item1").getValue()).get();
        Assertions.assertEquals(1, removedCount);
        Thread.sleep(500);

        Integer finalSize = client.getSize(setKey).get();
        Assertions.assertEquals(2, finalSize);
    }

    @Test
    void testUnorderedSetSimple() throws ExecutionException, InterruptedException {
        String setKey = "testSet" + UUID.randomUUID();

        Assertions.assertNotNull(client.createSet(setKey, List.of(p("elem1"))).get());

        Thread.sleep(500);

        client.addElementUnordered(setKey, List.of(p("elem2"))).get();

        Integer size = client.getSize(setKey).get();
        Assertions.assertEquals(2, size);
    }

    @Test
    void testOrderedSetRangeStreamingAndWeights() throws ExecutionException, InterruptedException {
        String zsetKey = "testOrderedSet" + UUID.randomUUID();

        OrderedPayload op1 = OrderedPayload.of(100L, bytes("player1"));
        OrderedPayload op2 = OrderedPayload.of(200L, bytes("player2"));

        Assertions.assertNotNull(client.createOrderedSet(zsetKey, List.of(op1, op2)).get());

        Thread.sleep(500);

        OrderedPayload op3 = OrderedPayload.of(150L, bytes("player3"));
        Integer added = client.addElementWithWeight(zsetKey, List.of(op3)).get();
        Assertions.assertTrue(added > 0);
        Thread.sleep(500);

        List<OrderedPayload> rangeResults = client.streamElementInRangeOrderedSet(zsetKey.getBytes(StandardCharsets.UTF_8), null, 100, 180, false, 0, null).get();

        Assertions.assertNotNull(rangeResults);
        Assertions.assertEquals(2, rangeResults.size());

        List<String> names = rangeResults.stream()
                .map(this::str)
                .toList();

        Assertions.assertTrue(names.contains("player1"));
        Assertions.assertTrue(names.contains("player3"));
        Assertions.assertFalse(names.contains("player2"));
    }

    @Test
    void testOrderedSetReverseRangeStreaming() throws ExecutionException, InterruptedException {
        String zsetKey = "testOrderedSetRev" + UUID.randomUUID();

        Assertions.assertNotNull(client.createOrderedSet(zsetKey, List.of(
                OrderedPayload.of(10L, bytes("a")),
                OrderedPayload.of(20L, bytes("b")),
                OrderedPayload.of(30L, bytes("c"))
        )).get());

        Thread.sleep(500);

        List<OrderedPayload> reverseResults = client.streamElementInRangeOrderedSet(zsetKey.getBytes(StandardCharsets.UTF_8), null, 10, 30, true, 0, null).get();

        Assertions.assertNotNull(reverseResults);
        Assertions.assertEquals(3, reverseResults.size());
        Assertions.assertArrayEquals(bytes("c"), reverseResults.get(0).getValue());
        Assertions.assertArrayEquals(bytes("b"), reverseResults.get(1).getValue());
        Assertions.assertArrayEquals(bytes("a"), reverseResults.get(2).getValue());
    }

    @Test
    void testOrderedSetNonReverseRangeStreaming() throws ExecutionException, InterruptedException {
        String zsetKey = "testOrderedSetRev" + UUID.randomUUID();

        Assertions.assertNotNull(client.createOrderedSet(zsetKey, List.of(
                OrderedPayload.of(10L, bytes("a")),
                OrderedPayload.of(20L, bytes("b")),
                OrderedPayload.of(30L, bytes("c"))
        )).get());

        Thread.sleep(500);

        List<OrderedPayload> reverseResults = client.streamElementInRangeOrderedSet(zsetKey.getBytes(StandardCharsets.UTF_8), null, 10, 30, false, 0, null).get();

        Assertions.assertNotNull(reverseResults);
        Assertions.assertEquals(3, reverseResults.size());

        Assertions.assertArrayEquals(bytes("a"), reverseResults.get(0).getValue());
        Assertions.assertArrayEquals(bytes("b"), reverseResults.get(1).getValue());
        Assertions.assertArrayEquals(bytes("c"), reverseResults.get(2).getValue());
    }
}