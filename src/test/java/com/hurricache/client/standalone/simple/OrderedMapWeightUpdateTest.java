package com.hurricache.client.standalone.simple;

import com.hurricache.TestBase;
import com.hurricache.client.intf.OrderedPayload;
import com.hurricache.client.intf.Variant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Обновление веса элемента OrderedMap через gRPC:
 * incrementOrder (delta) и update (абсолютный вес), поиск — точное совпадение (ключ+вес),
 * payload элемента при смене веса сохраняется.
 */
public class OrderedMapWeightUpdateTest extends TestBase {

    private String newKey() {
        return "omap_weight_" + UUID.randomUUID();
    }

    private void createSample(String key) throws Exception {
        Map<OrderedPayload, Variant> data = new HashMap<>();
        data.put(OrderedPayload.of("k1".getBytes(), 10L), Variant.ofBytes("v1".getBytes()));
        data.put(OrderedPayload.of("k2".getBytes(), 20L), Variant.ofBytes("v2".getBytes()));
        data.put(OrderedPayload.of("k3".getBytes(), 30L), Variant.ofBytes("v3".getBytes()));
        client.createOrderedMap(key, data).get();
    }

    private Variant valueAt(String key, String member, long weight) throws Exception {
        Map<OrderedPayload, Variant> m = client.getOrderedMap(key).get();
        for (var e : m.entrySet()) {
            if (new String(e.getKey().getValue()).equals(member) && e.getKey().getOrder() == weight) {
                return e.getValue();
            }
        }
        return null;
    }

    private boolean hasWeight(String key, String member, long weight) throws Exception {
        return valueAt(key, member, weight) != null;
    }

    @Test
    @DisplayName("incrementOrder: weight moves, payload preserved")
    void testIncrementOrderMap() throws Exception {
        String key = newKey();
        createSample(key);

        Boolean ok = client.incrementOrder(key, OrderedPayload.of(10L, "k1".getBytes()), 5).get();
        assertTrue(ok);

        assertFalse(hasWeight(key, "k1", 10L));
        Variant v = valueAt(key, "k1", 15L);
        assertNotNull(v);
        assertEquals("v1", new String(v.bytesValue()));
    }

    @Test
    @DisplayName("update: absolute weight, payload preserved")
    void testSetAbsoluteWeightMap() throws Exception {
        String key = newKey();
        createSample(key);

        Boolean ok = client.update(key, OrderedPayload.of(20L, "k2".getBytes()), 99).get();
        assertTrue(ok);

        assertFalse(hasWeight(key, "k2", 20L));
        assertNotNull(valueAt(key, "k2", 99L));
    }

    @Test
    @DisplayName("NOT_FOUND when weight does not match")
    void testWrongCurrentWeightMap() throws Exception {
        String key = newKey();
        createSample(key);

        assertThrows(ExecutionException.class,
                     () -> client.incrementOrder(key, OrderedPayload.of(999L, "k1".getBytes()), 1).get());
    }

    @Test
    @DisplayName("OUT_OF_RANGE on underflow, value untouched")
    void testUnderflowMap() throws Exception {
        String key = newKey();
        createSample(key);

        assertThrows(ExecutionException.class,
                     () -> client.incrementOrder(key, OrderedPayload.of(10L, "k1".getBytes()), -11).get());
        assertTrue(hasWeight(key, "k1", 10L));
    }
}
