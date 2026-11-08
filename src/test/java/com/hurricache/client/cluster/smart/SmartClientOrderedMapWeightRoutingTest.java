package com.hurricache.client.cluster.smart;

import com.hurricache.TestBaseCluster;
import com.hurricache.client.intf.Mode;
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
 * Кластерный роутинг обновления веса OrderedMap: incrementOrder/update идут на владельца,
 * репликация доставляет новый вес на BACKUP, payload сохраняется.
 */
public class SmartClientOrderedMapWeightRoutingTest extends TestBaseCluster {

    private Variant valueAt(String key, String member, long weight, Mode mode) throws Exception {
        Map<OrderedPayload, Variant> m = client.setMode(mode).getOrderedMap(key).get();
        for (var e : m.entrySet()) {
            if (new String(e.getKey().getValue()).equals(member) && e.getKey().getOrder() == weight) {
                return e.getValue();
            }
        }
        return null;
    }

    @Test
    @DisplayName("incrementOrder routed to owner, payload preserved, replicated to BACKUP")
    void testIncrementOrderRoutedAndReplicated() throws Exception {
        String key = "omap_weight_routing_" + UUID.randomUUID();
        Map<OrderedPayload, Variant> data = new HashMap<>();
        data.put(OrderedPayload.of("k1".getBytes(), 10L), Variant.ofBytes("v1".getBytes()));
        client.createOrderedMap(key, data).get();
        waitForCondition(() -> client.setMode(Mode.BACKUP).containsKey(key).get() && client.setMode(Mode.MASTER).containsKey(key).get());
        Boolean ok = client.incrementOrder(key, OrderedPayload.of(10L, "k1".getBytes()), 7).get();
        assertTrue(ok);

        Variant v = valueAt(key, "k1", 17L, Mode.MASTER);
        assertNotNull(v);
        assertEquals("v1", new String(v.bytesValue()));

        waitForCondition(() -> {
            try {
                Variant vb = valueAt(key, "k1", 17L, Mode.BACKUP);
                return vb != null && "v1".equals(new String(vb.bytesValue()));
            } catch (Exception e) {
                return false;
            }
        });
    }

    @Test
    @DisplayName("update absolute weight routed to owner, replicated to BACKUP")
    void testSetAbsoluteWeightRoutedAndReplicated() throws Exception {
        String key = "omap_weight_abs_routing_" + UUID.randomUUID();
        Map<OrderedPayload, Variant> data = new HashMap<>();
        data.put(OrderedPayload.of("k2".getBytes(), 20L), Variant.ofBytes("v2".getBytes()));
        client.createOrderedMap(key, data).get();

        Boolean ok = client.update(key, OrderedPayload.of(20L, "k2".getBytes()), 55).get();
        assertTrue(ok);

        assertNotNull(valueAt(key, "k2", 55L, Mode.MASTER));
        waitForCondition(() -> {
            try {
                return valueAt(key, "k2", 55L, Mode.BACKUP) != null;
            } catch (Exception e) {
                return false;
            }
        });
    }

    @Test
    @DisplayName("OUT_OF_RANGE underflow propagates through routing")
    void testUnderflowPropagates() throws Exception {
        String key = "omap_weight_oor_" + UUID.randomUUID();
        Map<OrderedPayload, Variant> data = new HashMap<>();
        data.put(OrderedPayload.of("k1".getBytes(), 5L), Variant.ofBytes("v1".getBytes()));
        client.createOrderedMap(key, data).get();

        assertThrows(ExecutionException.class,
                     () -> client.incrementOrder(key, OrderedPayload.of(5L, "k1".getBytes()), -6).get());
        assertNotNull(valueAt(key, "k1", 5L, Mode.MASTER));
    }
}