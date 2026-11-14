package com.hurricache.client.cluster.smart;

import com.hurricache.TestBaseCluster;
import com.hurricache.client.intf.Mode;
import com.hurricache.client.intf.OrderedPayload;
import com.hurricache.client.intf.OrderedVariant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Кластерный роутинг обновления веса OrderedSet: incrementOrder/update идут на владельца,
 * репликация доставляет новый вес на BACKUP.
 */
public class SmartClientOrderedSetWeightRoutingTest extends TestBaseCluster {

    private long weightOf(String key, String member, Mode mode) throws Exception {
        for (OrderedVariant ov : client.setMode(mode).getOrderedSet(key).get()) {
            if (new String(ov.bytesValue()).equals(member)) {
                return ov.getOrder();
            }
        }
        fail("member not found: " + member);
        return -1;
    }

    @Test
    @DisplayName("incrementOrder routed to owner, visible on MASTER and BACKUP")
    void testIncrementOrderRoutedAndReplicated() throws Exception {
        String key = "zset_weight_routing_" + UUID.randomUUID();
        client.createOrderedSet(key, List.of(
                OrderedVariant.ofBytes("alpha".getBytes(), 10L),
                OrderedVariant.ofBytes("beta".getBytes(), 20L)
        )).get();

        Boolean ok = client.incrementOrder(key, OrderedPayload.of(10L, "alpha".getBytes()), 7).get();
        assertTrue(ok);

        assertEquals(17L, weightOf(key, "alpha", Mode.MASTER));
        waitForCondition(() -> {
            try {
                return weightOf(key, "alpha", Mode.BACKUP) == 17L;
            } catch (Exception e) {
                return false;
            }
        });
    }

    @Test
    @DisplayName("update absolute weight routed to owner, replicated to BACKUP")
    void testSetAbsoluteWeightRoutedAndReplicated() throws Exception {
        String key = "zset_weight_abs_routing_" + UUID.randomUUID();
        client.createOrderedSet(key, List.of(OrderedVariant.ofBytes("beta".getBytes(), 20L))).get();

        Boolean ok = client.update(key, OrderedPayload.of(20L, "beta".getBytes()), 55).get();
        assertTrue(ok);

        assertEquals(55L, weightOf(key, "beta", Mode.MASTER));
        waitForCondition(() -> {
            try {
                return weightOf(key, "beta", Mode.BACKUP) == 55L;
            } catch (Exception e) {
                return false;
            }
        });
    }

    @Test
    @DisplayName("OUT_OF_RANGE underflow propagates through routing")
    void testUnderflowPropagates() throws Exception {
        String key = "zset_weight_oor_" + UUID.randomUUID();
        client.createOrderedSet(key, List.of(OrderedVariant.ofBytes("alpha".getBytes(), 5L))).get();

        assertThrows(ExecutionException.class,
                     () -> client.incrementOrder(key, OrderedPayload.of(5L, "alpha".getBytes()), -6).get());
        assertEquals(5L, weightOf(key, "alpha", Mode.MASTER));
    }
}
