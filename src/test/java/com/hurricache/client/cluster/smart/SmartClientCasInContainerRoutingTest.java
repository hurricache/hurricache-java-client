package com.hurricache.client.cluster.smart;

import com.hurricache.TestBaseCluster;
import com.hurricache.client.intf.Mode;
import com.hurricache.client.intf.Variant;
import com.hurricache.grpc.AtomicCasRes;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Кластерный роутинг CAS по полю контейнера: идёт на владельца, реплицируется на BACKUP.
 */
public class SmartClientCasInContainerRoutingTest extends TestBaseCluster {

    @Test
    @DisplayName("CAS on HashMap field routed to owner, replicated to BACKUP")
    void testCasFieldRoutedAndReplicated() throws Exception {
        String key = "cas_routing_" + UUID.randomUUID();
        Map<com.hurricache.client.intf.Payload, Variant> data = new HashMap<>();
        data.put(com.hurricache.client.intf.Payload.of("counter".getBytes()), Variant.ofLong(10));
        client.createMap(key, data).get();

        AtomicCasRes ok = client.compareAndSetInContainer(key, "counter".getBytes(), 10, 42).get();
        assertTrue(ok.getResult());

        assertEquals(42, client.setMode(Mode.MASTER).getInContainer(key, "counter".getBytes()).get().longValue());

        waitForCondition(() -> {
            try {
                return client.setMode(Mode.BACKUP).getInContainer(key, "counter".getBytes()).get().longValue() == 42;
            } catch (Exception e) {
                return false;
            }
        });

        // проигравший CAS не должен поменять значение ни на мастере, ни на бэкапе
        AtomicCasRes fail = client.compareAndSetInContainer(key, "counter".getBytes(), 10, 100).get();
        assertFalse(fail.getResult());
        assertEquals(42, client.setMode(Mode.MASTER).getInContainer(key, "counter".getBytes()).get().longValue());
    }
}
