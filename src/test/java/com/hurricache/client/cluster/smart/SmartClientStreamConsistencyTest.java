package com.hurricache.client.cluster.smart;

import com.hurricache.TestBaseCluster;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Mode;
import com.hurricache.client.intf.OrderedVariant;
import com.hurricache.client.intf.Payload;
import com.hurricache.client.intf.Variant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * SmartClient stream consistency tests for big containers.
 * Verifies that containers larger than the server streaming chunk (64 elements)
 * are replicated to BACKUP completely and read back identically on both roles.
 */
public class SmartClientStreamConsistencyTest extends TestBaseCluster {

    // =========================================================================
    // 1. LIST BIGGER THAN SERVER STREAMING CHUNK (64)
    // =========================================================================

    @Test
    @DisplayName("List of 200 elements pushed on MASTER streams identically on BACKUP")
    void testList200ElementsStreamConsistentOnMasterAndBackup() throws ExecutionException, InterruptedException {
        String key = "stream_list_200_" + UUID.randomUUID();

        KeyHintData hint = client.setMode(Mode.MASTER).createList(key, List.of()).get();
        assertNotNull(hint);
        Thread.sleep(500);
        List<Variant> data = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            data.add(v("item_" + i));
        }

        Integer added = client.setMode(Mode.MASTER).pushTail(key, hint, data).get();
        assertEquals(200, added);


        List<Variant> masterStream = client.setMode(Mode.MASTER).getList(key, hint).get();
        assertEquals(data, masterStream);
        Thread.sleep(500);
        List<Variant> backupStream = client.setMode(Mode.BACKUP).getList(key, hint).get();
        assertEquals(masterStream, backupStream);
    }

    // =========================================================================
    // 2. VECTOR BIGGER THAN SERVER STREAMING CHUNK (64)
    // =========================================================================

    @Test
    @DisplayName("Vector of 130 elements pushed on MASTER streams identically on BACKUP")
    void testVector130ElementsStreamConsistentOnMasterAndBackup() throws ExecutionException, InterruptedException {
        String key = "stream_vector_130_" + UUID.randomUUID();

        KeyHintData hint = client.setMode(Mode.MASTER).createVector(key, List.of()).get();
        assertNotNull(hint);
        Thread.sleep(500);
        List<Variant> data = new ArrayList<>();
        for (int i = 0; i < 130; i++) {
            data.add(vi(i));
        }

        Integer added = client.setMode(Mode.MASTER).pushTail(key, hint, data).get();
        assertEquals(130, added);
        Thread.sleep(500);

        List<Variant> masterStream = client.setMode(Mode.MASTER).getVector(key, hint).get();
        assertEquals(data, masterStream);

        List<Variant> backupStream = client.setMode(Mode.BACKUP).getVector(key, hint).get();
        assertEquals(masterStream, backupStream);
    }

    // =========================================================================
    // 3. MAP BIGGER THAN SERVER STREAMING CHUNK (64)
    // =========================================================================

    @Test
    @DisplayName("Map of 130 pairs created on MASTER streams as equal sets on MASTER and BACKUP")
    void testMap130PairsStreamConsistentOnMasterAndBackup() throws ExecutionException, InterruptedException {
        String key = "stream_map_130_" + UUID.randomUUID();

        Map<Payload, Variant> data = new LinkedHashMap<>();
        for (int i = 0; i < 130; i++) {
            data.put(p("key_" + i), v("value_" + i));
        }

        KeyHintData hint = client.setMode(Mode.MASTER).createMap(key, data).get();
        assertNotNull(hint);
        Thread.sleep(500);

        Map<Payload, Variant> masterStream = client.setMode(Mode.MASTER).getMap(key, hint).get();
        assertEquals(data, masterStream);

        Map<Payload, Variant> backupStream = client.setMode(Mode.BACKUP).getMap(key, hint).get();
        assertEquals(masterStream, backupStream);
    }

    // =========================================================================
    // 4. ORDERED SET BIGGER THAN SERVER STREAMING CHUNK (64)
    // =========================================================================

    @Test
    @DisplayName("Ordered set of 130 weighted elements: full and reverse ranges identical on MASTER and BACKUP")
    void testOrderedSet130ElementsRangeConsistentOnMasterAndBackup() throws ExecutionException, InterruptedException {
        String key = "stream_ordered_set_130_" + UUID.randomUUID();

        List<OrderedVariant> data = new ArrayList<>();
        for (int i = 0; i < 130; i++) {
            data.add(OrderedVariant.ofBytes(("element_" + i).getBytes(), (long) i));
        }

        KeyHintData hint = client.setMode(Mode.MASTER).createOrderedSet(key, data).get();
        assertNotNull(hint);
        Thread.sleep(500);

        // Full range, ascending
        List<OrderedVariant> masterRange = client.setMode(Mode.MASTER)
                .getElementInRangeOrderedSet(key, hint, 0L, 129L, false).get();
        assertEquals(130, masterRange.size());

        List<OrderedVariant> backupRange = client.setMode(Mode.BACKUP)
                .getElementInRangeOrderedSet(key, hint, 0L, 129L, false).get();
        assertEquals(masterRange, backupRange);

        // Full range, reverse
        List<OrderedVariant> masterReverse = client.setMode(Mode.MASTER)
                .getElementInRangeOrderedSet(key, hint, 0L, 129L, true).get();
        assertEquals(130, masterReverse.size());

        List<OrderedVariant> backupReverse = client.setMode(Mode.BACKUP)
                .getElementInRangeOrderedSet(key, hint, 0L, 129L, true).get();
        assertEquals(masterReverse, backupReverse);
    }
}