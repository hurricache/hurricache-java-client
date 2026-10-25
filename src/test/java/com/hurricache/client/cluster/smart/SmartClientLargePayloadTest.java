package com.hurricache.client.cluster.smart;

import com.hurricache.TestBaseCluster;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Mode;
import com.hurricache.client.intf.Variant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * SmartClient large payload replication tests.
 * Verifies that big keys/values and containers with big elements
 * survive the MASTER -> BACKUP replication byte-for-byte.
 */
public class SmartClientLargePayloadTest extends TestBaseCluster {

    // =========================================================================
    // 1. LARGE VALUES WITH ROUTING
    // =========================================================================

    @Test
    @DisplayName("100KB bytes value created on MASTER is replicated to BACKUP byte-for-byte")
    void testLargeValue100KBOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "large_value_100kb_" + UUID.randomUUID();
        Variant payload = createLargePayload(100 * 1024);

        KeyHintData hint = client.setMode(Mode.MASTER).create(key, payload).get();
        assertNotNull(hint);
        Thread.sleep(500);

        Variant masterValue = client.setMode(Mode.MASTER).get(key, hint).get();
        assertNotNull(masterValue);
        assertArrayEquals(payload.bytesValue(), masterValue.bytesValue());

        Variant backupValue = client.setMode(Mode.BACKUP).get(key, hint).get();
        assertNotNull(backupValue);
        assertArrayEquals(payload.bytesValue(), backupValue.bytesValue());
    }

    // =========================================================================
    // 2. LARGE KEYS WITH ROUTING
    // =========================================================================

    @Test
    @DisplayName("10KB key: create/get on MASTER and BACKUP")
    void testLargeKey10KBOnMasterThenBackup() throws ExecutionException, InterruptedException {
        byte[] key = createLargePayloadKey(10 * 1024);

        KeyHintData hint = client.setMode(Mode.MASTER).create(key, v("large_key_value")).get();
        assertNotNull(hint);
        Thread.sleep(500);

        Variant masterValue = client.setMode(Mode.MASTER).get(key, hint).get();
        assertNotNull(masterValue);
        assertEquals("large_key_value", str(masterValue));

        Variant backupValue = client.setMode(Mode.BACKUP).get(key, hint).get();
        assertNotNull(backupValue);
        assertEquals("large_key_value", str(backupValue));
    }

    // =========================================================================
    // 3. LIST WITH LARGE ELEMENTS WITH ROUTING
    // =========================================================================

    @Test
    @DisplayName("List of 50 elements x 10KB pushed on MASTER streams identically on BACKUP")
    void testListWithLargeElementsOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "large_list_50x10kb_" + UUID.randomUUID();

        KeyHintData hint = client.setMode(Mode.MASTER).createList(key, List.of()).get();
        assertNotNull(hint);

        List<Variant> data = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            byte[] element = new byte[10 * 1024];
            for (int j = 0; j < element.length; j++) {
                element[j] = (byte) ((i + j) % 256);
            }
            data.add(Variant.ofBytes(element));
        }

        Integer added = client.setMode(Mode.MASTER).pushTail(key, hint, data).get();
        assertEquals(50, added);
        Thread.sleep(500);

        List<Variant> masterStream = client.setMode(Mode.MASTER).getList(key, hint).get();
        assertEquals(data, masterStream);

        List<Variant> backupStream = client.setMode(Mode.BACKUP).getList(key, hint).get();
        assertEquals(masterStream, backupStream);
    }

    // =========================================================================
    // 4. BINARY BOUNDARY VALUES WITH ROUTING
    // =========================================================================

    @Test
    @DisplayName("Value with all byte values 0x00-0xFF replicated from MASTER to BACKUP")
    void testAllByteValuesOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "large_all_bytes_" + UUID.randomUUID();
        byte[] allBytes = new byte[256];
        for (int i = 0; i < 256; i++) {
            allBytes[i] = (byte) i;
        }

        KeyHintData hint = client.setMode(Mode.MASTER).create(key, Variant.ofBytes(allBytes)).get();
        assertNotNull(hint);
        Thread.sleep(500);

        Variant masterValue = client.setMode(Mode.MASTER).get(key, hint).get();
        assertNotNull(masterValue);
        assertArrayEquals(allBytes, masterValue.bytesValue());

        Variant backupValue = client.setMode(Mode.BACKUP).get(key, hint).get();
        assertNotNull(backupValue);
        assertArrayEquals(allBytes, backupValue.bytesValue());
    }
}
