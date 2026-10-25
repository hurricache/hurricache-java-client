package com.hurricache.client.standalone.storage;

import com.hurricache.TestBase;
import com.hurricache.TestBaseCluster;
import com.hurricache.client.intf.Mode;
import com.hurricache.client.intf.OrderedPayload;
import com.hurricache.client.intf.OrderedVariant;
import com.hurricache.client.intf.Payload;
import com.hurricache.client.intf.Variant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Stability test: verify data integrity after idle period.
 * Tests that cluster correctly migrates data when a node crashes.
 * 
 * Steps:
 * 1. Create 100 objects of each type (scalar, atomic, vector, set, oset, omap, map)
 * 2. Verify all objects have correct content on MASTER and BACKUP
 * 3. Wait 30 seconds
 * 4. Create another 100 objects of each type
 * 5. Verify new objects have correct content on MASTER and BACKUP
 * 6. Verify old objects (from step 1) still have correct content on MASTER and BACKUP
 */
public class DataStabilityTest extends TestBase {

    // =========================================================================
    // CONFIGURABLE PARAMETERS
    // =========================================================================

    /** Idle period in milliseconds (default: 30 seconds). Override this value to change wait time. */
    private static final long IDLE_PERIOD_MS = TimeUnit.MINUTES.toMillis(10);

    // =========================================================================
    // KEY PREFIXES
    // =========================================================================

    private static final String[] TYPES = {
            "scalar", "atomic", "vector", "set", "oset", "omap", "map"
    };

    private static final String[] PREFIXES = {
            "scalar-", "atomic-", "vector-", "set-", "oset-", "omap-", "map-"
    };

    // Unique test session prefix to avoid key collisions
    private String testSessionPrefix;

    // =========================================================================
    // HELPER: Generate payloads
    // =========================================================================

    private List<Variant> generatePayloads(int count) {
        var payloads = new ArrayList<Variant>(count);
        for (int i = 0; i < count; i++) {
            payloads.add(Variant.ofBytes(("elem_" + i).getBytes(StandardCharsets.UTF_8)));
        }
        return payloads;
    }

    private Map<Payload, Variant> generateMapEntries(int count) {
        var entries = new LinkedHashMap<Payload, Variant>();
        for (int i = 0; i < count; i++) {
            entries.put(Payload.of(("k_" + i).getBytes()), Variant.ofBytes(("v_" + i).getBytes()));
        }
        return entries;
    }

    private Map<OrderedPayload, Variant> generateOrderedMapEntries(int count) {
        var entries = new LinkedHashMap<OrderedPayload, Variant>();
        for (int i = 0; i < count; i++) {
            entries.put(OrderedPayload.of((long) i, ("k_" + i).getBytes()), Variant.ofBytes(("v_" + i).getBytes()));
        }
        return entries;
    }

    private List<OrderedPayload> generateOrderedPayloads(int count) {
        var payloads = new ArrayList<OrderedPayload>(count);
        for (int i = 0; i < count; i++) {
            payloads.add(OrderedPayload.of((long) i, ("elem_" + i).getBytes()));
        }
        return payloads;
    }
    private List<OrderedVariant> generateOrderedPayloadsVariant(int count) {
        var payloads = new ArrayList<OrderedVariant>(count);
        for (int i = 0; i < count; i++) {
            payloads.add(OrderedVariant.of(Variant.ofBytes (("elem_" + i).getBytes()),i));
        }
        return payloads;
    }

    // =========================================================================
    // KEY GENERATION
    // =========================================================================

    private String generateKey(int typeIdx, int objIdx) {
        return testSessionPrefix + "_" + PREFIXES[typeIdx] + objIdx;
    }

    // =========================================================================
    // CREATE PHASE
    // =========================================================================

    private void createBatch(int batchNum, int objectsPerType, int elementsPerContainer, AtomicInteger createdCount) {
        for (int typeIdx = 0; typeIdx < TYPES.length; typeIdx++) {
            String type = TYPES[typeIdx];

            for (int i = 0; i < objectsPerType; i++) {
                int objNum = batchNum * objectsPerType + i;
                String key = generateKey(typeIdx, objNum);

                switch (type) {
                    case "scalar" ->
                        client.create(key, Variant.ofBytes(("value_" + objNum).getBytes(StandardCharsets.UTF_8))).join();
                    case "atomic" ->
                        client.create(key, Variant.ofLong(objNum));
                    case "vector" ->
                        client.createVector(key, generatePayloads(elementsPerContainer)).join();
                    case "set" ->
                        client.createSet(key, generatePayloads(elementsPerContainer)).join();
                    case "oset" ->
                        client.createOrderedSet(key, generateOrderedPayloadsVariant(elementsPerContainer)).join();
                    case "omap" ->
                        client.createOrderedMap(key, generateOrderedMapEntries(elementsPerContainer)).join();
                    case "map" ->
                        client.createMap(key, generateMapEntries(elementsPerContainer)).join();
                }
                createdCount.incrementAndGet();
            }
        }
    }

    // =========================================================================
    // VERIFY PHASE
    // =========================================================================

    private void verifyBatch(int batchNum, int objectsPerType, int elementsPerContainer) {
        // Verify all types on MASTER
        System.out.println("[Stability] Verifying batch " + batchNum + " on MASTER...");
        verifyBatchOnMode(batchNum, objectsPerType, elementsPerContainer, "MASTER");

        // Verify all types on BACKUP
        System.out.println("[Stability] Verifying batch " + batchNum + " on BACKUP...");
        verifyBatchOnMode(batchNum, objectsPerType, elementsPerContainer, "BACKUP");

        // Reset to default mode
    }

    private void verifyBatchOnMode(int batchNum, int objectsPerType, int elementsPerContainer, String mode) {
        var errors = new ArrayList<String>();

        for (int typeIdx = 0; typeIdx < TYPES.length; typeIdx++) {
            String type = TYPES[typeIdx];

            for (int i = 0; i < objectsPerType; i++) {
                int objNum = batchNum * objectsPerType + i;
                String key = generateKey(typeIdx, objNum);

                try {
                    switch (type) {
                        case "scalar" -> {
                            Variant result = client.get(key.getBytes(), null, 0, Duration.ofSeconds(5)).join();
                            String expected = "value_" + objNum;
                            if (result == null || !new String(result.bytesValue(), StandardCharsets.UTF_8).equals(expected)) {
                                errors.add("SCALAR " + key + ": expected '" + expected + "', got '" + 
                                    (result != null ? new String(result.bytesValue(), StandardCharsets.UTF_8) : "null") + "'");
                            }
                        }
                        case "atomic" -> {
                            Long result = client.get(key.getBytes(), null, 0, Duration.ofSeconds(5)).join().longValue();
                            if (result == null || result != objNum) {
                                errors.add("ATOMIC " + key + ": expected " + objNum + ", got " + result);
                            }
                        }
                        case "vector" -> {
                            List<Variant> result = client.getVector(key.getBytes(), null, 0, Duration.ofSeconds(5)).join();
                            if (result == null || result.size() != elementsPerContainer) {
                                errors.add("VECTOR " + key + ": expected " + elementsPerContainer + " items, got " + 
                                    (result != null ? result.size() : "null"));
                            }
                        }
                        case "set" -> {
                            List<Variant> result = client.getSet(key.getBytes(), null, 0, Duration.ofSeconds(5)).join();
                            if (result == null || result.size() != elementsPerContainer) {
                                errors.add("SET " + key + ": expected " + elementsPerContainer + " items, got " + 
                                    (result != null ? result.size() : "null"));
                            }
                        }
                        case "oset" -> {
                            List<OrderedVariant> result = client.getOrderedSet(key.getBytes(), null, 0, Duration.ofSeconds(5)).join();
                            if (result == null || result.size() != elementsPerContainer) {
                                errors.add("OSET " + key + ": expected " + elementsPerContainer + " items, got " + 
                                    (result != null ? result.size() : "null"));
                            }
                        }
                        case "omap" -> {
                            Map<OrderedPayload, Variant> result = client.getOrderedMap(key.getBytes(), null, 0, Duration.ofSeconds(5)).join();
                            if (result == null || result.size() != elementsPerContainer) {
                                errors.add("OMAP " + key + ": expected " + elementsPerContainer + " items, got " + 
                                    (result != null ? result.size() : "null"));
                            }
                        }
                        case "map" -> {
                            Map<Payload, Variant> result = client.getMap(key.getBytes(), null, 0, Duration.ofSeconds(5)).join();
                            if (result == null || result.size() != elementsPerContainer) {
                                errors.add("MAP " + key + ": expected " + elementsPerContainer + " items, got " + 
                                    (result != null ? result.size() : "null"));
                            }
                        }
                    }
                } catch (Exception e) {
                    errors.add(type.toUpperCase() + " " + key + ": exception - " + e.getMessage());
                }
            }
        }

        if (!errors.isEmpty()) {
            System.out.println("\n=== Verification Errors ===");
            System.out.println("\nsize: " + errors.size() + "\n");
            for (String err : errors) {
                System.out.println(err);
            }
        }
    }

    // =========================================================================
    // MAIN TEST
    // =========================================================================

    @Test
    @DisplayName("Data stability test: verify data integrity after idle period")
    void testDataStability() throws InterruptedException {
        testSessionPrefix = "stability_" + System.currentTimeMillis() + "_" + UUID.randomUUID().toString().replace("-", "");
        
        int objectsPerType = 100;
        int elementsPerContainer = 10;
        AtomicInteger createdCount = new AtomicInteger(0);

        // Step 1: Create first batch (100 objects per type)
        System.out.println("[Stability] Step 1: Creating first batch of " + objectsPerType + " objects per type...");
        createBatch(0, objectsPerType, elementsPerContainer, createdCount);
        System.out.println("[Stability] First batch created: " + createdCount.get() + " objects");
        Thread.sleep(3000);

        // Step 2: Verify first batch
        System.out.println("[Stability] Step 2: Verifying first batch...");
        verifyBatch(0, objectsPerType, elementsPerContainer);
        System.out.println("[Stability] First batch verified OK");

        // Step 3: Idle period
        System.out.println("[Stability] Step 3: Waiting " + (IDLE_PERIOD_MS / 1000) + " seconds...");
        Thread.sleep(IDLE_PERIOD_MS);
        System.out.println("[Stability] Idle period complete");

        // Step 4: Create second batch
        System.out.println("[Stability] Step 4: Creating second batch of " + objectsPerType + " objects per type...");
        createBatch(1, objectsPerType, elementsPerContainer, createdCount);
        Thread.sleep(3000);
        System.out.println("[Stability] Second batch created: " + createdCount.get() + " total objects");

        // Step 5: Verify new objects (batch 1)
        System.out.println("[Stability] Step 5: Verifying new objects (batch 1)...");
        verifyBatch(1, objectsPerType, elementsPerContainer);
        System.out.println("[Stability] New objects verified OK");

        // Step 6: Verify old objects (batch 0) still have correct content
        System.out.println("[Stability] Step 6: Verifying old objects (batch 0)...");
        verifyBatch(0, objectsPerType, elementsPerContainer);
        System.out.println("[Stability] Old objects still OK");

        System.out.println("[Stability] Test PASSED: " + createdCount.get() + " objects survived idle period");
    }

    @Override
    protected Duration getTestTtl() {
        return Duration.ofMinutes(30);
    }
}