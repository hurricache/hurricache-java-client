package com.hurricache.client.cluster.leaks;

import com.hurricache.client.cluster.AdvancedTest;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Payload;
import com.hurricache.utils.Pair;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class VectorTestImproved extends AdvancedTest {

    @Test
    void createVectorValueLoopLeakTest1() {
        // Easily test standard values by passing the creator lambda
        ConcurrentHashMap<String, Pair<String, KeyHintData>> keyValueMap
                = loadSynchronousLeakBaseline((k, vBytes) -> client.createVector(k, List.of(Payload.of(vBytes))).get());

        AtomicInteger good = new AtomicInteger();
        keyValueMap.forEach((k, v) -> {
            try {
                Payload data = client.getHead(k, v.second).get();
                if (data != null && data.getValue() != null && v.first.equals(new String(data.getValue(), StandardCharsets.UTF_8))) {
                    good.getAndIncrement();
                }
            } catch (Exception ignored) {
            }
        });

        assertLeakResults("createVectorValueLoopLeakTest1", good.get());

        AtomicInteger goodDelete = new AtomicInteger();
        keyValueMap.forEach((k, v) -> {
            try {
                Boolean b = client.remove(k, v.second).get();
                if (b != null && b) {
                    goodDelete.getAndIncrement();
                }
            } catch (Exception ignored) {
            }
        });

        assertLeakResults("createVectorValueLoopLeakTest1_Delete", goodDelete.get());
    }

    @Test
    void createVectorValueLoopLeakTest2() {
        // Easily test standard values by passing the creator lambda
        ConcurrentHashMap<String, Pair<String, KeyHintData>> keyValueMap
                = loadSynchronousLeakBaseline((k, vBytes) -> client.createVector(k, List.of(Payload.of(vBytes))).get());

        AtomicInteger good = new AtomicInteger();
        keyValueMap.forEach((k, v) -> {
            try {
                Payload data = client.getAndRemoveFront(k, v.second).get();
                if (data != null && data.getValue() != null && v.first.equals(new String(data.getValue(), StandardCharsets.UTF_8))) {
                    good.getAndIncrement();
                }
            } catch (Exception ignored) {
            }
        });

        assertLeakResults("createVectorValueLoopLeakTest2", good.get());

        AtomicInteger goodDelete = new AtomicInteger();
        keyValueMap.forEach((k, v) -> {
            try {
                Boolean b = client.remove(k, v.second).get();
                if (b != null && b) {
                    goodDelete.getAndIncrement();
                }
            } catch (Exception ignored) {
            }
        });

        assertLeakResults("createVectorValueLoopLeakTest2_Delete", goodDelete.get());
    }

    @Test
    void createVectorValueLoopLeakTest3() {
        // Easily test standard values by passing the creator lambda
        ConcurrentHashMap<String, Pair<String, KeyHintData>> keyValueMap
                = loadSynchronousLeakBaseline((k, vBytes) -> client.createVector(k, List.of(Payload.of(vBytes))).get());

        AtomicInteger good = new AtomicInteger();
        keyValueMap.forEach((k, v) -> {
            try {
                Payload data = client.getAndRemoveElementAtPosition(client.serializeKey(k), v.second, 0).get();
                if (data != null && data.getValue() != null && v.first.equals(new String(data.getValue(), StandardCharsets.UTF_8))) {
                    good.getAndIncrement();
                }
            } catch (Exception ignored) {
            }
        });

        assertLeakResults("createVectorValueLoopLeakTest3", good.get());

        AtomicInteger goodDelete = new AtomicInteger();
        keyValueMap.forEach((k, v) -> {
            try {
                Boolean b = client.remove(k, v.second).get();
                if (b != null && b) {
                    goodDelete.getAndIncrement();
                }
            } catch (Exception ignored) {
            }
        });

        assertLeakResults("createVectorValueLoopLeakTest3_Delete", goodDelete.get());
    }

    @Test
    void createListValueLoopLeakTest4() {
        // Easily test standard values by passing the creator lambda
        ConcurrentHashMap<String, Pair<String, KeyHintData>> keyValueMap
                = loadSynchronousLeakBaseline((k, vBytes) -> client.createVector(k, List.of(Payload.of(vBytes))).get());

        AtomicInteger good = new AtomicInteger();
        keyValueMap.forEach((k, v) -> {
            try {
                Payload data = client.getAndRemoveTail(client.serializeKey(k), v.second).get();
                if (data != null && data.getValue() != null && v.first.equals(new String(data.getValue(), StandardCharsets.UTF_8))) {
                    good.getAndIncrement();
                }
            } catch (Exception ignored) {
            }
        });

        assertLeakResults("createListValueLoopLeakTest4", good.get());

        AtomicInteger goodDelete = new AtomicInteger();
        keyValueMap.forEach((k, v) -> {
            try {
                Boolean b = client.remove(k, v.second).get();
                if (b != null && b) {
                    goodDelete.getAndIncrement();
                }
            } catch (Exception ignored) {
            }
        });

        assertLeakResults("createListValueLoopLeakTest4_Delete", goodDelete.get());
    }

    /* ========================================================================
       Generic Code-Reuse Engines & Assertions
       ======================================================================== */
}