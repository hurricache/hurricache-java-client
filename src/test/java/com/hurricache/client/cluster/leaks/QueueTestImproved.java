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

public class QueueTestImproved extends AdvancedTest {

    @Test
    void createQueueValueLoopLeakTest1() {
        // Easily test standard values by passing the creator lambda
        ConcurrentHashMap<String, Pair<String, KeyHintData>> keyValueMap
                = loadSynchronousLeakBaseline((k, vBytes) -> client.createQueue(k, List.of(Payload.of(vBytes))).get());

        AtomicInteger good = new AtomicInteger();
        keyValueMap.forEach((k, v) -> {
            try {
                Payload data = client.getHead(k, v.second).get();
                if (data != null && v.first.equals(new String(data.getValue(), StandardCharsets.UTF_8))) {
                    good.getAndIncrement();
                }
            } catch (Exception ignored) {
            }
        });

        assertLeakResults("createQueueValueLoopLeakTest1", good.get());

        AtomicInteger goodDelete = new AtomicInteger();
        keyValueMap.forEach((k, v) -> {
            try {
                Boolean b = client.remove(k, v.second).get();
                if (Boolean.TRUE.equals(b)) {
                    goodDelete.getAndIncrement();
                }
            } catch (Exception ignored) {
            }
        });

        assertLeakResults("createQueueValueLoopLeakTest2", goodDelete.get());
    }

    @Test
    void createQueueValueLoopLeakTest2() {
        // Easily test standard values by passing the creator lambda
        ConcurrentHashMap<String, Pair<String, KeyHintData>> keyValueMap
                = loadSynchronousLeakBaseline((k, vBytes) -> client.createQueue(k, List.of(Payload.of(vBytes))).get());

        AtomicInteger good = new AtomicInteger();
        keyValueMap.forEach((k, v) -> {
            try {
                Payload data = client.getAndRemoveFront(k, v.second).get();
                if (data != null && v.first.equals(new String(data.getValue(), StandardCharsets.UTF_8))) {
                    good.getAndIncrement();
                }
            } catch (Exception ignored) {
            }
        });

        assertLeakResults("createQueueValueLoopLeakTest", good.get());

        AtomicInteger goodDelete = new AtomicInteger();
        keyValueMap.forEach((k, v) -> {
            try {
                Boolean b = client.remove(k, v.second).get();
                if (Boolean.TRUE.equals(b)) {
                    goodDelete.getAndIncrement();
                }
            } catch (Exception ignored) {
            }
        });

        assertLeakResults("createQueueValueLoopLeakTest", goodDelete.get());
    }
}