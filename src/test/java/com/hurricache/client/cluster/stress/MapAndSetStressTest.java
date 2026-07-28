package com.hurricache.client.cluster.stress;

import com.hurricache.TestBaseCluster;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Mode;
import com.hurricache.client.intf.OrderedPayload;
import com.hurricache.client.intf.Payload;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class MapAndSetStressTest extends TestBaseCluster {

    private Payload p(String val) {
        return Payload.of(val.getBytes(StandardCharsets.UTF_8));
    }

    private byte[] bytes(String val) {
        return val.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void stressTestConcurrentSetAdditions() throws ExecutionException, InterruptedException {
        String setKey = "stressSet" + UUID.randomUUID();

        // Создаем базовый сет
        KeyHintData keyHint = client.setMode(Mode.MASTER)
                .createSet(setKey, List.of(p("base_item")))
                .get();

        int threadsCount = 20;
        int itemsPerThread = 50;
        List<CompletableFuture<Boolean>> futures = new ArrayList<>();

        // Штурмуем сет параллельно из разных потоков вперемешку через Master и Backup
        for (int i = 0; i < threadsCount; i++) {
            final int threadId = i;
            CompletableFuture<Boolean> future = CompletableFuture.supplyAsync(() -> {
                try {
                    List<Payload> batch = new ArrayList<>();
                    for (int j = 0; j < itemsPerThread; j++) {
                        batch.add(p("item_" + threadId + "_" + j));
                    }
                    Mode targetMode = (threadId % 2 == 0) ? Mode.MASTER : Mode.BACKUP;
                    return client.setMode(targetMode)
                            .addElement(setKey, keyHint, batch)
                            .get();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            futures.add(future);
        }

        // Ждем завершения всех потоков
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
        Thread.sleep(1500); // Даем время на репликацию и финализацию

        // Проверяем итоговый размер: 1 базовый + (20 потоков * 50 элементов) = 1001
        Integer size = client.setMode(Mode.MASTER).getSize(setKey, keyHint).get();
        Assertions.assertEquals(1 + (threadsCount * itemsPerThread), size);
    }

    @Test
    void stressTestConcurrentOrderedSetWeights() throws ExecutionException, InterruptedException {
        String zsetKey = "stressZSet" + UUID.randomUUID();

        KeyHintData keyHint = client.setMode(Mode.BACKUP)
                .createOrderedSet(zsetKey, List.of(OrderedPayload.of(0L, bytes("root"))))
                .get();

        int threadsCount = 15;
        int itemsPerThread = 40;
        List<CompletableFuture<Integer>> futures = new ArrayList<>();

        for (int i = 0; i < threadsCount; i++) {
            final int threadId = i;
            CompletableFuture<Integer> future = CompletableFuture.supplyAsync(() -> {
                try {
                    List<OrderedPayload> batch = new ArrayList<>();
                    for (int j = 0; j < itemsPerThread; j++) {
                        long weight = threadId * 1000L + j;
                        batch.add(OrderedPayload.of(weight, bytes("p_" + threadId + "_" + j)));
                    }
                    Mode targetMode = (threadId % 2 == 0) ? Mode.MASTER : Mode.BACKUP;
                    return client.setMode(targetMode)
                            .addElementWithWeight(zsetKey, keyHint, batch)
                            .get();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            futures.add(future);
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
        Thread.sleep(1500);

        Integer size = client.setMode(Mode.BACKUP).getSize(zsetKey, keyHint).get();
        Assertions.assertEquals(1 + (threadsCount * itemsPerThread), size);
    }
}