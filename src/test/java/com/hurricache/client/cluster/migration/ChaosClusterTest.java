package com.hurricache.client.cluster.migration;

import com.hurricache.TestBaseCluster;
import com.hurricache.client.FastCacheAsyncSmartClient;
import com.hurricache.client.intf.OrderedPayload;
import com.hurricache.client.intf.Payload;
import com.hurricache.grpc.*;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Chaos test for HurriCache cluster.
 * Runs concurrent operations on all container types, counts errors.
 * 
 * Default config: 30 min duration, 2 threads, 100ms interval, 50K total keys, 50% prefill.
 */
public class ChaosClusterTest extends TestBaseCluster {

    // =========================================================================
    // CONFIGURATION
    // =========================================================================

    public static class ChaosTestConfig {
        public long durationMs = Duration.ofHours(10).toMillis();   // 30 minutes
        public int numThreads = 2;                    // 2 threads for stable chaos
        public long operationIntervalMs = 100;        // 100ms between operations
        public int totalKeys = 50_000;                // total keys
        public int elementsPerContainer = 100;        // elements per container
        public boolean enablePrefill = true;          // enable prefill phase
        public int prefillRatio = 50;                 // 50% of keys
    }

    // =========================================================================
    // STATISTICS
    // =========================================================================

    public static class ChaosTestStats {
        final AtomicInteger totalOperations = new AtomicInteger(0);
        final AtomicInteger successfulOperations = new AtomicInteger(0);
        final AtomicInteger timeoutErrors = new AtomicInteger(0);
        final AtomicInteger permissionDeniedErrors = new AtomicInteger(0);
        final AtomicInteger cantLockErrors = new AtomicInteger(0);
        final AtomicInteger cantUnlockErrors = new AtomicInteger(0);
        final AtomicInteger notFoundErrors = new AtomicInteger(0);
        final AtomicInteger otherErrors = new AtomicInteger(0);
        final AtomicLong totalDurationMs = new AtomicLong(0);

        // Prefill stats
        final AtomicInteger prefillCreated = new AtomicInteger(0);
        final AtomicInteger prefillFailed = new AtomicInteger(0);
    }

    // =========================================================================
    // KEY GENERATORS
    // =========================================================================

    private static final String[] KEY_PREFIXES = {
            "scalar-", "vector-", "list-", "queue-", "set-", "map-", "omap-", "oset-", "atomic-"
    };

    private static final String[] PRELOAD_TYPES = {
            "scalar", "vector", "list", "queue", "set", "map", "omap", "oset", "atomic"
    };

    // =========================================================================
    // HELPER: Generate payloads
    // =========================================================================

    private List<Payload> generatePayloads(Random random, int count) {
        var payloads = new ArrayList<Payload>(count);
        for (int i = 0; i < count; i++) {
            payloads.add(Payload.of(("elem_" + random.nextInt(100_000)).getBytes(StandardCharsets.UTF_8)));
        }
        return payloads;
    }

    private Map<Payload, Payload> generateMapEntries(Random random, int count) {
        var entries = new LinkedHashMap<Payload, Payload>();
        for (int i = 0; i < count; i++) {
            entries.put(Payload.of(("k_" + random.nextInt(1_000_000)).getBytes()), Payload.of(("v_" + random.nextInt(1_000_000)).getBytes()));
        }
        return entries;
    }

    private Map<OrderedPayload, Payload> generateOrderedMapEntries(Random random, int count) {
        var entries = new LinkedHashMap<OrderedPayload, Payload>();
        for (int i = 0; i < count; i++) {
            entries.put(OrderedPayload.of((long) random.nextInt(1_000_000), ("k_" + random.nextInt(1_000_000)).getBytes()), Payload.of(("v_" + random.nextInt(1_000_000)).getBytes()));
        }
        return entries;
    }

    private List<OrderedPayload> generateOrderedPayloads(Random random, int count) {
        var payloads = new ArrayList<OrderedPayload>(count);
        for (int i = 0; i < count; i++) {
            payloads.add(OrderedPayload.of((long) random.nextInt(1_000_000), ("elem_" + random.nextInt(1_000_000)).getBytes()));
        }
        return payloads;
    }

    // =========================================================================
    // CHAOS OPERATION INTERFACE
    // =========================================================================

    @FunctionalInterface
    private interface ChaosOperation {
        void execute(String key, int clientId, ChaosTestStats stats, FastCacheAsyncSmartClient client);
        default String getPrefix() { return ""; }
    }

    // =========================================================================
    // OPERATION TYPES
    // =========================================================================

    private ChaosOperation[] OPERATION_TYPES;

    /**
     * Instance initializer - creates operation types with access to 'this.client'.
     */
    {
        OPERATION_TYPES = createOperationTypes();
    }

    private ChaosOperation[] createOperationTypes() {
        Random rand = new Random();
        return new ChaosOperation[] {
            // Scalar operations
            new ChaosOperation() {
                public String getPrefix() { return "scalar-"; }
                public void execute(String key, int clientId, ChaosTestStats stats, FastCacheAsyncSmartClient c) {
                    c.createKeyValue(key, ("val_" + clientId).getBytes(StandardCharsets.UTF_8)).join();
                }
            },
            new ChaosOperation() {
                public String getPrefix() { return "scalar-"; }
                public void execute(String key, int clientId, ChaosTestStats stats, FastCacheAsyncSmartClient c) {
                    c.getValue(key.getBytes(), null, clientId, Duration.ofSeconds(5)).join();
                }
            },
            new ChaosOperation() {
                public String getPrefix() { return "scalar-"; }
                public void execute(String key, int clientId, ChaosTestStats stats, FastCacheAsyncSmartClient c) {
                    c.updateKeyValue(key.getBytes(), null, ("upd_" + System.currentTimeMillis()).getBytes(), null, clientId, Duration.ofSeconds(5)).join();
                }
            },
            new ChaosOperation() {
                public String getPrefix() { return "scalar-"; }
                public void execute(String key, int clientId, ChaosTestStats stats, FastCacheAsyncSmartClient c) {
                    c.remove(key.getBytes(), null, clientId).join();
                }
            },

            // Vector operations
            new ChaosOperation() {
                public String getPrefix() { return "vector-"; }
                public void execute(String key, int clientId, ChaosTestStats stats, FastCacheAsyncSmartClient c) {
                    c.createVector(key, generatePayloads(rand, 10)).join();
                }
            },
            new ChaosOperation() {
                public String getPrefix() { return "vector-"; }
                public void execute(String key, int clientId, ChaosTestStats stats, FastCacheAsyncSmartClient c) {
                    c.addElementToTail(key, null, List.of(Payload.of(("item_" + System.currentTimeMillis()).getBytes())), clientId).join();
                }
            },
            new ChaosOperation() {
                public String getPrefix() { return "vector-"; }
                public void execute(String key, int clientId, ChaosTestStats stats, FastCacheAsyncSmartClient c) {
                    c.streamVector(key.getBytes(), null, clientId, Duration.ofSeconds(5)).join();
                }
            },
            new ChaosOperation() {
                public String getPrefix() { return "vector-"; }
                public void execute(String key, int clientId, ChaosTestStats stats, FastCacheAsyncSmartClient c) {
                    c.getElementAtPosition(key.getBytes(), null, rand.nextInt(10), clientId, Duration.ofSeconds(5)).join();
                }
            },

            // List operations
            new ChaosOperation() {
                public String getPrefix() { return "list-"; }
                public void execute(String key, int clientId, ChaosTestStats stats, FastCacheAsyncSmartClient c) {
                    c.createList(key, generatePayloads(rand, 10)).join();
                }
            },
            new ChaosOperation() {
                public String getPrefix() { return "list-"; }
                public void execute(String key, int clientId, ChaosTestStats stats, FastCacheAsyncSmartClient c) {
                    c.addElementToTail(key, null, List.of(Payload.of(("item_" + System.currentTimeMillis()).getBytes())), clientId).join();
                }
            },
            new ChaosOperation() {
                public String getPrefix() { return "list-"; }
                public void execute(String key, int clientId, ChaosTestStats stats, FastCacheAsyncSmartClient c) {
                    c.addElementToHead(key, null, List.of(Payload.of(("item_" + System.currentTimeMillis()).getBytes())), clientId).join();
                }
            },
            new ChaosOperation() {
                public String getPrefix() { return "list-"; }
                public void execute(String key, int clientId, ChaosTestStats stats, FastCacheAsyncSmartClient c) {
                    c.getHead(key, null, clientId, Duration.ofSeconds(5)).join();
                }
            },
            new ChaosOperation() {
                public String getPrefix() { return "list-"; }
                public void execute(String key, int clientId, ChaosTestStats stats, FastCacheAsyncSmartClient c) {
                    c.getTail(key, null, clientId, Duration.ofSeconds(5)).join();
                }
            },
            new ChaosOperation() {
                public String getPrefix() { return "list-"; }
                public void execute(String key, int clientId, ChaosTestStats stats, FastCacheAsyncSmartClient c) {
                    c.getElementAtPosition(key.getBytes(), null, rand.nextInt(10), clientId, Duration.ofSeconds(5)).join();
                }
            },

            // Queue operations
            new ChaosOperation() {
                public String getPrefix() { return "queue-"; }
                public void execute(String key, int clientId, ChaosTestStats stats, FastCacheAsyncSmartClient c) {
                    c.createQueue(key, generatePayloads(rand, 10)).join();
                }
            },
            new ChaosOperation() {
                public String getPrefix() { return "queue-"; }
                public void execute(String key, int clientId, ChaosTestStats stats, FastCacheAsyncSmartClient c) {
                    c.addElementToTail(key, null, List.of(Payload.of(("item_" + System.currentTimeMillis()).getBytes())), clientId).join();
                }
            },
            new ChaosOperation() {
                public String getPrefix() { return "queue-"; }
                public void execute(String key, int clientId, ChaosTestStats stats, FastCacheAsyncSmartClient c) {
                    c.getAndRemoveFront(key, null, clientId, Duration.ofSeconds(5)).join();
                }
            },
            new ChaosOperation() {
                public String getPrefix() { return "queue-"; }
                public void execute(String key, int clientId, ChaosTestStats stats, FastCacheAsyncSmartClient c) {
                    c.getHead(key, null, clientId, Duration.ofSeconds(5)).join();
                }
            },

            // Set operations
            new ChaosOperation() {
                public String getPrefix() { return "set-"; }
                public void execute(String key, int clientId, ChaosTestStats stats, FastCacheAsyncSmartClient c) {
                    c.createSet(key, generatePayloads(rand, 10)).join();
                }
            },
            new ChaosOperation() {
                public String getPrefix() { return "set-"; }
                public void execute(String key, int clientId, ChaosTestStats stats, FastCacheAsyncSmartClient c) {
                    c.addElementUnordered(key, null, List.of(Payload.of(("elem_" + System.currentTimeMillis()).getBytes())), clientId).join();
                }
            },
            new ChaosOperation() {
                public String getPrefix() { return "set-"; }
                public void execute(String key, int clientId, ChaosTestStats stats, FastCacheAsyncSmartClient c) {
                    c.streamSet(key.getBytes(), null, clientId, Duration.ofSeconds(5)).join();
                }
            },
            new ChaosOperation() {
                public String getPrefix() { return "set-"; }
                public void execute(String key, int clientId, ChaosTestStats stats, FastCacheAsyncSmartClient c) {
                    c.removeFromContainer(key.getBytes(), null, "dummy".getBytes(), clientId, Duration.ofSeconds(5)).join();
                }
            },

            // Map operations
            new ChaosOperation() {
                public String getPrefix() { return "map-"; }
                public void execute(String key, int clientId, ChaosTestStats stats, FastCacheAsyncSmartClient c) {
                    c.createMap(key, generateMapEntries(rand, 10)).join();
                }
            },
            new ChaosOperation() {
                public String getPrefix() { return "map-"; }
                public void execute(String key, int clientId, ChaosTestStats stats, FastCacheAsyncSmartClient c) {
                    c.streamMap(key, null, clientId, Duration.ofSeconds(5)).join();
                }
            },

            // Ordered Map operations
            new ChaosOperation() {
                public String getPrefix() { return "omap-"; }
                public void execute(String key, int clientId, ChaosTestStats stats, FastCacheAsyncSmartClient c) {
                    c.createOrderedMap(key, generateOrderedMapEntries(rand, 10)).join();
                }
            },
            new ChaosOperation() {
                public String getPrefix() { return "omap-"; }
                public void execute(String key, int clientId, ChaosTestStats stats, FastCacheAsyncSmartClient c) {
                    c.streamOrderedMap(key, null, clientId, Duration.ofSeconds(5)).join();
                }
            },

            // Ordered Set operations
            new ChaosOperation() {
                public String getPrefix() { return "oset-"; }
                public void execute(String key, int clientId, ChaosTestStats stats, FastCacheAsyncSmartClient c) {
                    c.createOrderedSet(key, generateOrderedPayloads(rand, 10)).join();
                }
            },
            new ChaosOperation() {
                public String getPrefix() { return "oset-"; }
                public void execute(String key, int clientId, ChaosTestStats stats, FastCacheAsyncSmartClient c) {
                    c.addElementWithWeight(key, null, List.of(OrderedPayload.of(System.currentTimeMillis(), ("elem_" + System.currentTimeMillis()).getBytes())), clientId).join();
                }
            },
            new ChaosOperation() {
                public String getPrefix() { return "oset-"; }
                public void execute(String key, int clientId, ChaosTestStats stats, FastCacheAsyncSmartClient c) {
                    c.streamOrderedSet(key, null, clientId, Duration.ofSeconds(5)).join();
                }
            },
            new ChaosOperation() {
                public String getPrefix() { return "oset-"; }
                public void execute(String key, int clientId, ChaosTestStats stats, FastCacheAsyncSmartClient c) {
                    c.getElementWithWeight(key.getBytes(), null, rand.nextInt(10), clientId, Duration.ofSeconds(5)).join();
                }
            },

            // Atomic operations
            new ChaosOperation() {
                public String getPrefix() { return "atomic-"; }
                public void execute(String key, int clientId, ChaosTestStats stats, FastCacheAsyncSmartClient c) {
                    c.atomicCreate(key, (Math.abs(rand.nextLong()) % 1_000_000) + 1).join();
                }
            },
            new ChaosOperation() {
                public String getPrefix() { return "atomic-"; }
                public void execute(String key, int clientId, ChaosTestStats stats, FastCacheAsyncSmartClient c) {
                    c.atomicLoad(key, null, clientId, Duration.ofSeconds(5)).join();
                }
            },
            new ChaosOperation() {
                public String getPrefix() { return "atomic-"; }
                public void execute(String key, int clientId, ChaosTestStats stats, FastCacheAsyncSmartClient c) {
                    c.atomicStore(key, null, rand.nextLong()).join();
                }
            },
            new ChaosOperation() {
                public String getPrefix() { return "atomic-"; }
                public void execute(String key, int clientId, ChaosTestStats stats, FastCacheAsyncSmartClient c) {
                    c.atomicAdd(key, null, 1).join();
                }
            },
            new ChaosOperation() {
                public String getPrefix() { return "atomic-"; }
                public void execute(String key, int clientId, ChaosTestStats stats, FastCacheAsyncSmartClient c) {
                    c.atomicSub(key, null, 1).join();
                }
            },
            new ChaosOperation() {
                public String getPrefix() { return "atomic-"; }
                public void execute(String key, int clientId, ChaosTestStats stats, FastCacheAsyncSmartClient c) {
                    c.atomicCompareAndSet(key, 0L, 1L).join();
                }
            },
            new ChaosOperation() {
                public String getPrefix() { return "atomic-"; }
                public void execute(String key, int clientId, ChaosTestStats stats, FastCacheAsyncSmartClient c) {
                    c.remove(key.getBytes(), null, clientId).join();
                }
            },

            // Lock operations (use scalar- as base type for locks)
            new ChaosOperation() {
                public String getPrefix() { return "scalar-"; }
                public void execute(String key, int clientId, ChaosTestStats stats, FastCacheAsyncSmartClient c) {
                    c.lockObject(key, null, LockType.READ_LOCK, clientId, Duration.ofSeconds(30)).join();
                }
            },
            new ChaosOperation() {
                public String getPrefix() { return "scalar-"; }
                public void execute(String key, int clientId, ChaosTestStats stats, FastCacheAsyncSmartClient c) {
                    c.lockObject(key, null, LockType.WRITE_LOCK, clientId, Duration.ofSeconds(30)).join();
                }
            },
            new ChaosOperation() {
                public String getPrefix() { return "scalar-"; }
                public void execute(String key, int clientId, ChaosTestStats stats, FastCacheAsyncSmartClient c) {
                    c.lockObject(key, null, LockType.GLOBAL, clientId, Duration.ofSeconds(30)).join();
                }
            },
            new ChaosOperation() {
                public String getPrefix() { return "scalar-"; }
                public void execute(String key, int clientId, ChaosTestStats stats, FastCacheAsyncSmartClient c) {
                    c.unlockObject(key, null, clientId).join();
                }
            },

            // Admin operations (use scalar- as base type)
            new ChaosOperation() {
                public String getPrefix() { return "scalar-"; }
                public void execute(String key, int clientId, ChaosTestStats stats, FastCacheAsyncSmartClient c) {
                    c.setTtl(key, null, 300000, clientId).join();
                }
            },
            new ChaosOperation() {
                public String getPrefix() { return "scalar-"; }
                public void execute(String key, int clientId, ChaosTestStats stats, FastCacheAsyncSmartClient c) {
                    c.getTtl(key, null, clientId).join();
                }
            },
            new ChaosOperation() {
                public String getPrefix() { return "scalar-"; }
                public void execute(String key, int clientId, ChaosTestStats stats, FastCacheAsyncSmartClient c) {
                    c.existKey(key.getBytes(), null, clientId, Duration.ofSeconds(5)).join();
                }
            },
            new ChaosOperation() {
                public String getPrefix() { return "scalar-"; }
                public void execute(String key, int clientId, ChaosTestStats stats, FastCacheAsyncSmartClient c) {
                    c.getSize(key.getBytes(), null, clientId, Duration.ofSeconds(5)).join();
                }
            },
        };
    }

    // =========================================================================
    // ERROR CLASSIFICATION
    // =========================================================================

    private void classifyError(Exception e, ChaosTestStats stats) {
        if (e instanceof CancellationException || e instanceof TimeoutException) {
            stats.timeoutErrors.incrementAndGet();
        } else if (e instanceof ExecutionException ee) {
            Throwable cause = ee.getCause();
            if (cause instanceof StatusRuntimeException sre) {
                switch (sre.getStatus().getCode()) {
                    case PERMISSION_DENIED -> stats.permissionDeniedErrors.incrementAndGet();
                    case DEADLINE_EXCEEDED -> stats.timeoutErrors.incrementAndGet();
                    case NOT_FOUND -> stats.notFoundErrors.incrementAndGet();
                    default -> stats.otherErrors.incrementAndGet();
                }
            } else {
                stats.otherErrors.incrementAndGet();
            }
        } else {
            stats.otherErrors.incrementAndGet();
        }
    }

    // =========================================================================
    // PREFILL PHASE
    // =========================================================================

    private void runPrefillPhase(ChaosTestConfig config, ChaosTestStats stats) {
        int prefillKeys = (int) (config.totalKeys * config.prefillRatio / 100.0); // 25,000
        int keysPerType = prefillKeys / PRELOAD_TYPES.length; // ~2,778 per type

        var executor = Executors.newFixedThreadPool(4);
        List<CompletableFuture<?>> futures = new ArrayList<>();

        for (int t = 0; t < config.numThreads; t++) {
            final int threadId = t;
            futures.add(CompletableFuture.runAsync(() -> {
                var random = ThreadLocalRandom.current();
                for (int i = 0; i < keysPerType; i++) {
                    String type = PRELOAD_TYPES[i % PRELOAD_TYPES.length];
                    String key = type + "-" + (random.nextInt(1_000_000) + 1);
                    try {
                        preloadOperation(key, threadId, config, random);
                        stats.prefillCreated.incrementAndGet();
                    } catch (Exception e) {
                        stats.prefillFailed.incrementAndGet();
                    }
                }
            }, executor));
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        executor.shutdown();
    }

    private void preloadOperation(String key, int clientId, ChaosTestConfig config, Random random) {
        String type = key.split("-")[0];
        switch (type) {
            case "scalar" -> client.createKeyValue(key, ("value_" + clientId).getBytes(StandardCharsets.UTF_8)).join();
            case "vector" -> client.createVector(key, generatePayloads(random, config.elementsPerContainer)).join();
            case "list" -> client.createList(key, generatePayloads(random, config.elementsPerContainer)).join();
            case "queue" -> client.createQueue(key, generatePayloads(random, config.elementsPerContainer)).join();
            case "set" -> client.createSet(key, generatePayloads(random, config.elementsPerContainer)).join();
            case "map" -> client.createMap(key, generateMapEntries(random, config.elementsPerContainer)).join();
            case "omap" -> client.createOrderedMap(key, generateOrderedMapEntries(random, config.elementsPerContainer)).join();
            case "oset" -> client.createOrderedSet(key, generateOrderedPayloads(random, config.elementsPerContainer)).join();
            case "atomic" -> client.atomicCreate(key, (Math.abs(random.nextLong()) % 1_000_000) + 1).join();
        }
    }

    // =========================================================================
    // CHAOS PHASE
    // =========================================================================

    private void runChaosPhase(ChaosTestConfig config, ChaosTestStats stats) {
        var executor = Executors.newFixedThreadPool(4);
        List<CompletableFuture<?>> futures = new ArrayList<>();

        final ChaosTestConfig finalConfig = config;
        final ChaosTestStats finalStats = stats;
        long endTime = System.currentTimeMillis() + config.durationMs;

        for (int t = 0; t < config.numThreads; t++) {
            final int threadNum = t;
            futures.add(CompletableFuture.runAsync(() -> {
                runChaosWorker(threadNum, finalConfig, finalStats, endTime);
            }, executor));
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        executor.shutdown();
    }

    private void runChaosWorker(int threadId, ChaosTestConfig config, ChaosTestStats stats, long endTime) {
        var random = ThreadLocalRandom.current();
        long lastProgressPrint = System.currentTimeMillis();
        long progressInterval = 30_000; // Print progress every 30 seconds
        long lastErrorCount = 0;

        while (System.currentTimeMillis() < endTime) {
            // Pick random operation and generate key with matching prefix
            ChaosOperation op = OPERATION_TYPES[random.nextInt(OPERATION_TYPES.length)];
            String key = op.getPrefix() + (random.nextInt(1_000_000) + 1);

            long startTime = System.currentTimeMillis();
            try {
                op.execute(key, threadId, stats, client);
                stats.successfulOperations.incrementAndGet();
            } catch (Exception e) {
                classifyError(e, stats);
            } finally {
                stats.totalOperations.incrementAndGet();
                long elapsed = System.currentTimeMillis() - startTime;
                stats.totalDurationMs.addAndGet(elapsed);
            }

            // Print progress every 30 seconds
            long now = System.currentTimeMillis();
            if (now - lastProgressPrint >= progressInterval) {
                lastProgressPrint = now;
                long totalElapsed = now - (endTime - config.durationMs);
                long remaining = endTime - now;
                double opsPerSec = (double) stats.totalOperations.get() / (totalElapsed / 1000.0);
                double successRate = stats.totalOperations.get() > 0
                    ? (double) stats.successfulOperations.get() / stats.totalOperations.get() * 100
                    : 100;
                long currentErrors = stats.timeoutErrors.get() + stats.permissionDeniedErrors.get() + stats.otherErrors.get();
                long errorDelta = currentErrors - lastErrorCount;
                lastErrorCount = currentErrors;
                System.out.printf("[Thread-%d] Ops: %d | Success: %.1f%% | Errors: %d (+%d since last) | Rate: %.1f ops/s | Remaining: %d min%n",
                    threadId, stats.totalOperations.get(), successRate, currentErrors, errorDelta,
                    opsPerSec, remaining / 60_000);
            }

            if (config.operationIntervalMs > 0) {
                try {
                    Thread.sleep(config.operationIntervalMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    // =========================================================================
    // MAIN TEST
    // =========================================================================

    @Test
    @DisplayName("Chaos test: concurrent operations on all container types with prefill")
    void testChaosCluster() throws Exception {
        ChaosTestConfig config = new ChaosTestConfig();
        config.totalKeys = 50_000;
        config.elementsPerContainer = 100;
        config.enablePrefill = true;
        config.prefillRatio = 50;

        ChaosTestStats stats = new ChaosTestStats();

        // Phase 1: Prefill — create 50% of keys
        if (config.enablePrefill) {
            System.out.println("[CHaos] Phase 1: Prefill " + config.prefillRatio + "% of " + config.totalKeys + " keys...");
            runPrefillPhase(config, stats);
            System.out.println("[CHaos] Prefill complete: " + stats.prefillCreated.get() + " created, " +
                    stats.prefillFailed.get() + " failed");
        }

        // Phase 2: Chaos — concurrent operations
        System.out.println("[CHaos] Phase 2: Chaos for " + (config.durationMs / 60_000) + " minutes with " + config.numThreads + " threads...");
        runChaosPhase(config, stats);

        // Print statistics
        printStats(stats);

        // Check success rate > 90%
        double successRate = (double) stats.successfulOperations.get() / stats.totalOperations.get();
        Assertions.assertTrue(successRate > 0.9,
                "Success rate " + (successRate * 100) + "% is below 90% threshold");
    }

    @Override
    protected Duration getTestTtl() {
        return Duration.ofMinutes(20);
    }
    // =========================================================================
    // STATS OUTPUT
    // =========================================================================

    private void printStats(ChaosTestStats stats) {
        System.out.println("\n========== Chaos Test Results ==========");
        System.out.println("Total operations: " + stats.totalOperations.get());
        System.out.println("Successful: " + stats.successfulOperations.get());
        double successRate = (double) stats.successfulOperations.get() / stats.totalOperations.get() * 100;
        System.out.println("Success rate: " + String.format("%.2f", successRate) + "%");
        System.out.println("Timeout errors: " + stats.timeoutErrors.get());
        System.out.println("Permission denied: " + stats.permissionDeniedErrors.get());
        System.out.println("Cant lock: " + stats.cantLockErrors.get());
        System.out.println("Cant unlock: " + stats.cantUnlockErrors.get());
        System.out.println("Not found: " + stats.notFoundErrors.get());
        System.out.println("Other errors: " + stats.otherErrors.get());
        double avgTime = stats.totalDurationMs.get() / (double) stats.totalOperations.get();
        System.out.println("Avg operation time: " + String.format("%.2f", avgTime) + "ms");

        if (stats.prefillCreated.get() > 0) {
            System.out.println("\n--- Prefill Stats ---");
            System.out.println("Created: " + stats.prefillCreated.get());
            System.out.println("Failed: " + stats.prefillFailed.get());
        }
        System.out.println("========================================\n");
    }
}