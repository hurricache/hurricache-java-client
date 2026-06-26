package com.hurricache.client;

import com.hurricache.client.intf.HurriCacheClientInterface;
import com.hurricache.grpc.KeyHint;
import com.hurricache.grpc.LockStatus;
import com.hurricache.grpc.LockType;
import com.hurricache.grpc.coordinator.CoordinatorServiceGrpc;
import com.hurricache.grpc.coordinator.NodeRole;
import com.hurricache.grpc.coordinator.PeerRouting;
import com.hurricache.grpc.coordinator.Void;
import com.hurricache.utils.Pair;
import com.hurricache.utils.RoutingObserver;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.hurricache.grpc.coordinator.NodeRole.BACKUP;
import static com.hurricache.grpc.coordinator.NodeRole.MASTER;

public class FastCacheAsyncSmartClient implements HurriCacheClientInterface {
    private final ManagedChannel channel;

    // ThreadLocalRandom eliminates global CPU cache contention entirely during LB_SMART routing
    private final Mode mode = Mode.MASTER_THAN_BACKUP;
    private Mode configuredMode = Mode.MASTER_THAN_BACKUP;

    record RoutingInfo(int max_shards,
                       ConcurrentHashMap<Pair<NodeRole, Integer>, HurriCacheClientInterface> routingTable,
                       ConcurrentHashMap<String, HurriCacheClientInterface> routingTableTarget) {
    }

    private final CoordinatorServiceGrpc.CoordinatorServiceStub asyncStub;
    private final int defaultClientId;
    private final Duration defaultTimeout;
    private final Duration readyTimeout = Duration.ofSeconds(60);

    final AtomicReference<RoutingInfo> routing_info = new AtomicReference<>(new RoutingInfo(1024,
            new ConcurrentHashMap<>(),
            new ConcurrentHashMap<>()));

    private final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(2);
    private final CountDownLatch readyLatch = new CountDownLatch(1);
    private final AtomicBoolean readyFlag = new AtomicBoolean(false);
    private final AtomicBoolean isUpdating = new AtomicBoolean(false);
    private final AtomicInteger randomShard = new AtomicInteger(0);
    private static final Logger log = LogManager.getLogger(FastCacheAsyncSmartClient.class);

    public FastCacheAsyncSmartClient(String coordinatorHost,
                                     int coordinatorPort,
                                     int defaultClientId,
                                     Duration timeout) {
        this.channel = ManagedChannelBuilder.forAddress(coordinatorHost, coordinatorPort)
                .directExecutor()
                .usePlaintext()
                .build();
        this.asyncStub = CoordinatorServiceGrpc.newStub(channel);
        this.defaultClientId = defaultClientId;
        this.defaultTimeout = timeout;
        this.configuredMode = Mode.MASTER_THAN_BACKUP;
        this.scheduledExecutorService.scheduleAtFixedRate(this::init, 0, 30, TimeUnit.SECONDS);
    }

    public FastCacheAsyncSmartClient(ManagedChannel ch, int defaultClientId, Duration timeout) {
        this.channel = ch;
        this.asyncStub = CoordinatorServiceGrpc.newStub(channel);
        this.defaultClientId = defaultClientId;
        this.defaultTimeout = timeout;
        this.configuredMode = Mode.MASTER_THAN_BACKUP;
        this.scheduledExecutorService.scheduleAtFixedRate(this::init, 0, 30, TimeUnit.SECONDS);
    }

    private void init() {
        if (!isUpdating.compareAndSet(false, true)) {
            return;
        }

        CompletableFuture<List<PeerRouting>> future = new CompletableFuture<>();
        RoutingObserver responseObserver = new RoutingObserver(future);

        asyncStub.provideGlobalRoutingInfo(Void.newBuilder().build(), responseObserver);
        future.orTimeout(defaultTimeout.toMillis(), TimeUnit.MILLISECONDS).thenAccept(peerRoutingList -> {
            try {
                int maxShards = responseObserver.getMaxShards();
                RoutingInfo currentInfo = routing_info.get();

                ConcurrentHashMap<String, HurriCacheClientInterface> newRoutingTableTarget = new ConcurrentHashMap<>(
                        currentInfo.routingTableTarget);
                ConcurrentHashMap<Pair<NodeRole, Integer>, HurriCacheClientInterface> newRoutingTable
                        = new ConcurrentHashMap<>();

                Set<String> newTargets = peerRoutingList.stream()
                        .map(PeerRouting::getTarget)
                        .collect(Collectors.toSet());
                Set<String> oldTargets = currentInfo.routingTableTarget.keySet();

                newTargets.stream()
                        .filter(t -> !oldTargets.contains(t))
                        .forEach(target -> newRoutingTableTarget.put(target, newFastCacheClient(target)));
                oldTargets.stream().filter(t -> !newTargets.contains(t)).forEach(target -> {
                    HurriCacheClientInterface oldClient = newRoutingTableTarget.remove(target);
                    if (oldClient != null) {
                        oldClient.shutdown();
                    }
                });

                peerRoutingList.forEach(item -> item.getPartitionIdsList()
                        .forEach(id -> newRoutingTable.put(Pair.of(item.getRole(), id),
                                newRoutingTableTarget.get(item.getTarget()))));

                routing_info.set(new RoutingInfo(maxShards, newRoutingTable, newRoutingTableTarget));

                if (readyFlag.compareAndSet(false, true)) {
                    readyLatch.countDown();
                }
            } finally {
                isUpdating.set(false);
            }
        }).exceptionally(ex -> {
            isUpdating.set(false);
            log.atWarn().log("Exception obtaining routing information asynchronously", ex);
            return null;
        });
    }

    private HurriCacheClientInterface newFastCacheClient(String target) {
        return new FastCacheAsyncSimpleClient(ManagedChannelBuilder.forTarget(target)
                .usePlaintext()
                .directExecutor()
                .build(), defaultClientId, defaultTimeout) {
            @Override
            public Duration getDefaultTtl() {
                return FastCacheAsyncSmartClient.this.getDefaultTtl();
            }

            @Override
            public int getDefaultClientId() {
                return FastCacheAsyncSmartClient.this.getDefaultClientId();
            }

            @Override
            public Duration getDefaultTimeout() {
                return FastCacheAsyncSmartClient.this.getDefaultTimeout();
            }
        };
    }

    public boolean getReadyFlag() {
        return readyFlag.get();
    }

    @Override
    public String getTarget() {
        return asyncStub.getChannel().toString();
    }

    @Override
    public int getDefaultClientId() {
        return defaultClientId;
    }

    @Override
    public Duration getDefaultTimeout() {
        return defaultTimeout;
    }

    // --- PIPELINED CORE EXECUTION GATEWAY ---
    private <T> CompletableFuture<T> execute(KeyHint hint,
                                             Function<HurriCacheClientInterface, CompletableFuture<T>> action) {
        if (!readyFlag.get()) {
            try {
                if (!readyLatch.await(readyTimeout.toMillis(), TimeUnit.MILLISECONDS)) {
                    throw new RuntimeException("FastCacheClient boot timeout reached.");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }

        RoutingInfo routingInfo = routing_info.get();
        int shard = (hint == null)
                ? (randomShard.incrementAndGet() & Integer.MAX_VALUE) % routingInfo.max_shards
                : (int) (Integer.toUnsignedLong(hint.getWeekHash()) % routingInfo.max_shards);

        HurriCacheClientInterface master = getRoute(routingInfo, shard, MASTER);
        HurriCacheClientInterface backup = getRoute(routingInfo, shard, BACKUP);

        if (master == null && backup == null) {
            return CompletableFuture.failedFuture(new RuntimeException(
                    "No healthy endpoints available for shard allocation"));
        }

        Mode effectiveMode = configuredMode;
        if (master == null) {
            effectiveMode = Mode.BACKUP;
        }
        if (backup == null) {
            effectiveMode = Mode.MASTER;
        }

        return switch (effectiveMode) {
            case MASTER -> action.apply(master)
                    .handle(fallbackGuard(shard, routingInfo, action, master, null))
                    .thenCompose(Function.identity());
            case BACKUP -> action.apply(backup)
                    .handle(fallbackGuard(shard, routingInfo, action, backup, null))
                    .thenCompose(Function.identity());
            case MASTER_THAN_BACKUP -> action.apply(master)
                    .handle(fallbackGuard(shard, routingInfo, action, master, backup))
                    .thenCompose(Function.identity());
            case LB_SMART -> {
                // Completely lock-free load balancing execution sequence
                boolean tryMasterFirst = ThreadLocalRandom.current().nextBoolean();
                HurriCacheClientInterface primary = tryMasterFirst
                        ? master
                        : backup;
                HurriCacheClientInterface secondary = tryMasterFirst
                        ? backup
                        : master;
                yield action.apply(primary)
                        .handle(fallbackGuard(shard, routingInfo, action, primary, secondary))
                        .thenCompose(Function.identity());
            }
        };
    }

    // Centrally managed, highly performant unified routing exception fallback pipeline
    private <T> BiFunction<T, Throwable, CompletableFuture<T>> fallbackGuard(int shard,
                                                                             RoutingInfo routingInfo,
                                                                             Function<HurriCacheClientInterface, CompletableFuture<T>> action,
                                                                             HurriCacheClientInterface currentEndpoint,
                                                                             HurriCacheClientInterface fallbackEndpoint) {

        return (result, ex) -> {
            if (ex == null) {
                return CompletableFuture.completedFuture(result);
            }

            // 1. Precise Server-Side Dynamic Reroute Request
            if (isReroute(ex)) {
                String targetRoute = getRerouteTarget(ex);
                log.atDebug().log("[REROUTE] Redirecting payload to dynamic target: {}", targetRoute);
                HurriCacheClientInterface directClient = getRoute(routingInfo, targetRoute);
                return directClient != null
                        ? action.apply(directClient)
                        : CompletableFuture.failedFuture(ex);
            }

            // 2. Client-Side Transport Failure Recovery (Failover Layer)
            if (isUnavailable(ex)) {
                log.atDebug().log("[FAILOVER] Node cluster connection dead: {}", currentEndpoint.getTarget());
                scheduledExecutorService.execute(this::init); // Trigger background asynchronous routing refresh

                if (fallbackEndpoint != null) {
                    log.atInfo()
                            .log("[FAILOVER] Failing over pipeline execution target to: {}",
                                    fallbackEndpoint.getTarget());
                    return action.apply(fallbackEndpoint);
                }
            }

            return CompletableFuture.failedFuture(ex);
        };
    }

    // --- EXPLICIT IMPLEMENTATION OF DISPATCH INTERFACES ---
    @Override
    public CompletableFuture<Boolean> setTtl(byte[] key, KeyHint hint, long ttl, int clientId, Duration timeout) {
        return execute(hint, c -> c.setTtl(key, hint, ttl, clientId, timeout));
    }

    @Override
    public CompletableFuture<Long> getTtl(byte[] key, KeyHint hint, int clientId, Duration timeout) {
        return execute(hint, c -> c.getTtl(key, hint, clientId, timeout));
    }

    @Override
    public CompletableFuture<byte[]> getAndDeleteValue(byte[] key, KeyHint hint, int clientId, Duration timeout) {
        return execute(hint, c -> c.getAndDeleteValue(key, hint, clientId, timeout));
    }

    @Override
    public CompletableFuture<KeyHint> createKeyValue(byte[] key,
                                                     KeyHint hint,
                                                     byte[] value,
                                                     Duration ttl,
                                                     int clientId,
                                                     Duration timeout) {
        return execute(hint, c -> c.createKeyValue(key, hint, value, ttl, clientId, timeout));
    }

    @Override
    public CompletableFuture<byte[]> getValue(byte[] key, KeyHint hint, int clientId, Duration timeout) {
        return execute(hint, c -> c.getValue(key, hint, clientId, timeout));
    }

    @Override
    public CompletableFuture<byte[]> updateKeyValue(byte[] key,
                                                    KeyHint hint,
                                                    byte[] value,
                                                    Duration ttl,
                                                    int clientId,
                                                    Duration timeout) {
        return execute(hint, c -> c.updateKeyValue(key, hint, value, ttl, clientId, timeout));
    }

    @Override
    public CompletableFuture<Boolean> existKey(byte[] key, KeyHint hint, int clientId, Duration timeout) {
        return execute(hint, c -> c.existKey(key, hint, clientId, timeout));
    }

    @Override
    public CompletableFuture<Boolean> remove(byte[] key, KeyHint hint, int clientId, Duration timeout) {
        return execute(hint, c -> c.remove(key, hint, clientId, timeout));
    }

    @Override
    public CompletableFuture<KeyHint> createQueue(byte[] key,
                                                  List<byte[]> initialValue,
                                                  Duration ttl,
                                                  int clientId,
                                                  Duration timeout) {
        return execute(getKeyHint(key), c -> c.createQueue(key, initialValue, ttl, clientId, timeout));
    }

    @Override
    public CompletableFuture<KeyHint> createList(byte[] key,
                                                 List<byte[]> initialValue,
                                                 Duration ttl,
                                                 int clientId,
                                                 Duration timeout) {
        return execute(getKeyHint(key), c -> c.createList(key, initialValue, ttl, clientId, timeout));
    }

    @Override
    public CompletableFuture<KeyHint> createVector(byte[] key,
                                                   List<byte[]> initialValue,
                                                   Duration ttl,
                                                   int clientId,
                                                   Duration timeout) {
        return execute(getKeyHint(key), c -> c.createVector(key, initialValue, ttl, clientId, timeout));
    }

    @Override
    public CompletableFuture<byte[]> getAndRemoveTail(byte[] key, KeyHint hint, int clientId, Duration timeout) {
        return execute(hint, c -> c.getAndRemoveTail(key, hint, clientId, timeout));
    }

    @Override
    public CompletableFuture<byte[]> getAndRemoveFront(byte[] key, KeyHint hint, int clientId, Duration timeout) {
        return execute(hint, c -> c.getAndRemoveFront(key, hint, clientId, timeout));
    }

    @Override
    public CompletableFuture<byte[]> getFront(byte[] key, KeyHint hint, int clientId, Duration timeout) {
        return execute(hint, c -> c.getFront(key, hint, clientId, timeout));
    }

    @Override
    public CompletableFuture<Boolean> addElementToTail(byte[] key,
                                                       KeyHint hint,
                                                       List<byte[]> data,
                                                       int clientId,
                                                       Duration timeout) {
        return execute(hint, c -> c.addElementToTail(key, hint, data, clientId, timeout));
    }

    @Override
    public CompletableFuture<byte[]> getElementAtPosition(byte[] key,
                                                          KeyHint hint,
                                                          int pos,
                                                          int clientId,
                                                          Duration timeout) {
        return execute(hint, c -> c.getElementAtPosition(key, hint, pos, clientId, timeout));
    }

    @Override
    public CompletableFuture<List<byte[]>> streamList(byte[] key, KeyHint hint, int clientId, Duration timeout) {
        return execute(hint, c -> c.streamList(key, hint, clientId, timeout));
    }

    @Override
    public CompletableFuture<LockStatus> lockObject(byte[] key,
                                                    KeyHint hint,
                                                    LockType type,
                                                    int clientId,
                                                    Duration duration,
                                                    Duration timeout) {
        return execute(hint, c -> c.lockObject(key, hint, type, clientId, duration, timeout));
    }

    @Override
    public CompletableFuture<LockStatus> unlockObject(byte[] key, KeyHint hint, int clientId, Duration timeout) {
        return execute(hint, c -> c.unlockObject(key, hint, clientId, timeout));
    }

    @Override
    public CompletableFuture<List<byte[]>> streamElementInRange(byte[] key,
                                                                KeyHint hint,
                                                                boolean isArray,
                                                                int start,
                                                                int end,
                                                                int clientId,
                                                                Duration timeout) {
        return execute(hint, c -> c.streamElementInRange(key, hint, isArray, start, end, clientId, timeout));
    }

    @Override
    public CompletableFuture<List<byte[]>> streamVector(byte[] key, KeyHint hint, int clientId, Duration timeout) {
        return execute(hint, c -> c.streamVector(key, hint, clientId, timeout));
    }

    @Override
    public CompletableFuture<byte[]> getAndRemoveElementAtPosition(byte[] key,
                                                                   KeyHint hint,
                                                                   int pos,
                                                                   int clientId,
                                                                   Duration timeout) {
        return execute(hint, c -> c.getAndRemoveElementAtPosition(key, hint, pos, clientId, timeout));
    }

    @Override
    public CompletableFuture<Boolean> addElementToHead(byte[] key,
                                                       KeyHint hint,
                                                       List<byte[]> data,
                                                       int clientId,
                                                       Duration timeout) {
        return execute(hint, c -> c.addElementToHead(key, hint, data, clientId, timeout));
    }

    @Override
    public CompletableFuture<Boolean> addElementToPosition(byte[] key,
                                                           KeyHint hint,
                                                           List<byte[]> data,
                                                           int pos,
                                                           int clientId,
                                                           Duration timeout) {
        return execute(hint, c -> c.addElementToPosition(key, hint, data, pos, clientId, timeout));
    }

    @Override
    public CompletableFuture<Boolean> removeTail(byte[] key, KeyHint hint, int clientId, Duration timeout) {
        return execute(hint, c -> c.removeTail(key, hint, clientId, timeout));
    }

    @Override
    public CompletableFuture<Boolean> removeHead(byte[] key, KeyHint hint, int clientId, Duration timeout) {
        return execute(hint, c -> c.removeHead(key, hint, clientId, timeout));
    }

    @Override
    public CompletableFuture<Boolean> removeElementAtPosition(byte[] key,
                                                              KeyHint hint,
                                                              int pos,
                                                              int endPos,
                                                              int clientId,
                                                              Duration timeout) {
        return execute(hint, c -> c.removeElementAtPosition(key, hint, pos, endPos, clientId, timeout));
    }

    @Override
    public CompletableFuture<byte[]> getHead(byte[] key, KeyHint hint, int clientId, Duration timeout) {
        return execute(hint, c -> c.getHead(key, hint, clientId, timeout));
    }

    @Override
    public CompletableFuture<byte[]> getTail(byte[] key, KeyHint hint, int clientId, Duration timeout) {
        return execute(hint, c -> c.getTail(key, hint, clientId, timeout));
    }

    @Override
    public void shutdown() {
        channel.shutdown();
        scheduledExecutorService.shutdown();
        routing_info.get().routingTable.values().forEach(HurriCacheClientInterface::shutdown);
    }

    private HurriCacheClientInterface getRoute(RoutingInfo info, int shard, NodeRole role) {
        return info.routingTable.get(Pair.of(role, shard));
    }

    private HurriCacheClientInterface getRoute(RoutingInfo info, String target) {
        return info.routingTableTarget.get(target);
    }

    private boolean isUnavailable(Throwable ex) {
        Throwable c = (ex instanceof CompletionException)
                ? ex.getCause()
                : ex;
        return c instanceof StatusRuntimeException sre && sre.getStatus().getCode() == Status.Code.UNAVAILABLE;
    }

    private boolean isTimeout(Throwable ex) {
        Throwable c = (ex instanceof CompletionException)
                ? ex.getCause()
                : ex;
        return c instanceof StatusRuntimeException sre && sre.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED;
    }

    private boolean isReroute(Throwable ex) {
        Throwable c = (ex instanceof CompletionException)
                ? ex.getCause()
                : ex;
        return c instanceof StatusRuntimeException sre && sre.getStatus().getCode() == Status.Code.FAILED_PRECONDITION;
    }

    private String getRerouteTarget(Throwable ex) {
        Throwable c = (ex instanceof CompletionException)
                ? ex.getCause()
                : ex;
        if (c instanceof StatusRuntimeException sre
                && sre.getStatus().getCode() == Status.Code.FAILED_PRECONDITION
                && sre.getTrailers() != null) {
            return sre.getTrailers().get(Metadata.Key.of("x-fastcache-route", Metadata.ASCII_STRING_MARSHALLER));
        }
        return null;
    }

    public FastCacheAsyncSmartClient setMode(Mode mode) {
        this.configuredMode = mode;
        return this;
    }

    public enum Mode {
        MASTER,
        MASTER_THAN_BACKUP,
        BACKUP,
        LB_SMART
    }
}