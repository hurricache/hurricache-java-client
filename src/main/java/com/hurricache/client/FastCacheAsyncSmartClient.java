package com.hurricache.client;

import com.hurricache.client.intf.HurriCacheClientInterface;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Mode;
import com.hurricache.client.intf.OrderedPayload;
import com.hurricache.client.intf.Payload;
import com.hurricache.grpc.AtomicCasRes;
import com.hurricache.grpc.ContainerType;
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
import java.util.Map;
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

    private final Mode mode = Mode.MASTER_THAN_BACKUP;
    private Mode configuredMode = Mode.MASTER_THAN_BACKUP;
    private final ThreadLocal<Mode> currentModeOverride = new ThreadLocal<>();

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
        return new FastCacheAsyncSimpleClient(ManagedChannelBuilder.forTarget(target).maxInboundMessageSize(64 * 1024 * 1024)
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

    private <T> CompletableFuture<T> executeWrite(KeyHintData hint,
                                                  Function<HurriCacheClientInterface, CompletableFuture<T>> action) {
        return execute(hint, Mode.MASTER_THAN_BACKUP, action);
    }

    private <T> CompletableFuture<T> execute(KeyHintData hint,
                                             Mode methodDefaultMode,
                                             Function<HurriCacheClientInterface, CompletableFuture<T>> action) {

        Mode baseMode = currentModeOverride.get();
        currentModeOverride.remove();

        if (baseMode == null) {
            baseMode = (methodDefaultMode != null) ? methodDefaultMode : configuredMode;
        }

        if (!readyFlag.get()) {
            try {
                if (!readyLatch.await(readyTimeout.toMillis(), TimeUnit.MILLISECONDS)) {
                    return CompletableFuture.failedFuture(new RuntimeException("FastCacheClient boot timeout reached."));
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return CompletableFuture.failedFuture(new RuntimeException(e));
            }
        }

        RoutingInfo routingInfo = routing_info.get();
        int shard = (hint == null || hint.getWeek_hash() == null)
                    ? (randomShard.incrementAndGet() & Integer.MAX_VALUE) % routingInfo.max_shards
                    : (int) (Integer.toUnsignedLong(hint.getWeek_hash()) % routingInfo.max_shards);

        HurriCacheClientInterface master = getRoute(routingInfo, shard, MASTER);
        HurriCacheClientInterface backup = getRoute(routingInfo, shard, BACKUP);

        if (master == null && backup == null) {
            return CompletableFuture.failedFuture(new RuntimeException(
                    "No healthy endpoints available for shard allocation"));
        }

        Mode effectiveMode = baseMode;
        if (master == null) effectiveMode = Mode.BACKUP;
        if (backup == null) effectiveMode = Mode.MASTER;

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
                boolean tryMasterFirst = ThreadLocalRandom.current().nextBoolean();
                HurriCacheClientInterface primary = tryMasterFirst ? master : backup;
                HurriCacheClientInterface secondary = tryMasterFirst ? backup : master;
                yield action.apply(primary)
                        .handle(fallbackGuard(shard, routingInfo, action, primary, secondary))
                        .thenCompose(Function.identity());
            }
        };
    }

    private <T> CompletableFuture<T> execute(KeyHintData hint, Function<HurriCacheClientInterface, CompletableFuture<T>> action) {
        return execute(hint, null, action);
    }

    private <T> BiFunction<T, Throwable, CompletableFuture<T>> fallbackGuard(int shard,
                                                                             RoutingInfo routingInfo,
                                                                             Function<HurriCacheClientInterface, CompletableFuture<T>> action,
                                                                             HurriCacheClientInterface currentEndpoint,
                                                                             HurriCacheClientInterface fallbackEndpoint) {

        return (result, ex) -> {
            if (ex == null) {
                return CompletableFuture.completedFuture(result);
            }

            if (isReroute(ex)) {
                String targetRoute = getRerouteTarget(ex);
                log.atDebug().log("[REROUTE] Redirecting payload to dynamic target: {}", targetRoute);
                HurriCacheClientInterface directClient = getRoute(routingInfo, targetRoute);
                return directClient != null
                       ? action.apply(directClient)
                       : CompletableFuture.failedFuture(ex);
            }

            if (isUnavailable(ex)) {
                log.atDebug().log("[FAILOVER] Node cluster connection dead: {}", currentEndpoint.getTarget());
                scheduledExecutorService.execute(this::init);

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

    // =========================================================================
    // EXPLICIT IMPLEMENTATION OF DISPATCH INTERFACES
    // =========================================================================

    @Override
    public CompletableFuture<Boolean> setTtl(byte[] key, KeyHintData hint, long ttl, int clientId, Duration timeout) {
        return executeWrite(hint, c -> c.setTtl(key, hint, ttl, clientId, timeout));
    }

    @Override
    public CompletableFuture<Long> getTtl(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return execute(hint, c -> c.getTtl(key, hint, clientId, timeout));
    }

    @Override
    public CompletableFuture<byte[]> getAndDeleteValue(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return execute(hint, c -> c.getAndDeleteValue(key, hint, clientId, timeout));
    }

    @Override
    public CompletableFuture<KeyHintData> createKeyValue(byte[] key,
                                                         KeyHintData hint,
                                                         byte[] value,
                                                         Duration ttl,
                                                         int clientId,
                                                         Duration timeout) {
        return execute(hint, c -> c.createKeyValue(key, hint, value, ttl, clientId, timeout));
    }

    @Override
    public CompletableFuture<byte[]> getValue(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return execute(hint, c -> c.getValue(key, hint, clientId, timeout));
    }

    @Override
    public CompletableFuture<byte[]> updateKeyValue(byte[] key,
                                                    KeyHintData hint,
                                                    byte[] value,
                                                    Duration ttl,
                                                    int clientId,
                                                    Duration timeout) {
        return execute(hint, c -> c.updateKeyValue(key, hint, value, ttl, clientId, timeout));
    }

    @Override
    public CompletableFuture<Boolean> existKey(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return execute(hint, c -> c.existKey(key, hint, clientId, timeout));
    }

    @Override
    public CompletableFuture<Boolean> remove(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return execute(hint, c -> c.remove(key, hint, clientId, timeout));
    }

    @Override
    public CompletableFuture<KeyHintData> createQueue(byte[] key, KeyHintData keyHint, List<Payload> initialValue,
                                                      Duration ttl,
                                                      int clientId,
                                                      Duration timeout) {
        return execute(keyHint, c -> c.createQueue(key,keyHint , initialValue, ttl, clientId, timeout));
    }

    @Override
    public CompletableFuture<KeyHintData> createList(byte[] key, KeyHintData keyHint, List<Payload> initialValue,
                                                     Duration ttl,
                                                     int clientId,
                                                     Duration timeout) {
        return execute(keyHint, c -> c.createList(key,keyHint , initialValue, ttl, clientId, timeout ));
    }

    @Override
    public CompletableFuture<KeyHintData> createVector(byte[] key, KeyHintData keyHint, List<Payload> initialValue,
                                                       Duration ttl,
                                                       int clientId,
                                                       Duration timeout) {
        return execute(keyHint, c -> c.createVector(key, keyHint, initialValue, ttl, clientId, timeout));
    }

    @Override
    public CompletableFuture<Payload> getAndRemoveTail(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return executeWrite(hint, c -> c.getAndRemoveTail(key, hint, clientId, timeout));
    }

    @Override
    public CompletableFuture<Payload> getAndRemoveFront(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return executeWrite(hint, c -> c.getAndRemoveFront(key, hint, clientId, timeout));
    }

    @Override
    public CompletableFuture<Payload> getFront(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return execute(hint, c -> c.getFront(key, hint, clientId, timeout));
    }

    @Override
    public CompletableFuture<Boolean> addElementToTail(byte[] key,
                                                       KeyHintData hint,
                                                       List<Payload> data,
                                                       int clientId,
                                                       Duration timeout) {
        return executeWrite(hint, c -> c.addElementToTail(key, hint, data, clientId, timeout));
    }

    @Override
    public CompletableFuture<Payload> getElementAtPosition(byte[] key,
                                                           KeyHintData hint,
                                                           int pos,
                                                           int clientId,
                                                           Duration timeout) {
        return execute(hint, c -> c.getElementAtPosition(key, hint, pos, clientId, timeout));
    }

    @Override
    public CompletableFuture<List<Payload>> streamList(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return execute(hint, c -> c.streamList(key, hint, clientId, timeout));
    }

    @Override
    public CompletableFuture<LockStatus> lockObject(byte[] key,
                                                    KeyHintData hint,
                                                    LockType type,
                                                    int clientId,
                                                    Duration duration,
                                                    Duration timeout) {
        return executeWrite(hint, c -> c.lockObject(key, hint, type, clientId, duration, timeout));
    }

    @Override
    public CompletableFuture<LockStatus> unlockObject(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return executeWrite(hint, c -> c.unlockObject(key, hint, clientId, timeout));
    }

    @Override
    public CompletableFuture<List<Payload>> streamElementInRangeUnordered(byte[] key,
                                                                          KeyHintData hint,
                                                                          ContainerType containerType,
                                                                          int start,
                                                                          int end,
                                                                          int clientId,
                                                                          Duration timeout) {
        return execute(hint, c -> c.streamElementInRangeUnordered(key, hint,
                                                                  containerType, start, end, clientId, timeout));
    }

    @Override
    public CompletableFuture<List<OrderedPayload>> streamElementInRangeOrderedSet(byte[] key,
                                                                                  KeyHintData hint,
                                                                                  long startWeight,
                                                                                  long endWeight,
                                                                                  boolean reverse,
                                                                                  int clientId,
                                                                                  Duration timeout) {
        return execute(hint, c -> c.streamElementInRangeOrderedSet(key, hint, startWeight, endWeight, reverse, clientId, timeout));
    }

    @Override
    public CompletableFuture<List<Payload>> streamVector(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return execute(hint, c -> c.streamVector(key, hint, clientId, timeout));
    }

    @Override
    public CompletableFuture<Payload> getAndRemoveElementAtPosition(byte[] key,
                                                                    KeyHintData hint,
                                                                    int pos,
                                                                    int clientId,
                                                                    Duration timeout) {
        return executeWrite(hint, c -> c.getAndRemoveElementAtPosition(key, hint, pos, clientId, timeout));
    }

    @Override
    public CompletableFuture<Boolean> addElementToHead(byte[] key,
                                                       KeyHintData hint,
                                                       List<Payload> data,
                                                       int clientId,
                                                       Duration timeout) {
        return executeWrite(hint, c -> c.addElementToHead(key, hint, data, clientId, timeout));
    }

    @Override
    public CompletableFuture<Integer> addElementToPosition(byte[] key,
                                                           KeyHintData hint,
                                                           List<Payload> data,
                                                           int pos,
                                                           int clientId,
                                                           Duration timeout) {
        return executeWrite(hint, c -> c.addElementToPosition(key, hint, data, pos, clientId, timeout));
    }

    @Override
    public CompletableFuture<Boolean> removeTail(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return executeWrite(hint, c -> c.removeTail(key, hint, clientId, timeout));
    }

    @Override
    public CompletableFuture<Boolean> removeHead(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return executeWrite(hint, c -> c.removeHead(key, hint, clientId, timeout));
    }

    @Override
    public CompletableFuture<Boolean> removeElementAtPosition(byte[] key,
                                                              KeyHintData hint,
                                                              int pos,
                                                              int endPos,
                                                              int clientId,
                                                              Duration timeout) {
        return executeWrite(hint, c -> c.removeElementAtPosition(key, hint, pos, endPos, clientId, timeout));
    }

    @Override
    public CompletableFuture<Payload> getHead(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return execute(hint, c -> c.getHead(key, hint, clientId, timeout));
    }

    @Override
    public CompletableFuture<Payload> getTail(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return execute(hint, c -> c.getTail(key, hint, clientId, timeout));
    }

    @Override
    public void shutdown() {
        scheduledExecutorService.shutdown();
        try {
            if (!scheduledExecutorService.awaitTermination(3, TimeUnit.SECONDS)) {
                scheduledExecutorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduledExecutorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
        channel.shutdown();
        routing_info.get().routingTable.values().forEach(HurriCacheClientInterface::shutdown);
    }

    private HurriCacheClientInterface getRoute(RoutingInfo info, int shard, NodeRole role) {
        return info.routingTable.get(Pair.of(role, shard));
    }

    private HurriCacheClientInterface getRoute(RoutingInfo info, String target) {
        return info.routingTableTarget.get(target);
    }

    private boolean isUnavailable(Throwable ex) {
        Throwable c = (ex instanceof CompletionException) ? ex.getCause() : ex;
        return c instanceof StatusRuntimeException sre && sre.getStatus().getCode() == Status.Code.UNAVAILABLE;
    }

    private boolean isTimeout(Throwable ex) {
        Throwable c = (ex instanceof CompletionException) ? ex.getCause() : ex;
        return c instanceof StatusRuntimeException sre && sre.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED;
    }

    private boolean isReroute(Throwable ex) {
        Throwable c = (ex instanceof CompletionException) ? ex.getCause() : ex;
        return c instanceof StatusRuntimeException sre && sre.getStatus().getCode() == Status.Code.FAILED_PRECONDITION;
    }

    private String getRerouteTarget(Throwable ex) {
        Throwable c = (ex instanceof CompletionException) ? ex.getCause() : ex;
        if (c instanceof StatusRuntimeException sre
            && sre.getStatus().getCode() == Status.Code.FAILED_PRECONDITION
            && sre.getTrailers() != null) {
            return sre.getTrailers().get(Metadata.Key.of("x-fastcache-route", Metadata.ASCII_STRING_MARSHALLER));
        }
        return null;
    }

    // =========================================================================
    // ATOMIC OPERATIONS DISPATCH IMPLEMENTATION
    // =========================================================================

    @Override
    public CompletableFuture<KeyHintData> atomicCreate(byte[] key, KeyHintData hint, long value, Duration ttl, int clientId, Duration timeout) {
        return executeWrite(hint, c -> c.atomicCreate(key, hint, value, ttl, clientId, timeout));
    }

    @Override
    public CompletableFuture<KeyHintData> atomicStore(byte[] key, KeyHintData hint, long value, Duration ttl, int clientId, Duration timeout) {
        return executeWrite(hint, c -> c.atomicStore(key, hint, value, ttl, clientId, timeout));
    }

    @Override
    public CompletableFuture<Long> atomicExchange(byte[] key, KeyHintData hint, long value, Duration ttl, int clientId, Duration timeout) {
        return executeWrite(hint, c -> c.atomicExchange(key, hint, value, ttl, clientId, timeout));
    }

    @Override
    public CompletableFuture<Long> atomicAdd(byte[] key, KeyHintData hint, long delta, Duration ttl, int clientId, Duration timeout) {
        return executeWrite(hint, c -> c.atomicAdd(key, hint, delta, ttl, clientId, timeout));
    }

    @Override
    public CompletableFuture<Long> atomicSub(byte[] key, KeyHintData hint, long delta, Duration ttl, int clientId, Duration timeout) {
        return executeWrite(hint, c -> c.atomicSub(key, hint, delta, ttl, clientId, timeout));
    }

    @Override
    public CompletableFuture<Long> atomicAnd(byte[] key, KeyHintData hint, long mask, Duration ttl, int clientId, Duration timeout) {
        return executeWrite(hint, c -> c.atomicAnd(key, hint, mask, ttl, clientId, timeout));
    }

    @Override
    public CompletableFuture<Long> atomicOr(byte[] key, KeyHintData hint, long mask, Duration ttl, int clientId, Duration timeout) {
        return executeWrite(hint, c -> c.atomicOr(key, hint, mask, ttl, clientId, timeout));
    }

    @Override
    public CompletableFuture<Long> atomicXor(byte[] key, KeyHintData hint, long mask, Duration ttl, int clientId, Duration timeout) {
        return executeWrite(hint, c -> c.atomicXor(key, hint, mask, ttl, clientId, timeout));
    }

    @Override
    public CompletableFuture<AtomicCasRes> atomicCompareAndSet(byte[] key, KeyHintData hint, long expectedValue, long newValue, Duration ttl, int clientId, Duration timeout) {
        return executeWrite(hint, c -> c.atomicCompareAndSet(key, hint, expectedValue, newValue, ttl, clientId, timeout));
    }

    public FastCacheAsyncSmartClient setMode(Mode mode) {
        this.currentModeOverride.set(mode);
        return this;
    }

    @Override
    public CompletableFuture<Boolean> addElementToPositionBefore(byte[] key, KeyHintData hint, List<Payload> data, Payload pos, int clientId, Duration timeout) {
        return executeWrite(hint, c -> c.addElementToPositionBefore(key, hint, data, pos, clientId, timeout));
    }

    @Override
    public CompletableFuture<Boolean> addElementToPositionAfter(byte[] key, KeyHintData hint, List<Payload> data, Payload pos, int clientId, Duration timeout) {
        return executeWrite(hint, c -> c.addElementToPositionAfter(key, hint, data, pos, clientId, timeout));
    }

    // =========================================================================
    // CONTAINER & SIZE OPERATIONS DISPATCH
    // =========================================================================

    @Override
    public CompletableFuture<Integer> getSize(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return execute(hint, c -> c.getSize(key, hint, clientId, timeout));
    }

    @Override
    public CompletableFuture<Boolean> addElement(byte[] key, KeyHintData hint, List<Payload> data, int clientId, Duration timeout) {
        return executeWrite(hint, c -> c.addElement(key, hint, data, clientId, timeout));
    }

    @Override
    public CompletableFuture<KeyHintData> createSet(byte[] key,
                                                    KeyHintData keyHint,
                                                    List<Payload> initialValue, Duration ttl, int clientId, Duration timeout) {
        return execute(keyHint, c -> c.createSet(key,keyHint , initialValue, ttl, clientId, timeout));
    }

    @Override
    public CompletableFuture<Integer> removeFromContainer(byte[] key,
                                                          KeyHintData hint,
                                                          ContainerType type,
                                                          List<Payload> values,
                                                          List<Payload> keys,
                                                          int clientId,
                                                          Duration timeout) {
        return executeWrite(hint, c -> c.removeFromContainer(key, hint, type, values, keys, clientId, timeout));
    }

    // =========================================================================
    // ATOMIC READ OPERATIONS DISPATCH
    // =========================================================================

    @Override
    public CompletableFuture<Long> atomicLoad(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return execute(hint, c -> c.atomicLoad(key, hint, clientId, timeout));
    }

    @Override
    public CompletableFuture<Long> atomicLoadAndDelete(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return executeWrite(hint, c -> c.atomicLoadAndDelete(key, hint, clientId, timeout));
    }

    @Override
    public CompletableFuture<KeyHintData> createOrderedSet(byte[] key, List<OrderedPayload> initialValue, Duration ttl, int clientId, Duration timeout) {
        return execute(null, c -> c.createOrderedSet(key, initialValue, ttl, clientId, timeout));
    }

    @Override
    public CompletableFuture<KeyHintData> createMap(byte[] key, Map<Payload, Payload> initialValue, Duration ttl, int clientId, Duration timeout) {
        return execute(null, c -> c.createMap(key, initialValue, ttl, clientId, timeout));
    }

    @Override
    public CompletableFuture<KeyHintData> createOrderedMap(byte[] key, Map<OrderedPayload, Payload> initialValue, Duration ttl, int clientId, Duration timeout) {
        return execute(null, c -> c.createOrderedMap(key, initialValue, ttl, clientId, timeout));
    }

    @Override
    public CompletableFuture<Integer> addElementWithWeight(byte[] key,
                                                           KeyHintData hint,
                                                           List<OrderedPayload> data,
                                                           int clientId,
                                                           Duration timeout) {
        return executeWrite(hint, c -> c.addElementWithWeight(key, hint, data, clientId, timeout));
    }

    @Override
    public CompletableFuture<Map<Payload, Payload>> streamMap(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return execute(hint, c -> c.streamMap(key, hint, clientId, timeout));
    }

    @Override
    public CompletableFuture<Map<OrderedPayload, Payload>> streamOrderedMap(byte[] key,
                                                                            KeyHintData hint,
                                                                            int clientId,
                                                                            Duration timeout) {
        return execute(hint, c -> c.streamOrderedMap(key, hint, clientId, timeout));
    }

    @Override
    public CompletableFuture<byte[]> getContainerValue(byte[] key,
                                                       KeyHintData hint,
                                                       byte[] elementKey,
                                                       int clientId,
                                                       Duration timeout) {
        return execute(hint, c -> c.getContainerValue(key, hint,elementKey, clientId, timeout));
    }

    @Override
    public CompletableFuture<byte[]> getAndRemoveContainerValue(byte[] key,
                                                                KeyHintData hint,
                                                                byte[] elementKey,
                                                                int clientId,
                                                                Duration timeout) {
        return execute(hint, c -> c.getAndRemoveContainerValue(key, hint,elementKey, clientId, timeout));
    }

    @Override
    public CompletableFuture<Boolean> containsContainerKey(byte[] key,
                                                           KeyHintData hint,
                                                           byte[] elementKey,
                                                           int clientId,
                                                           Duration timeout) {
        return execute(hint, c -> c.containsContainerKey(key, hint,elementKey, clientId, timeout));
    }

    @Override
    public CompletableFuture<byte[]> updateContainerValue(byte[] key,
                                                          KeyHintData hint,
                                                          byte[] elementKey,
                                                          byte[] value,
                                                          int clientId,
                                                          Duration timeout) {
        return execute(hint, c -> c.updateContainerValue(key, hint,elementKey,value, clientId, timeout));
    }

    @Override
    public CompletableFuture<Integer> removeFromContainer(byte[] key,
                                                          KeyHintData hint,
                                                          byte[] elementKey,
                                                          int clientId,
                                                          Duration timeout) {
        return execute(hint, c -> c.removeFromContainer(key, hint,elementKey, clientId, timeout));
    }

    @Override
    public CompletableFuture<Integer> addElementHashMap(byte[] key,
                                                        KeyHintData hint,
                                                        List<Payload> container_keys,
                                                        List<Payload> container_values,
                                                        int clientId,
                                                        Duration timeout) {
        return execute(hint, c -> c.addElementHashMap(key, hint,container_keys,container_values, clientId, timeout));
    }

    @Override
    public CompletableFuture<Integer> addElementOrderedMap(byte[] key,
                                                           KeyHintData hint,
                                                           List<OrderedPayload> container_keys,
                                                           List<Payload> container_values,
                                                           int clientId,
                                                           Duration timeout) {
        return execute(hint, c -> c.addElementOrderedMap(key, hint,container_keys,container_values, clientId, timeout));    }

    @Override
    public CompletableFuture<Boolean> addElementOrdered(byte[] key,
                                                        KeyHintData hint,
                                                        List<OrderedPayload> data,
                                                        int clientId,
                                                        Duration timeout) {
        return execute(hint, c -> c.addElementOrdered(key, hint,data, clientId, timeout));
    }
}