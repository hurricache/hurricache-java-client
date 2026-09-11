package com.hurricache.client;

import com.hurricache.client.intf.HurriCacheClientInterface;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Mode;
import com.hurricache.client.intf.OrderedPayload;
import com.hurricache.client.intf.Payload;
import com.hurricache.grpc.AtomicCasRes;
import com.hurricache.grpc.ContainerType;
import com.hurricache.grpc.Key;
import com.hurricache.grpc.LockStatus;
import com.hurricache.grpc.LockType;
import com.hurricache.grpc.OrderedKey;
import com.hurricache.grpc.OrderedValue;
import com.hurricache.grpc.Value;
import com.hurricache.grpc.coordinator.CoordinatorServiceGrpc;
import com.hurricache.grpc.coordinator.NodeRole;
import com.hurricache.grpc.coordinator.PeerRouting;
import com.hurricache.utils.CompressionUtils;
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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.hurricache.grpc.coordinator.NodeRole.BACKUP;
import static com.hurricache.grpc.coordinator.NodeRole.MASTER;

public class FastCacheAsyncSmartClient implements HurriCacheClientInterface {

    private final List<String> coordinatorAddresses;
    private final AtomicInteger activeCoordinatorIndex = new AtomicInteger(0);
    private final AtomicReference<ManagedChannel> activeCoordinatorChannel = new AtomicReference<>();
    private final AtomicReference<CoordinatorServiceGrpc.CoordinatorServiceStub> activeCoordinatorStub = new AtomicReference<>();

    private final Mode mode = Mode.MASTER_THAN_BACKUP;
    private final Mode configuredMode = Mode.MASTER_THAN_BACKUP;
    private int defaultCompressionThreshold;
    private final ThreadLocal<Mode> currentModeOverride = new ThreadLocal<>();

    record RoutingInfo(int max_shards,
                       ConcurrentHashMap<Pair<NodeRole, Integer>, HurriCacheClientInterface> routingTable,
                       ConcurrentHashMap<String, HurriCacheClientInterface> routingTableTarget) {
    }

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

    public FastCacheAsyncSmartClient(String coordinatorAddresses, int defaultClientId, Duration timeout) {
        this(List.of(coordinatorAddresses), defaultClientId, timeout);
        this.defaultCompressionThreshold = DEFAULT_COMPRESSION_THRESHOLD;
    }

    public FastCacheAsyncSmartClient(String coordinatorAddresses,
                                     int defaultClientId,
                                     Duration timeout,
                                     int defaultCompressionThreshold) {
        this(List.of(coordinatorAddresses), defaultClientId, timeout);
        this.defaultCompressionThreshold = defaultCompressionThreshold;
    }

    public FastCacheAsyncSmartClient(List<String> coordinatorAddresses,
                                     int defaultClientId,
                                     Duration timeout,
                                     int defaultCompressionThreshold) {
        if (coordinatorAddresses == null || coordinatorAddresses.isEmpty()) {
            throw new IllegalArgumentException("Coordinator addresses list cannot be empty");
        }
        this.coordinatorAddresses = List.copyOf(coordinatorAddresses);
        this.defaultClientId = defaultClientId;
        this.defaultTimeout = timeout;
        this.defaultCompressionThreshold = defaultCompressionThreshold;
        initCoordinatorChannel(0);
        this.scheduledExecutorService.scheduleAtFixedRate(this::init, 0, 30, TimeUnit.SECONDS);
    }

    public FastCacheAsyncSmartClient(List<String> coordinatorAddresses, int defaultClientId, Duration timeout) {
        if (coordinatorAddresses == null || coordinatorAddresses.isEmpty()) {
            throw new IllegalArgumentException("Coordinator addresses list cannot be empty");
        }
        this.coordinatorAddresses = List.copyOf(coordinatorAddresses);
        this.defaultClientId = defaultClientId;
        this.defaultTimeout = timeout;
        this.defaultCompressionThreshold = DEFAULT_COMPRESSION_THRESHOLD;
        initCoordinatorChannel(0);
        this.scheduledExecutorService.scheduleAtFixedRate(this::init, 0, 30, TimeUnit.SECONDS);
    }

    public FastCacheAsyncSmartClient(String coordinatorHost,
                                     int coordinatorPort,
                                     int defaultClientId,
                                     Duration timeout) {
        this(List.of(coordinatorHost + ":" + coordinatorPort), defaultClientId, timeout);
    }

    private synchronized void initCoordinatorChannel(int index) {
        int targetIdx = Math.abs(index % coordinatorAddresses.size());
        String address = coordinatorAddresses.get(targetIdx);

        ManagedChannel oldChannel = activeCoordinatorChannel.get();
        if (oldChannel != null && !oldChannel.isShutdown()) {
            try {
                oldChannel.shutdown();
            } catch (Exception e) {
                log.atDebug().log("Error closing old coordinator channel", e);
            }
        }

        ManagedChannel newChannel = ManagedChannelBuilder.forTarget(address).directExecutor().usePlaintext().build();

        activeCoordinatorChannel.set(newChannel);
        activeCoordinatorStub.set(CoordinatorServiceGrpc.newStub(newChannel));
        activeCoordinatorIndex.set(targetIdx);
        log.atInfo().log("[COORDINATOR] Connected to coordinator node: {}", address);
    }

    private CoordinatorServiceGrpc.CoordinatorServiceStub getOrSwitchCoordinatorStub(boolean forceSwitch) {
        if (forceSwitch) {
            synchronized (this) {
                int nextIndex = activeCoordinatorIndex.get() + 1;
                initCoordinatorChannel(nextIndex);
            }
        }
        return activeCoordinatorStub.get();
    }

    private void init() {
        if (!isUpdating.compareAndSet(false, true)) {
            return;
        }
        requestRoutingInfoFromCoordinator(0);
    }

    private void requestRoutingInfoFromCoordinator(int attemptCount) {
        if (attemptCount >= coordinatorAddresses.size()) {
            log.atError().log("[COORDINATOR FAILOVER] All coordinator nodes are unavailable!");
            isUpdating.set(false);
            return;
        }

        boolean forceSwitch = (attemptCount > 0);
        CoordinatorServiceGrpc.CoordinatorServiceStub stub = getOrSwitchCoordinatorStub(forceSwitch);

        CompletableFuture<List<PeerRouting>> future = new CompletableFuture<>();
        RoutingObserver responseObserver = new RoutingObserver(future);

        stub.provideGlobalRoutingInfo(com.hurricache.grpc.coordinator.Void.newBuilder().build(), responseObserver);

        future.orTimeout(defaultTimeout.toMillis(), TimeUnit.MILLISECONDS).thenAccept(peerRoutingList -> {
            try {
                int maxShards = responseObserver.getMaxShards();
                RoutingInfo currentInfo = routing_info.get();

                ConcurrentHashMap<String, HurriCacheClientInterface> newRoutingTableTarget = new ConcurrentHashMap<>(
                        currentInfo.routingTableTarget);
                ConcurrentHashMap<Pair<NodeRole, Integer>, HurriCacheClientInterface> newRoutingTable = new ConcurrentHashMap<>();

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
            log.atWarn().log("[COORDINATOR FAILOVER] Failed to fetch topology from coordinator. Switching to next node...", ex);
            long delayMs = 100L * (attemptCount + 1);
            scheduledExecutorService.schedule(() -> requestRoutingInfoFromCoordinator(attemptCount + 1), delayMs, TimeUnit.MILLISECONDS);
            return null;
        });
    }

    private HurriCacheClientInterface newFastCacheClient(String target) {
        return new FastCacheAsyncSimpleClient(ManagedChannelBuilder.forTarget(target)
                                                      .maxInboundMessageSize(64 * 1024 * 1024)
                                                      .usePlaintext()
                                                      .directExecutor()
                                                      .build(),
                                              defaultClientId,
                                              defaultTimeout,
                                              getDefaultCompressionThreshold()) {
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

            @Override
            public int getDefaultCompressionThreshold() {
                return FastCacheAsyncSmartClient.this.defaultCompressionThreshold;
            }
        };
    }

    public boolean getReadyFlag() {
        return readyFlag.get();
    }

    @Override
    public String getTarget() {
        ManagedChannel channel = activeCoordinatorChannel.get();
        return channel != null ? channel.toString() : "UNKNOWN";
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
        Mode methodDefaultMode = currentModeOverride.get();
        if (methodDefaultMode != null){
            return execute(hint, methodDefaultMode, action);
        }
        return execute(hint, Mode.MASTER_THAN_BACKUP, action);
    }

    private <T> CompletableFuture<T> execute(KeyHintData hint,
                                             Mode methodDefaultMode,
                                             Function<HurriCacheClientInterface, CompletableFuture<T>> action) {
        return ensureReady().thenCompose(v -> {
            Mode baseMode = (methodDefaultMode != null) ? methodDefaultMode : configuredMode;
            RoutingInfo routingInfo = routing_info.get();

            int shard;
            if (hint == null || hint.getWeek_hash() == null) {
                shard = (randomShard.incrementAndGet() & Integer.MAX_VALUE) % routingInfo.max_shards;
            } else {
                long unsignedHash = Integer.toUnsignedLong(hint.getWeek_hash());
                shard = (int) (unsignedHash % routingInfo.max_shards);
            }

            HurriCacheClientInterface master = getRoute(routingInfo, shard, MASTER);
            HurriCacheClientInterface backup = getRoute(routingInfo, shard, BACKUP);

            if (master == null && backup == null) {
                return CompletableFuture.failedFuture(new RuntimeException("No healthy endpoints available for shard allocation"));
            }

            Mode effectiveMode = baseMode;
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
                    boolean tryMasterFirst = ThreadLocalRandom.current().nextBoolean();
                    HurriCacheClientInterface primary = tryMasterFirst ? master : backup;
                    HurriCacheClientInterface secondary = tryMasterFirst ? backup : master;
                    yield action.apply(primary)
                            .handle(fallbackGuard(shard, routingInfo, action, primary, secondary))
                            .thenCompose(Function.identity());
                }
            };
        });
    }

    private <T> CompletableFuture<T> execute(KeyHintData hint,
                                             Function<HurriCacheClientInterface, CompletableFuture<T>> action) {
        Mode overrideMode = currentModeOverride.get();
        if (overrideMode != null) {
            return execute(hint, overrideMode, action);
        }
        return execute(hint, configuredMode, action);
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
                    log.atInfo().log("[FAILOVER] Failing over pipeline execution target to: {}", fallbackEndpoint.getTarget());
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
        return executeWrite(hint, c -> c.getAndDeleteValue(key, hint, clientId, timeout));
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
        return execute(hint, c -> c.streamElementInRangeUnordered(key, hint, containerType, start, end, clientId, timeout));
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

        ManagedChannel coordinatorChannel = activeCoordinatorChannel.get();
        if (coordinatorChannel != null) {
            coordinatorChannel.shutdown();
        }

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
    public CompletableFuture<KeyHintData> atomicCreate(byte[] key,
                                                       KeyHintData hint,
                                                       long value,
                                                       Duration ttl,
                                                       int clientId,
                                                       Duration timeout) {
        return executeWrite(hint, c -> c.atomicCreate(key, hint, value, ttl, clientId, timeout));
    }

    @Override
    public CompletableFuture<KeyHintData> atomicStore(byte[] key,
                                                      KeyHintData hint,
                                                      long value,
                                                      Duration ttl,
                                                      int clientId,
                                                      Duration timeout) {
        return executeWrite(hint, c -> c.atomicStore(key, hint, value, ttl, clientId, timeout));
    }

    @Override
    public CompletableFuture<Long> atomicExchange(byte[] key,
                                                  KeyHintData hint,
                                                  long value,
                                                  Duration ttl,
                                                  int clientId,
                                                  Duration timeout) {
        return executeWrite(hint, c -> c.atomicExchange(key, hint, value, ttl, clientId, timeout));
    }

    @Override
    public CompletableFuture<Long> atomicAdd(byte[] key,
                                             KeyHintData hint,
                                             long delta,
                                             Duration ttl,
                                             int clientId,
                                             Duration timeout) {
        return executeWrite(hint, c -> c.atomicAdd(key, hint, delta, ttl, clientId, timeout));
    }

    @Override
    public CompletableFuture<Long> atomicSub(byte[] key,
                                             KeyHintData hint,
                                             long delta,
                                             Duration ttl,
                                             int clientId,
                                             Duration timeout) {
        return executeWrite(hint, c -> c.atomicSub(key, hint, delta, ttl, clientId, timeout));
    }

    @Override
    public CompletableFuture<Long> atomicAnd(byte[] key,
                                             KeyHintData hint,
                                             long mask,
                                             Duration ttl,
                                             int clientId,
                                             Duration timeout) {
        return executeWrite(hint, c -> c.atomicAnd(key, hint, mask, ttl, clientId, timeout));
    }

    @Override
    public CompletableFuture<Long> atomicOr(byte[] key,
                                            KeyHintData hint,
                                            long mask,
                                            Duration ttl,
                                            int clientId,
                                            Duration timeout) {
        return executeWrite(hint, c -> c.atomicOr(key, hint, mask, ttl, clientId, timeout));
    }

    @Override
    public CompletableFuture<Long> atomicXor(byte[] key,
                                             KeyHintData hint,
                                             long mask,
                                             Duration ttl,
                                             int clientId,
                                             Duration timeout) {
        return executeWrite(hint, c -> c.atomicXor(key, hint, mask, ttl, clientId, timeout));
    }

    @Override
    public CompletableFuture<AtomicCasRes> atomicCompareAndSet(byte[] key,
                                                               KeyHintData hint,
                                                               long expectedValue,
                                                               long newValue,
                                                               Duration ttl,
                                                               int clientId,
                                                               Duration timeout) {
        return executeWrite(hint, c -> c.atomicCompareAndSet(key, hint, expectedValue, newValue, ttl, clientId, timeout));
    }

    public FastCacheAsyncSmartClient setMode(Mode mode) {
        this.currentModeOverride.set(mode);
        return this;
    }

    // =========================================================================
    // CONTAINER & SIZE OPERATIONS DISPATCH
    // =========================================================================

    @Override
    public CompletableFuture<Integer> getSize(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return execute(hint, c -> c.getSize(key, hint, clientId, timeout));
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

    @Override
    public CompletableFuture<Long> atomicLoad(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return execute(hint, c -> c.atomicLoad(key, hint, clientId, timeout));
    }

    @Override
    public CompletableFuture<Long> atomicLoadAndDelete(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return executeWrite(hint, c -> c.atomicLoadAndDelete(key, hint, clientId, timeout));
    }

    @Override
    public CompletableFuture<Map<Payload, Payload>> streamMap(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return execute(hint, c -> c.streamMap(key, hint, clientId, timeout));
    }

    @Override
    public CompletableFuture<Map<OrderedPayload, Payload>> streamOrderedMap(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return execute(hint, c -> c.streamOrderedMap(key, hint, clientId, timeout));
    }
    @Override
    public CompletableFuture<List<Payload>> streamSet(byte[] key, KeyHintData hint,int clientId,
                                                      Duration timeout){
        return execute(hint, c -> c.streamSet(key, hint, clientId, timeout));
    }

    @Override
    public CompletableFuture<byte[]> getContainerValue(byte[] key, KeyHintData hint, byte[] elementKey, int clientId, Duration timeout) {
        return execute(hint, c -> c.getContainerValue(key, hint, elementKey, clientId, timeout));
    }

    @Override
    public CompletableFuture<byte[]> getAndRemoveContainerValue(byte[] key, KeyHintData hint, byte[] elementKey, int clientId, Duration timeout) {
        return executeWrite(hint, c -> c.getAndRemoveContainerValue(key, hint, elementKey, clientId, timeout));
    }

    @Override
    public CompletableFuture<Boolean> containsContainerKey(byte[] key, KeyHintData hint, byte[] elementKey, int clientId, Duration timeout) {
        return execute(hint, c -> c.containsContainerKey(key, hint, elementKey, clientId, timeout));
    }

    @Override
    public CompletableFuture<byte[]> updateContainerValue(byte[] key, KeyHintData hint, byte[] elementKey, byte[] value, int clientId, Duration timeout) {
        return executeWrite(hint, c -> c.updateContainerValue(key, hint, elementKey, value, clientId, timeout));
    }

    @Override
    public CompletableFuture<Integer> removeFromContainer(byte[] key, KeyHintData hint, byte[] elementKey, int clientId, Duration timeout) {
        return executeWrite(hint, c -> c.removeFromContainer(key, hint, elementKey, clientId, timeout));
    }

    private CompletableFuture<Void> ensureReady() {
        if (readyFlag.get()) {
            return CompletableFuture.completedFuture(null);
        }
        return CompletableFuture.runAsync(() -> {
            try {
                if (!readyLatch.await(readyTimeout.toMillis(), TimeUnit.MILLISECONDS)) {
                    throw new CompletionException(new TimeoutException("FastCacheClient boot timeout reached."));
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new CompletionException(e);
            }
        });
    }

    @Override
    public int getDefaultCompressionThreshold() {
        return defaultCompressionThreshold;
    }

    private static void repDelay() {
        try {
            Thread.sleep(500);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }

    // =========================================================================
    // CONTAINER CREATION WITH CHUNKING & ROUTING
    // =========================================================================

    @FunctionalInterface
    private interface DirectChunkSender<K, V> {
        CompletableFuture<?> send(HurriCacheClientInterface simpleClient, KeyHintData hint, List<K> keys, List<V> values);
    }

    @Override
    public CompletableFuture<KeyHintData> createQueue(byte[] key,
                                                      KeyHintData keyHint,
                                                      List<Payload> initialValue,
                                                      Duration ttl,
                                                      int clientId,
                                                      Duration timeout) {
        return createUnorderedContainerSmart(key, keyHint, initialValue, ttl, clientId, timeout, ContainerType.QUEUE);
    }

    @Override
    public CompletableFuture<KeyHintData> createList(byte[] key,
                                                     KeyHintData keyHint,
                                                     List<Payload> initialValue,
                                                     Duration ttl,
                                                     int clientId,
                                                     Duration timeout) {
        return createUnorderedContainerSmart(key, keyHint, initialValue, ttl, clientId, timeout, ContainerType.LIST);
    }

    @Override
    public CompletableFuture<KeyHintData> createVector(byte[] key,
                                                       KeyHintData keyHint,
                                                       List<Payload> initialValue,
                                                       Duration ttl,
                                                       int clientId,
                                                       Duration timeout) {
        return createUnorderedContainerSmart(key, keyHint, initialValue, ttl, clientId, timeout, ContainerType.VECTOR);
    }

    @Override
    public CompletableFuture<KeyHintData> createSet(byte[] key,
                                                    KeyHintData keyHint,
                                                    List<Payload> initialValue,
                                                    Duration ttl,
                                                    int clientId,
                                                    Duration timeout) {
        return createUnorderedContainerSmart(key, keyHint, initialValue, ttl, clientId, timeout, ContainerType.SET);
    }

    @Override
    public CompletableFuture<KeyHintData> createOrderedSet(byte[] key,
                                                           KeyHintData keyHint,
                                                           List<OrderedPayload> initialValue,
                                                           Duration ttl,
                                                           int clientId,
                                                           Duration timeout) {
        List<OrderedPayload> firstChunk = new ArrayList<>();
        List<OrderedPayload> remaining = new ArrayList<>();
        splitOrderedPayloads(initialValue, firstChunk, remaining, clientId);

        return createContainerWithChunks(key, keyHint, ttl, clientId, timeout,
                                         c -> c.createOrderedSet(key, keyHint, firstChunk, ttl, clientId, timeout),
                                         remaining, null,
                                         (simpleClient, resHint, chunkKeys, chunkValues) ->
                                                 simpleClient.addElementOrdered(key, resHint, chunkKeys, clientId, timeout));
    }

    @Override
    public CompletableFuture<KeyHintData> createMap(byte[] key,
                                                    KeyHintData keyHint,
                                                    Map<Payload, Payload> initialValue,
                                                    Duration ttl,
                                                    int clientId,
                                                    Duration timeout) {
        Map<Payload, Payload> firstChunkMap = new LinkedHashMap<>();
        List<Payload> remainingKeys = new ArrayList<>();
        List<Payload> remainingValues = new ArrayList<>();

        splitMapEntries(initialValue, firstChunkMap, remainingKeys, remainingValues, clientId, ttl);

        return createContainerWithChunks(key, keyHint, ttl, clientId, timeout,
                                         c -> c.createMap(key, keyHint, firstChunkMap, ttl, clientId, timeout),
                                         remainingKeys, remainingValues,
                                         (simpleClient, resHint, chunkKeys, chunkValues) ->
                                                 simpleClient.addElementHashMap(key, resHint, chunkKeys, chunkValues, clientId, timeout));
    }

    @Override
    public CompletableFuture<KeyHintData> createOrderedMap(byte[] key,
                                                           KeyHintData keyHint,
                                                           Map<OrderedPayload, Payload> initialValue,
                                                           Duration ttl,
                                                           int clientId,
                                                           Duration timeout) {
        Map<OrderedPayload, Payload> firstChunkMap = new LinkedHashMap<>();
        List<OrderedPayload> remainingKeys = new ArrayList<>();
        List<Payload> remainingValues = new ArrayList<>();

        splitOrderedMapEntries(initialValue, firstChunkMap, remainingKeys, remainingValues, clientId, ttl);

        return createContainerWithChunks(key, keyHint, ttl, clientId, timeout,
                                         c -> c.createOrderedMap(key, keyHint, firstChunkMap, ttl, clientId, timeout),
                                         remainingKeys, remainingValues,
                                         (simpleClient, resHint, chunkKeys, chunkValues) ->
                                                 simpleClient.addElementOrderedMap(key, resHint, chunkKeys, chunkValues, clientId, timeout));
    }

    private CompletableFuture<KeyHintData> createUnorderedContainerSmart(byte[] key,
                                                                         KeyHintData keyHint,
                                                                         List<Payload> initialValue,
                                                                         Duration ttl,
                                                                         int clientId,
                                                                         Duration timeout,
                                                                         ContainerType type) {
        List<Payload> firstChunk = new ArrayList<>();
        List<Payload> remaining = new ArrayList<>();
        splitPayloads(initialValue, firstChunk, remaining, clientId);

        return createContainerWithChunks(key, keyHint, ttl, clientId, timeout,
                                         c -> switch (type) {
                                             case QUEUE -> c.createQueue(key, keyHint, firstChunk, ttl, clientId, timeout);
                                             case LIST -> c.createList(key, keyHint, firstChunk, ttl, clientId, timeout);
                                             case VECTOR -> c.createVector(key, keyHint, firstChunk, ttl, clientId, timeout);
                                             case SET -> c.createSet(key, keyHint, firstChunk, ttl, clientId, timeout);
                                             default -> throw new IllegalArgumentException("Unsupported type: " + type);
                                         },
                                         remaining, null,
                                         (simpleClient, resHint, chunkKeys, chunkValues) -> switch (type) {
                                             case QUEUE, LIST, VECTOR -> simpleClient.addElementToTail(key, resHint, chunkKeys, clientId, timeout);
                                             case SET -> simpleClient.addElement(key, resHint, chunkKeys, clientId, timeout);
                                             default -> throw new IllegalArgumentException("Unsupported type: " + type);
                                         }
        );
    }

    private <K, V> CompletableFuture<KeyHintData> createContainerWithChunks(byte[] key,
                                                                            KeyHintData hint,
                                                                            Duration ttl,
                                                                            int clientId,
                                                                            Duration timeout,
                                                                            Function<HurriCacheClientInterface, CompletableFuture<KeyHintData>> createCall,
                                                                            List<K> remainingKeys,
                                                                            List<V> remainingValues,
                                                                            DirectChunkSender<K, V> chunkSender) {
        CompletableFuture<KeyHintData> createFuture = executeWrite(hint, createCall);

        if (remainingKeys == null || remainingKeys.isEmpty()) {
            return createFuture;
        }

        return createFuture.thenCompose(resHint ->
                                                CompletableFuture.runAsync(FastCacheAsyncSmartClient::repDelay)
                                                        .thenCompose(ignored -> sendRemainingChunksInPipeline(resHint,
                                                                                                              remainingKeys,
                                                                                                              remainingValues,
                                                                                                              clientId,
                                                                                                              chunkSender))
                                                        .thenApply(ignored -> resHint)
        );
    }

    private <K, V> CompletableFuture<Void> sendRemainingChunksInPipeline(KeyHintData hint,
                                                                         List<K> keys,
                                                                         List<V> values,
                                                                         int clientId,
                                                                         DirectChunkSender<K, V> chunkSender) {
        if (keys == null || keys.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        List<Pair<List<K>, List<V>>> chunks = partitionChunks(keys, values, clientId);
        CompletableFuture<Void> pipeline = CompletableFuture.completedFuture(null);

        for (Pair<List<K>, List<V>> chunk : chunks) {
            pipeline = pipeline.thenCompose(v -> executeWrite(hint, simpleClient ->
                    chunkSender.send(simpleClient, hint, chunk.first, chunk.second)
            )).<Void>thenApply(res -> null);
        }

        return pipeline;
    }

    private <K, V> List<Pair<List<K>, List<V>>> partitionChunks(List<K> keys, List<V> values, int clientId) {
        List<Pair<List<K>, List<V>>> chunks = new ArrayList<>();
        List<K> currentKeys = new ArrayList<>();
        List<V> currentValues = new ArrayList<>();
        long currentChunkSize = 0;

        for (int i = 0; i < keys.size(); i++) {
            K k = keys.get(i);
            V v = (values != null && i < values.size()) ? values.get(i) : null;

            long itemSize = calculateSerializedSize(k, v, clientId);

            if (!currentKeys.isEmpty() && (currentChunkSize + itemSize > MAX_RPC_SIZE)) {
                chunks.add(Pair.of(new ArrayList<>(currentKeys), new ArrayList<>(currentValues)));
                currentKeys.clear();
                currentValues.clear();
                currentChunkSize = 0;
            }

            currentKeys.add(k);
            if (v != null) {
                currentValues.add(v);
            }
            currentChunkSize += itemSize;
        }

        if (!currentKeys.isEmpty()) {
            chunks.add(Pair.of(currentKeys, currentValues));
        }

        return chunks;
    }

    private long calculateSerializedSize(Object keyOrPayload, Object valuePayload, int clientId) {
        long size = 0;
        if (keyOrPayload instanceof OrderedPayload op) {
            long order = op.getOrder() != null ? op.getOrder() : 0L;
            size += KeyValueUtils.createOrderedKey(op.getValue(), order, clientId,getDefaultCompressionThreshold()).build().getSerializedSize();
        } else if (keyOrPayload instanceof Payload p) {
            size += KeyValueUtils.createUnorderedKey(p.getValue(), clientId, getDefaultCompressionThreshold()).build().getSerializedSize();
        }

        if (valuePayload instanceof Payload vp) {
            size += KeyValueUtils.createUnorderedValue(vp.getValue(), null, getDefaultCompressionThreshold()).build().getSerializedSize();
        }
        return size;
    }

    private void splitPayloads(List<Payload> source, List<Payload> firstChunk, List<Payload> remaining, int clientId) {
        if (source == null) return;
        long currentChunkSize = 128;

        for (Payload p : source) {
            long elemSize = CompressionUtils.compressIfNeeded(p.getValue(), getDefaultCompressionThreshold()).build().getSerializedSize();
            if (remaining.isEmpty() && (currentChunkSize + elemSize <= MAX_RPC_SIZE)) {
                firstChunk.add(p);
                currentChunkSize += elemSize;
            } else {
                remaining.add(p);
            }
        }
    }

    private void splitOrderedPayloads(List<OrderedPayload> source, List<OrderedPayload> firstChunk, List<OrderedPayload> remaining, int clientId) {
        if (source == null) return;
        long currentChunkSize = 128;
        for (OrderedPayload op : source) {
            long order = op.getOrder() != null ? op.getOrder() : 0L;
            long elemSize = KeyValueUtils.createOrderedValue(op.getValue(), order, null, getDefaultCompressionThreshold()).build().getSerializedSize();
            if (remaining.isEmpty() && (currentChunkSize + elemSize <= MAX_RPC_SIZE)) {
                firstChunk.add(op);
                currentChunkSize += elemSize;
            } else {
                remaining.add(op);
            }
        }
    }

    private void splitMapEntries(Map<Payload, Payload> source, Map<Payload, Payload> firstChunk, List<Payload> remainingKeys, List<Payload> remainingValues, int clientId, Duration ttl) {
        if (source == null) return;
        long currentChunkSize = 128;
        for (Map.Entry<Payload, Payload> entry : source.entrySet()) {
            Key kVal = KeyValueUtils.createUnorderedKey(entry.getKey().getValue(), clientId, null).build();
            Value vVal = KeyValueUtils.createUnorderedValue(entry.getValue().getValue(), ttl, null).build();
            long pairSize = kVal.getSerializedSize() + vVal.getSerializedSize();

            if (remainingKeys.isEmpty() && (currentChunkSize + pairSize <= MAX_RPC_SIZE)) {
                firstChunk.put(entry.getKey(), entry.getValue());
                currentChunkSize += pairSize;
            } else {
                remainingKeys.add(entry.getKey());
                remainingValues.add(entry.getValue());
            }
        }
    }

    private void splitOrderedMapEntries(Map<OrderedPayload, Payload> source, Map<OrderedPayload, Payload> firstChunk, List<OrderedPayload> remainingKeys, List<Payload> remainingValues, int clientId, Duration ttl) {
        if (source == null) return;
        long currentChunkSize = 128;
        for (Map.Entry<OrderedPayload, Payload> entry : source.entrySet()) {
            long order = entry.getKey().getOrder() != null ? entry.getKey().getOrder() : 0L;
            OrderedKey kVal = KeyValueUtils.createOrderedKey(entry.getKey().getValue(), order, clientId,getDefaultCompressionThreshold()).build();
            Value vVal = KeyValueUtils.createUnorderedValue(entry.getValue().getValue(), ttl, getDefaultCompressionThreshold()).build();
            long pairSize = kVal.getSerializedSize() + vVal.getSerializedSize();

            if (remainingKeys.isEmpty() && (currentChunkSize + pairSize <= MAX_RPC_SIZE)) {
                firstChunk.put(entry.getKey(), entry.getValue());
                currentChunkSize += pairSize;
            } else {
                remainingKeys.add(entry.getKey());
                remainingValues.add(entry.getValue());
            }
        }
    }

    // =========================================================================
    // CHUNKED ELEMENT ADDITION METHODS
    // =========================================================================

    @Override
    public CompletableFuture<Integer> addElement(byte[] key, KeyHintData hint, List<Payload> data, int clientId, Duration timeout) {
        if (data == null || data.isEmpty()) {
            return CompletableFuture.completedFuture(0);
        }
        List<List<Payload>> chunks = splitUnorderedPayloads(key, hint, data, clientId);
        CompletableFuture<Integer> future = CompletableFuture.completedFuture(0);

        for (List<Payload> chunk : chunks) {
            future = future.thenCompose(addedCount ->
                                                executeWrite(hint, c -> c.addElement(key, hint, chunk, clientId, timeout))
                                                        .thenApply(res -> addedCount + res)
            );
        }
        return future;
    }

    @Override
    public CompletableFuture<Integer> addElementToTail(byte[] key, KeyHintData hint, List<Payload> data, int clientId, Duration timeout) {
        if (data == null || data.isEmpty()) {
            return CompletableFuture.completedFuture(0);
        }
        List<List<Payload>> chunks = splitUnorderedPayloads(key, hint, data, clientId);
        CompletableFuture<Integer> future = CompletableFuture.completedFuture(0);

        for (List<Payload> chunk : chunks) {
            future = future.thenCompose(addedCount ->
                                                executeWrite(hint, c -> c.addElementToTail(key, hint, chunk, clientId, timeout))
                                                        .thenApply(res -> addedCount + res)
            );
        }
        return future;
    }

    @Override
    public CompletableFuture<Integer> addElementToHead(byte[] key, KeyHintData hint, List<Payload> data, int clientId, Duration timeout) {
        if (data == null || data.isEmpty()) {
            return CompletableFuture.completedFuture(0);
        }
        List<List<Payload>> chunks = splitUnorderedPayloads(key, hint, data, clientId);
        CompletableFuture<Integer> future = CompletableFuture.completedFuture(0);

        for (int i = chunks.size() - 1; i >= 0; i--) {
            List<Payload> chunk = chunks.get(i);
            future = future.thenCompose(addedCount ->
                                                executeWrite(hint, c -> c.addElementToHead(key, hint, chunk, clientId, timeout))
                                                        .thenApply(res -> addedCount + res)
            );
        }
        return future;
    }

    @Override
    public CompletableFuture<Integer> addElementOrdered(byte[] key, KeyHintData hint, List<OrderedPayload> data, int clientId, Duration timeout) {
        if (data == null || data.isEmpty()) {
            return CompletableFuture.completedFuture(0);
        }
        List<List<OrderedPayload>> chunks = splitOrderedPayloads(key, hint, data, clientId);
        CompletableFuture<Integer> future = CompletableFuture.completedFuture(0);

        for (List<OrderedPayload> chunk : chunks) {
            future = future.thenCompose(addedCount ->
                                                executeWrite(hint, c -> c.addElementOrdered(key, hint, chunk, clientId, timeout))
                                                        .thenApply(res -> addedCount + res)
            );
        }
        return future;
    }

    @Override
    public CompletableFuture<Integer> addElementWithWeight(byte[] key, KeyHintData hint, List<OrderedPayload> data, int clientId, Duration timeout) {
        if (data == null || data.isEmpty()) {
            return CompletableFuture.completedFuture(0);
        }
        List<List<OrderedPayload>> chunks = splitOrderedPayloads(key, hint, data, clientId);
        CompletableFuture<Integer> future = CompletableFuture.completedFuture(0);

        for (List<OrderedPayload> chunk : chunks) {
            future = future.thenCompose(addedCount ->
                                                executeWrite(hint, c -> c.addElementWithWeight(key, hint, chunk, clientId, timeout))
                                                        .thenApply(res -> addedCount + res)
            );
        }
        return future;
    }

    @Override
    public CompletableFuture<Integer> addElementToPosition(byte[] key, KeyHintData hint, List<Payload> data, int pos, int clientId, Duration timeout) {
        if (data == null || data.isEmpty()) {
            return CompletableFuture.completedFuture(0);
        }
        List<List<Payload>> chunks = splitUnorderedPayloads(key, hint, data, clientId);
        CompletableFuture<Integer> future = CompletableFuture.completedFuture(0);

        int currentPos = pos;
        for (List<Payload> chunk : chunks) {
            final int targetPos = currentPos;
            final int chunkSize = chunk.size();
            future = future.thenCompose(addedCount ->
                                                executeWrite(hint, c -> c.addElementToPosition(key, hint, chunk, targetPos, clientId, timeout))
                                                        .thenApply(res -> addedCount + res)
            );
            currentPos += chunkSize;
        }
        return future;
    }

    @Override
    public CompletableFuture<Integer> addElementToPositionBefore(byte[] key, KeyHintData hint, List<Payload> data, Payload pos, int clientId, Duration timeout) {
        if (data == null || data.isEmpty()) {
            return CompletableFuture.completedFuture(0);
        }

        List<List<Payload>> chunks = splitUnorderedPayloads(key, hint, data, clientId);

        CompletableFuture<Integer> future = executeWrite(hint, c -> c.addElementToPositionBefore(key, hint, chunks.get(0), pos, clientId, timeout));
        Payload currentPivot = chunks.get(0).get(chunks.get(0).size() - 1);

        for (int i = 1; i < chunks.size(); i++) {
            List<Payload> chunk = chunks.get(i);
            Payload nextPivot = chunk.get(chunk.size() - 1);
            Payload anchor = currentPivot;

            future = future.thenCompose(addedCount ->
                                                executeWrite(hint, c -> c.addElementToPositionAfter(key, hint, chunk, anchor, clientId, timeout))
                                                        .thenApply(res -> addedCount + res)
            );
            currentPivot = nextPivot;
        }
        return future;
    }

    @Override
    public CompletableFuture<Integer> addElementToPositionAfter(byte[] key, KeyHintData hint, List<Payload> data, Payload pos, int clientId, Duration timeout) {
        if (data == null || data.isEmpty()) {
            return CompletableFuture.completedFuture(0);
        }
        List<List<Payload>> chunks = splitUnorderedPayloads(key, hint, data, clientId);
        CompletableFuture<Integer> future = CompletableFuture.completedFuture(0);

        Payload currentPivot = pos;
        for (List<Payload> chunk : chunks) {
            Payload anchor = currentPivot;
            Payload nextPivot = chunk.get(chunk.size() - 1);

            future = future.thenCompose(addedCount ->
                                                executeWrite(hint, c -> c.addElementToPositionAfter(key, hint, chunk, anchor, clientId, timeout))
                                                        .thenApply(res -> addedCount + res)
            );
            currentPivot = nextPivot;
        }
        return future;
    }

    // =========================================================================
    // MAP CHUNKED ELEMENT ADDITION METHODS
    // =========================================================================

    @Override
    public CompletableFuture<Integer> addElementHashMap(byte[] key,
                                                        KeyHintData hint,
                                                        List<Payload> container_keys,
                                                        List<Payload> container_values,
                                                        int clientId,
                                                        Duration timeout) {
        if (container_keys == null || container_keys.isEmpty()) {
            return CompletableFuture.completedFuture(0);
        }

        List<Pair<List<Payload>, List<Payload>>> chunks = splitMapPayloads(key, hint, container_keys, container_values, clientId);
        CompletableFuture<Integer> future = CompletableFuture.completedFuture(0);

        for (Pair<List<Payload>, List<Payload>> chunk : chunks) {
            future = future.thenCompose(addedCount ->
                                                executeWrite(hint, c -> c.addElementHashMap(key, hint, chunk.first, chunk.second, clientId, timeout))
                                                        .thenApply(res -> addedCount + res)
            );
        }
        return future;
    }

    @Override
    public CompletableFuture<Integer> addElementOrderedMap(byte[] key,
                                                           KeyHintData hint,
                                                           List<OrderedPayload> container_keys,
                                                           List<Payload> container_values,
                                                           int clientId,
                                                           Duration timeout) {
        if (container_keys == null || container_keys.isEmpty()) {
            return CompletableFuture.completedFuture(0);
        }

        List<Pair<List<OrderedPayload>, List<Payload>>> chunks = splitOrderedMapPayloads(key, hint, container_keys, container_values, clientId);
        CompletableFuture<Integer> future = CompletableFuture.completedFuture(0);

        for (Pair<List<OrderedPayload>, List<Payload>> chunk : chunks) {
            future = future.thenCompose(addedCount ->
                                                executeWrite(hint, c -> c.addElementOrderedMap(key, hint, chunk.first, chunk.second, clientId, timeout))
                                                        .thenApply(res -> addedCount + res)
            );
        }
        return future;
    }

    private List<Pair<List<Payload>, List<Payload>>> splitMapPayloads(byte[] key,
                                                                      KeyHintData hint,
                                                                      List<Payload> keys,
                                                                      List<Payload> values,
                                                                      int clientId) {
        List<Pair<List<Payload>, List<Payload>>> chunks = new ArrayList<>();
        Key protoKey = KeyValueUtils.createUnorderedKey(key, clientId, null).build();
        long currentChunkSize = protoKey.getSerializedSize() + 32;

        List<Payload> currentKeys = new ArrayList<>();
        List<Payload> currentValues = new ArrayList<>();

        for (int i = 0; i < keys.size(); i++) {
            Payload k = keys.get(i);
            Payload v = (values != null && i < values.size()) ? values.get(i) : null;

            Key kProto = KeyValueUtils.createUnorderedKey(k.getValue(), clientId, null).build();
            Value vProto = v != null
                           ? KeyValueUtils.createUnorderedValue(v.getValue(), null, getDefaultCompressionThreshold()).build()
                           : Value.getDefaultInstance();

            long pairSize = kProto.getSerializedSize() + vProto.getSerializedSize();

            if (!currentKeys.isEmpty() && (currentChunkSize + pairSize > MAX_RPC_SIZE)) {
                chunks.add(Pair.of(currentKeys, currentValues));
                currentKeys = new ArrayList<>();
                currentValues = new ArrayList<>();
                currentChunkSize = protoKey.getSerializedSize() + 32;
            }

            currentKeys.add(k);
            if (v != null) {
                currentValues.add(v);
            }
            currentChunkSize += pairSize;
        }

        if (!currentKeys.isEmpty()) {
            chunks.add(Pair.of(currentKeys, currentValues));
        }

        return chunks;
    }

    private List<Pair<List<OrderedPayload>, List<Payload>>> splitOrderedMapPayloads(byte[] key,
                                                                                    KeyHintData hint,
                                                                                    List<OrderedPayload> keys,
                                                                                    List<Payload> values,
                                                                                    int clientId) {
        List<Pair<List<OrderedPayload>, List<Payload>>> chunks = new ArrayList<>();
        Key protoKey = KeyValueUtils.createUnorderedKey(key, clientId, null).build();
        long currentChunkSize = protoKey.getSerializedSize() + 32;

        List<OrderedPayload> currentKeys = new ArrayList<>();
        List<Payload> currentValues = new ArrayList<>();

        for (int i = 0; i < keys.size(); i++) {
            OrderedPayload k = keys.get(i);
            Payload v = (values != null && i < values.size()) ? values.get(i) : null;

            long order = k.getOrder() != null ? k.getOrder() : 0L;
            OrderedKey kProto = KeyValueUtils.createOrderedKey(k.getValue(), order, clientId,getDefaultCompressionThreshold()).build();
            Value vProto = v != null
                           ? KeyValueUtils.createUnorderedValue(v.getValue(), null, getDefaultCompressionThreshold()).build()
                           : Value.getDefaultInstance();

            long pairSize = kProto.getSerializedSize() + vProto.getSerializedSize();

            if (!currentKeys.isEmpty() && (currentChunkSize + pairSize > MAX_RPC_SIZE)) {
                chunks.add(Pair.of(currentKeys, currentValues));
                currentKeys = new ArrayList<>();
                currentValues = new ArrayList<>();
                currentChunkSize = protoKey.getSerializedSize() + 32;
            }

            currentKeys.add(k);
            if (v != null) {
                currentValues.add(v);
            }
            currentChunkSize += pairSize;
        }

        if (!currentKeys.isEmpty()) {
            chunks.add(Pair.of(currentKeys, currentValues));
        }

        return chunks;
    }

    private List<List<Payload>> splitUnorderedPayloads(byte[] key, KeyHintData hint, List<Payload> data, int clientId) {
        List<List<Payload>> chunks = new ArrayList<>();
        Key protoKey = KeyValueUtils.createUnorderedKey(key, clientId, null).build();
        long currentChunkSize = protoKey.getSerializedSize() + 32;

        List<Payload> currentChunk = new ArrayList<>();

        for (Payload payload : data) {
            Value.Builder compressedValue = CompressionUtils.compressIfNeeded(payload.getValue(), getDefaultCompressionThreshold());
            int elemSize = compressedValue.build().getSerializedSize();

            if (!currentChunk.isEmpty() && (currentChunkSize + elemSize > MAX_RPC_SIZE)) {
                chunks.add(currentChunk);
                currentChunk = new ArrayList<>();
                currentChunkSize = protoKey.getSerializedSize() + 32;
            }

            currentChunk.add(payload);
            currentChunkSize += elemSize;
        }

        if (!currentChunk.isEmpty()) {
            chunks.add(currentChunk);
        }

        return chunks;
    }

    private List<List<OrderedPayload>> splitOrderedPayloads(byte[] key, KeyHintData hint, List<OrderedPayload> data, int clientId) {
        List<List<OrderedPayload>> chunks = new ArrayList<>();
        Key protoKey = KeyValueUtils.createUnorderedKey(key, clientId, null).build();
        long currentChunkSize = protoKey.getSerializedSize() + 32;

        List<OrderedPayload> currentChunk = new ArrayList<>();

        for (OrderedPayload payload : data) {
            long order = payload.getOrder() != null ? payload.getOrder() : 0L;
            OrderedValue orderedValue = KeyValueUtils.createOrderedValue(payload.getValue(), order, null, getDefaultCompressionThreshold()).build();
            int elemSize = orderedValue.getSerializedSize();

            if (!currentChunk.isEmpty() && (currentChunkSize + elemSize > MAX_RPC_SIZE)) {
                chunks.add(currentChunk);
                currentChunk = new ArrayList<>();
                currentChunkSize = protoKey.getSerializedSize() + 32;
            }

            currentChunk.add(payload);
            currentChunkSize += elemSize;
        }

        if (!currentChunk.isEmpty()) {
            chunks.add(currentChunk);
        }

        return chunks;
    }
}