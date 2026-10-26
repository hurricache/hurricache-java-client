package com.hurricache.client;

import com.hurricache.client.intf.HurriCacheClientInterface;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Mode;
import com.hurricache.client.intf.OrderedPayload;
import com.hurricache.client.intf.OrderedVariant;
import com.hurricache.client.intf.Payload;
import com.hurricache.client.intf.Variant;
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
import com.hurricache.utils.Pair;
import com.hurricache.utils.RoutingObserver;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
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
    private final ScheduledExecutorService delayExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "fastcache-smart-delay");
        t.setDaemon(true);
        return t;
    });
    private final CompletableFuture<Void> readyFuture = new CompletableFuture<>();
    private final ConcurrentHashMap<String, HurriCacheClientInterface> rerouteClients = new ConcurrentHashMap<>();
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
                    readyFuture.complete(null);
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
        return new FastCacheAsyncSimpleClient(NettyChannelBuilder.forTarget(target).flowControlWindow(4 * 1024 * 1024)
                                                      .maxInboundMessageSize(64 * 1024 * 1024)
                                                      .usePlaintext()
                                                      .build(),
                                              defaultClientId,
                                              defaultTimeout,
                                              getDefaultCompressionThreshold(),
                                              true) {
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
        return coordinatorAddresses.get(activeCoordinatorIndex.get());
    }

    @Override
    public int getDefaultClientId() {
        return defaultClientId;
    }

    @Override
    public Duration getDefaultTimeout() {
        return defaultTimeout;
    }

    /**
     * Executes a write operation with fallback according to the effective {@link Mode}.
     * <p>
     * Fallback semantics are <b>at-least-once</b>: if the primary endpoint fails with
     * {@code UNAVAILABLE} or {@code DEADLINE_EXCEEDED}, the operation is retried on the
     * backup endpoint. Non-idempotent writes (pop, pushTail, add, ...) may therefore be
     * applied twice when the primary actually accepted the write before the failure was
     * observed. Callers requiring exactly-once semantics must deduplicate on their side.
     */
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

            int maxShards = routingInfo.max_shards;
            if (maxShards <= 0) {
                return CompletableFuture.failedFuture(
                        new IllegalStateException("Routing info is not ready: max_shards=" + maxShards));
            }

            int shard;
            if (hint == null || hint.getWeek_hash() == null) {
                shard = (randomShard.incrementAndGet() & Integer.MAX_VALUE) % maxShards;
            } else {
                long unsignedHash = Integer.toUnsignedLong(hint.getWeek_hash());
                shard = (int) (unsignedHash % maxShards);
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

            // =========================================================================
            // 1. ОБРАБОТКА РЕРОУТИНГА (FAILED_PRECONDITION)
            // =========================================================================
            if (isReroute(ex)) {
                RerouteTargets routes = getRerouteTargets(ex);
                if (routes != null) {
                    String currentTarget = currentEndpoint != null ? currentEndpoint.getTarget() : "";

                    String masterTarget = (routes.masterTarget() != null && !routes.masterTarget().equals(currentTarget))
                                          ? routes.masterTarget() : null;
                    String backupTarget = (routes.backupTarget() != null && !routes.backupTarget().equals(currentTarget))
                                          ? routes.backupTarget() : null;

                    HurriCacheClientInterface newMaster = masterTarget != null
                                                          ? resolveRerouteClient(routingInfo, masterTarget)
                                                          : null;

                    HurriCacheClientInterface newBackup = backupTarget != null
                                                          ? resolveRerouteClient(routingInfo, backupTarget)
                                                          : fallbackEndpoint;

                    log.atDebug().log("[REROUTE] Dynamic redirect from {} -> Master: {}, Backup: {}",
                                      currentTarget, masterTarget, backupTarget);

                    if (newMaster != null) {
                        return action.apply(newMaster)
                                .handle(fallbackGuard(shard, routingInfo, action, newMaster, newBackup))
                                .thenCompose(Function.identity());
                    }

                    if (newBackup != null && !newBackup.equals(currentEndpoint)) {
                        log.atWarn().log("[REROUTE FAILOVER] Master target invalid/dead. Directing straight to Backup: {}",
                                         newBackup.getTarget());
                        return action.apply(newBackup)
                                .handle(fallbackGuard(shard, routingInfo, action, newBackup, null))
                                .thenCompose(Function.identity());
                    }
                }
            }

            // =========================================================================
            // 2. ОБРАБОТКА НЕДОСТУПНОСТИ/ТАЙМАУТА НОДЫ (UNAVAILABLE, DEADLINE_EXCEEDED)
            // =========================================================================
            if (isUnavailable(ex) || isTimeout(ex)) {
                String deadTarget = currentEndpoint != null ? currentEndpoint.getTarget() : "UNKNOWN";
                log.atWarn().log("[FAILOVER] Target node is dead: {}. Triggering topology update...", deadTarget);

                scheduledExecutorService.execute(this::init);

                if (fallbackEndpoint != null && !fallbackEndpoint.equals(currentEndpoint)) {
                    log.atInfo().log("[FAILOVER] Retrying action on backup target: {}", fallbackEndpoint.getTarget());
                    return action.apply(fallbackEndpoint)
                            .handle(fallbackGuard(shard, routingInfo, action, fallbackEndpoint, null))
                            .thenCompose(Function.identity());
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
    public CompletableFuture<Variant> pop(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return executeWrite(hint, c -> c.pop(key, hint, clientId, timeout));
    }

    @Override
    public CompletableFuture<KeyHintData> create(byte[] key,
                                                 KeyHintData hint,
                                                 Variant value,
                                                 Duration ttl,
                                                 int clientId,
                                                 Duration timeout) {
        return execute(hint, c -> c.create(key, hint, value, ttl, clientId, timeout));
    }

    @Override
    public CompletableFuture<Variant> get(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return execute(hint, c -> c.get(key, hint, clientId, timeout));
    }

    @Override
    public CompletableFuture<Variant> replace(byte[] key,
                                              KeyHintData hint,
                                              Variant value,
                                              Duration ttl,
                                              int clientId,
                                              Duration timeout) {
        return execute(hint, c -> c.replace(key, hint, value, ttl, clientId, timeout));
    }

    @Override
    public CompletableFuture<Boolean> containsKey(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return execute(hint, c -> c.containsKey(key, hint, clientId, timeout));
    }

    @Override
    public CompletableFuture<Boolean> remove(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return execute(hint, c -> c.remove(key, hint, clientId, timeout));
    }

    @Override
    public CompletableFuture<Variant> popTail(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return executeWrite(hint, c -> c.popTail(key, hint, clientId, timeout));
    }

    @Override
    public CompletableFuture<Variant> popHead(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return executeWrite(hint, c -> c.popHead(key, hint, clientId, timeout));
    }


    @Override
    public CompletableFuture<Variant> getAt(byte[] key,
                                            KeyHintData hint,
                                            int pos,
                                            int clientId,
                                            Duration timeout) {
        return execute(hint, c -> c.getAt(key, hint, pos, clientId, timeout));
    }

    @Override
    public CompletableFuture<Variant> getElementWithWeight(byte[] key,
                                                           KeyHintData hint,
                                                           int pos,
                                                           int clientId,
                                                           Duration timeout) {
        return execute(hint, c -> c.getElementWithWeight(key, hint, pos, clientId, timeout));
    }

    @Override
    public CompletableFuture<Variant> getAndRemoveElementWithWeight(byte[] key,
                                                                    KeyHintData hint,
                                                                    int pos,
                                                                    int clientId,
                                                                    Duration timeout) {
        return execute(hint, c -> c.getAndRemoveElementWithWeight(key, hint, pos, clientId, timeout));
    }

    @Override
    public CompletableFuture<List<Variant>> getList(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return execute(hint, c -> c.getList(key, hint, clientId, timeout));
    }

    @Override
    public CompletableFuture<Map<OrderedPayload, Variant>> getElementInRangeOrderedMap(byte[] key,
                                                                                       KeyHintData hint,
                                                                                       long startWeight,
                                                                                       long endWeight,
                                                                                       boolean reverse,
                                                                                       int clientId,
                                                                                       Duration timeout) {
        return execute(hint, c -> c.getElementInRangeOrderedMap(key, hint, startWeight, endWeight, reverse, clientId, timeout));
    }

    @Override
    public CompletableFuture<LockStatus> lock(byte[] key,
                                              KeyHintData hint,
                                              LockType type,
                                              int clientId,
                                              Duration duration,
                                              Duration timeout) {
        return executeWrite(hint, c -> c.lock(key, hint, type, clientId, duration, timeout));
    }

    @Override
    public CompletableFuture<LockStatus> unlock(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return executeWrite(hint, c -> c.unlock(key, hint, clientId, timeout));
    }

    @Override
    public CompletableFuture<List<Variant>> getRange(byte[] key,
                                                     KeyHintData hint,
                                                     ContainerType containerType,
                                                     int start,
                                                     int end,
                                                     int clientId,
                                                     Duration timeout) {
        return execute(hint, c -> c.getRange(key, hint, containerType, start, end, clientId, timeout));
    }

    @Override
    public CompletableFuture<List<OrderedVariant>> getElementInRangeOrderedSet(byte[] key,
                                                                               KeyHintData hint,
                                                                               long startWeight,
                                                                               long endWeight,
                                                                               boolean reverse,
                                                                               int clientId,
                                                                               Duration timeout) {
        return execute(hint, c -> c.getElementInRangeOrderedSet(key, hint, startWeight, endWeight, reverse, clientId, timeout));
    }

    @Override
    public CompletableFuture<List<Variant>> getVector(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return execute(hint, c -> c.getVector(key, hint, clientId, timeout));
    }

    @Override
    public CompletableFuture<List<OrderedVariant>> getOrderedSet(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return execute(hint, c -> c.getOrderedSet(key, hint, clientId, timeout));
    }

    @Override
    public CompletableFuture<Variant> popAt(byte[] key,
                                            KeyHintData hint,
                                            int pos,
                                            int clientId,
                                            Duration timeout) {
        return executeWrite(hint, c -> c.popAt(key, hint, pos, clientId, timeout));
    }

    @Override
    public CompletableFuture<Integer> deleteTail(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return executeWrite(hint, c -> c.deleteTail(key, hint, clientId, timeout));
    }

    @Override
    public CompletableFuture<Integer> deleteHead(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return executeWrite(hint, c -> c.deleteHead(key, hint, clientId, timeout));
    }

    @Override
    public CompletableFuture<Integer> removeAt(byte[] key,
                                               KeyHintData hint,
                                               long pos,
                                               long endPos,
                                               int clientId,
                                               Duration timeout) {
        return executeWrite(hint, c -> c.removeAt(key, hint, pos, endPos, clientId, timeout));
    }

    @Override
    public CompletableFuture<Variant> peekHead(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return execute(hint, c -> c.peekHead(key, hint, clientId, timeout));
    }

    @Override
    public CompletableFuture<Variant> peekTail(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return execute(hint, c -> c.peekTail(key, hint, clientId, timeout));
    }

    @Override
    public void shutdown() {
        currentModeOverride.remove();
        readyFuture.completeExceptionally(new IllegalStateException("FastCacheAsyncSmartClient is shut down"));

        scheduledExecutorService.shutdown();
        delayExecutor.shutdown();
        try {
            if (!scheduledExecutorService.awaitTermination(3, TimeUnit.SECONDS)) {
                scheduledExecutorService.shutdownNow();
            }
            if (!delayExecutor.awaitTermination(3, TimeUnit.SECONDS)) {
                delayExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduledExecutorService.shutdownNow();
            delayExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        ManagedChannel coordinatorChannel = activeCoordinatorChannel.get();
        if (coordinatorChannel != null) {
            coordinatorChannel.shutdown();
        }

        RoutingInfo routingInfo = routing_info.get();
        Set<HurriCacheClientInterface> allClients = new HashSet<>(routingInfo.routingTableTarget.values());
        allClients.addAll(routingInfo.routingTable.values());
        allClients.addAll(rerouteClients.values());
        allClients.forEach(HurriCacheClientInterface::shutdown);
    }

    private HurriCacheClientInterface getRoute(RoutingInfo info, int shard, NodeRole role) {
        return info.routingTable.get(Pair.of(role, shard));
    }

    private HurriCacheClientInterface getRoute(RoutingInfo info, String target) {
        return info.routingTableTarget.get(target);
    }

    private HurriCacheClientInterface resolveRerouteClient(RoutingInfo routingInfo, String target) {
        HurriCacheClientInterface existing = routingInfo.routingTableTarget().get(target);
        return existing != null ? existing : rerouteClients.computeIfAbsent(target, this::newFastCacheClient);
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

    private RerouteTargets getRerouteTargets(Throwable ex) {
        Throwable c = (ex instanceof CompletionException) ? ex.getCause() : ex;
        if (c instanceof StatusRuntimeException sre && sre.getStatus().getCode() == Status.Code.FAILED_PRECONDITION) {
            String routeData = null;

            if (sre.getTrailers() != null) {
                routeData = sre.getTrailers().get(Metadata.Key.of("x-fastcache-route", Metadata.ASCII_STRING_MARSHALLER));
            }

            if (routeData == null || routeData.isBlank()) {
                routeData = sre.getStatus().getDescription();
            }

            if (routeData != null && !routeData.isBlank()) {
                String[] parts = routeData.split(",");
                String master = parts[0].trim();
                String backup = (parts.length > 1) ? parts[1].trim() : null;
                return new RerouteTargets(master, backup);
            }
        }
        return null;
    }

    // =========================================================================
    // ATOMIC OPERATIONS DISPATCH IMPLEMENTATION
    // =========================================================================

    @Override
    public CompletableFuture<Number> exchange(byte[] key,
                                              KeyHintData hint,
                                              long value,
                                              Duration ttl,
                                              int clientId,
                                              Duration timeout) {
        return executeWrite(hint, c -> c.exchange(key, hint, value, ttl, clientId, timeout));
    }

    @Override
    public CompletableFuture<Number> add(byte[] key,
                                         KeyHintData hint,
                                         long delta,
                                         Duration ttl,
                                         int clientId,
                                         Duration timeout) {
        return executeWrite(hint, c -> c.add(key, hint, delta, ttl, clientId, timeout));
    }

    @Override
    public CompletableFuture<Number> sub(byte[] key,
                                         KeyHintData hint,
                                         long delta,
                                         Duration ttl,
                                         int clientId,
                                         Duration timeout) {
        return executeWrite(hint, c -> c.sub(key, hint, delta, ttl, clientId, timeout));
    }

    @Override
    public CompletableFuture<Long> and(byte[] key,
                                       KeyHintData hint,
                                       long mask,
                                       Duration ttl,
                                       int clientId,
                                       Duration timeout) {
        return executeWrite(hint, c -> c.and(key, hint, mask, ttl, clientId, timeout));
    }

    @Override
    public CompletableFuture<Long> or(byte[] key,
                                      KeyHintData hint,
                                      long mask,
                                      Duration ttl,
                                      int clientId,
                                      Duration timeout) {
        return executeWrite(hint, c -> c.or(key, hint, mask, ttl, clientId, timeout));
    }

    @Override
    public CompletableFuture<Long> xor(byte[] key,
                                       KeyHintData hint,
                                       long mask,
                                       Duration ttl,
                                       int clientId,
                                       Duration timeout) {
        return executeWrite(hint, c -> c.xor(key, hint, mask, ttl, clientId, timeout));
    }

    @Override
    public CompletableFuture<AtomicCasRes> compareAndSet(byte[] key,
                                                         KeyHintData hint,
                                                         long expectedValue,
                                                         long newValue,
                                                         Duration ttl,
                                                         int clientId,
                                                         Duration timeout) {
        return executeWrite(hint, c -> c.compareAndSet(key, hint, expectedValue, newValue, ttl, clientId, timeout));
    }

    /**
     * Overrides the routing {@link Mode} for the <b>current thread</b> (sticky semantics:
     * the override applies to this and all subsequent calls from the same thread until cleared).
     * Passing {@code null} is equivalent to calling {@link #clearMode()}.
     * <p>
     * The override is stored in a {@link ThreadLocal}; when the thread belongs to a pool,
     * the value survives across unrelated tasks. Always call {@link #clearMode()} when done,
     * preferably in a {@code finally} block.
     */
    public FastCacheAsyncSmartClient setMode(Mode mode) {
        if (mode == null) {
            this.currentModeOverride.remove();
        } else {
            this.currentModeOverride.set(mode);
        }
        return this;
    }

    /**
     * Clears the per-thread routing mode override set by {@link #setMode(Mode)}.
     * Must be called after a series of mode-overridden calls to avoid leaking the mode
     * (and the ThreadLocal entry) to unrelated work executed on the same pooled thread.
     */
    public FastCacheAsyncSmartClient clearMode() {
        this.currentModeOverride.remove();
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
    public CompletableFuture<Integer> removeFromContainerByKeyAndValue(byte[] key,
                                                                       KeyHintData hint, List<Payload> keys,
                                                                       List<Variant> values,
                                                                       int clientId,
                                                                       Duration timeout) {
        return executeWrite(hint, c -> c.removeFromContainerByKeyAndValue(key, hint, keys, values, clientId, timeout));
    }

    public CompletableFuture<Integer> removeFromContainerByKey(byte[] key,
                                                               KeyHintData hint,
                                                               Variant containerKey,
                                                               int clientId,
                                                               Duration timeout) {

        return executeWrite(hint,c -> c.removeFromContainerByKey(key, hint, containerKey, clientId, timeout));
    }
    @Override
    public CompletableFuture<Integer> removeFromContainerByKey(byte[] key, KeyHintData hint,
                                                               Payload keys, int clientId, Duration timeout) {
        return executeWrite(hint,c -> c.removeFromContainerByKey(key, hint, keys, clientId, timeout));
    }

    @Override
    public CompletableFuture<Map<Payload, Variant>> getMap(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return execute(hint, c -> c.getMap(key, hint, clientId, timeout));
    }

    @Override
    public CompletableFuture<Map<OrderedPayload, Variant>> getOrderedMap(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return execute(hint, c -> c.getOrderedMap(key, hint, clientId, timeout));
    }
    @Override
    public CompletableFuture<List<Variant>> getSet(byte[] key, KeyHintData hint, int clientId,
                                                   Duration timeout){
        return execute(hint, c -> c.getSet(key, hint, clientId, timeout));
    }

    @Override
    public CompletableFuture<Variant> getInContainer(byte[] key, KeyHintData hint, byte[] elementKey, int clientId, Duration timeout) {
        return execute(hint, c -> c.getInContainer(key, hint, elementKey, clientId, timeout));
    }

    @Override
    public CompletableFuture<Variant> popInContainer(byte[] key, KeyHintData hint, byte[] elementKey, int clientId, Duration timeout) {
        return executeWrite(hint, c -> c.popInContainer(key, hint, elementKey, clientId, timeout));
    }

    @Override
    public CompletableFuture<Boolean> containsKey(byte[] key, KeyHintData hint, byte[] elementKey, int clientId, Duration timeout) {
        return execute(hint, c -> c.containsKey(key, hint, elementKey, clientId, timeout));
    }

    @Override
    public CompletableFuture<Variant> update(byte[] key, KeyHintData hint, byte[] elementKey, Variant value, int clientId, Duration timeout) {
        return executeWrite(hint, c -> c.update(key, hint, elementKey, value, clientId, timeout));
    }

    @Override
    public CompletableFuture<Integer> remove(byte[] key,
                                             KeyHintData hint,
                                             byte[] elementKey,
                                             int clientId,
                                             Duration timeout) {
        return executeWrite(hint, c -> c.remove(key, hint, elementKey, clientId, timeout));
    }

    private CompletableFuture<Void> ensureReady() {
        if (readyFlag.get()) {
            return CompletableFuture.completedFuture(null);
        }
        CompletableFuture<Void> wait = new CompletableFuture<>();
        readyFuture.whenComplete((v, ex) -> {
            if (ex != null) {
                wait.completeExceptionally(ex);
            } else {
                wait.complete(null);
            }
        });
        try {
            delayExecutor.schedule(() -> wait.completeExceptionally(new TimeoutException("FastCacheClient boot timeout reached.")),
                                   readyTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            wait.completeExceptionally(e);
        }
        return wait;
    }

    @Override
    public int getDefaultCompressionThreshold() {
        return defaultCompressionThreshold;
    }

    private CompletableFuture<Void> repDelay() {
        CompletableFuture<Void> delay = new CompletableFuture<>();
        try {
            delayExecutor.schedule(() -> delay.complete(null), 500, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            delay.completeExceptionally(e);
        }
        return delay;
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
                                                      List<Variant> initialValue,
                                                      Duration ttl,
                                                      int clientId,
                                                      Duration timeout) {
        return createUnorderedContainerSmart(key, keyHint, initialValue, ttl, clientId, timeout, ContainerType.QUEUE);
    }

    @Override
    public CompletableFuture<KeyHintData> createList(byte[] key,
                                                     KeyHintData keyHint,
                                                     List<Variant> initialValue,
                                                     Duration ttl,
                                                     int clientId,
                                                     Duration timeout) {
        return createUnorderedContainerSmart(key, keyHint, initialValue, ttl, clientId, timeout, ContainerType.LIST);
    }

    @Override
    public CompletableFuture<KeyHintData> createVector(byte[] key,
                                                       KeyHintData keyHint,
                                                       List<Variant> initialValue,
                                                       Duration ttl,
                                                       int clientId,
                                                       Duration timeout) {
        return createUnorderedContainerSmart(key, keyHint, initialValue, ttl, clientId, timeout, ContainerType.VECTOR);
    }

    @Override
    public CompletableFuture<KeyHintData> createSet(byte[] key,
                                                    KeyHintData keyHint,
                                                    List<Variant> initialValue,
                                                    Duration ttl,
                                                    int clientId,
                                                    Duration timeout) {
        return createUnorderedContainerSmart(key, keyHint, initialValue, ttl, clientId, timeout, ContainerType.SET);
    }

    @Override
    public CompletableFuture<KeyHintData> createOrderedSet(byte[] key,
                                                           KeyHintData keyHint,
                                                           List<OrderedVariant> initialValue,
                                                           Duration ttl,
                                                           int clientId,
                                                           Duration timeout) {
        List<OrderedVariant> firstChunk = new ArrayList<>();
        List<OrderedVariant> remaining = new ArrayList<>();
        splitOrderedPayloads(initialValue, firstChunk, remaining, clientId);

        return createContainerWithChunks(key, keyHint, ttl, clientId, timeout,
                                         c -> c.createOrderedSet(key, keyHint, firstChunk, ttl, clientId, timeout),
                                         remaining, null,
                                         (simpleClient, resHint, chunkKeys, chunkValues) ->
                                                 simpleClient.addElementWithWeight(key, resHint, chunkKeys, clientId, timeout));
    }

    @Override
    public CompletableFuture<KeyHintData> createMap(byte[] key,
                                                    KeyHintData keyHint,
                                                    Map<Payload, Variant> initialValue,
                                                    Duration ttl,
                                                    int clientId,
                                                    Duration timeout) {
        Map<Payload, Variant> firstChunkMap = new LinkedHashMap<>();
        List<Payload> remainingKeys = new ArrayList<>();
        List<Variant> remainingValues = new ArrayList<>();

        splitMapEntries(initialValue, firstChunkMap, remainingKeys, remainingValues, clientId, ttl);

        return createContainerWithChunks(key, keyHint, ttl, clientId, timeout,
                                         c -> c.createMap(key, keyHint, firstChunkMap, ttl, clientId, timeout),
                                         remainingKeys, remainingValues,
                                         (simpleClient, resHint, chunkKeys, chunkValues) ->
                                                 simpleClient.putAll(key, resHint, chunkKeys, chunkValues, clientId, timeout));
    }

    @Override
    public CompletableFuture<KeyHintData> createOrderedMap(byte[] key,
                                                           KeyHintData keyHint,
                                                           Map<OrderedPayload, Variant> initialValue,
                                                           Duration ttl,
                                                           int clientId,
                                                           Duration timeout) {
        Map<OrderedPayload, Variant> firstChunkMap = new LinkedHashMap<>();
        List<OrderedPayload> remainingKeys = new ArrayList<>();
        List<Variant> remainingValues = new ArrayList<>();

        splitOrderedMapEntries(initialValue, firstChunkMap, remainingKeys, remainingValues, clientId, ttl);

        return createContainerWithChunks(key, keyHint, ttl, clientId, timeout,
                                         c -> c.createOrderedMap(key, keyHint, firstChunkMap, ttl, clientId, timeout),
                                         remainingKeys, remainingValues,
                                         (simpleClient, resHint, chunkKeys, chunkValues) ->
                                                 simpleClient.addElementOrderedMap(key, resHint, chunkKeys, chunkValues, clientId, timeout));
    }

    private CompletableFuture<KeyHintData> createUnorderedContainerSmart(byte[] key,
                                                                         KeyHintData keyHint,
                                                                         List<Variant> initialValue,
                                                                         Duration ttl,
                                                                         int clientId,
                                                                         Duration timeout,
                                                                         ContainerType type) {
        List<Variant> firstChunk = new ArrayList<>();
        List<Variant> remaining = new ArrayList<>();
        splitVariants(initialValue, firstChunk, remaining, clientId);

        return createContainerWithChunks(key, keyHint, ttl, clientId, timeout,
                                         c -> switch (type) {
                                             case QUEUE -> c.createQueue(key, keyHint, firstChunk, ttl, clientId, timeout);
                                             case LIST -> c.createList(key, keyHint, firstChunk, ttl, clientId, timeout);
                                             case VECTOR -> c.createVector(key, keyHint, firstChunk, ttl, clientId, timeout);
                                             case SET -> c.createSet(key, keyHint, firstChunk, ttl, clientId, timeout);
                                             default -> throw new IllegalArgumentException("Unsupported type: " + type);
                                         },
                                         remaining, (List<Variant>) null,
                                         (simpleClient, resHint, chunkKeys, chunkValues) -> switch (type) {
                                             case QUEUE, LIST, VECTOR -> simpleClient.pushTail(key, resHint, chunkKeys, clientId, timeout);
                                             case SET -> simpleClient.addElementUnordered(key, resHint, chunkKeys, clientId, timeout);
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
                                                repDelay()
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

        if (valuePayload instanceof Variant vp) {
            size += KeyValueUtils.buildValueFromVariantCompressed(vp, getDefaultCompressionThreshold()).build().getSerializedSize();
        }
        return size;
    }

//    private void splitOrderedPayloads(List<OrderedPayload> source, List<OrderedPayload> firstChunk, List<OrderedPayload> remaining, int clientId) {
//        if (source == null) return;
//        long currentChunkSize = 128;
//        for (OrderedPayload op : source) {
//            long order = op.getOrder() != null ? op.getOrder() : 0L;
//            long elemSize = KeyValueUtils.createOrderedValue(op.getValue(), order, null, getDefaultCompressionThreshold()).build().getSerializedSize();
//            if (remaining.isEmpty() && (currentChunkSize + elemSize <= MAX_RPC_SIZE)) {
//                firstChunk.add(op);
//                currentChunkSize += elemSize;
//            } else {
//                remaining.add(op);
//            }
//        }
//    }

    private void splitOrderedPayloads(List<OrderedVariant> source, List<OrderedVariant> firstChunk, List<OrderedVariant> remaining, int clientId) {
        if (source == null) return;
        long currentChunkSize = 128;
        for (OrderedVariant op : source) {
            long order = op.getOrder() != null ? op.getOrder() : 0L;
            long elemSize = KeyValueUtils.buildOrderedValueFromVariant(op,  null, getDefaultCompressionThreshold()).build().getSerializedSize();
            if (remaining.isEmpty() && (currentChunkSize + elemSize <= MAX_RPC_SIZE)) {
                firstChunk.add(op);
                currentChunkSize += elemSize;
            } else {
                remaining.add(op);
            }
        }
    }

    private void splitMapEntries(Map<Payload, Variant> source, Map<Payload, Variant> firstChunk, List<Payload> remainingKeys, List<Variant> remainingValues, int clientId, Duration ttl) {
        if (source == null) return;
        long currentChunkSize = 128;
        for (Map.Entry<Payload, Variant> entry : source.entrySet()) {
            Key kVal = KeyValueUtils.createUnorderedKey(entry.getKey().getValue(), clientId, getDefaultCompressionThreshold()).build();
            Value vVal = KeyValueUtils.buildValueFromVariantCompressed(entry.getValue(), getDefaultCompressionThreshold()).build();
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

    private void splitOrderedMapEntries(Map<OrderedPayload, Variant> source, Map<OrderedPayload, Variant> firstChunk, List<OrderedPayload> remainingKeys, List<Variant> remainingValues, int clientId, Duration ttl) {
        if (source == null) return;
        long currentChunkSize = 128;
        for (Map.Entry<OrderedPayload, Variant> entry : source.entrySet()) {
            long order = entry.getKey().getOrder() != null ? entry.getKey().getOrder() : 0L;
            OrderedKey kVal = KeyValueUtils.createOrderedKey(entry.getKey().getValue(), order, clientId, getDefaultCompressionThreshold()).build();
            Value vVal = KeyValueUtils.buildValueFromVariantCompressed(entry.getValue(), getDefaultCompressionThreshold()).build();
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
    public CompletableFuture<Integer> addElementUnordered(byte[] key, KeyHintData hint, List<Variant> data, int clientId, Duration timeout) {
        if (data == null || data.isEmpty()) {
            return CompletableFuture.completedFuture(0);
        }
        List<List<Variant>> chunks = splitVariants(key, hint, data, clientId);
        CompletableFuture<Integer> future = CompletableFuture.completedFuture(0);

        for (List<Variant> chunk : chunks) {
            future = future.thenCompose(addedCount ->
                                                executeWrite(hint, c -> c.addElementUnordered(key, hint, chunk, clientId, timeout))
                                                        .thenApply(res -> addedCount + res)
            );
        }
        return future;
    }

    @Override
    public CompletableFuture<Integer> pushTail(byte[] key, KeyHintData hint, List<Variant> data, int clientId, Duration timeout) {
        if (data == null || data.isEmpty()) {
            return CompletableFuture.completedFuture(0);
        }
        List<List<Variant>> chunks = splitVariants(key, hint, data, clientId);
        CompletableFuture<Integer> future = CompletableFuture.completedFuture(0);

        for (List<Variant> chunk : chunks) {
            future = future.thenCompose(addedCount ->
                                                executeWrite(hint, c -> c.pushTail(key, hint, chunk, clientId, timeout))
                                                        .thenApply(res -> addedCount + res)
            );
        }
        return future;
    }

    @Override
    public CompletableFuture<Integer> pushHead(byte[] key, KeyHintData hint, List<Variant> data, int clientId, Duration timeout) {
        if (data == null || data.isEmpty()) {
            return CompletableFuture.completedFuture(0);
        }
        List<List<Variant>> chunks = splitVariants(key, hint, data, clientId);
        CompletableFuture<Integer> future = CompletableFuture.completedFuture(0);

        for (int i = chunks.size() - 1; i >= 0; i--) {
            List<Variant> chunk = chunks.get(i);
            future = future.thenCompose(addedCount ->
                                                executeWrite(hint, c -> c.pushHead(key, hint, chunk, clientId, timeout))
                                                        .thenApply(res -> addedCount + res)
            );
        }
        return future;
    }

    @Override
    public CompletableFuture<Integer> addElementWithWeight(byte[] key, KeyHintData hint, List<OrderedVariant> data, int clientId, Duration timeout) {
        if (data == null || data.isEmpty()) {
            return CompletableFuture.completedFuture(0);
        }
        List<List<OrderedVariant>> chunks = splitOrderedPayloadsVariant(key, hint, data, clientId);
        CompletableFuture<Integer> future = CompletableFuture.completedFuture(0);

        for (List<OrderedVariant> chunk : chunks) {
            future = future.thenCompose(addedCount ->
                                                executeWrite(hint, c -> c.addElementWithWeight(key, hint, chunk, clientId, timeout))
                                                        .thenApply(res -> addedCount + res)
            );
        }
        return future;
    }



    @Override
    public CompletableFuture<Integer> insertAt(byte[] key, KeyHintData hint, List<Variant> data, int pos, int clientId, Duration timeout) {
        if (data == null || data.isEmpty()) {
            return CompletableFuture.completedFuture(0);
        }
        List<List<Variant>> chunks = splitVariants(key, hint, data, clientId);
        CompletableFuture<Integer> future = CompletableFuture.completedFuture(0);

        int currentPos = pos;
        for (List<Variant> chunk : chunks) {
            final int targetPos = currentPos;
            final int chunkSize = chunk.size();
            future = future.thenCompose(addedCount ->
                                                executeWrite(hint, c -> c.insertAt(key, hint, chunk, targetPos, clientId, timeout))
                                                        .thenApply(res -> addedCount + res)
            );
            currentPos += chunkSize;
        }
        return future;
    }

    @Override
    public CompletableFuture<Integer> insertBefore(byte[] key, KeyHintData hint, List<Variant> data, Variant pivot, int clientId, Duration timeout) {
        if (data == null || data.isEmpty()) {
            return CompletableFuture.completedFuture(0);
        }

        List<List<Variant>> chunks = splitVariants(key, hint, data, clientId);

        CompletableFuture<Integer> future = executeWrite(hint, c -> c.insertBefore(key, hint, chunks.get(0), pivot, clientId, timeout));
        Variant currentPivot = chunks.get(0).get(chunks.get(0).size() - 1);

        for (int i = 1; i < chunks.size(); i++) {
            List<Variant> chunk = chunks.get(i);
            Variant nextPivot = chunk.get(chunk.size() - 1);
            Variant anchor = currentPivot;

            future = future.thenCompose(addedCount ->
                                                executeWrite(hint, c -> c.insertAfter(key, hint, chunk, anchor, clientId, timeout))
                                                        .thenApply(res -> addedCount + res)
            );
            currentPivot = nextPivot;
        }
        return future;
    }

    @Override
    public CompletableFuture<Integer> insertAfter(byte[] key, KeyHintData hint, List<Variant> data, Variant pivot, int clientId, Duration timeout) {
        if (data == null || data.isEmpty()) {
            return CompletableFuture.completedFuture(0);
        }
        List<List<Variant>> chunks = splitVariants(key, hint, data, clientId);
        CompletableFuture<Integer> future = CompletableFuture.completedFuture(0);

        Variant currentPivot = pivot;
        for (List<Variant> chunk : chunks) {
            Variant anchor = currentPivot;
            Variant nextPivot = chunk.get(chunk.size() - 1);

            future = future.thenCompose(addedCount ->
                                                executeWrite(hint, c -> c.insertAfter(key, hint, chunk, anchor, clientId, timeout))
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
    public CompletableFuture<Integer> putAll(byte[] key,
                                             KeyHintData hint,
                                             List<Payload> container_keys,
                                             List<Variant> container_values,
                                             int clientId,
                                             Duration timeout) {
        if (container_keys == null || container_keys.isEmpty()) {
            return CompletableFuture.completedFuture(0);
        }

        List<Pair<List<Payload>, List<Variant>>> chunks = splitMapPayloads(key, hint, container_keys, container_values, clientId);
        CompletableFuture<Integer> future = CompletableFuture.completedFuture(0);

        for (Pair<List<Payload>, List<Variant>> chunk : chunks) {
            future = future.thenCompose(addedCount ->
                                                executeWrite(hint, c -> c.putAll(key, hint, chunk.first, chunk.second, clientId, timeout))
                                                        .thenApply(res -> addedCount + res)
            );
        }
        return future;
    }

    @Override
    public CompletableFuture<Integer> addElementOrderedMap(byte[] key,
                                                           KeyHintData hint,
                                                           List<OrderedPayload> container_keys,
                                                           List<Variant> container_values,
                                                           int clientId,
                                                           Duration timeout) {
        if (container_keys == null || container_keys.isEmpty()) {
            return CompletableFuture.completedFuture(0);
        }

        List<Pair<List<OrderedPayload>, List<Variant>>> chunks = splitOrderedMapPayloads(key, hint, container_keys, container_values, clientId);
        CompletableFuture<Integer> future = CompletableFuture.completedFuture(0);

        for (Pair<List<OrderedPayload>, List<Variant>> chunk : chunks) {
            future = future.thenCompose(addedCount ->
                                                executeWrite(hint, c -> c.addElementOrderedMap(key, hint, chunk.first, chunk.second, clientId, timeout))
                                                        .thenApply(res -> addedCount + res)
            );
        }
        return future;
    }

    private List<Pair<List<Payload>, List<Variant>>> splitMapPayloads(byte[] key,
                                                                      KeyHintData hint,
                                                                      List<Payload> keys,
                                                                      List<Variant> values,
                                                                      int clientId) {
        List<Pair<List<Payload>, List<Variant>>> chunks = new ArrayList<>();
        Key protoKey = KeyValueUtils.createUnorderedKey(key, clientId, null).build();
        long currentChunkSize = protoKey.getSerializedSize() + 32;

        List<Payload> currentKeys = new ArrayList<>();
        List<Variant> currentValues = new ArrayList<>();

        for (int i = 0; i < keys.size(); i++) {
            Payload k = keys.get(i);
            Variant v = (values != null && i < values.size()) ? values.get(i) : null;

            Key kProto = KeyValueUtils.createUnorderedKey(k.getValue(), clientId, null).build();
            long pairSize = kProto.getSerializedSize() + 32;

            if (v != null) {
                pairSize += KeyValueUtils.buildValueFromVariantCompressed(v, getDefaultCompressionThreshold()).build().getSerializedSize();
            }

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

    private List<Pair<List<OrderedPayload>, List<Variant>>> splitOrderedMapPayloads(byte[] key,
                                                                                    KeyHintData hint,
                                                                                    List<OrderedPayload> keys,
                                                                                    List<Variant> values,
                                                                                    int clientId) {
        List<Pair<List<OrderedPayload>, List<Variant>>> chunks = new ArrayList<>();
        Key protoKey = KeyValueUtils.createUnorderedKey(key, clientId, null).build();
        long currentChunkSize = protoKey.getSerializedSize() + 32;

        List<OrderedPayload> currentKeys = new ArrayList<>();
        List<Variant> currentValues = new ArrayList<>();

        for (int i = 0; i < keys.size(); i++) {
            OrderedPayload k = keys.get(i);
            Variant v = (values != null && i < values.size()) ? values.get(i) : null;

            long order = k.getOrder() != null ? k.getOrder() : 0L;
            OrderedKey kProto = KeyValueUtils.createOrderedKey(k.getValue(), order, clientId,getDefaultCompressionThreshold()).build();
            long pairSize = kProto.getSerializedSize() + 32;

            if (v != null) {
                pairSize += KeyValueUtils.buildValueFromVariantCompressed(v, getDefaultCompressionThreshold()).build().getSerializedSize();
            }

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

    private void splitVariants(List<Variant> source, List<Variant> firstChunk, List<Variant> remaining, int clientId) {
        if (source == null) return;
        long currentChunkSize = 128;
        for (Variant v : source) {
            long elemSize = KeyValueUtils.buildValueFromVariantCompressed(v, getDefaultCompressionThreshold()).build().getSerializedSize();
            if (remaining.isEmpty() && (currentChunkSize + elemSize <= MAX_RPC_SIZE)) {
                firstChunk.add(v);
                currentChunkSize += elemSize;
            } else {
                remaining.add(v);
            }
        }
    }

    private List<List<Variant>> splitVariants(byte[] key, KeyHintData hint, List<Variant> data, int clientId) {
        List<List<Variant>> chunks = new ArrayList<>();
        Key protoKey = KeyValueUtils.createUnorderedKey(key, clientId, null).build();
        long currentChunkSize = protoKey.getSerializedSize() + 32;

        List<Variant> currentChunk = new ArrayList<>();

        for (Variant variant : data) {
            long elemSize = KeyValueUtils.buildValueFromVariantCompressed(variant, getDefaultCompressionThreshold()).build().getSerializedSize();

            if (!currentChunk.isEmpty() && (currentChunkSize + elemSize > MAX_RPC_SIZE)) {
                chunks.add(currentChunk);
                currentChunk = new ArrayList<>();
                currentChunkSize = protoKey.getSerializedSize() + 32;
            }

            currentChunk.add(variant);
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

    private List<List<OrderedVariant>> splitOrderedPayloadsVariant(byte[] key, KeyHintData hint, List<OrderedVariant> data, int clientId) {
        List<List<OrderedVariant>> chunks = new ArrayList<>();
        Key protoKey = KeyValueUtils.createUnorderedKey(key, clientId, null).build();
        long currentChunkSize = protoKey.getSerializedSize() + 32;

        List<OrderedVariant> currentChunk = new ArrayList<>();

        for (OrderedVariant payload : data) {
            OrderedValue orderedValue = KeyValueUtils.buildOrderedValueFromVariant(payload,  null, getDefaultCompressionThreshold()).build();
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


    private record RerouteTargets(String masterTarget, String backupTarget) {}

    // =========================================================================
    // DOUBLE OVERLOADS FOR ATOMIC OPERATIONS
    // =========================================================================

    @Override
    public CompletableFuture<Number> exchange(byte[] key, KeyHintData hint, double value, Duration ttl, int clientId, Duration timeout) {
        return executeWrite(hint, c -> c.exchange(key, hint, value, ttl, clientId, timeout));
    }

    @Override
    public CompletableFuture<Number> add(byte[] key, KeyHintData hint, double delta, Duration ttl, int clientId, Duration timeout) {
        return executeWrite(hint, c -> c.add(key, hint, delta, ttl, clientId, timeout));
    }

    @Override
    public CompletableFuture<Number> sub(byte[] key, KeyHintData hint, double delta, Duration ttl, int clientId, Duration timeout) {
        return executeWrite(hint, c -> c.sub(key, hint, delta, ttl, clientId, timeout));
    }

    @Override
    public CompletableFuture<AtomicCasRes> compareAndSet(byte[] key, KeyHintData hint, double expectedValue, double newValue, Duration ttl, int clientId, Duration timeout) {
        return executeWrite(hint, c -> c.compareAndSet(key, hint, expectedValue, newValue, ttl, clientId, timeout));
    }
}