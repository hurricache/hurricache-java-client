package com.hurricache.client;

import com.google.protobuf.UnsafeByteOperations;
import com.hurricache.client.intf.HurriCacheClientInterface;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.OrderedPayload;
import com.hurricache.client.intf.OrderedVariant;
import com.hurricache.client.intf.Payload;
import com.hurricache.client.intf.Variant;
import com.hurricache.grpc.AddToRequest;
import com.hurricache.grpc.AddToValRequest;
import com.hurricache.grpc.AtomicCas;
import com.hurricache.grpc.AtomicCasRes;
import com.hurricache.grpc.BinaryPayload;
import com.hurricache.grpc.BoolResponse;
import com.hurricache.grpc.ContainerGetRequest;
import com.hurricache.grpc.ContainerType;
import com.hurricache.grpc.CreateContainerRequest;
import com.hurricache.grpc.CreateRequest;
import com.hurricache.grpc.GetRequest;
import com.hurricache.grpc.HurriCacheGrpcServiceGrpc;
import com.hurricache.grpc.IntResponse;
import com.hurricache.grpc.Key;
import com.hurricache.grpc.KeyBinaryPayload;
import com.hurricache.grpc.KeyHint;
import com.hurricache.grpc.KeyPositionRequest;
import com.hurricache.grpc.LockRequest;
import com.hurricache.grpc.LockResponse;
import com.hurricache.grpc.LockStatus;
import com.hurricache.grpc.LockType;
import com.hurricache.grpc.OrderedKey;
import com.hurricache.grpc.OrderedValue;
import com.hurricache.grpc.RemoveFromContainerRequest;
import com.hurricache.grpc.TtlRequest;
import com.hurricache.grpc.UnLockRequest;
import com.hurricache.grpc.UnlockResponse;
import com.hurricache.grpc.UpdateContainerRequest;
import com.hurricache.grpc.UpdateRequest;
import com.hurricache.grpc.Value;
import com.hurricache.grpc.ValueResponse;
import com.hurricache.grpc.ValueType;
import com.hurricache.utils.CompletableFutureObserver;
import com.hurricache.utils.CompressionUtils;
import com.hurricache.utils.DecompressingVariantObserver;
import com.hurricache.utils.StreamBatchMapVariantObserver;
import com.hurricache.utils.StreamBatchOrderedMapVariantObserver;
import com.hurricache.utils.StreamBatchOrderedObserver;
import com.hurricache.utils.StreamBatchUnorderedObserver;
import com.hurricache.utils.StreamBatchVariantObserver;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static com.hurricache.client.KeyValueUtils.buildAtomicCreateRequest;
import static com.hurricache.client.KeyValueUtils.buildGetReq;
import static com.hurricache.client.KeyValueUtils.buildKey;
import static com.hurricache.client.KeyValueUtils.buildValueFromVariant;
import static com.hurricache.client.KeyValueUtils.toVariant;

public class FastCacheAsyncSimpleClient implements HurriCacheClientInterface {

    private final HurriCacheGrpcServiceGrpc.HurriCacheGrpcServiceStub asyncStub;
    private final ManagedChannel channel;
    private final int defaultClientId;
    private final Duration defaultTimeout;
    private final int defaultCompressionThreshold;
    private final String target;
    private final boolean ownedChannel;

    public FastCacheAsyncSimpleClient(String host,
                                      int port,
                                      int defaultClientId,
                                      Duration timeout,
                                      int defaultCompressionThreshold) {
        this.channel = NettyChannelBuilder.forAddress(host, port)
                .flowControlWindow(4 * 1024 * 1024)
                .maxInboundMessageSize(64 * 1024 * 1024)
                .usePlaintext()
                .build();
        this.asyncStub = HurriCacheGrpcServiceGrpc.newStub(channel);
        this.defaultClientId = defaultClientId;
        this.defaultTimeout = timeout;
        this.defaultCompressionThreshold = defaultCompressionThreshold;
        this.target = host + ":" + port;
        this.ownedChannel = true;
    }

    public FastCacheAsyncSimpleClient(String host, int port, int defaultClientId, Duration timeout) {
        this(host, port, defaultClientId, timeout, DEFAULT_COMPRESSION_THRESHOLD);
    }

    public FastCacheAsyncSimpleClient(String host, int port, int clientId) {
        this(host, port, clientId, Duration.ofSeconds(1));
    }

    public FastCacheAsyncSimpleClient(String host, int port) {
        this(host, port, 0, Duration.ofSeconds(1));
    }

    public FastCacheAsyncSimpleClient(String host, int port, Duration duration) {
        this(host, port, 0, duration);
    }

    public FastCacheAsyncSimpleClient(ManagedChannel channel) {
        this(channel, 0);
    }

    public FastCacheAsyncSimpleClient(ManagedChannel channel, int clientId) {
        this(channel, clientId, Duration.ofSeconds(1));
    }

    public FastCacheAsyncSimpleClient(ManagedChannel channel, int defaultClientId, Duration duration) {
        this(channel, defaultClientId, duration, DEFAULT_COMPRESSION_THRESHOLD);
    }

    public FastCacheAsyncSimpleClient(ManagedChannel channel,
                                      int defaultClientId,
                                      Duration duration,
                                      int defaultCompressionThreshold) {
        this(channel, defaultClientId, duration, defaultCompressionThreshold, false);
    }

    public FastCacheAsyncSimpleClient(ManagedChannel channel,
                                      int defaultClientId,
                                      Duration duration,
                                      int defaultCompressionThreshold,
                                      boolean ownedChannel) {
        this.channel = channel;
        this.asyncStub = HurriCacheGrpcServiceGrpc.newStub(channel);
        this.defaultClientId = defaultClientId;
        this.defaultTimeout = duration;
        this.defaultCompressionThreshold = defaultCompressionThreshold;
        this.target = channel.authority();
        this.ownedChannel = ownedChannel;
    }

    @Override
    public String toString() {
        return "FastCacheAsyncSimpleClient{" + "target='" + target + '\'' + '}';
    }

    @Override
    public String getTarget() {
        return target;
    }

    @Override
    public int getDefaultClientId() {
        return defaultClientId;
    }

    @Override
    public Duration getDefaultTimeout() {
        return defaultTimeout;
    }

    @Override
    public int getDefaultCompressionThreshold() {
        return defaultCompressionThreshold;
    }

    // =========================================================================
    // TTL MANAGEMENT
    // =========================================================================

    @Override
    public CompletableFuture<Boolean> setTtl(byte[] key, KeyHintData hint, long ttl, int clientId, Duration timeout) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        TtlRequest request = TtlRequest.newBuilder()
                .setKey(KeyValueUtils.createUnorderedKey(key, hint, clientId, getDefaultCompressionThreshold()))
                .setTtl(System.currentTimeMillis() + ttl)
                .build();
        getStub(timeout).setTtl(request, new CompletableFutureObserver<>(future, BoolResponse::getValue));
        return future;
    }

    @Override
    public CompletableFuture<Long> getTtl(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        GetRequest ttlRequest = GetRequest.newBuilder()
                .setKey(KeyValueUtils.createUnorderedKey(key, hint, clientId, getDefaultCompressionThreshold()))
                .build();
        getStub(timeout).getTtl(ttlRequest,
                                new CompletableFutureObserver<>(future,
                                                                item -> item.hasTtl()
                                                                        ? item.getTtl() - System.currentTimeMillis()
                                                                        : -1L));
        return future;
    }

    // =========================================================================
    // KEY-VALUE OPERATIONS
    // =========================================================================

    @Override
    public CompletableFuture<Variant> pop(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        CompletableFuture<Variant> future = new CompletableFuture<>();
        GetRequest request = GetRequest.newBuilder()
                .setKey(KeyValueUtils.createUnorderedKey(key, hint, clientId, getDefaultCompressionThreshold()))
                .build();
        getStub(timeout).getAndDeleteValue(request, new CompletableFutureObserver<>(future, KeyValueUtils::toVariant));
        return future;
    }

    @Override
    public CompletableFuture<KeyHintData> create(byte[] key,
                                                 KeyHintData hint,
                                                 Variant value,
                                                 Duration ttl,
                                                 int clientId,
                                                 Duration timeout) {
        CompletableFuture<KeyHintData> future = new CompletableFuture<>();
        CreateRequest req = CreateRequest.newBuilder()
                .setKey(KeyValueUtils.createUnorderedKey(key, hint, clientId, getDefaultCompressionThreshold()))
                .setValue(buildValueFromVariant(value, ttl))
                .build();
        getStub(timeout).createKeyValue(req, new CompletableFutureObserver<>(future, keyHintResponse -> {
            KeyHint keyHint = keyHintResponse.getKeyHint();
            return KeyHintData.of(keyHint.getStrongHash(), keyHint.getWeakHash(), keyHintResponse.getRelayed());
        }));
        return future;
    }

    @Override
    public CompletableFuture<Variant> get(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        CompletableFuture<Variant> future = new CompletableFuture<>();
        getStub(timeout).getValue(buildGetReq(key, hint, clientId, getDefaultCompressionThreshold()),
                                  new CompletableFutureObserver<>(future, KeyValueUtils::toVariant));
        return future;
    }

    @Override
    public CompletableFuture<Variant> replace(byte[] key,
                                              KeyHintData hint,
                                              Variant value,
                                              Duration ttl,
                                              int clientId,
                                              Duration timeout) {
        CompletableFuture<Variant> future = new CompletableFuture<>();
        UpdateRequest req = UpdateRequest.newBuilder()
                .setKey(buildKey(key, hint, clientId, getDefaultCompressionThreshold()))
                .setValue(buildValueFromVariant(value, ttl))
                .build();
        getStub(timeout).updateValue(req, new CompletableFutureObserver<>(future, resp -> {
            if (resp.hasValue()) {
                return toVariant(resp);
            }
            return Variant.none();
        }));
        return future;
    }

    @Override
    public CompletableFuture<Boolean> containsKey(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        getStub(timeout).existKey(buildGetReq(key, hint, clientId, getDefaultCompressionThreshold()),
                                  new CompletableFutureObserver<>(future, BoolResponse::getValue));
        return future;
    }

    @Override
    public CompletableFuture<Boolean> remove(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        getStub(timeout).remove(buildGetReq(key, hint, clientId, getDefaultCompressionThreshold()),
                                new CompletableFutureObserver<>(future, BoolResponse::getValue));
        return future;
    }

    // =========================================================================
    // CONTAINER CREATION (UNORDERED & ORDERED)
    // =========================================================================

    @Override
    public CompletableFuture<KeyHintData> createQueue(byte[] key,
                                                      KeyHintData keyHint,
                                                      List<Variant> initialValue,
                                                      Duration ttl,
                                                      int clientId,
                                                      Duration timeout) {
        return createUnorderedContainer(key, keyHint, initialValue, ContainerType.QUEUE, ttl, clientId, timeout);
    }

    @Override
    public CompletableFuture<KeyHintData> createList(byte[] key,
                                                     KeyHintData keyHint,
                                                     List<Variant> initialValue,
                                                     Duration ttl,
                                                     int clientId,
                                                     Duration timeout) {
        return createUnorderedContainer(key, keyHint, initialValue, ContainerType.LIST, ttl, clientId, timeout);
    }

    @Override
    public CompletableFuture<KeyHintData> createVector(byte[] key,
                                                       KeyHintData keyHint,
                                                       List<Variant> initialValue,
                                                       Duration ttl,
                                                       int clientId,
                                                       Duration timeout) {
        return createUnorderedContainer(key, keyHint, initialValue, ContainerType.VECTOR, ttl, clientId, timeout);
    }

    @Override
    public CompletableFuture<KeyHintData> createSet(byte[] key,
                                                    KeyHintData keyHint,
                                                    List<Variant> initialValue,
                                                    Duration ttl,
                                                    int clientId,
                                                    Duration timeout) {
        return createUnorderedContainer(key, keyHint, initialValue, ContainerType.SET, ttl, clientId, timeout);
    }

    private CompletableFuture<KeyHintData> createUnorderedContainer(byte[] key,
                                                                    KeyHintData keyHint,
                                                                    List<Variant> initialValue,
                                                                    ContainerType type,
                                                                    Duration ttl,
                                                                    int clientId,
                                                                    Duration timeout) {
        CreateContainerRequest.Builder builder = CreateContainerRequest.newBuilder();
        Key.Builder protoKey = KeyValueUtils.createUnorderedKey(key,
                                                                keyHint,
                                                                clientId,
                                                                getDefaultCompressionThreshold());
        builder.setKey(protoKey).setType(type);

        if (ttl != null && !ttl.isZero()) {
            builder.setTtl(System.currentTimeMillis() + ttl.toMillis());
        }

        if (initialValue != null) {
            for (Variant payload : initialValue) {
                Value.Builder compressedValue = KeyValueUtils.buildValueFromVariantCompressed(payload,
                                                                                              getDefaultCompressionThreshold());
                builder.addValueUnordered(compressedValue);
            }
        }

        CompletableFuture<KeyHintData> createFuture = new CompletableFuture<>();
        getStub(timeout).createContainer(builder.build(),
                                         new CompletableFutureObserver<>(createFuture, keyHintResponse -> {
                                             KeyHint keyHint1 = keyHintResponse.getKeyHint();
                                             return KeyHintData.of(keyHint1.getStrongHash(),
                                                                   keyHint1.getWeakHash(),
                                                                   keyHintResponse.getRelayed());
                                         }));

        return createFuture;
    }

    @Override
    public CompletableFuture<KeyHintData> createOrderedSet(byte[] key,
                                                           KeyHintData keyHint,
                                                           List<OrderedVariant> initialValue,
                                                           Duration ttl,
                                                           int clientId,
                                                           Duration timeout) {
        CreateContainerRequest.Builder builder = CreateContainerRequest.newBuilder();
        Key protoKey = KeyValueUtils.createUnorderedKey(key, keyHint, clientId, getDefaultCompressionThreshold())
                .build();
        builder.setKey(protoKey).setType(ContainerType.ORDERED_SET);

        if (ttl != null && !ttl.isZero()) {
            builder.setTtl(System.currentTimeMillis() + ttl.toMillis());
        }

        if (initialValue != null) {
            for (OrderedVariant payload : initialValue) {
                OrderedValue orderedValue = KeyValueUtils.buildOrderedValueFromVariant(payload,
                                                                             ttl,
                                                                             getDefaultCompressionThreshold()).build();
                builder.addValueOrdered(orderedValue);
            }
        }

        CompletableFuture<KeyHintData> createFuture = new CompletableFuture<>();
        getStub(timeout).createContainer(builder.build(),
                                         new CompletableFutureObserver<>(createFuture, keyHintResponse -> {
                                             KeyHint keyHint1 = keyHintResponse.getKeyHint();
                                             return KeyHintData.of(keyHint1.getStrongHash(),
                                                                   keyHint1.getWeakHash(),
                                                                   keyHintResponse.getRelayed());
                                         }));
        return createFuture;
    }

    @Override
    public CompletableFuture<KeyHintData> createMap(byte[] key,
                                                    KeyHintData keyHint,
                                                    Map<Payload, Variant> initialValue,
                                                    Duration ttl,
                                                    int clientId,
                                                    Duration timeout) {
        CreateContainerRequest.Builder builder = CreateContainerRequest.newBuilder();
        if (ttl != null && !ttl.isZero()) {
            builder.setTtl(System.currentTimeMillis() + ttl.toMillis());
        }

        Key protoKey = KeyValueUtils.createUnorderedKey(key, keyHint, clientId, getDefaultCompressionThreshold())
                .build();
        builder.setKey(protoKey).setType(ContainerType.MAP);

        if (initialValue != null) {
            for (Map.Entry<Payload, Variant> entry : initialValue.entrySet()) {
                Key kVal = KeyValueUtils.createUnorderedKey(entry.getKey().getValue(),
                                                            clientId,
                                                            getDefaultCompressionThreshold()).build();
                Value.Builder vVal = KeyValueUtils.buildValueFromVariantCompressed(entry.getValue(),
                                                                                   getDefaultCompressionThreshold());
                if (ttl != null && !ttl.isZero()) {
                    vVal.setTtl(System.currentTimeMillis() + ttl.toMillis());
                }
                builder.addKeyUnordered(kVal);
                builder.addValueUnordered(vVal);
            }
        }
        CompletableFuture<KeyHintData> createFuture = new CompletableFuture<>();
        getStub(timeout).createContainer(builder.build(),
                                         new CompletableFutureObserver<>(createFuture, keyHintResponse -> {
                                             KeyHint keyHint1 = keyHintResponse.getKeyHint();
                                             return KeyHintData.of(keyHint1.getStrongHash(),
                                                                   keyHint1.getWeakHash(),
                                                                   keyHintResponse.getRelayed());
                                         }));

        return createFuture;
    }

    public CompletableFuture<KeyHintData> createOrderedMap(byte[] key,
                                                           KeyHintData keyHint,
                                                           Map<OrderedPayload, Variant> initialValue,
                                                           Duration ttl,
                                                           int clientId,
                                                           Duration timeout) {
        CreateContainerRequest.Builder builder = CreateContainerRequest.newBuilder();
        if (ttl != null && !ttl.isZero()) {
            builder.setTtl(System.currentTimeMillis() + ttl.toMillis());
        }
        Key protoKey = KeyValueUtils.createUnorderedKey(key, keyHint, clientId, getDefaultCompressionThreshold())
                .build();
        builder.setKey(protoKey).setType(ContainerType.ORDERED_MAP);

        if (initialValue != null) {
            for (Map.Entry<OrderedPayload, Variant> entry : initialValue.entrySet()) {
                long order = entry.getKey().getOrder() != null
                             ? entry.getKey().getOrder()
                             : 0L;
                OrderedKey kVal = KeyValueUtils.createOrderedKey(entry.getKey().getValue(),
                                                                 order,
                                                                 clientId,
                                                                 getDefaultCompressionThreshold()).build();
                Value.Builder vVal = KeyValueUtils.buildValueFromVariantCompressed(entry.getValue(),
                                                                                   getDefaultCompressionThreshold());
                if (ttl != null && !ttl.isZero()) {
                    vVal.setTtl(System.currentTimeMillis() + ttl.toMillis());
                }
                builder.addKeyOrdered(kVal);
                builder.addValueUnordered(vVal);
            }
        }

        CompletableFuture<KeyHintData> createFuture = new CompletableFuture<>();

        getStub(timeout).createContainer(builder.build(),
                                         new CompletableFutureObserver<>(createFuture, keyHintResponse -> {
                                             KeyHint keyHint1 = keyHintResponse.getKeyHint();
                                             return KeyHintData.of(keyHint1.getStrongHash(),
                                                                   keyHint1.getWeakHash(),
                                                                   keyHintResponse.getRelayed());
                                         }));
        return createFuture;
    }

    @Override
    public CompletableFuture<Variant> getElementWithWeight(byte[] key,
                                                           KeyHintData hint,
                                                           int pos,
                                                           int clientId,
                                                           Duration timeout) {
        CompletableFuture<Variant> future = new CompletableFuture<>();
        KeyPositionRequest request = KeyPositionRequest.newBuilder()
                .setKey(KeyValueUtils.createUnorderedKey(key, hint, clientId, getDefaultCompressionThreshold()))
                .setPos(pos)
                .build();
        getStub(timeout).getElementAtPosition(request, new DecompressingVariantObserver(future));
        return future;
    }

    @Override
    public CompletableFuture<Variant> getAndRemoveElementWithWeight(byte[] key,
                                                                    KeyHintData hint,
                                                                    int pos,
                                                                    int clientId,
                                                                    Duration timeout) {
        CompletableFuture<Variant> future = new CompletableFuture<>();
        KeyPositionRequest request = KeyPositionRequest.newBuilder()
                .setKey(KeyValueUtils.createUnorderedKey(key, hint, clientId, getDefaultCompressionThreshold()))
                .setPos(pos)
                .build();
        getStub(timeout).getAndDeleteElementAtPosition(request, new DecompressingVariantObserver(future));
        return future;
    }

    @Override
    public CompletableFuture<Integer> getSize(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        CompletableFuture<Integer> future = new CompletableFuture<>();
        getStub(timeout).getSize(buildGetReq(key, hint, clientId, getDefaultCompressionThreshold()),
                                 new CompletableFutureObserver<>(future, IntResponse::getSize));
        return future;
    }

    @Override
    public CompletableFuture<Variant> popHead(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        CompletableFuture<Variant> future = new CompletableFuture<>();
        getStub(timeout).getAndDeleteHead(buildGetReq(key, hint, clientId, getDefaultCompressionThreshold()),
                                          new DecompressingVariantObserver(future));
        return future;
    }

    @Override
    public CompletableFuture<Variant> peekHead(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        CompletableFuture<Variant> future = new CompletableFuture<>();
        getStub(timeout).getHead(buildGetReq(key, hint, clientId, getDefaultCompressionThreshold()),
                                 new DecompressingVariantObserver(future));
        return future;
    }

    @Override
    public CompletableFuture<Variant> peekTail(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        CompletableFuture<Variant> future = new CompletableFuture<>();
        getStub(timeout).getTail(buildGetReq(key, hint, clientId, getDefaultCompressionThreshold()),
                                 new DecompressingVariantObserver(future));
        return future;
    }

    @Override
    public CompletableFuture<Variant> getAt(byte[] key,
                                            KeyHintData hint,
                                            int pos,
                                            int clientId,
                                            Duration timeout) {
        CompletableFuture<Variant> future = new CompletableFuture<>();
        KeyPositionRequest req = KeyPositionRequest.newBuilder()
                .setKey(buildKey(key, hint, clientId, getDefaultCompressionThreshold()))
                .setPos(pos)
                .build();
        getStub(timeout).getElementAtPosition(req, new DecompressingVariantObserver(future));
        return future;
    }

    // =========================================================================
    // STREAMING READ OPERATIONS
    // =========================================================================

    @Override
    public CompletableFuture<List<Variant>> getList(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        CompletableFuture<List<Variant>> future = new CompletableFuture<>();
        getStub(timeout).getContainer(buildGetReq(key, hint, clientId, getDefaultCompressionThreshold()),
                                      new StreamBatchVariantObserver(future));
        return future;
    }

    @Override
    public CompletableFuture<List<Variant>> getVector(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        CompletableFuture<List<Variant>> future = new CompletableFuture<>();
        getStub(timeout).getContainer(buildGetReq(key, hint, clientId, getDefaultCompressionThreshold()),
                                      new StreamBatchVariantObserver(future));
        return future;
    }

    @Override
    public CompletableFuture<List<Variant>> getSet(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        CompletableFuture<List<Variant>> future = new CompletableFuture<>();
        getStub(timeout).getContainer(buildGetReq(key, hint, clientId, getDefaultCompressionThreshold()),
                                      new StreamBatchVariantObserver(future));
        return future;
    }

    @Override
    public CompletableFuture<Map<Payload, Variant>> getMap(byte[] key,
                                                           KeyHintData hint,
                                                           int clientId,
                                                           Duration timeout) {
        CompletableFuture<Map<Payload, Variant>> future = new CompletableFuture<>();
        getStub(timeout).getContainer(buildGetReq(key, hint, clientId, getDefaultCompressionThreshold()),
                                      new StreamBatchMapVariantObserver(future));
        return future;
    }

    @Override
    public CompletableFuture<List<OrderedVariant>> getOrderedSet(byte[] key,
                                                                 KeyHintData hint,
                                                                 int clientId,
                                                                 Duration timeout) {
        CompletableFuture<List<OrderedVariant>> future = new CompletableFuture<>();
        getStub(timeout).getContainer(buildGetReq(key, hint, clientId, getDefaultCompressionThreshold()),
                                      new StreamBatchOrderedObserver(future));
        return future;
    }

    @Override
    public CompletableFuture<Map<OrderedPayload, Variant>> getOrderedMap(byte[] key,
                                                                         KeyHintData hint,
                                                                         int clientId,
                                                                         Duration timeout) {
        CompletableFuture<Map<OrderedPayload, Variant>> future = new CompletableFuture<>();
        getStub(timeout).getContainer(buildGetReq(key, hint, clientId, getDefaultCompressionThreshold()),
                                      new StreamBatchOrderedMapVariantObserver(future));
        return future;
    }

    @Override
    public CompletableFuture<List<Variant>> getRange(byte[] key,
                                                     KeyHintData hint,
                                                     ContainerType containerType,
                                                     int start,
                                                     int end,
                                                     int clientId,
                                                     Duration timeout) {
        KeyPositionRequest.Builder request = KeyPositionRequest.newBuilder()
                .setKey(KeyValueUtils.createUnorderedKey(key, hint, clientId, getDefaultCompressionThreshold()))
                .setPos(start)
                .setEnd(end);
        CompletableFuture<List<Variant>> rawFuture = new CompletableFuture<>();

        switch (containerType) {
            case LIST, VECTOR, SET -> getStub(timeout).getElementsInRange(request.setType(containerType).build(),
                                                                          new StreamBatchUnorderedObserver(rawFuture));
            default -> throw new IllegalArgumentException("Unsupported container type for stream operation: "
                                                          + containerType);
        }

        return rawFuture;
    }

    @Override
    public CompletableFuture<List<OrderedVariant>> getElementInRangeOrderedSet(byte[] key,
                                                                               KeyHintData hint,
                                                                               long startWeight,
                                                                               long endWeight,
                                                                               boolean reverse,
                                                                               int clientId,
                                                                               Duration timeout) {
        KeyPositionRequest request = KeyPositionRequest.newBuilder()
                .setKey(KeyValueUtils.createUnorderedKey(key, hint, clientId, getDefaultCompressionThreshold()))
                .setType(ContainerType.ORDERED_SET)
                .setPos(startWeight)
                .setEnd(endWeight)
                .setReverse(reverse)
                .build();

        CompletableFuture<List<OrderedVariant>> rawFuture = new CompletableFuture<>();
        getStub(timeout).getElementsInRange(request, new StreamBatchOrderedObserver(rawFuture));
        return rawFuture;
    }

    @Override
    public CompletableFuture<Map<OrderedPayload, Variant>> getElementInRangeOrderedMap(byte[] key,
                                                                                       KeyHintData hint,
                                                                                       long startWeight,
                                                                                       long endWeight,
                                                                                       boolean reverse,
                                                                                       int clientId,
                                                                                       Duration timeout) {
        KeyPositionRequest request = KeyPositionRequest.newBuilder()
                .setKey(KeyValueUtils.createUnorderedKey(key, hint, clientId, getDefaultCompressionThreshold()))
                .setType(ContainerType.ORDERED_MAP)
                .setPos(startWeight)
                .setEnd(endWeight)
                .setReverse(reverse)
                .build();

        CompletableFuture<Map<OrderedPayload, Variant>> rawFuture = new CompletableFuture<>();
        getStub(timeout).getElementsInRange(request, new StreamBatchOrderedMapVariantObserver(rawFuture));
        return rawFuture;
    }

    // =========================================================================
    // INSERTION OPERATIONS
    // =========================================================================

    @Override
    public CompletableFuture<Integer> addElementUnordered(byte[] key,
                                                          KeyHintData hint,
                                                          List<Variant> data,
                                                          int clientId,
                                                          Duration timeout) {
        if (data == null || data.isEmpty()) {
            return CompletableFuture.completedFuture(0);
        }
        Key protoKey = buildKey(key, hint, clientId, getDefaultCompressionThreshold());
        AddToRequest.Builder builder = AddToRequest.newBuilder().setKey(protoKey).setPos(-1);

        for (Variant datum : data) {
            builder.addValueUnordered(KeyValueUtils.buildValueFromVariantCompressed(datum,
                                                                                    getDefaultCompressionThreshold()));
        }

        CompletableFuture<Integer> future = new CompletableFuture<>();
        getStub(timeout).addElement(builder.build(), new CompletableFutureObserver<>(future, IntResponse::getSize));
        return future;
    }

    @Override
    public CompletableFuture<Integer> addElementWithWeight(byte[] key,
                                                           KeyHintData hint,
                                                           List<OrderedVariant> data,
                                                           int clientId,
                                                           Duration timeout) {
        if (data == null || data.isEmpty()) {
            return CompletableFuture.completedFuture(0);
        }
        AddToRequest.Builder builder = AddToRequest.newBuilder()
                .setKey(buildKey(key, hint, clientId, getDefaultCompressionThreshold()));
        for (OrderedVariant datum : data) {
            OrderedValue.Builder compressedValue = KeyValueUtils.buildOrderedValueFromVariant(datum,
                                                                                    Duration.ZERO,
                                                                                    getDefaultCompressionThreshold());
            builder.addValueOrdered(compressedValue);
        }

        CompletableFuture<Integer> chunkFuture = new CompletableFuture<>();

        getStub(timeout).addElement(builder.build(),
                                    new CompletableFutureObserver<>(chunkFuture, IntResponse::getSize));
        return chunkFuture;
    }

    @Override
    public CompletableFuture<Integer> pushTail(byte[] key,
                                               KeyHintData hint,
                                               List<Variant> data,
                                               int clientId,
                                               Duration timeout) {
        if (data == null || data.isEmpty()) {
            return CompletableFuture.completedFuture(0);
        }
        Key protoKey = buildKey(key, hint, clientId, getDefaultCompressionThreshold());
        AddToRequest.Builder builder = AddToRequest.newBuilder().setKey(protoKey);

        for (Variant datum : data) {
            builder.addValueUnordered(KeyValueUtils.buildValueFromVariantCompressed(datum,
                                                                                    getDefaultCompressionThreshold()));
        }

        CompletableFuture<Integer> future = new CompletableFuture<>();
        getStub(timeout).addElementToTail(builder.build(),
                                          new CompletableFutureObserver<>(future, IntResponse::getSize));
        return future;
    }

    @Override
    public CompletableFuture<Integer> pushHead(byte[] key,
                                               KeyHintData hint,
                                               List<Variant> data,
                                               int clientId,
                                               Duration timeout) {
        if (data == null || data.isEmpty()) {
            return CompletableFuture.completedFuture(0);
        }
        Key protoKey = buildKey(key, hint, clientId, getDefaultCompressionThreshold());
        AddToRequest.Builder builder = AddToRequest.newBuilder().setKey(protoKey);

        for (Variant datum : data) {
            builder.addValueUnordered(KeyValueUtils.buildValueFromVariantCompressed(datum,
                                                                                    getDefaultCompressionThreshold()));
        }

        CompletableFuture<Integer> future = new CompletableFuture<>();
        getStub(timeout).addElementToHead(builder.build(),
                                          new CompletableFutureObserver<>(future, IntResponse::getSize));
        return future;
    }

    @Override
    public CompletableFuture<Integer> insertAt(byte[] key,
                                               KeyHintData hint,
                                               List<Variant> data,
                                               int pos,
                                               int clientId,
                                               Duration timeout) {
        if (data == null || data.isEmpty()) {
            return CompletableFuture.completedFuture(0);
        }
        Key protoKey = buildKey(key, hint, clientId, getDefaultCompressionThreshold());
        AddToRequest.Builder builder = AddToRequest.newBuilder().setKey(protoKey).setPos(pos);

        for (Variant datum : data) {
            builder.addValueUnordered(KeyValueUtils.buildValueFromVariantCompressed(datum,
                                                                                    getDefaultCompressionThreshold()));
        }

        CompletableFuture<Integer> future = new CompletableFuture<>();
        getStub(timeout).addElement(builder.build(), new CompletableFutureObserver<>(future, IntResponse::getSize));
        return future;
    }

    @Override
    public CompletableFuture<Integer> insertBefore(byte[] key,
                                                   KeyHintData hint,
                                                   List<Variant> data,
                                                   Variant pivot,
                                                   int clientId,
                                                   Duration timeout) {
        if (data == null || data.isEmpty()) {
            return CompletableFuture.completedFuture(0);
        }
        CompletableFuture<Integer> future = new CompletableFuture<>();
        AddToValRequest.Builder builder = AddToValRequest.newBuilder();
        builder.setKey(buildKey(key, hint, clientId, getDefaultCompressionThreshold()));
        builder.setIsBefore(true);
        data.forEach(element -> builder.addValue(KeyValueUtils.buildValueFromVariantCompressed(element,
                                                                                               getDefaultCompressionThreshold())
                                                         .build()));
        builder.setPos(KeyValueUtils.buildValueFromVariantCompressed(pivot, getDefaultCompressionThreshold()).build());
        getStub(timeout).addElementToPositionByValue(builder.build(),
                                                     new CompletableFutureObserver<>(future, IntResponse::getSize));
        return future;
    }

    @Override
    public CompletableFuture<Integer> insertAfter(byte[] key,
                                                  KeyHintData hint,
                                                  List<Variant> data,
                                                  Variant pivot,
                                                  int clientId,
                                                  Duration timeout) {
        if (data == null || data.isEmpty()) {
            return CompletableFuture.completedFuture(0);
        }
        CompletableFuture<Integer> future = new CompletableFuture<>();
        AddToValRequest.Builder builder = AddToValRequest.newBuilder();
        builder.setKey(buildKey(key, hint, clientId, getDefaultCompressionThreshold()));
        builder.setIsBefore(false);
        data.forEach(element -> builder.addValue(KeyValueUtils.buildValueFromVariantCompressed(element,
                                                                                               getDefaultCompressionThreshold())
                                                         .build()));
        builder.setPos(KeyValueUtils.buildValueFromVariantCompressed(pivot, getDefaultCompressionThreshold()).build());
        getStub(timeout).addElementToPositionByValue(builder.build(),
                                                     new CompletableFutureObserver<>(future, IntResponse::getSize));
        return future;
    }

    // =========================================================================
    // POP & DELETION OPERATIONS
    // =========================================================================

    @Override
    public CompletableFuture<Variant> popTail(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        CompletableFuture<Variant> future = new CompletableFuture<>();
        getStub(timeout).getAndDeleteTail(buildGetReq(key, hint, clientId, getDefaultCompressionThreshold()),
                                          new DecompressingVariantObserver(future));
        return future;
    }

    @Override
    public CompletableFuture<Variant> popAt(byte[] key,
                                            KeyHintData hint,
                                            int pos,
                                            int clientId,
                                            Duration timeout) {
        CompletableFuture<Variant> future = new CompletableFuture<>();
        KeyPositionRequest request = KeyPositionRequest.newBuilder()
                .setKey(KeyValueUtils.createUnorderedKey(key, hint, clientId, getDefaultCompressionThreshold()))
                .setPos(pos)
                .build();
        getStub(timeout).getAndDeleteElementAtPosition(request, new DecompressingVariantObserver(future));
        return future;
    }

    @Override
    public CompletableFuture<Integer> deleteTail(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        CompletableFuture<Integer> future = new CompletableFuture<>();
        getStub(timeout).deleteTail(buildGetReq(key, hint, clientId, getDefaultCompressionThreshold()),
                                    new CompletableFutureObserver<>(future, IntResponse::getSize));
        return future;
    }

    @Override
    public CompletableFuture<Integer> deleteHead(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        CompletableFuture<Integer> future = new CompletableFuture<>();
        getStub(timeout).deleteHead(buildGetReq(key, hint, clientId, getDefaultCompressionThreshold()),
                                    new CompletableFutureObserver<>(future, IntResponse::getSize));
        return future;
    }

    @Override
    public CompletableFuture<Integer> removeAt(byte[] key,
                                               KeyHintData hint,
                                               long pos,
                                               long endPos,
                                               int clientId,
                                               Duration timeout) {
        CompletableFuture<Integer> future = new CompletableFuture<>();
        KeyPositionRequest.Builder builder = KeyPositionRequest.newBuilder()
                .setKey(KeyValueUtils.createUnorderedKey(key, hint, clientId, getDefaultCompressionThreshold()))
                .setPos(pos);
        if (endPos > pos) {
            builder.setEnd(endPos);
        }
        getStub(timeout).deleteElementAtPosition(builder.build(),
                                                 new CompletableFutureObserver<>(future, IntResponse::getSize));
        return future;
    }

    @Override
    public CompletableFuture<Integer> removeFromContainerByKeyAndValue(byte[] key,
                                                                       KeyHintData hint,
                                                                       List<Payload> keys,
                                                                       List<Variant> values,
                                                                       int clientId,
                                                                       Duration timeout) {
        CompletableFuture<Integer> future = new CompletableFuture<>();
        RemoveFromContainerRequest.Builder builder = RemoveFromContainerRequest.newBuilder()
                .setKey(buildKey(key, hint, clientId, getDefaultCompressionThreshold()));

        if (values != null) {
            for (Variant val : values) {
                builder.addValues(KeyValueUtils.buildValueFromVariantCompressed(val, getDefaultCompressionThreshold()));
            }
        }

        if (keys != null) {
            for (Payload k : keys) {
                builder.addKeys(KeyValueUtils.createUnorderedKey(k.getValue(),
                                                                 clientId,
                                                                 getDefaultCompressionThreshold()).build());
            }
        }

        getStub(timeout).deleteFromContainerByKeyValue(builder.build(),
                                                       new CompletableFutureObserver<>(future, IntResponse::getSize));
        return future;
    }



    @Override
    public CompletableFuture<Integer> removeFromContainerByKey(byte[] key,
                                                               KeyHintData hint,
                                                               Payload containerKey,
                                                               int clientId,
                                                               Duration timeout) {
        CompletableFuture<Integer> future = new CompletableFuture<>();
        ContainerGetRequest.Builder builder = ContainerGetRequest.newBuilder()
                .setKey(buildKey(key, hint, clientId, getDefaultCompressionThreshold()));

        builder.setElementKey(KeyValueUtils.createUnorderedKey(containerKey.getValue(), clientId, getDefaultCompressionThreshold())
                                      .build());

        getStub(timeout).deleteFromContainerByKey(builder.build(),
                                                       new CompletableFutureObserver<>(future, IntResponse::getSize));
        return future;
    }

    @Override
    public CompletableFuture<Integer> removeFromContainerByKey(byte[] key,
                                                               KeyHintData hint,
                                                               Variant containerKey,
                                                               int clientId,
                                                               Duration timeout) {
        CompletableFuture<Integer> future = new CompletableFuture<>();
        ContainerGetRequest.Builder builder = ContainerGetRequest.newBuilder()
                .setKey(buildKey(key, hint, clientId, getDefaultCompressionThreshold()));

        builder.setElementKey(KeyValueUtils.createUnorderedKey(containerKey.bytesValue(), clientId, getDefaultCompressionThreshold())
                                      .build());

        getStub(timeout).deleteFromContainerByKey(builder.build(),
                                                       new CompletableFutureObserver<>(future, IntResponse::getSize));
        return future;
    }


    // =========================================================================
    // LOCKING OPERATIONS
    // =========================================================================

    @Override
    public CompletableFuture<LockStatus> lock(byte[] key,
                                              KeyHintData hint,
                                              LockType type,
                                              int clientId,
                                              Duration duration,
                                              Duration timeout) {
        CompletableFuture<LockStatus> future = new CompletableFuture<>();
        LockRequest.Builder req = LockRequest.newBuilder()
                .setKey(buildKey(key, hint, clientId, getDefaultCompressionThreshold()))
                .setLockType(type)
                .setClientId(clientId);
        if (duration != null && !duration.isZero()) {
            req.setLockDuration((int) (TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()
                                                                       + duration.toMillis())));
        }

        getStub(timeout).lockObject(req.build(), new CompletableFutureObserver<>(future, LockResponse::getResult));
        return future;
    }

    @Override
    public CompletableFuture<LockStatus> unlock(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        CompletableFuture<LockStatus> future = new CompletableFuture<>();
        UnLockRequest req = UnLockRequest.newBuilder()
                .setKey(buildKey(key, hint, clientId, getDefaultCompressionThreshold()))
                .setClientId(clientId)
                .build();
        getStub(timeout).unlockObject(req, new CompletableFutureObserver<>(future, UnlockResponse::getResult));
        return future;
    }

    // =========================================================================
    // ATOMIC OPERATIONS
    // =========================================================================

    @Override
    public CompletableFuture<Number> exchange(byte[] key,
                                              KeyHintData hint,
                                              long value,
                                              Duration ttl,
                                              int clientId,
                                              Duration timeout) {
        CompletableFuture<Number> future = new CompletableFuture<>();
        getStub(timeout).atomicExchange(buildAtomicCreateRequest(key,
                                                                    hint,
                                                                    value,
                                                                    ttl,
                                                                    clientId,
                                                                    getDefaultCompressionThreshold()),
                                        new CompletableFutureObserver<>(future,
                                                                        resp -> KeyValueUtils.toVariant(resp)
                                                                                .numberValue()));
        return future;
    }

    @Override
    public CompletableFuture<Number> exchange(byte[] key,
                                              KeyHintData hint,
                                              double value,
                                              Duration ttl,
                                              int clientId,
                                              Duration timeout) {
        CompletableFuture<Number> future = new CompletableFuture<>();
        getStub(timeout).atomicExchange(buildAtomicCreateRequest(key,
                                                                    hint,
                                                                    value,
                                                                    ttl,
                                                                    clientId,
                                                                    getDefaultCompressionThreshold()),
                                        new CompletableFutureObserver<>(future,
                                                                        resp -> KeyValueUtils.toVariant(resp)
                                                                                .numberValue()));
        return future;
    }

    @Override
    public CompletableFuture<Number> add(byte[] key,
                                         KeyHintData hint,
                                         long delta,
                                         Duration ttl,
                                         int clientId,
                                         Duration timeout) {
        CompletableFuture<Number> future = new CompletableFuture<>();
        getStub(timeout).atomicAdd(buildAtomicCreateRequest(key,
                                                               hint,
                                                               delta,
                                                               ttl,
                                                               clientId,
                                                               getDefaultCompressionThreshold()),
                                   new CompletableFutureObserver<>(future,
                                                                   resp -> KeyValueUtils.toVariant(resp)
                                                                           .numberValue()));
        return future;
    }

    @Override
    public CompletableFuture<Number> add(byte[] key,
                                         KeyHintData hint,
                                         double delta,
                                         Duration ttl,
                                         int clientId,
                                         Duration timeout) {
        CompletableFuture<Number> future = new CompletableFuture<>();
        getStub(timeout).atomicAdd(buildAtomicCreateRequest(key,
                                                               hint,
                                                               delta,
                                                               ttl,
                                                               clientId,
                                                               getDefaultCompressionThreshold()),
                                   new CompletableFutureObserver<>(future,
                                                                   resp -> KeyValueUtils.toVariant(resp)
                                                                           .numberValue()));
        return future;
    }

    @Override
    public CompletableFuture<Number> sub(byte[] key,
                                         KeyHintData hint,
                                         long delta,
                                         Duration ttl,
                                         int clientId,
                                         Duration timeout) {
        CompletableFuture<Number> future = new CompletableFuture<>();
        getStub(timeout).atomicSub(buildAtomicCreateRequest(key,
                                                               hint,
                                                               delta,
                                                               ttl,
                                                               clientId,
                                                               getDefaultCompressionThreshold()),
                                   new CompletableFutureObserver<>(future,
                                                                   resp -> KeyValueUtils.toVariant(resp)
                                                                           .numberValue()));
        return future;
    }

    @Override
    public CompletableFuture<Number> sub(byte[] key,
                                         KeyHintData hint,
                                         double delta,
                                         Duration ttl,
                                         int clientId,
                                         Duration timeout) {
        CompletableFuture<Number> future = new CompletableFuture<>();
        getStub(timeout).atomicSub(buildAtomicCreateRequest(key,
                                                               hint,
                                                               delta,
                                                               ttl,
                                                               clientId,
                                                               getDefaultCompressionThreshold()),
                                   new CompletableFutureObserver<>(future,
                                                                   resp -> KeyValueUtils.toVariant(resp)
                                                                           .numberValue()));
        return future;
    }

    @Override
    public CompletableFuture<Long> and(byte[] key,
                                       KeyHintData hint,
                                       long mask,
                                       Duration ttl,
                                       int clientId,
                                       Duration timeout) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        getStub(timeout).atomicAnd(buildAtomicCreateRequest(key,
                                                               hint,
                                                               mask,
                                                               ttl,
                                                               clientId,
                                                               getDefaultCompressionThreshold()),
                                   new CompletableFutureObserver<>(future, resp -> {
                                       Variant variant = KeyValueUtils.toVariant(resp);
                                       return variant.isNone()
                                              ? 0L
                                              : variant.longValue();
                                   }));
        return future;
    }

    @Override
    public CompletableFuture<Long> or(byte[] key,
                                      KeyHintData hint,
                                      long mask,
                                      Duration ttl,
                                      int clientId,
                                      Duration timeout) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        getStub(timeout).atomicOr(buildAtomicCreateRequest(key,
                                                              hint,
                                                              mask,
                                                              ttl,
                                                              clientId,
                                                              getDefaultCompressionThreshold()),
                                  new CompletableFutureObserver<>(future, resp -> {
                                      Variant variant = KeyValueUtils.toVariant(resp);
                                      return variant.isNone()
                                             ? 0L
                                             : variant.longValue();
                                  }));
        return future;
    }

    @Override
    public CompletableFuture<Long> xor(byte[] key,
                                       KeyHintData hint,
                                       long mask,
                                       Duration ttl,
                                       int clientId,
                                       Duration timeout) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        getStub(timeout).atomicXor(buildAtomicCreateRequest(key,
                                                               hint,
                                                               mask,
                                                               ttl,
                                                               clientId,
                                                               getDefaultCompressionThreshold()),
                                   new CompletableFutureObserver<>(future, resp -> {
                                       Variant variant = KeyValueUtils.toVariant(resp);
                                       return variant.isNone()
                                              ? 0L
                                              : variant.longValue();
                                   }));
        return future;
    }

    @Override
    public CompletableFuture<AtomicCasRes> compareAndSet(byte[] key,
                                                         KeyHintData hint,
                                                         long expectedValue,
                                                         long newValue,
                                                         Duration ttl,
                                                         int clientId,
                                                         Duration timeout) {
        CompletableFuture<AtomicCasRes> future = new CompletableFuture<>();
        Value.Builder expectedBuilder = Value.newBuilder()
                .setValue(BinaryPayload.newBuilder().setIntPayload(expectedValue))
                .setValueType(ValueType.INT);
        Value.Builder toSetBuilder = Value.newBuilder()
                .setValue(BinaryPayload.newBuilder().setIntPayload(newValue))
                .setValueType(ValueType.INT);
        if (ttl != null && !ttl.isZero()) {
            toSetBuilder.setTtl(System.currentTimeMillis() + ttl.toMillis());
        }
        AtomicCas req = AtomicCas.newBuilder()
                .setKey(buildKey(key, hint, clientId, getDefaultCompressionThreshold()))
                .setExpected(expectedBuilder)
                .setToSet(toSetBuilder)
                .build();
        getStub(timeout).atomicCompareAndSet(req, new CompletableFutureObserver<>(future));
        return future;
    }

    @Override
    public CompletableFuture<AtomicCasRes> compareAndSet(byte[] key,
                                                         KeyHintData hint,
                                                         double expectedValue,
                                                         double newValue,
                                                         Duration ttl,
                                                         int clientId,
                                                         Duration timeout) {
        CompletableFuture<AtomicCasRes> future = new CompletableFuture<>();
        Value.Builder expectedBuilder = Value.newBuilder()
                .setValue(BinaryPayload.newBuilder().setDoublePayload(expectedValue))
                .setValueType(ValueType.DOUBLE);
        Value.Builder toSetBuilder = Value.newBuilder()
                .setValue(BinaryPayload.newBuilder().setDoublePayload(newValue))
                .setValueType(ValueType.DOUBLE);
        if (ttl != null && !ttl.isZero()) {
            toSetBuilder.setTtl(System.currentTimeMillis() + ttl.toMillis());
        }
        AtomicCas req = AtomicCas.newBuilder()
                .setKey(buildKey(key, hint, clientId, getDefaultCompressionThreshold()))
                .setExpected(expectedBuilder)
                .setToSet(toSetBuilder)
                .build();
        getStub(timeout).atomicCompareAndSet(req, new CompletableFutureObserver<>(future));
        return future;
    }

    // =========================================================================
    // HELPER & UTILITY METHODS
    // =========================================================================

    private HurriCacheGrpcServiceGrpc.HurriCacheGrpcServiceStub getStub(Duration timeout) {
        Duration effectiveTimeout = (timeout == null || timeout.isZero()) ? defaultTimeout : timeout;
        if (effectiveTimeout == null || effectiveTimeout.isZero()) {
            return asyncStub;
        }
        return asyncStub.withDeadlineAfter(effectiveTimeout);
    }

    @Override
    public CompletableFuture<Variant> getInContainer(byte[] key,
                                                     KeyHintData hint,
                                                     byte[] elementKey,
                                                     int clientId,
                                                     Duration timeout) {
        CompletableFuture<Variant> future = new CompletableFuture<>();
        ContainerGetRequest request = ContainerGetRequest.newBuilder()
                .setKey(KeyValueUtils.createUnorderedKey(key, hint, clientId, getDefaultCompressionThreshold()))
                .setElementKey(KeyValueUtils.createUnorderedKey(elementKey, null, clientId, null))
                .build();
        getStub(timeout).getValueInContainerByKey(request,
                                                  new CompletableFutureObserver<>(future, KeyValueUtils::toVariant));
        return future;
    }

    @Override
    public CompletableFuture<Variant> popInContainer(byte[] key,
                                                     KeyHintData hint,
                                                     byte[] elementKey,
                                                     int clientId,
                                                     Duration timeout) {
        CompletableFuture<Variant> future = new CompletableFuture<>();
        ContainerGetRequest request = ContainerGetRequest.newBuilder()
                .setKey(KeyValueUtils.createUnorderedKey(key, hint, clientId, getDefaultCompressionThreshold()))
                .setElementKey(KeyValueUtils.createUnorderedKey(elementKey, null, clientId, null))
                .build();
        getStub(timeout).getAndDeleteValueInContainer(request,
                                                      new CompletableFutureObserver<>(future,
                                                                                      KeyValueUtils::toVariant));
        return future;
    }

    @Override
    public CompletableFuture<Boolean> containsKey(byte[] key,
                                                  KeyHintData hint,
                                                  byte[] elementKey,
                                                  int clientId,
                                                  Duration timeout) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        ContainerGetRequest request = ContainerGetRequest.newBuilder()
                .setKey(KeyValueUtils.createUnorderedKey(key, hint, clientId, getDefaultCompressionThreshold()))
                .setElementKey(KeyValueUtils.createUnorderedKey(elementKey, null, clientId, null))
                .build();
        getStub(timeout).existInContainerByKey(request,
                                               new CompletableFutureObserver<>(future, BoolResponse::getValue));
        return future;
    }

    @Override
    public CompletableFuture<Variant> update(byte[] key,
                                             KeyHintData hint,
                                             byte[] elementKey,
                                             Variant value,
                                             int clientId,
                                             Duration timeout) {
        CompletableFuture<Variant> future = new CompletableFuture<>();
        UpdateContainerRequest request = UpdateContainerRequest.newBuilder()
                .setKey(KeyValueUtils.createUnorderedKey(key, hint, clientId, getDefaultCompressionThreshold()))
                .setElementKey(KeyValueUtils.createUnorderedKey(elementKey, null, clientId, null))
                .setValue(buildValueFromVariant(value, null))
                .build();
        getStub(timeout).updateValueInContainer(request, new CompletableFutureObserver<>(future, resp -> {
            if (resp.hasValue()) {
                return toVariant(ValueResponse.newBuilder().setValueUnordered(resp.getValue()).build());
            }
            return Variant.none();
        }));
        return future;
    }

    @Override
    public CompletableFuture<Integer> remove(byte[] key,
                                             KeyHintData hint,
                                             byte[] elementKey,
                                             int clientId,
                                             Duration timeout) {
        CompletableFuture<Integer> future = new CompletableFuture<>();
        ContainerGetRequest request = ContainerGetRequest.newBuilder()
                .setKey(KeyValueUtils.createUnorderedKey(key, hint, clientId, getDefaultCompressionThreshold()))
                .setElementKey(KeyValueUtils.createUnorderedKey(elementKey, null, clientId, null))
                .build();
        getStub(timeout).deleteFromContainerByKey(request,
                                                  new CompletableFutureObserver<>(future, IntResponse::getSize));
        return future;
    }

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
        if (container_values == null || container_values.isEmpty()) {
            return CompletableFuture.completedFuture(0);
        }
        if (container_keys.size() != container_values.size()) {
            throw new IllegalArgumentException("container_keys and container_values must have the same size");
        }
        int size = Math.min(container_keys.size(), container_values.size());

        Key protoKey = buildKey(key, hint, clientId, getDefaultCompressionThreshold());
        AddToRequest.Builder builder = AddToRequest.newBuilder().setKey(protoKey);

        for (int i = 0; i < size; i++) {
            Payload payload = container_keys.get(i);
            Variant upayload = container_values.get(i);
            Key uk = Key.newBuilder()
                    .setPayload(KeyBinaryPayload.newBuilder()
                                        .setSize(payload.getValue().length)
                                        .setPayload(UnsafeByteOperations.unsafeWrap(payload.getValue()))
                                        .build())
                    .build();

            Value.Builder unorderedValueBuilder = KeyValueUtils.buildValueFromVariantCompressed(upayload,
                                                                                                getDefaultCompressionThreshold());

            builder.addKeyUnordered(uk).addValueUnordered(unorderedValueBuilder);
        }
        builder.setType(ContainerType.MAP);
        CompletableFuture<Integer> future = new CompletableFuture<>();
        getStub(timeout).addElement(builder.build(), new CompletableFutureObserver<>(future, IntResponse::getSize));
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
        if (container_values == null || container_values.isEmpty()) {
            return CompletableFuture.completedFuture(0);
        }
        if (container_keys.size() != container_values.size()) {
            throw new IllegalArgumentException("container_keys and container_values must be the same size");
        }
        int size = Math.min(container_keys.size(), container_values.size());

        Key protoKey = buildKey(key, hint, clientId, getDefaultCompressionThreshold());
        AddToRequest.Builder builder = AddToRequest.newBuilder().setKey(protoKey);

        for (int i = 0; i < size; i++) {
            OrderedPayload payload = container_keys.get(i);
            Variant upayload = container_values.get(i);
            long order = payload.getOrder() != null
                         ? payload.getOrder()
                         : 0L;
            OrderedKey orderedValue = CompressionUtils.compressKeyIfNeeded(payload.getValue(),
                                                                           payload.getOrder(),
                                                                           clientId,
                                                                           getDefaultCompressionThreshold()).build();

            Value.Builder unorderedValueBuilder = KeyValueUtils.buildValueFromVariantCompressed(upayload,
                                                                                                getDefaultCompressionThreshold());
            builder.addKeyOrdered(orderedValue).addValueUnordered(unorderedValueBuilder);
        }
        builder.setType(ContainerType.ORDERED_MAP);
        CompletableFuture<Integer> future = new CompletableFuture<>();
        getStub(timeout).addElement(builder.build(), new CompletableFutureObserver<>(future, IntResponse::getSize));
        return future;
    }

    // =========================================================================
    // LIFECYCLE & OVERRIDES
    // =========================================================================

    @Override
    public void shutdown() {
        if (!ownedChannel) {
            return;
        }
        channel.shutdown();
        try {
            if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                channel.shutdownNow();
            }
        } catch (InterruptedException e) {
            channel.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FastCacheAsyncSimpleClient that = (FastCacheAsyncSimpleClient) o;
        return Objects.equals(target, that.target);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(target);
    }
}