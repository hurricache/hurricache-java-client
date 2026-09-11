package com.hurricache.client;

import com.google.protobuf.ByteString;
import com.hurricache.client.intf.HurriCacheClientInterface;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.OrderedPayload;
import com.hurricache.client.intf.Payload;
import com.hurricache.grpc.AddToRequest;
import com.hurricache.grpc.AddToValRequest;
import com.hurricache.grpc.AtomicCas;
import com.hurricache.grpc.AtomicCasRes;
import com.hurricache.grpc.AtomicCreate;
import com.hurricache.grpc.AtomicValue;
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
import com.hurricache.grpc.LockInfo;
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
import com.hurricache.utils.CompletableFutureObserver;
import com.hurricache.utils.CompressionUtils;
import com.hurricache.utils.DecompressingObserver;
import com.hurricache.utils.StreamBatchMapObserver;
import com.hurricache.utils.StreamBatchOrderedMapObserver;
import com.hurricache.utils.StreamBatchOrderedObserver;
import com.hurricache.utils.StreamBatchUnorderedObserver;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class FastCacheAsyncSimpleClient implements HurriCacheClientInterface {

    private final HurriCacheGrpcServiceGrpc.HurriCacheGrpcServiceStub asyncStub;
    private final ManagedChannel channel;
    private final int defaultClientId;
    private final Duration defaultTimeout;
    private final int defaultCompressionThreshold;
    private final String target;

    public FastCacheAsyncSimpleClient(String host,
                                      int port,
                                      int defaultClientId,
                                      Duration timeout,
                                      int defaultCompressionThreshold) {
        this.channel = ManagedChannelBuilder.forAddress(host, port).directExecutor().usePlaintext().build();
        this.asyncStub = HurriCacheGrpcServiceGrpc.newStub(channel);
        this.defaultClientId = defaultClientId;
        this.defaultTimeout = timeout;
        this.defaultCompressionThreshold = defaultCompressionThreshold;
        this.target = host + ":" + port;
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
        this.channel = channel;
        this.asyncStub = HurriCacheGrpcServiceGrpc.newStub(channel);
        this.defaultClientId = defaultClientId;
        this.defaultTimeout = duration;
        this.defaultCompressionThreshold = defaultCompressionThreshold;
        this.target = channel.toString();
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
    public CompletableFuture<byte[]> getAndDeleteValue(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        GetRequest request = GetRequest.newBuilder()
                .setKey(KeyValueUtils.createUnorderedKey(key, hint, clientId, getDefaultCompressionThreshold()))
                .build();
        getStub(timeout).getAndDeleteValue(request, new DecompressingObserver(future));
        return future;
    }

    @Override
    public CompletableFuture<KeyHintData> createKeyValue(byte[] key,
                                                         KeyHintData hint,
                                                         byte[] value,
                                                         Duration ttl,
                                                         int clientId,
                                                         Duration timeout) {
        CompletableFuture<KeyHintData> future = new CompletableFuture<>();
        Value.Builder valueBuilder = CompressionUtils.compressIfNeeded(value, getDefaultCompressionThreshold());
        if (ttl != null && !ttl.isZero()) {
            valueBuilder.setTtl(System.currentTimeMillis() + ttl.toMillis());
        }
        valueBuilder.setLockInfo(LockInfo.newBuilder().setLockedBy(clientId).setType(LockType.NO_LOCK).build());
        CreateRequest req = CreateRequest.newBuilder()
                .setKey(KeyValueUtils.createUnorderedKey(key, hint, clientId, getDefaultCompressionThreshold()))
                .setValue(valueBuilder)
                .build();
        getStub(timeout).createKeyValue(req, new CompletableFutureObserver<>(future, keyHintResponse -> {
            KeyHint keyHint = keyHintResponse.getKeyHint();
            return KeyHintData.of(keyHint.getStrongHash(), keyHint.getWeekHash());
        }));
        return future;
    }

    @Override
    public CompletableFuture<byte[]> getValue(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        getStub(timeout).getValue(buildGetReq(key, hint, clientId), new DecompressingObserver(future));
        return future;
    }

    @Override
    public CompletableFuture<byte[]> updateKeyValue(byte[] key,
                                                    KeyHintData hint,
                                                    byte[] value,
                                                    Duration ttl,
                                                    int clientId,
                                                    Duration timeout) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        Value.Builder builderForValue = CompressionUtils.compressIfNeeded(value, getDefaultCompressionThreshold());
        if (ttl != null && !ttl.isZero()) {
            builderForValue.setTtl(System.currentTimeMillis() + ttl.toMillis());
        }
        UpdateRequest.Builder req = UpdateRequest.newBuilder()
                .setKey(buildKey(key, hint, clientId))
                .setValue(builderForValue);

        getStub(timeout).updateValue(req.build(), new DecompressingObserver.Update(future));
        return future;
    }

    @Override
    public CompletableFuture<Boolean> existKey(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        getStub(timeout).existKey(buildGetReq(key, hint, clientId),
                                  new CompletableFutureObserver<>(future, BoolResponse::getValue));
        return future;
    }

    @Override
    public CompletableFuture<Boolean> remove(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        getStub(timeout).remove(buildGetReq(key, hint, clientId),
                                new CompletableFutureObserver<>(future, BoolResponse::getValue));
        return future;
    }

    // =========================================================================
    // CONTAINER CREATION (UNORDERED & ORDERED)
    // =========================================================================

    @Override
    public CompletableFuture<KeyHintData> createQueue(byte[] key,
                                                      KeyHintData keyHint,
                                                      List<Payload> initialValue,
                                                      Duration ttl,
                                                      int clientId,
                                                      Duration timeout) {
        return createUnorderedContainer(key, keyHint, initialValue, ContainerType.QUEUE, ttl, clientId, timeout);
    }

    @Override
    public CompletableFuture<KeyHintData> createList(byte[] key,
                                                     KeyHintData keyHint,
                                                     List<Payload> initialValue,
                                                     Duration ttl,
                                                     int clientId,
                                                     Duration timeout) {
        return createUnorderedContainer(key, keyHint, initialValue, ContainerType.LIST, ttl, clientId, timeout);
    }

    @Override
    public CompletableFuture<KeyHintData> createVector(byte[] key,
                                                       KeyHintData keyHint,
                                                       List<Payload> initialValue,
                                                       Duration ttl,
                                                       int clientId,
                                                       Duration timeout) {
        return createUnorderedContainer(key, keyHint, initialValue, ContainerType.VECTOR, ttl, clientId, timeout);
    }

    @Override
    public CompletableFuture<KeyHintData> createSet(byte[] key,
                                                    KeyHintData keyHint,
                                                    List<Payload> initialValue,
                                                    Duration ttl,
                                                    int clientId,
                                                    Duration timeout) {
        return createUnorderedContainer(key, keyHint, initialValue, ContainerType.SET, ttl, clientId, timeout);
    }

    private CompletableFuture<KeyHintData> createUnorderedContainer(byte[] key,
                                                                    KeyHintData keyHint,
                                                                    List<Payload> initialValue,
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
            for (Payload payload : initialValue) {
                Value.Builder compressedValue = CompressionUtils.compressIfNeeded(payload.getValue(), null);
                builder.addValueUnordered(compressedValue);
            }
        }

        CompletableFuture<KeyHintData> createFuture = new CompletableFuture<>();
        getStub(timeout).createContainer(builder.build(),
                                         new CompletableFutureObserver<>(createFuture, keyHintResponse -> {
                                             KeyHint keyHint1 = keyHintResponse.getKeyHint();
                                             return KeyHintData.of(keyHint1.getStrongHash(), keyHint1.getWeekHash());
                                         }));

        return createFuture;
    }

    @Override
    public CompletableFuture<KeyHintData> createOrderedSet(byte[] key,
                                                           KeyHintData keyHint,
                                                           List<OrderedPayload> initialValue,
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
            for (OrderedPayload payload : initialValue) {
                long order = payload.getOrder() != null
                             ? payload.getOrder()
                             : 0L;
                OrderedValue orderedValue = KeyValueUtils.createOrderedValue(payload.getValue(), order, ttl, getDefaultCompressionThreshold()).build();
                builder.addValueOrdered(orderedValue);
            }
        }

        CompletableFuture<KeyHintData> createFuture = new CompletableFuture<>();
        getStub(timeout).createContainer(builder.build(),
                                         new CompletableFutureObserver<>(createFuture, keyHintResponse -> {
                                             KeyHint keyHint1 = keyHintResponse.getKeyHint();
                                             return KeyHintData.of(keyHint1.getStrongHash(), keyHint1.getWeekHash());
                                         }));
        return createFuture;
    }

    @Override
    public CompletableFuture<KeyHintData> createMap(byte[] key,
                                                    KeyHintData keyHint,
                                                    Map<Payload, Payload> initialValue,
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
            for (Map.Entry<Payload, Payload> entry : initialValue.entrySet()) {
                Key kVal = KeyValueUtils.createUnorderedKey(entry.getKey().getValue(), clientId, null).build();
                Value vVal = KeyValueUtils.createUnorderedValue(entry.getValue().getValue(), ttl, null).build();
                builder.addKeyUnordered(kVal);
                builder.addValueUnordered(vVal);
            }
        }
        CompletableFuture<KeyHintData> createFuture = new CompletableFuture<>();
        getStub(timeout).createContainer(builder.build(),
                                         new CompletableFutureObserver<>(createFuture, keyHintResponse -> {
                                             KeyHint keyHint1 = keyHintResponse.getKeyHint();
                                             return KeyHintData.of(keyHint1.getStrongHash(), keyHint1.getWeekHash());
                                         }));

        return createFuture;
    }

    public CompletableFuture<KeyHintData> createOrderedMap(byte[] key,
                                                           KeyHintData keyHint,
                                                           Map<OrderedPayload, Payload> initialValue,
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
            for (Map.Entry<OrderedPayload, Payload> entry : initialValue.entrySet()) {
                long order = entry.getKey().getOrder() != null
                             ? entry.getKey().getOrder()
                             : 0L;
                OrderedKey kVal = KeyValueUtils.createOrderedKey(entry.getKey().getValue(), order, clientId,getDefaultCompressionThreshold()).build();
                Value vVal = KeyValueUtils.createUnorderedValue(entry.getValue().getValue(),
                                                                ttl,
                                                                getDefaultCompressionThreshold()).build();
                builder.addKeyOrdered(kVal);
                builder.addValueUnordered(vVal);
            }
        }

        CompletableFuture<KeyHintData> createFuture = new CompletableFuture<>();

        getStub(timeout).createContainer(builder.build(),
                                         new CompletableFutureObserver<>(createFuture, keyHintResponse -> {
                                             KeyHint keyHint1 = keyHintResponse.getKeyHint();
                                             return KeyHintData.of(keyHint1.getStrongHash(), keyHint1.getWeekHash());
                                         }));
        return createFuture;
    }


    @Override
    public CompletableFuture<Integer> getSize(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        CompletableFuture<Integer> future = new CompletableFuture<>();
        getStub(timeout).getSize(buildGetReq(key, hint, clientId),
                                 new CompletableFutureObserver<>(future, IntResponse::getSize));
        return future;
    }

    @Override
    public CompletableFuture<Payload> getAndRemoveFront(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        CompletableFuture<byte[]> rawFuture = new CompletableFuture<>();
        getStub(timeout).getAndRemoveFront(buildGetReq(key, hint, clientId), new DecompressingObserver(rawFuture));
        return rawFuture.thenApply(bytes -> bytes != null
                                            ? Payload.of(bytes)
                                            : null);
    }

    @Override
    public CompletableFuture<Payload> getFront(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        CompletableFuture<byte[]> rawFuture = new CompletableFuture<>();
        getStub(timeout).getHead(buildGetReq(key, hint, clientId), new DecompressingObserver(rawFuture));
        return rawFuture.thenApply(bytes -> bytes != null
                                            ? Payload.of(bytes)
                                            : null);
    }

    @Override
    public CompletableFuture<Payload> getHead(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        CompletableFuture<byte[]> rawFuture = new CompletableFuture<>();
        getStub(timeout).getHead(buildGetReq(key, hint, clientId), new DecompressingObserver(rawFuture));
        return rawFuture.thenApply(bytes -> bytes != null
                                            ? Payload.of(bytes)
                                            : null);
    }

    @Override
    public CompletableFuture<Payload> getTail(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        CompletableFuture<byte[]> rawFuture = new CompletableFuture<>();
        getStub(timeout).getTail(buildGetReq(key, hint, clientId), new DecompressingObserver(rawFuture));
        return rawFuture.thenApply(bytes -> bytes != null
                                            ? Payload.of(bytes)
                                            : null);
    }

    @Override
    public CompletableFuture<Payload> getElementAtPosition(byte[] key,
                                                           KeyHintData hint,
                                                           int pos,
                                                           int clientId,
                                                           Duration timeout) {
        CompletableFuture<byte[]> rawFuture = new CompletableFuture<>();
        KeyPositionRequest req = KeyPositionRequest.newBuilder()
                .setKey(buildKey(key, hint, clientId))
                .setPos(pos)
                .build();
        getStub(timeout).getElementAtPosition(req, new DecompressingObserver(rawFuture));
        return rawFuture.thenApply(bytes -> bytes != null
                                            ? Payload.of(bytes)
                                            : null);
    }

    // =========================================================================
    // STREAMING READ OPERATIONS
    // =========================================================================

    @Override
    public CompletableFuture<List<Payload>> streamList(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        CompletableFuture<List<Payload>> rawFuture = new CompletableFuture<>();
        getStub(timeout).getContainer(buildGetReq(key, hint, clientId), new StreamBatchUnorderedObserver(rawFuture));
        return rawFuture;
    }

    @Override
    public CompletableFuture<List<Payload>> streamVector(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        CompletableFuture<List<Payload>> rawFuture = new CompletableFuture<>();
        getStub(timeout).getContainer(buildGetReq(key, hint, clientId), new StreamBatchUnorderedObserver(rawFuture));
        return rawFuture;
    }

    @Override
    public CompletableFuture<List<Payload>> streamSet(byte[] key, KeyHintData hint,int clientId,
                                               Duration timeout){
        CompletableFuture<List<Payload>> rawFuture = new CompletableFuture<>();
        getStub(timeout).getContainer(buildGetReq(key, hint, clientId), new StreamBatchUnorderedObserver(rawFuture));
        return rawFuture;
    }

    @Override
    public CompletableFuture<Map<Payload, Payload>> streamMap(byte[] key,
                                                              KeyHintData hint,
                                                              int clientId,
                                                              Duration timeout) {
        CompletableFuture<Map<Payload, Payload>> rawFuture = new CompletableFuture<>();
        getStub(timeout).getContainer(buildGetReq(key, hint, clientId), new StreamBatchMapObserver(rawFuture));
        return rawFuture;
    }

    @Override
    public CompletableFuture<Map<OrderedPayload, Payload>> streamOrderedMap(byte[] key,
                                                                            KeyHintData hint,
                                                                            int clientId,
                                                                            Duration timeout) {
        CompletableFuture<Map<OrderedPayload, Payload>> rawFuture = new CompletableFuture<>();
        getStub(timeout).getContainer(buildGetReq(key, hint, clientId), new StreamBatchOrderedMapObserver(rawFuture));
        return rawFuture;
    }

    @Override
    public CompletableFuture<List<Payload>> streamElementInRangeUnordered(byte[] key,
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
        CompletableFuture<List<Payload>> rawFuture = new CompletableFuture<>();

        switch (containerType) {
            case LIST, VECTOR, SET -> getStub(timeout).getElementInRange(request.setType(containerType).build(),
                                                                         new StreamBatchUnorderedObserver(rawFuture));
            default -> throw new IllegalArgumentException("Unsupported container type for stream operation: "
                                                          + containerType);
        }

        return rawFuture;
    }

    @Override
    public CompletableFuture<List<OrderedPayload>> streamElementInRangeOrderedSet(byte[] key,
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

        CompletableFuture<List<OrderedPayload>> rawFuture = new CompletableFuture<>();
        getStub(timeout).getElementInRange(request, new StreamBatchOrderedObserver(rawFuture));
        return rawFuture;
    }

    // =========================================================================
    // INSERTION OPERATIONS
    // =========================================================================

    @Override
    public CompletableFuture<Integer> addElement(byte[] key,
                                                 KeyHintData hint,
                                                 List<Payload> data,
                                                 int clientId,
                                                 Duration timeout) {
        if (data == null || data.isEmpty()) {
            return CompletableFuture.completedFuture(0);
        }
        Key protoKey = buildKey(key, hint, clientId);
        AddToRequest.Builder builder = AddToRequest.newBuilder()
                .setKey(protoKey)
                .setPos(-1);

        for (Payload datum : data) {
            builder.addValueUnordered(KeyValueUtils.createUnorderedValue(datum.getValue(), Duration.ZERO, null));
        }

        CompletableFuture<Integer> future = new CompletableFuture<>();
        getStub(timeout).addElement(builder.build(),
                                    new CompletableFutureObserver<>(future, IntResponse::getSize));
        return future;
    }

    @Override
    public CompletableFuture<Integer> addElementOrdered(byte[] key,
                                                        KeyHintData hint,
                                                        List<OrderedPayload> data,
                                                        int clientId,
                                                        Duration timeout) {
        if (data == null || data.isEmpty()) {
            return CompletableFuture.completedFuture(0);
        }
        AddToRequest.Builder builder = AddToRequest.newBuilder().setKey(buildKey(key, hint, clientId));
        for (OrderedPayload datum : data) {
            OrderedValue.Builder compressedValue = KeyValueUtils.createOrderedValue(datum.getValue(),
                                                                                    datum.getOrder(),
                                                                                    Duration.ZERO, getDefaultCompressionThreshold());
            builder.addValueOrdered(compressedValue);
        }


        CompletableFuture<Integer> chunkFuture = new CompletableFuture<>();

        getStub(timeout).addElement(builder.build(),
                                    new CompletableFutureObserver<>(chunkFuture, IntResponse::getSize));
        return chunkFuture;
    }

    @Override
    public CompletableFuture<Integer> addElementToTail(byte[] key,
                                                       KeyHintData hint,
                                                       List<Payload> data,
                                                       int clientId,
                                                       Duration timeout) {
        if (data == null || data.isEmpty()) {
            return CompletableFuture.completedFuture(0);
        }
        Key protoKey = buildKey(key, hint, clientId);
        AddToRequest.Builder builder = AddToRequest.newBuilder().setKey(protoKey);

        for (Payload datum : data) {
            builder.addValueUnordered(KeyValueUtils.createUnorderedValue(datum.getValue(), Duration.ZERO, null));
        }

        CompletableFuture<Integer> future = new CompletableFuture<>();
        getStub(timeout).addElementToTail(builder.build(),
                                          new CompletableFutureObserver<>(future, IntResponse::getSize));
        return future;
    }

    @Override
    public CompletableFuture<Integer> addElementToHead(byte[] key,
                                                       KeyHintData hint,
                                                       List<Payload> data,
                                                       int clientId,
                                                       Duration timeout) {
        if (data == null || data.isEmpty()) {
            return CompletableFuture.completedFuture(0);
        }
        Key protoKey = buildKey(key, hint, clientId);
        AddToRequest.Builder builder = AddToRequest.newBuilder().setKey(protoKey);

        for (Payload datum : data) {
            builder.addValueUnordered(KeyValueUtils.createUnorderedValue(datum.getValue(), Duration.ZERO, null));
        }

        CompletableFuture<Integer> future = new CompletableFuture<>();
        getStub(timeout).addElementToHead(builder.build(),
                                          new CompletableFutureObserver<>(future, IntResponse::getSize));
        return future;
    }

    @Override
    public CompletableFuture<Integer> addElementToPosition(byte[] key,
                                                           KeyHintData hint,
                                                           List<Payload> data,
                                                           int pos,
                                                           int clientId,
                                                           Duration timeout) {
        if (data == null || data.isEmpty()) {
            return CompletableFuture.completedFuture(0);
        }
        Key protoKey = buildKey(key, hint, clientId);
        AddToRequest.Builder builder = AddToRequest.newBuilder()
                .setKey(protoKey)
                .setPos(pos);

        for (Payload datum : data) {
            builder.addValueUnordered(KeyValueUtils.createUnorderedValue(datum.getValue(), Duration.ZERO, null));
        }

        CompletableFuture<Integer> future = new CompletableFuture<>();
        getStub(timeout).addElement(builder.build(),
                                    new CompletableFutureObserver<>(future, IntResponse::getSize));
        return future;
    }

    @Override
    public CompletableFuture<Integer> addElementWithWeight(byte[] key,
                                                           KeyHintData hint,
                                                           List<OrderedPayload> data,
                                                           int clientId,
                                                           Duration timeout) {
        if (data == null || data.isEmpty()) {
            return CompletableFuture.completedFuture(0);
        }

        Key protoKey = buildKey(key, hint, clientId);
        AddToRequest.Builder builder = AddToRequest.newBuilder().setKey(protoKey);

        for (OrderedPayload payload : data) {
            long order = payload.getOrder() != null
                         ? payload.getOrder()
                         : 0L;
            OrderedValue orderedValue = OrderedValue.newBuilder()
                    .setOrder(order)
                    .setValue(BinaryPayload.newBuilder()
                                      .setSize(payload.getValue().length)
                                      .setPayload(ByteString.copyFrom(payload.getValue()))
                                      .build())
                    .build();
            builder.addValueOrdered(orderedValue);
        }

        CompletableFuture<Integer> future = new CompletableFuture<>();
        getStub(timeout).addElement(builder.build(), new CompletableFutureObserver<>(future, IntResponse::getSize));
        return future;
    }

    @Override
    public CompletableFuture<Integer> addElementToPositionBefore(byte[] key,
                                                                 KeyHintData hint,
                                                                 List<Payload> data,
                                                                 Payload pivot,
                                                                 int clientId,
                                                                 Duration timeout) {
        if (data == null || data.isEmpty()) {
            return CompletableFuture.completedFuture(0);
        }
        CompletableFuture<Integer> future = new CompletableFuture<>();
        AddToValRequest.Builder builder = AddToValRequest.newBuilder();
        builder.setKey(buildKey(key, hint, clientId));
        builder.setIsBefore(true);
        data.forEach(element -> builder.addValue(KeyValueUtils.createUnorderedValue(element.getValue(),
                                                                                    Duration.ZERO,
                                                                                    null)));
        builder.setPos(KeyValueUtils.createUnorderedValue(pivot.getValue(), Duration.ZERO, null));
        getStub(timeout).addElementToPositionByValue(builder.build(),
                                                     new CompletableFutureObserver<>(future, IntResponse::getSize));
        return future;
    }

    @Override
    public CompletableFuture<Integer> addElementToPositionAfter(byte[] key,
                                                                KeyHintData hint,
                                                                List<Payload> data,
                                                                Payload pivot,
                                                                int clientId,
                                                                Duration timeout) {
        if (data == null || data.isEmpty()) {
            return CompletableFuture.completedFuture(0);
        }
        CompletableFuture<Integer> future = new CompletableFuture<>();
        AddToValRequest.Builder builder = AddToValRequest.newBuilder();
        builder.setKey(buildKey(key, hint, clientId));
        builder.setIsBefore(false);
        data.forEach(element -> builder.addValue(KeyValueUtils.createUnorderedValue(element.getValue(),
                                                                                    Duration.ZERO,
                                                                                    null)));
        builder.setPos(KeyValueUtils.createUnorderedValue(pivot.getValue(), Duration.ZERO, null));
        getStub(timeout).addElementToPositionByValue(builder.build(),
                                                     new CompletableFutureObserver<>(future, IntResponse::getSize));
        return future;
    }

    // =========================================================================
    // POP & DELETION OPERATIONS
    // =========================================================================

    @Override
    public CompletableFuture<Payload> getAndRemoveTail(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        CompletableFuture<byte[]> rawFuture = new CompletableFuture<>();
        getStub(timeout).getAndRemoveTail(buildGetReq(key, hint, clientId), new DecompressingObserver(rawFuture));
        return rawFuture.thenApply(bytes -> bytes != null
                                            ? Payload.of(bytes)
                                            : null);
    }

    @Override
    public CompletableFuture<Payload> getAndRemoveElementAtPosition(byte[] key,
                                                                    KeyHintData hint,
                                                                    int pos,
                                                                    int clientId,
                                                                    Duration timeout) {
        CompletableFuture<byte[]> rawFuture = new CompletableFuture<>();
        KeyPositionRequest request = KeyPositionRequest.newBuilder()
                .setKey(KeyValueUtils.createUnorderedKey(key, hint, clientId, getDefaultCompressionThreshold()))
                .setPos(pos)
                .build();
        getStub(timeout).getAndRemoveElementAtPosition(request, new DecompressingObserver(rawFuture));
        return rawFuture.thenApply(bytes -> bytes != null
                                            ? Payload.of(bytes)
                                            : null);
    }

    @Override
    public CompletableFuture<Boolean> removeTail(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        getStub(timeout).removeTail(buildGetReq(key, hint, clientId),
                                    new CompletableFutureObserver<>(future, BoolResponse::getValue));
        return future;
    }

    @Override
    public CompletableFuture<Boolean> removeHead(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        getStub(timeout).removeHead(buildGetReq(key, hint, clientId),
                                    new CompletableFutureObserver<>(future, BoolResponse::getValue));
        return future;
    }

    @Override
    public CompletableFuture<Boolean> removeElementAtPosition(byte[] key,
                                                              KeyHintData hint,
                                                              int pos,
                                                              int endPos,
                                                              int clientId,
                                                              Duration timeout) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        KeyPositionRequest.Builder builder = KeyPositionRequest.newBuilder()
                .setKey(KeyValueUtils.createUnorderedKey(key, hint, clientId, getDefaultCompressionThreshold()))
                .setPos(pos)
                .setEnd(endPos);
        getStub(timeout).removeElementAtPosition(builder.build(),
                                                 new CompletableFutureObserver<>(future, BoolResponse::getValue));
        return future;
    }

    @Override
    public CompletableFuture<Integer> removeFromContainer(byte[] key,
                                                          KeyHintData hint,
                                                          ContainerType type,
                                                          List<Payload> values,
                                                          List<Payload> keys,
                                                          int clientId,
                                                          Duration timeout) {
        CompletableFuture<Integer> future = new CompletableFuture<>();
        RemoveFromContainerRequest.Builder builder = RemoveFromContainerRequest.newBuilder()
                .setKey(buildKey(key, hint, clientId))
                .setType(type);

        if (values != null) {
            for (Payload val : values) {
                builder.addValues(CompressionUtils.compressIfNeeded(val.getValue(), null));
            }
        }

        if (keys != null) {
            for (Payload k : keys) {
                builder.addKeys(KeyValueUtils.createUnorderedKey(k.getValue(),
                                                                 clientId,
                                                                 getDefaultCompressionThreshold()));
            }
        }

        getStub(timeout).removeFromContainerByKeyValue(builder.build(),
                                                       new CompletableFutureObserver<>(future, IntResponse::getSize));
        return future;
    }

    // =========================================================================
    // LOCKING OPERATIONS
    // =========================================================================

    @Override
    public CompletableFuture<LockStatus> lockObject(byte[] key,
                                                    KeyHintData hint,
                                                    LockType type,
                                                    int clientId,
                                                    Duration duration,
                                                    Duration timeout) {
        CompletableFuture<LockStatus> future = new CompletableFuture<>();
        LockRequest.Builder req = LockRequest.newBuilder()
                .setKey(buildKey(key, hint, clientId))
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
    public CompletableFuture<LockStatus> unlockObject(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        CompletableFuture<LockStatus> future = new CompletableFuture<>();
        UnLockRequest req = UnLockRequest.newBuilder()
                .setKey(buildKey(key, hint, clientId))
                .setClientId(clientId)
                .build();
        getStub(timeout).unlockObject(req, new CompletableFutureObserver<>(future, UnlockResponse::getResult));
        return future;
    }

    // =========================================================================
    // ATOMIC OPERATIONS
    // =========================================================================

    @Override
    public CompletableFuture<Long> atomicLoad(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        getStub(timeout).atomicLoad(buildGetReq(key, hint, clientId),
                                    new CompletableFutureObserver<>(future, AtomicValue::getVal));
        return future;
    }

    @Override
    public CompletableFuture<Long> atomicLoadAndDelete(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        getStub(timeout).atomicLoadAndDelete(buildGetReq(key, hint, clientId),
                                             new CompletableFutureObserver<>(future, AtomicValue::getVal));
        return future;
    }

    private AtomicCreate buildAtomicCreateReq(byte[] key, KeyHintData hint, long value, Duration ttl, int clientId) {
        AtomicValue atomicVal = AtomicValue.newBuilder().setVal(value).build();

        AtomicCreate.Builder builder = AtomicCreate.newBuilder()
                .setKey(buildKey(key, hint, clientId))
                .setVal(atomicVal);

        if (ttl != null && !ttl.isZero()) {
            builder.setTtl(System.currentTimeMillis() + ttl.toMillis());
        }
        return builder.build();
    }

    @Override
    public CompletableFuture<KeyHintData> atomicCreate(byte[] key,
                                                       KeyHintData hint,
                                                       long value,
                                                       Duration ttl,
                                                       int clientId,
                                                       Duration timeout) {
        CompletableFuture<KeyHintData> future = new CompletableFuture<>();
        AtomicCreate req = buildAtomicCreateReq(key, hint, value, ttl, clientId);
        getStub(timeout).atomicCreate(req, new CompletableFutureObserver<>(future, keyHintResponse -> {
            KeyHint keyHint = keyHintResponse.getKeyHint();
            return KeyHintData.of(keyHint.getStrongHash(), keyHint.getWeekHash());
        }));
        return future;
    }

    @Override
    public CompletableFuture<KeyHintData> atomicStore(byte[] key,
                                                      KeyHintData hint,
                                                      long value,
                                                      Duration ttl,
                                                      int clientId,
                                                      Duration timeout) {
        CompletableFuture<KeyHintData> future = new CompletableFuture<>();
        AtomicCreate req = buildAtomicCreateReq(key, hint, value, ttl, clientId);
        getStub(timeout).atomicStore(req, new CompletableFutureObserver<>(future, keyHintResponse -> {
            KeyHint keyHint = keyHintResponse.getKeyHint();
            return KeyHintData.of(keyHint.getStrongHash(), keyHint.getWeekHash());
        }));
        return future;
    }

    @Override
    public CompletableFuture<Long> atomicExchange(byte[] key,
                                                  KeyHintData hint,
                                                  long value,
                                                  Duration ttl,
                                                  int clientId,
                                                  Duration timeout) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        AtomicCreate req = buildAtomicCreateReq(key, hint, value, ttl, clientId);
        getStub(timeout).atomicExchange(req, new CompletableFutureObserver<>(future, AtomicValue::getVal));
        return future;
    }

    @Override
    public CompletableFuture<Long> atomicAdd(byte[] key,
                                             KeyHintData hint,
                                             long delta,
                                             Duration ttl,
                                             int clientId,
                                             Duration timeout) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        AtomicCreate req = buildAtomicCreateReq(key, hint, delta, ttl, clientId);
        getStub(timeout).atomicAdd(req, new CompletableFutureObserver<>(future, AtomicValue::getVal));
        return future;
    }

    @Override
    public CompletableFuture<Long> atomicSub(byte[] key,
                                             KeyHintData hint,
                                             long delta,
                                             Duration ttl,
                                             int clientId,
                                             Duration timeout) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        AtomicCreate req = buildAtomicCreateReq(key, hint, delta, ttl, clientId);
        getStub(timeout).atomicSub(req, new CompletableFutureObserver<>(future, AtomicValue::getVal));
        return future;
    }

    @Override
    public CompletableFuture<Long> atomicAnd(byte[] key,
                                             KeyHintData hint,
                                             long mask,
                                             Duration ttl,
                                             int clientId,
                                             Duration timeout) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        AtomicCreate req = buildAtomicCreateReq(key, hint, mask, ttl, clientId);
        getStub(timeout).atomicAnd(req, new CompletableFutureObserver<>(future, AtomicValue::getVal));
        return future;
    }

    @Override
    public CompletableFuture<Long> atomicOr(byte[] key,
                                            KeyHintData hint,
                                            long mask,
                                            Duration ttl,
                                            int clientId,
                                            Duration timeout) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        AtomicCreate req = buildAtomicCreateReq(key, hint, mask, ttl, clientId);
        getStub(timeout).atomicOr(req, new CompletableFutureObserver<>(future, AtomicValue::getVal));
        return future;
    }

    @Override
    public CompletableFuture<Long> atomicXor(byte[] key,
                                             KeyHintData hint,
                                             long mask,
                                             Duration ttl,
                                             int clientId,
                                             Duration timeout) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        AtomicCreate req = buildAtomicCreateReq(key, hint, mask, ttl, clientId);
        getStub(timeout).atomicXor(req, new CompletableFutureObserver<>(future, AtomicValue::getVal));
        return future;
    }

    @Override
    public CompletableFuture<AtomicCasRes> atomicCompareAndSet(byte[] key,
                                                               KeyHintData hint,
                                                               long expectedValue,
                                                               long newValue,
                                                               Duration ttl,
                                                               int clientId,
                                                               Duration timeout) {
        CompletableFuture<AtomicCasRes> future = new CompletableFuture<>();

        AtomicValue expectedAtomic = AtomicValue.newBuilder().setVal(expectedValue).build();
        AtomicValue newAtomic = AtomicValue.newBuilder().setVal(newValue).build();

        AtomicCas.Builder builder = AtomicCas.newBuilder()
                .setKey(buildKey(key, hint, clientId))
                .setExpected(expectedAtomic)
                .setToSet(newAtomic);

        if (ttl != null && !ttl.isZero()) {
            builder.setTtl(System.currentTimeMillis() + ttl.toMillis());
        }

        getStub(timeout).atomicCompareAndSet(builder.build(), new CompletableFutureObserver<>(future));
        return future;
    }

    // =========================================================================
    // HELPER & UTILITY METHODS
    // =========================================================================

    private Key buildKey(byte[] key, KeyHintData hint, int clientId) {
        int cid = (clientId != 0)
                  ? clientId
                  : defaultClientId;
        return KeyValueUtils.createUnorderedKey(key, hint, cid, getDefaultCompressionThreshold()).build();
    }

    private GetRequest buildGetReq(byte[] key, KeyHintData hint, Integer clientId) {
        return GetRequest.newBuilder().setKey(buildKey(key, hint, clientId)).build();
    }

    private HurriCacheGrpcServiceGrpc.HurriCacheGrpcServiceStub getStub(Duration timeout) {
        if (timeout == null || timeout.isZero()) {
            return asyncStub;
        }
        return asyncStub.withDeadlineAfter(timeout);
    }


    @Override
    public CompletableFuture<byte[]> getContainerValue(byte[] key,
                                                       KeyHintData hint,
                                                       byte[] elementKey,
                                                       int clientId,
                                                       Duration timeout) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        ContainerGetRequest request = ContainerGetRequest.newBuilder()
                .setKey(KeyValueUtils.createUnorderedKey(key, hint, clientId, getDefaultCompressionThreshold()))
                .setElementKey(KeyValueUtils.createUnorderedKey(elementKey, null, clientId, null))
                .build();
        getStub(timeout).getValueInContainer(request, new DecompressingObserver(future));
        return future;
    }

    @Override
    public CompletableFuture<byte[]> getAndRemoveContainerValue(byte[] key,
                                                                KeyHintData hint,
                                                                byte[] elementKey,
                                                                int clientId,
                                                                Duration timeout) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        ContainerGetRequest request = ContainerGetRequest.newBuilder()
                .setKey(KeyValueUtils.createUnorderedKey(key, hint, clientId, getDefaultCompressionThreshold()))
                .setElementKey(KeyValueUtils.createUnorderedKey(elementKey, null, clientId, null))
                .build();
        getStub(timeout).getAndDeleteValueInContainer(request, new DecompressingObserver(future));
        return future;
    }

    @Override
    public CompletableFuture<Boolean> containsContainerKey(byte[] key,
                                                           KeyHintData hint,
                                                           byte[] elementKey,
                                                           int clientId,
                                                           Duration timeout) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        ContainerGetRequest request = ContainerGetRequest.newBuilder()
                .setKey(KeyValueUtils.createUnorderedKey(key, hint, clientId, getDefaultCompressionThreshold()))
                .setElementKey(KeyValueUtils.createUnorderedKey(elementKey, null, clientId, null))
                .build();
        getStub(timeout).existKeyInContainer(request, new CompletableFutureObserver<>(future, BoolResponse::getValue));
        return future;
    }

    @Override
    public CompletableFuture<byte[]> updateContainerValue(byte[] key,
                                                          KeyHintData hint,
                                                          byte[] elementKey,
                                                          byte[] value,
                                                          int clientId,
                                                          Duration timeout) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        Value.Builder builderForValue = CompressionUtils.compressIfNeeded(value, null);
        UpdateContainerRequest request = UpdateContainerRequest.newBuilder()
                .setKey(KeyValueUtils.createUnorderedKey(key, hint, clientId, getDefaultCompressionThreshold()))
                .setElementKey(KeyValueUtils.createUnorderedKey(elementKey, null, clientId, null))
                .setValue(builderForValue)
                .build();
        getStub(timeout).updateValueInContainer(request, new DecompressingObserver.Update(future));
        return future;
    }

    @Override
    public CompletableFuture<Integer> removeFromContainer(byte[] key,
                                                          KeyHintData hint,
                                                          byte[] elementKey,
                                                          int clientId,
                                                          Duration timeout) {
        CompletableFuture<Integer> future = new CompletableFuture<>();
        ContainerGetRequest request = ContainerGetRequest.newBuilder()
                .setKey(KeyValueUtils.createUnorderedKey(key, hint, clientId, getDefaultCompressionThreshold()))
                .setElementKey(KeyValueUtils.createUnorderedKey(elementKey, null, clientId, null))
                .build();
        getStub(timeout).removeInContainer(request, new CompletableFutureObserver<>(future, IntResponse::getSize));
        return future;
    }

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
        if (container_values == null || container_values.isEmpty()) {
            return CompletableFuture.completedFuture(0);
        }
        if (container_keys.size() != container_values.size()) {
            throw new IllegalArgumentException("container_keys and container_values must have the same size");
        }
        int size = Math.min(container_keys.size(), container_values.size());

        Key protoKey = buildKey(key, hint, clientId);
        AddToRequest.Builder builder = AddToRequest.newBuilder().setKey(protoKey);

        for (int i = 0; i < size; i++) {
            Payload payload = container_keys.get(i);
            Payload upayload = container_values.get(i);
            Key.Builder uk = Key.newBuilder()
                    .setPayload(KeyBinaryPayload.newBuilder()
                                        .setSize(payload.getValue().length)
                                        .setPayload(ByteString.copyFrom(payload.getValue()))
                                        .build());

            Value.Builder unorderedValueBuilder = Value.newBuilder()
                    .setValue(BinaryPayload.newBuilder()
                                      .setSize(upayload.getValue().length)
                                      .setPayload(ByteString.copyFrom(upayload.getValue()))
                                      .build());

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
                                                           List<Payload> container_values,
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

        Key protoKey = buildKey(key, hint, clientId);
        AddToRequest.Builder builder = AddToRequest.newBuilder().setKey(protoKey);

        for (int i = 0; i < size; i++) {
            OrderedPayload payload = container_keys.get(i);
            Payload upayload = container_values.get(i);
            long order = payload.getOrder() != null
                         ? payload.getOrder()
                         : 0L;
            OrderedKey.Builder orderedValue = OrderedKey.newBuilder()
                    .setOrder(order)
                    .setPayload(KeyBinaryPayload.newBuilder()
                                        .setSize(payload.getValue().length)
                                        .setPayload(ByteString.copyFrom(payload.getValue()))
                                        .build());

            Value.Builder unorderedValueBuilder = Value.newBuilder()
                    .setValue(BinaryPayload.newBuilder()
                                      .setSize(payload.getValue().length)
                                      .setPayload(ByteString.copyFrom(upayload.getValue()))
                                      .build());

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
        channel.shutdown();
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