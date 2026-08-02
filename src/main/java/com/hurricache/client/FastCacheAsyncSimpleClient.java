package com.hurricache.client;

import com.google.protobuf.ByteString;
import com.hurricache.client.intf.HurriCacheClientInterface;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.OrderedPayload;
import com.hurricache.client.intf.Payload;
import com.hurricache.grpc.*;
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

public class FastCacheAsyncSimpleClient implements HurriCacheClientInterface {

    private final HurriCacheGrpcServiceGrpc.HurriCacheGrpcServiceStub asyncStub;
    private final ManagedChannel channel;
    private final int defaultClientId;
    private final Duration defaultTimeout;
    private final String target;
    private static final long MAX_RPC_SIZE = 4 * 1024 * 1024 - 1024 * 1024 / 2;

    private enum AddType { TAIL, HEAD, POSITION,NON_POSITION }

    public FastCacheAsyncSimpleClient(String host, int port, int defaultClientId, Duration timeout) {
        this.channel = ManagedChannelBuilder.forAddress(host, port).directExecutor().usePlaintext().build();
        this.asyncStub = HurriCacheGrpcServiceGrpc.newStub(channel);
        this.defaultClientId = defaultClientId;
        this.defaultTimeout = timeout;
        this.target = host + ":" + port;
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
        this.channel = channel;
        this.asyncStub = HurriCacheGrpcServiceGrpc.newStub(channel);
        this.defaultClientId = defaultClientId;
        this.defaultTimeout = duration;
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

    // =========================================================================
    // TTL MANAGEMENT
    // =========================================================================

    @Override
    public CompletableFuture<Boolean> setTtl(byte[] key, KeyHintData hint, long ttl, int clientId, Duration timeout) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        TtlRequest request = TtlRequest.newBuilder()
                .setKey(KeyValueUtils.createUnorderedKey(key, hint, clientId))
                .setTtl(System.currentTimeMillis() + ttl)
                .build();
        getStub(timeout).setTtl(request, new CompletableFutureObserver<>(future, BoolResponse::getValue));
        return future;
    }

    @Override
    public CompletableFuture<Long> getTtl(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        GetRequest ttlRequest = GetRequest.newBuilder()
                .setKey(KeyValueUtils.createUnorderedKey(key, hint, clientId))
                .build();
        getStub(timeout).getTtl(ttlRequest, new CompletableFutureObserver<>(future, item -> item.hasTtl() ? item.getTtl() - System.currentTimeMillis() : -1L));
        return future;
    }

    // =========================================================================
    // KEY-VALUE OPERATIONS
    // =========================================================================

    @Override
    public CompletableFuture<byte[]> getAndDeleteValue(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        GetRequest request = GetRequest.newBuilder()
                .setKey(KeyValueUtils.createUnorderedKey(key, hint, clientId))
                .build();
        getStub(timeout).getAndDeleteValue(request, new DecompressingObserver(future));
        return future;
    }

    @Override
    public CompletableFuture<KeyHintData> createKeyValue(byte[] key, KeyHintData hint, byte[] value, Duration ttl, int clientId, Duration timeout) {
        CompletableFuture<KeyHintData> future = new CompletableFuture<>();
        Value.Builder valueBuilder = CompressionUtils.compressIfNeeded(value);
        if (ttl != null && !ttl.isZero()) {
            valueBuilder.setTtl(System.currentTimeMillis() + ttl.toMillis());
        }
        valueBuilder.setLockInfo(LockInfo.newBuilder().setLockedBy(clientId).setType(LockType.NO_LOCK).build());
        CreateRequest req = CreateRequest.newBuilder()
                .setKey(KeyValueUtils.createUnorderedKey(key, hint, clientId))
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
    public CompletableFuture<byte[]> updateKeyValue(byte[] key, KeyHintData hint, byte[] value, Duration ttl, int clientId, Duration timeout) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        Value.Builder builderForValue = CompressionUtils.compressIfNeeded(value);
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
        getStub(timeout).existKey(buildGetReq(key, hint, clientId), new CompletableFutureObserver<>(future, BoolResponse::getValue));
        return future;
    }

    @Override
    public CompletableFuture<Boolean> remove(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        getStub(timeout).remove(buildGetReq(key, hint, clientId), new CompletableFutureObserver<>(future, BoolResponse::getValue));
        return future;
    }

    // =========================================================================
    // CONTAINER CREATION (UNORDERED & ORDERED)
    // =========================================================================

    @Override
    public CompletableFuture<KeyHintData> createQueue(byte[] key,
                                                      KeyHintData keyHint,
                                                      List<Payload> initialValue, Duration ttl, int clientId, Duration timeout) {
        return createUnorderedContainer(key, keyHint, initialValue, ContainerType.QUEUE, ttl, clientId, timeout);
    }

    @Override
    public CompletableFuture<KeyHintData> createList(byte[] key,
                                                     KeyHintData keyHint,
                                                     List<Payload> initialValue, Duration ttl, int clientId, Duration timeout) {
        return createUnorderedContainer(key, keyHint, initialValue, ContainerType.LIST, ttl, clientId, timeout);
    }

    @Override
    public CompletableFuture<KeyHintData> createVector(byte[] key,
                                                       KeyHintData keyHint,
                                                       List<Payload> initialValue, Duration ttl, int clientId, Duration timeout) {
        return createUnorderedContainer(key,keyHint , initialValue, ContainerType.VECTOR, ttl, clientId, timeout);
    }

    @Override
    public CompletableFuture<KeyHintData> createSet(byte[] key,
                                                    KeyHintData keyHint,
                                                    List<Payload> initialValue, Duration ttl, int clientId, Duration timeout) {
        return createUnorderedContainer(key, keyHint, initialValue, ContainerType.SET, ttl, clientId, timeout);
    }

    private CompletableFuture<KeyHintData> createUnorderedContainer(byte[] key,
                                                                    KeyHintData keyHint,
                                                                    List<Payload> initialValue, ContainerType type, Duration ttl, int clientId, Duration timeout) {
        CreateContainerRequest.Builder builder = CreateContainerRequest.newBuilder();
        if (ttl != null && !ttl.isZero()) {
            builder.setTtl(System.currentTimeMillis() + ttl.toMillis());
        }
        Key.Builder protoKey = KeyValueUtils.createUnorderedKey(key, clientId);
        builder.setKey(protoKey).setType(type);
        if (keyHint != null) {
            KeyHint.Builder khb = KeyHint.newBuilder();
            if (keyHint.hasWeekHash()) khb.setWeekHash(keyHint.getWeek_hash());
            if (keyHint.hasStrongHash()) khb.setStrongHash(keyHint.getStrong_hash());
            protoKey.setKeyHint(khb);
        }
        Key keyProto = protoKey.build();
        long currentChunkSize = keyProto.getSerializedSize();
        int splitIndex = 0;

        if (initialValue != null) {
            for (Payload payload : initialValue) {
                Value.Builder compressedValue = CompressionUtils.compressIfNeeded(payload.getValue());
                int elemSize = compressedValue.build().getSerializedSize();

                if (currentChunkSize + elemSize > MAX_RPC_SIZE) {
                    break;
                }

                builder.addValueUnordered(compressedValue);
                currentChunkSize += elemSize;
                splitIndex++;
            }
        }

        CompletableFuture<KeyHintData> createFuture = new CompletableFuture<>();
        getStub(timeout).createContainer(builder.build(), new CompletableFutureObserver<>(createFuture, keyHintResponse -> {
            KeyHint keyHint1 = keyHintResponse.getKeyHint();
            return KeyHintData.of(keyHint1.getStrongHash(), keyHint1.getWeekHash());
        }));

        List<Payload> remainingTail = (initialValue != null) ? initialValue.subList(splitIndex, initialValue.size()) : List.of();

        return createFuture.thenCompose(keyHint1 -> {
            if (remainingTail.isEmpty()) {
                return CompletableFuture.completedFuture(keyHint1);
            } else {
                repDelay();
                return sendTailInChunks(keyProto, keyHint1, remainingTail, timeout);
            }
        });
    }

    @Override
    public CompletableFuture<KeyHintData> createOrderedSet(byte[] key, List<OrderedPayload> initialValue, Duration ttl, int clientId, Duration timeout) {
        CreateContainerRequest.Builder builder = CreateContainerRequest.newBuilder();
        if (ttl != null && !ttl.isZero()) {
            builder.setTtl(System.currentTimeMillis() + ttl.toMillis());
        }
        Key protoKey = KeyValueUtils.createUnorderedKey(key, clientId).build();
        builder.setKey(protoKey).setType(ContainerType.ORDERED_SET);

        long currentChunkSize = protoKey.getSerializedSize();
        int splitIndex = 0;

        if (initialValue != null) {
            for (OrderedPayload payload : initialValue) {
                long order = payload.getOrder() != null ? payload.getOrder() : 0L;
                OrderedValue orderedValue = KeyValueUtils.createOrderedValue(payload.getValue(), order, ttl).build();
                int elemSize = orderedValue.getSerializedSize();

                if (currentChunkSize + elemSize > MAX_RPC_SIZE) {
                    break;
                }

                builder.addValueOrdered(orderedValue);
                currentChunkSize += elemSize;
                splitIndex++;
            }
        }

        CompletableFuture<KeyHintData> createFuture = new CompletableFuture<>();
        getStub(timeout).createContainer(builder.build(), new CompletableFutureObserver<>(createFuture, keyHintResponse -> {
            KeyHint keyHint = keyHintResponse.getKeyHint();
            return KeyHintData.of(keyHint.getStrongHash(), keyHint.getWeekHash());
        }));

        List<OrderedPayload> remainingTail = (initialValue != null) ? initialValue.subList(splitIndex, initialValue.size()) : List.of();

        return createFuture.thenCompose(keyHint -> {
            if (remainingTail.isEmpty()) {
                return CompletableFuture.completedFuture(keyHint);
            } else {
                repDelay();
                return sendTailInChunksOrdered(protoKey, keyHint, remainingTail, ttl, timeout);
            }
        });
    }

    @Override
    public CompletableFuture<KeyHintData> createMap(byte[] key, Map<Payload, Payload> initialValue, Duration ttl, int clientId, Duration timeout) {
        CreateContainerRequest.Builder builder = CreateContainerRequest.newBuilder();
        if (ttl != null && !ttl.isZero()) {
            builder.setTtl(System.currentTimeMillis() + ttl.toMillis());
        }
        Key protoKey = KeyValueUtils.createUnorderedKey(key, clientId).build();
        builder.setKey(protoKey).setType(ContainerType.MAP);

        long currentChunkSize = protoKey.getSerializedSize();

        if (initialValue != null) {
            for (Map.Entry<Payload, Payload> entry : initialValue.entrySet()) {
                Key kVal = KeyValueUtils.createUnorderedKey(entry.getKey().getValue(), clientId).build();
                Value vVal = KeyValueUtils.createUnorderedValue(entry.getValue().getValue(), ttl).build();

                int pairSize = kVal.getSerializedSize() + vVal.getSerializedSize();
                if (currentChunkSize + pairSize > MAX_RPC_SIZE) {
                    break;
                }
                builder.addKeyUnordered(kVal);
                builder.addValueUnordered(vVal);

                currentChunkSize += pairSize;
            }
        }

        CompletableFuture<KeyHintData> createFuture = new CompletableFuture<>();
        getStub(timeout).createContainer(builder.build(), new CompletableFutureObserver<>(createFuture, keyHintResponse -> {
            KeyHint keyHint = keyHintResponse.getKeyHint();
            return KeyHintData.of(keyHint.getStrongHash(), keyHint.getWeekHash());
        }));
        return createFuture;
    }

    @Override
    public CompletableFuture<KeyHintData> createOrderedMap(byte[] key, Map<OrderedPayload, Payload> initialValue, Duration ttl, int clientId, Duration timeout) {
        CreateContainerRequest.Builder builder = CreateContainerRequest.newBuilder();
        if (ttl != null && !ttl.isZero()) {
            builder.setTtl(System.currentTimeMillis() + ttl.toMillis());
        }
        Key protoKey = KeyValueUtils.createUnorderedKey(key, clientId).build();
        builder.setKey(protoKey).setType(ContainerType.ORDERED_MAP);

        long currentChunkSize = protoKey.getSerializedSize();

        if (initialValue != null) {
            for (Map.Entry<OrderedPayload, Payload> entry : initialValue.entrySet()) {
                long order = entry.getKey().getOrder() != null ? entry.getKey().getOrder() : 0L;
                OrderedKey kVal = KeyValueUtils.createOrderedKey(entry.getKey().getValue(), order, clientId).build();
                Value vVal = KeyValueUtils.createUnorderedValue(entry.getValue().getValue(), ttl).build();

                int pairSize = kVal.getSerializedSize() + vVal.getSerializedSize();
                if (currentChunkSize + pairSize > MAX_RPC_SIZE) {
                    break;
                }
                builder.addKeyOrdered(kVal);
                builder.addValueUnordered(vVal);

                currentChunkSize += pairSize;
            }
        }

        CompletableFuture<KeyHintData> createFuture = new CompletableFuture<>();
        getStub(timeout).createContainer(builder.build(), new CompletableFutureObserver<>(createFuture, keyHintResponse -> {
            KeyHint keyHint = keyHintResponse.getKeyHint();
            return KeyHintData.of(keyHint.getStrongHash(), keyHint.getWeekHash());
        }));
        return createFuture;
    }

    // =========================================================================
    // BOUNDARY & POSITIONAL READS / CONTAINER INFO
    // =========================================================================

    @Override
    public CompletableFuture<Integer> getSize(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        CompletableFuture<Integer> future = new CompletableFuture<>();
        getStub(timeout).getSize(buildGetReq(key, hint, clientId), new CompletableFutureObserver<>(future, IntResponse::getSize));
        return future;
    }

    @Override
    public CompletableFuture<Payload> getAndRemoveFront(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        CompletableFuture<byte[]> rawFuture = new CompletableFuture<>();
        getStub(timeout).getAndRemoveFront(buildGetReq(key, hint, clientId), new DecompressingObserver(rawFuture));
        return rawFuture.thenApply(bytes -> bytes != null ? Payload.of(bytes) : null);
    }

    @Override
    public CompletableFuture<Payload> getFront(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        CompletableFuture<byte[]> rawFuture = new CompletableFuture<>();
        getStub(timeout).getHead(buildGetReq(key, hint, clientId), new DecompressingObserver(rawFuture));
        return rawFuture.thenApply(bytes -> bytes != null ? Payload.of(bytes) : null);
    }

    @Override
    public CompletableFuture<Payload> getHead(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        CompletableFuture<byte[]> rawFuture = new CompletableFuture<>();
        getStub(timeout).getHead(buildGetReq(key, hint, clientId), new DecompressingObserver(rawFuture));
        return rawFuture.thenApply(bytes -> bytes != null ? Payload.of(bytes) : null);
    }

    @Override
    public CompletableFuture<Payload> getTail(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        CompletableFuture<byte[]> rawFuture = new CompletableFuture<>();
        getStub(timeout).getTail(buildGetReq(key, hint, clientId), new DecompressingObserver(rawFuture));
        return rawFuture.thenApply(bytes -> bytes != null ? Payload.of(bytes) : null);
    }

    @Override
    public CompletableFuture<Payload> getElementAtPosition(byte[] key, KeyHintData hint, int pos, int clientId, Duration timeout) {
        CompletableFuture<byte[]> rawFuture = new CompletableFuture<>();
        KeyPositionRequest req = KeyPositionRequest.newBuilder()
                .setKey(buildKey(key, hint, clientId))
                .setPos(pos)
                .build();
        getStub(timeout).getElementAtPosition(req, new DecompressingObserver(rawFuture));
        return rawFuture.thenApply(bytes -> bytes != null ? Payload.of(bytes) : null);
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
    public CompletableFuture<Map<Payload, Payload>> streamMap(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        CompletableFuture<Map<Payload, Payload>> rawFuture = new CompletableFuture<>();
        getStub(timeout).getContainer(buildGetReq(key, hint, clientId), new StreamBatchMapObserver(rawFuture));
        return rawFuture;
    }

    @Override
    public CompletableFuture<Map<OrderedPayload, Payload>> streamOrderedMap(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        CompletableFuture<Map<OrderedPayload, Payload>> rawFuture = new CompletableFuture<>();
        getStub(timeout).getContainer(buildGetReq(key, hint, clientId), new StreamBatchOrderedMapObserver(rawFuture));
        return rawFuture;
    }

    @Override
    public CompletableFuture<List<Payload>> streamElementInRangeUnordered(byte[] key, KeyHintData hint, ContainerType containerType, int start, int end, int clientId, Duration timeout) {
        KeyPositionRequest.Builder request = KeyPositionRequest.newBuilder()
                .setKey(KeyValueUtils.createUnorderedKey(key, hint, clientId))
                .setPos(start)
                .setEnd(end);
        CompletableFuture<List<Payload>> rawFuture = new CompletableFuture<>();

        switch (containerType) {
            case LIST, VECTOR, SET -> getStub(timeout).getElementInRange(request.setType(containerType).build(), new StreamBatchUnorderedObserver(rawFuture));
            default -> throw new IllegalArgumentException("Unsupported container type for stream operation: " + containerType);
        }

        return rawFuture;
    }

    @Override
    public CompletableFuture<List<OrderedPayload>> streamElementInRangeOrderedSet(byte[] key, KeyHintData hint, long startWeight, long endWeight, boolean reverse, int clientId, Duration timeout) {
        KeyPositionRequest request = KeyPositionRequest.newBuilder()
                .setKey(KeyValueUtils.createUnorderedKey(key, hint, clientId))
                .setType(ContainerType.ORDERED_SET)
                .setPos(startWeight)
                .setEnd(endWeight)
                .build();

        CompletableFuture<List<OrderedPayload>> rawFuture = new CompletableFuture<>();
        getStub(timeout).getElementInRange(request, new StreamBatchOrderedObserver(rawFuture));
        return rawFuture;
    }

    // =========================================================================
    // INSERTION OPERATIONS
    // =========================================================================

    @Override
    public CompletableFuture<Boolean> addElement(byte[] key, KeyHintData hint, List<Payload> data, int clientId, Duration timeout) {
        if (data == null || data.isEmpty()) {
            return CompletableFuture.completedFuture(true);
        }
        Key protoKey = buildKey(key, hint, clientId);
        return sendAddRequestInChunks(protoKey, data, -1, AddType.POSITION, timeout);
    }

    @Override
    public CompletableFuture<Boolean> addElementToTail(byte[] key, KeyHintData hint, List<Payload> data, int clientId, Duration timeout) {
        if (data == null || data.isEmpty()) {
            return CompletableFuture.completedFuture(true);
        }
        Key protoKey = buildKey(key, hint, clientId);
        return sendAddRequestInChunks(protoKey, data, -1, AddType.TAIL, timeout);
    }

    @Override
    public CompletableFuture<Boolean> addElementToHead(byte[] key, KeyHintData hint, List<Payload> data, int clientId, Duration timeout) {
        if (data == null || data.isEmpty()) {
            return CompletableFuture.completedFuture(true);
        }
        Key protoKey = buildKey(key, hint, clientId);
        return sendAddRequestInChunks(protoKey, data, -1, AddType.HEAD, timeout);
    }

    @Override
    public CompletableFuture<Integer> addElementToPosition(byte[] key, KeyHintData hint, List<Payload> data, int pos, int clientId, Duration timeout) {
        if (data == null || data.isEmpty()) {
            return CompletableFuture.completedFuture(0);
        }
        Key protoKey = buildKey(key, hint, clientId);
        return sendAddRequestInChunks(protoKey, data, pos, timeout);
    }

    @Override
    public CompletableFuture<Integer> addElementWithWeight(byte[] key, KeyHintData hint, List<OrderedPayload> data, int clientId, Duration timeout) {
        if (data == null || data.isEmpty()) {
            return CompletableFuture.completedFuture(0);
        }

        Key protoKey = buildKey(key, hint, clientId);
        AddToRequest.Builder builder = AddToRequest.newBuilder().setKey(protoKey);

        for (OrderedPayload payload : data) {
            long order = payload.getOrder() != null ? payload.getOrder() : 0L;
            OrderedValue orderedValue = OrderedValue.newBuilder()
                    .setOrder(order)
                    .setValue(BinaryPayload.newBuilder().setSize(payload.getValue().length).setPayload(ByteString.copyFrom(payload.getValue())).build())
                    .build();
            builder.addValueOrdered(orderedValue);
        }

        CompletableFuture<Integer> future = new CompletableFuture<>();
        getStub(timeout).addElement(builder.build(), new CompletableFutureObserver<>(future, IntResponse::getSize));
        return future;
    }

    @Override
    public CompletableFuture<Boolean> addElementToPositionBefore(byte[] key, KeyHintData hint, List<Payload> data, Payload pivot, int clientId, Duration timeout) {
        if (data == null || data.isEmpty()) {
            return CompletableFuture.completedFuture(true);
        }
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        AddToValRequest.Builder builder = AddToValRequest.newBuilder();
        builder.setKey(buildKey(key, hint, clientId));
        builder.setIsBefore(true);
        data.forEach(element -> builder.addValue(KeyValueUtils.createUnorderedValue(element.getValue(), Duration.ZERO)));
        builder.setPos(KeyValueUtils.createUnorderedValue(pivot.getValue(), Duration.ZERO));
        getStub(timeout).addElementToPositionByValue(builder.build(), new CompletableFutureObserver<>(future, BoolResponse::getValue));
        return future;
    }

    @Override
    public CompletableFuture<Boolean> addElementToPositionAfter(byte[] key, KeyHintData hint, List<Payload> data, Payload pivot, int clientId, Duration timeout) {
        if (data == null || data.isEmpty()) {
            return CompletableFuture.completedFuture(true);
        }
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        AddToValRequest.Builder builder = AddToValRequest.newBuilder();
        builder.setKey(buildKey(key, hint, clientId));
        builder.setIsBefore(false);
        data.forEach(element -> builder.addValue(KeyValueUtils.createUnorderedValue(element.getValue(), Duration.ZERO)));
        builder.setPos(KeyValueUtils.createUnorderedValue(pivot.getValue(), Duration.ZERO));
        getStub(timeout).addElementToPositionByValue(builder.build(), new CompletableFutureObserver<>(future, BoolResponse::getValue));
        return future;
    }

    // =========================================================================
    // POP & DELETION OPERATIONS
    // =========================================================================

    @Override
    public CompletableFuture<Payload> getAndRemoveTail(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        CompletableFuture<byte[]> rawFuture = new CompletableFuture<>();
        getStub(timeout).getAndRemoveTail(buildGetReq(key, hint, clientId), new DecompressingObserver(rawFuture));
        return rawFuture.thenApply(bytes -> bytes != null ? Payload.of(bytes) : null);
    }

    @Override
    public CompletableFuture<Payload> getAndRemoveElementAtPosition(byte[] key, KeyHintData hint, int pos, int clientId, Duration timeout) {
        CompletableFuture<byte[]> rawFuture = new CompletableFuture<>();
        KeyPositionRequest request = KeyPositionRequest.newBuilder()
                .setKey(KeyValueUtils.createUnorderedKey(key, hint, clientId))
                .setPos(pos)
                .build();
        getStub(timeout).getAndRemoveElementAtPosition(request, new DecompressingObserver(rawFuture));
        return rawFuture.thenApply(bytes -> bytes != null ? Payload.of(bytes) : null);
    }

    @Override
    public CompletableFuture<Boolean> removeTail(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        getStub(timeout).removeTail(buildGetReq(key, hint, clientId), new CompletableFutureObserver<>(future, BoolResponse::getValue));
        return future;
    }

    @Override
    public CompletableFuture<Boolean> removeHead(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        getStub(timeout).removeHead(buildGetReq(key, hint, clientId), new CompletableFutureObserver<>(future, BoolResponse::getValue));
        return future;
    }

    @Override
    public CompletableFuture<Boolean> removeElementAtPosition(byte[] key, KeyHintData hint, int pos, int endPos, int clientId, Duration timeout) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        KeyPositionRequest.Builder builder = KeyPositionRequest.newBuilder()
                .setKey(KeyValueUtils.createUnorderedKey(key, hint, clientId))
                .setPos(pos)
                .setEnd(endPos);
        getStub(timeout).removeElementAtPosition(builder.build(), new CompletableFutureObserver<>(future, BoolResponse::getValue));
        return future;
    }

    @Override
    public CompletableFuture<Integer> removeFromContainer(byte[] key, KeyHintData hint, ContainerType type, List<Payload> values, List<Payload> keys, int clientId, Duration timeout) {
        CompletableFuture<Integer> future = new CompletableFuture<>();
        RemoveFromContainerRequest.Builder builder = RemoveFromContainerRequest.newBuilder()
                .setKey(buildKey(key, hint, clientId))
                .setType(type);

        if (values != null) {
            for (Payload val : values) {
                builder.addValues(CompressionUtils.compressIfNeeded(val.getValue()));
            }
        }

        if (keys != null) {
            for (Payload k : keys) {
                builder.addKeys(KeyValueUtils.createUnorderedKey(k.getValue(), clientId));
            }
        }

        getStub(timeout).removeFromContainerByKeyValue(builder.build(), new CompletableFutureObserver<>(future, IntResponse::getSize));
        return future;
    }

    // =========================================================================
    // LOCKING OPERATIONS
    // =========================================================================

    @Override
    public CompletableFuture<LockStatus> lockObject(byte[] key, KeyHintData hint, LockType type, int clientId, Duration duration, Duration timeout) {
        CompletableFuture<LockStatus> future = new CompletableFuture<>();
        LockRequest req = LockRequest.newBuilder()
                .setKey(buildKey(key, hint, clientId))
                .setLockType(type)
                .setClientId(clientId)
                .setLockDuration((int) duration.toSeconds())
                .build();
        getStub(timeout).lockObject(req, new CompletableFutureObserver<>(future, LockResponse::getResult));
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
        getStub(timeout).atomicLoad(buildGetReq(key, hint, clientId), new CompletableFutureObserver<>(future, AtomicValue::getVal));
        return future;
    }

    @Override
    public CompletableFuture<Long> atomicLoadAndDelete(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        getStub(timeout).atomicLoadAndDelete(buildGetReq(key, hint, clientId), new CompletableFutureObserver<>(future, AtomicValue::getVal));
        return future;
    }

    private AtomicCreate buildAtomicCreateReq(byte[] key, KeyHintData hint, long value, Duration ttl, int clientId) {
        AtomicValue atomicVal = AtomicValue.newBuilder()
                .setVal(value)
                .build();

        AtomicCreate.Builder builder = AtomicCreate.newBuilder()
                .setKey(buildKey(key, hint, clientId))
                .setVal(atomicVal);

        if (ttl != null && !ttl.isZero()) {
            builder.setTtl(System.currentTimeMillis() + ttl.toMillis());
        }
        return builder.build();
    }

    @Override
    public CompletableFuture<KeyHintData> atomicCreate(byte[] key, KeyHintData hint, long value, Duration ttl, int clientId, Duration timeout) {
        CompletableFuture<KeyHintData> future = new CompletableFuture<>();
        AtomicCreate req = buildAtomicCreateReq(key, hint, value, ttl, clientId);
        getStub(timeout).atomicCreate(req, new CompletableFutureObserver<>(future, keyHintResponse -> {
            KeyHint keyHint = keyHintResponse.getKeyHint();
            return KeyHintData.of(keyHint.getStrongHash(), keyHint.getWeekHash());
        }));
        return future;
    }

    @Override
    public CompletableFuture<KeyHintData> atomicStore(byte[] key, KeyHintData hint, long value, Duration ttl, int clientId, Duration timeout) {
        CompletableFuture<KeyHintData> future = new CompletableFuture<>();
        AtomicCreate req = buildAtomicCreateReq(key, hint, value, ttl, clientId);
        getStub(timeout).atomicStore(req, new CompletableFutureObserver<>(future, keyHintResponse -> {
            KeyHint keyHint = keyHintResponse.getKeyHint();
            return KeyHintData.of(keyHint.getStrongHash(), keyHint.getWeekHash());
        }));
        return future;
    }

    @Override
    public CompletableFuture<Long> atomicExchange(byte[] key, KeyHintData hint, long value, Duration ttl, int clientId, Duration timeout) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        AtomicCreate req = buildAtomicCreateReq(key, hint, value, ttl, clientId);
        getStub(timeout).atomicExchange(req, new CompletableFutureObserver<>(future, AtomicValue::getVal));
        return future;
    }

    @Override
    public CompletableFuture<Long> atomicAdd(byte[] key, KeyHintData hint, long delta, Duration ttl, int clientId, Duration timeout) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        AtomicCreate req = buildAtomicCreateReq(key, hint, delta, ttl, clientId);
        getStub(timeout).atomicAdd(req, new CompletableFutureObserver<>(future, AtomicValue::getVal));
        return future;
    }

    @Override
    public CompletableFuture<Long> atomicSub(byte[] key, KeyHintData hint, long delta, Duration ttl, int clientId, Duration timeout) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        AtomicCreate req = buildAtomicCreateReq(key, hint, delta, ttl, clientId);
        getStub(timeout).atomicSub(req, new CompletableFutureObserver<>(future, AtomicValue::getVal));
        return future;
    }

    @Override
    public CompletableFuture<Long> atomicAnd(byte[] key, KeyHintData hint, long mask, Duration ttl, int clientId, Duration timeout) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        AtomicCreate req = buildAtomicCreateReq(key, hint, mask, ttl, clientId);
        getStub(timeout).atomicAnd(req, new CompletableFutureObserver<>(future, AtomicValue::getVal));
        return future;
    }

    @Override
    public CompletableFuture<Long> atomicOr(byte[] key, KeyHintData hint, long mask, Duration ttl, int clientId, Duration timeout) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        AtomicCreate req = buildAtomicCreateReq(key, hint, mask, ttl, clientId);
        getStub(timeout).atomicOr(req, new CompletableFutureObserver<>(future, AtomicValue::getVal));
        return future;
    }

    @Override
    public CompletableFuture<Long> atomicXor(byte[] key, KeyHintData hint, long mask, Duration ttl, int clientId, Duration timeout) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        AtomicCreate req = buildAtomicCreateReq(key, hint, mask, ttl, clientId);
        getStub(timeout).atomicXor(req, new CompletableFutureObserver<>(future, AtomicValue::getVal));
        return future;
    }

    @Override
    public CompletableFuture<AtomicCasRes> atomicCompareAndSet(byte[] key, KeyHintData hint, long expectedValue, long newValue, Duration ttl, int clientId, Duration timeout) {
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

    private static void repDelay() {
        try {
            Thread.sleep(150);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }

    private Key buildKey(byte[] key, KeyHintData hint, int clientId) {
        int cid = (clientId != 0) ? clientId : defaultClientId;
        return KeyValueUtils.createUnorderedKey(key, hint, cid).build();
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

    private KeyHint buildProtoKeyHint(KeyHintData hintData) {
        if (hintData == null) {
            return KeyHint.getDefaultInstance();
        }
        KeyHint.Builder builder = KeyHint.newBuilder();
        if (hintData.getStrong_hash() != null) {
            builder.setStrongHash(hintData.getStrong_hash());
        }
        if (hintData.getWeek_hash() != null) {
            builder.setWeekHash(hintData.getWeek_hash());
        }
        return builder.build();
    }

    private CompletableFuture<KeyHintData> sendTailInChunks(Key protoKey, KeyHintData originalHint, List<Payload> tail, Duration timeout) {
        KeyHint originalProtoHint = buildProtoKeyHint(originalHint);
        long currentChunkSize = originalProtoHint.getSerializedSize() + 32;

        AddToRequest.Builder builder = AddToRequest.newBuilder()
                .setKey(protoKey.toBuilder().setKeyHint(originalProtoHint).build());

        int splitIndex = 0;
        for (Payload payload : tail) {
            Value.Builder compressedValue = CompressionUtils.compressIfNeeded(payload.getValue());
            int elemSize = compressedValue.build().getSerializedSize();

            if (currentChunkSize + elemSize > MAX_RPC_SIZE) {
                break;
            }

            builder.addValueUnordered(compressedValue);
            currentChunkSize += elemSize;
            splitIndex++;
        }

        CompletableFuture<BoolResponse> tailFuture = new CompletableFuture<>();
        getStub(timeout).addElementToTail(builder.build(), new CompletableFutureObserver<>(tailFuture));

        List<Payload> nextTail = tail.subList(splitIndex, tail.size());

        return tailFuture.thenCompose(response -> {
            if (nextTail.isEmpty() && response.getValue()) {
                return CompletableFuture.completedFuture(originalHint);
            } else {
                return sendTailInChunks(protoKey, originalHint, nextTail, timeout);
            }
        });
    }

    private CompletableFuture<KeyHintData> sendTailInChunksOrdered(Key protoKey, KeyHintData originalHint, List<OrderedPayload> tail, Duration ttl, Duration timeout) {
        KeyHint originalProtoHint = buildProtoKeyHint(originalHint);
        long currentChunkSize = originalProtoHint.getSerializedSize() + 32;

        AddToRequest.Builder builder = AddToRequest.newBuilder()
                .setKey(protoKey.toBuilder().setKeyHint(originalProtoHint).build());

        int splitIndex = 0;
        for (OrderedPayload payload : tail) {
            long order = payload.getOrder() != null ? payload.getOrder() : 0L;
            OrderedValue orderedValue = KeyValueUtils.createOrderedValue(payload.getValue(), order, ttl).build();
            int elemSize = orderedValue.getSerializedSize();

            if (currentChunkSize + elemSize > MAX_RPC_SIZE) {
                break;
            }

            builder.addValueOrdered(orderedValue);
            currentChunkSize += elemSize;
            splitIndex++;
        }

        CompletableFuture<BoolResponse> tailFuture = new CompletableFuture<>();
        getStub(timeout).addElementToTail(builder.build(), new CompletableFutureObserver<>(tailFuture));

        List<OrderedPayload> nextTail = tail.subList(splitIndex, tail.size());

        return tailFuture.thenCompose(response -> {
            if (nextTail.isEmpty() && response.getValue()) {
                return CompletableFuture.completedFuture(originalHint);
            } else {
                return sendTailInChunksOrdered(protoKey, originalHint, nextTail, ttl, timeout);
            }
        });
    }

    private CompletableFuture<Boolean> sendAddRequestInChunks(Key protoKey, List<Payload> data, int currentPos, AddType type, Duration timeout) {
        long currentChunkSize = protoKey.getSerializedSize() + 32;
        AddToRequest.Builder builder = AddToRequest.newBuilder().setKey(protoKey);

        if (type == AddType.POSITION) {
            builder.setPos(currentPos);
        }

        int splitIndex = 0;
        for (Payload datum : data) {
            Value.Builder compressedValue = KeyValueUtils.createUnorderedValue(datum.getValue(), Duration.ZERO);

            int elemSize = compressedValue.build().getSerializedSize();
            if (currentChunkSize + elemSize > MAX_RPC_SIZE) {
                break;
            }
            builder.addValueUnordered(compressedValue);
            currentChunkSize += elemSize;
            splitIndex++;
        }

        CompletableFuture<Boolean> chunkFuture = new CompletableFuture<>();

        switch (type) {
            case TAIL -> getStub(timeout).addElementToTail(builder.build(), new CompletableFutureObserver<>(chunkFuture, BoolResponse::getValue));
            case HEAD -> getStub(timeout).addElementToHead(builder.build(), new CompletableFutureObserver<>(chunkFuture, BoolResponse::getValue));
            case POSITION -> getStub(timeout).addElement(builder.build(), new CompletableFutureObserver<>(chunkFuture, res -> res.getSize() > 0));
        }

        List<Payload> nextTail = data.subList(splitIndex, data.size());
        int finalSplitIndex = splitIndex;

        return chunkFuture.thenCompose(success -> {
            if (!success) {
                return CompletableFuture.completedFuture(false);
            }
            if (nextTail.isEmpty()) {
                return CompletableFuture.completedFuture(true);
            }

            int nextPos = (type == AddType.POSITION) ? currentPos + finalSplitIndex : currentPos;
            return sendAddRequestInChunks(protoKey, nextTail, nextPos, type, timeout);
        });
    }

    private CompletableFuture<Integer> sendAddRequestInChunks(Key protoKey, List<Payload> data, int currentPos, Duration timeout) {
        long currentChunkSize = protoKey.getSerializedSize() + 32;
        AddToRequest.Builder builder = AddToRequest.newBuilder().setKey(protoKey).setPos(currentPos);

        int splitIndex = 0;
        for (Payload datum : data) {
            Value.Builder compressedValue = KeyValueUtils.createUnorderedValue(datum.getValue(), Duration.ZERO);

            int elemSize = compressedValue.build().getSerializedSize();
            if (currentChunkSize + elemSize > MAX_RPC_SIZE) {
                break;
            }
            builder.addValueUnordered(compressedValue);
            currentChunkSize += elemSize;
            splitIndex++;
        }

        CompletableFuture<Integer> chunkFuture = new CompletableFuture<>();

        getStub(timeout).addElement(builder.build(), new CompletableFutureObserver<>(chunkFuture, IntResponse::getSize));
        List<Payload> nextTail = data.subList(splitIndex, data.size());

        int finalSplitIndex = splitIndex;
        return chunkFuture.thenCompose(success -> {
            if (nextTail.isEmpty()) {
                return CompletableFuture.completedFuture(finalSplitIndex);
            }

            int nextPos = currentPos + finalSplitIndex;
            return sendAddRequestInChunks(protoKey, nextTail, nextPos, timeout);
        });
    }

    @Override
    public CompletableFuture<byte[]> getContainerValue(byte[] key,
                                                       KeyHintData hint,
                                                       byte[] elementKey,
                                                       int clientId,
                                                       Duration timeout) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        ContainerGetRequest request = ContainerGetRequest.newBuilder()
                .setKey(KeyValueUtils.createUnorderedKey(key, hint, clientId)).setElementKey(KeyValueUtils.createUnorderedKey(elementKey, null, clientId))
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
                .setKey(KeyValueUtils.createUnorderedKey(key, hint, clientId)).setElementKey(KeyValueUtils.createUnorderedKey(elementKey, null, clientId))
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
                .setKey(KeyValueUtils.createUnorderedKey(key, hint, clientId)).setElementKey(KeyValueUtils.createUnorderedKey(elementKey, null, clientId))
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
        Value.Builder builderForValue = CompressionUtils.compressIfNeeded(value);
//        if (ttl != null && !ttl.isZero()) {
//            builderForValue.setTtl(System.currentTimeMillis() + ttl.toMillis());
//        }
        UpdateContainerRequest request = UpdateContainerRequest.newBuilder()
                .setKey(KeyValueUtils.createUnorderedKey(key, hint, clientId)).setElementKey(KeyValueUtils.createUnorderedKey(elementKey, null, clientId)).setValue(builderForValue)
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
                .setKey(KeyValueUtils.createUnorderedKey(key, hint, clientId)).setElementKey(KeyValueUtils.createUnorderedKey(elementKey, null, clientId))
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
        int size = Math.min(container_keys.size(), container_values.size());

        Key protoKey = buildKey(key, hint, clientId);
        AddToRequest.Builder builder = AddToRequest.newBuilder().setKey(protoKey);

        for (int i = 0; i < size; i++) {
            Payload payload = container_keys.get(i);
            Payload upayload = container_values.get(i);
            Key.Builder uk = Key.newBuilder()
                    .setPayload(KeyBinaryPayload.newBuilder().setSize(payload.getValue().length).setPayload(ByteString.copyFrom(payload.getValue())).build());

            Value.Builder unorderedValueBuilder = Value.newBuilder()
                    .setValue(BinaryPayload.newBuilder().setSize(payload.getValue().length).setPayload(ByteString.copyFrom(upayload.getValue())).build());

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
        int size = Math.min(container_keys.size(), container_values.size());

        Key protoKey = buildKey(key, hint, clientId);
        AddToRequest.Builder builder = AddToRequest.newBuilder().setKey(protoKey);

        for (int i = 0; i < size; i++) {
            OrderedPayload payload = container_keys.get(i);
            Payload upayload = container_values.get(i);
            long order = payload.getOrder() != null ? payload.getOrder() : 0L;
            OrderedKey.Builder orderedValue = OrderedKey.newBuilder()
                    .setOrder(order)
                    .setPayload(KeyBinaryPayload.newBuilder().setSize(payload.getValue().length).setPayload(ByteString.copyFrom(payload.getValue())).build());

            Value.Builder unorderedValueBuilder = Value.newBuilder()
                    .setValue(BinaryPayload.newBuilder().setSize(payload.getValue().length).setPayload(ByteString.copyFrom(upayload.getValue())).build());

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