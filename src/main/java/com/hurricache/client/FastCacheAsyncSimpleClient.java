package com.hurricache.client;

import com.hurricache.grpc.*;
import com.hurricache.grpc.HurriCacheGrpcServiceGrpc;
import com.hurricache.client.intf.HurriCacheClientInterface;
import com.hurricache.utils.CompletableFutureObserver;
import com.hurricache.utils.CompressionUtils;
import com.hurricache.utils.DecompressingObserver;
import com.hurricache.utils.StreamBatchObserver;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public class FastCacheAsyncSimpleClient implements HurriCacheClientInterface {

    private final HurriCacheGrpcServiceGrpc.HurriCacheGrpcServiceStub asyncStub;
    private final ManagedChannel channel;
    private final int defaultClientId;
    private final Duration defaultTimeout;
    private final String target;
    private static final long MAX_RPC_SIZE = 4 * 1024 * 1024 - 1024 * 1024/2;
    private enum AddType { TAIL, HEAD, POSITION }
    public FastCacheAsyncSimpleClient(String host, int port, int defaultClientId, Duration timeout) {
        this.channel = ManagedChannelBuilder.forAddress(host, port).directExecutor().usePlaintext().build();
        this.asyncStub = HurriCacheGrpcServiceGrpc.newStub(channel);
        this.defaultClientId = defaultClientId;
        this.defaultTimeout = timeout;
        target = host + ":" + port;
    }

    @Override
    public String toString() {
        return "FastCacheAsyncSimpleClient{" + "target='" + target + '\'' + '}';
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

    /**
     * Set TTL for key
     *
     * @param key      key - must exist
     * @param hint     key hint with strong and weak hashes
     * @param ttl      time to leave in milliseconds
     * @param clientId client id 0 - default client id
     * @param timeout  call timeout
     * @return CompletableFuture with response
     */
    @Override
    public CompletableFuture<Boolean> setTtl(byte[] key, KeyHint hint, long ttl, int clientId, Duration timeout) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        TtlRequest request = TtlRequest.newBuilder()
                .setKey(hint == null
                        ? KeyUtils.createKey(key, clientId)
                        : KeyUtils.createKey(key, hint, clientId))
                .setTtl(System.currentTimeMillis() + ttl)
                .build();
        getStub(timeout).setTtl(request, new CompletableFutureObserver<>(future, BoolResponse::getValue));
        return future;
    }


    /**
     * Get TTL for existing key
     *
     * @param key      key - must exist
     * @param hint     key hint with strong and weak hashes
     * @param clientId client id 0 - default client id
     * @param timeout  call timeout
     * @return CompletableFuture with response
     */
    @Override
    public CompletableFuture<Long> getTtl(byte[] key, KeyHint hint, int clientId, Duration timeout) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        GetRequest ttlRequest = GetRequest.newBuilder()
                .setKey(hint == null
                        ? KeyUtils.createKey(key, clientId)
                        : KeyUtils.createKey(key, hint, clientId))
                .build();
        getStub(timeout).getTtl(ttlRequest, new CompletableFutureObserver<>(future, item -> item.hasTtl() ? item.getTtl() - System.currentTimeMillis() : -1L));
        return future;
    }

    /**
     * Get and delete key from database
     *
     * @param key      key - must exist
     * @param hint     key hint with strong and weak hashes
     * @param clientId client id 0 - default client id
     * @param timeout  call timeout
     * @return CompletableFuture with response
     */
    @Override
    public CompletableFuture<byte[]> getAndDeleteValue(byte[] key, KeyHint hint, int clientId, Duration timeout) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        GetRequest request = GetRequest.newBuilder()
                .setKey(KeyUtils.createKey(key, getKeyHint(key, hint), clientId))
                .build();
        getStub(timeout).getAndDeleteValue(request, new DecompressingObserver(future));
        return future;
    }

    /**
     * Create key value pair
     *
     * @param key      key for value
     * @param value    value
     * @param clientId client id 0 - default client id
     * @param timeout  call timeout
     * @return CompletableFuture with response
     */
    @Override
    public CompletableFuture<KeyHint> createKeyValue(byte[] key, KeyHint hint, byte[] value, Duration ttl, int clientId, Duration timeout) {
        CompletableFuture<KeyHint> future = new CompletableFuture<>();
        Value.Builder valueBuilder = CompressionUtils.compressIfNeeded(value);
        if (ttl != null && !ttl.isZero()) {
            valueBuilder.setTtl(System.currentTimeMillis() + ttl.toMillis());
        }
        CreateRequest req = CreateRequest.newBuilder()
                .setKey(KeyUtils.createKey(key, getKeyHint(key, hint), clientId))
                .setValue(valueBuilder)
                .build();
        getStub(timeout).createKeyValue(req, new CompletableFutureObserver<>(future, KeyHintResponse::getKeyHint));
        return future;
    }

    /**
     * Get value for exact key. Applicable for RAW values only
     *
     * @param key      key for value
     * @param hint     key hint with strong and weak hashes
     * @param clientId client id 0 - default client id
     * @param timeout  call timeout
     * @return CompletableFuture with response
     */
    @Override
    public CompletableFuture<byte[]> getValue(byte[] key, KeyHint hint, int clientId, Duration timeout) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        getStub(timeout).getValue(buildGetReq(key, getKeyHint(key, hint), clientId), new DecompressingObserver(future));
        return future;
    }

    /**
     * Update key. Old value will be in response
     *
     * @param key      key
     * @param hint     key hint with strong and weak hashes
     * @param value    new value
     * @param clientId client id 0 - default client id
     * @param timeout  call timeout
     * @return CompletableFuture with response
     */
    @Override
    public CompletableFuture<byte[]> updateKeyValue(byte[] key,
                                                    KeyHint hint,
                                                    byte[] value,
                                                    Duration ttl,
                                                    int clientId,
                                                    Duration timeout) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        Value.Builder builderForValue = CompressionUtils.compressIfNeeded(value);
        if (ttl != null && !ttl.isZero()) {
            builderForValue.setTtl(System.currentTimeMillis() + ttl.toMillis());
        }
        UpdateRequest.Builder req = UpdateRequest.newBuilder()
                .setKey(buildKey(key, getKeyHint(key, hint), clientId))
                .setValue(builderForValue);

        getStub(timeout).updateValue(req.build(), new DecompressingObserver.Update(future));
        return future;
    }

    /**
     * Checks if key is exist in storage
     *
     * @param hint     key hint with strong and weak hashes
     * @param clientId client id 0 - default client id
     * @param timeout  call timeout
     * @return CompletableFuture with response
     */
    @Override
    public CompletableFuture<Boolean> existKey(byte[] key, KeyHint hint, int clientId, Duration timeout) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        getStub(timeout).existKey(buildGetReq(key, getKeyHint(key, hint), clientId),
                new CompletableFutureObserver<>(future, BoolResponse::getValue));
        return future;
    }

    /**
     * Removes exact key
     *
     * @param hint     key hint with strong and weak hashes
     * @param clientId client id 0 - default client id
     * @param timeout  call timeout
     * @return CompletableFuture with response
     */
    @Override
    public CompletableFuture<Boolean> remove(byte[] key, KeyHint hint, int clientId, Duration timeout) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        getStub(timeout)
                .remove(buildGetReq(key, getKeyHint(key, hint), clientId),
                        new CompletableFutureObserver<>(future, BoolResponse::getValue));
        return future;
    }

    /**
     * Creates queue with initial values
     *
     * @param key      key
     * @param clientId client id 0 - default client id
     * @param timeout  call timeout
     * @return CompletableFuture with response
     */
    @Override
    public CompletableFuture<KeyHint> createQueue(byte[] key,
                                                  List<byte[]> initialValue,
                                                  Duration ttl,
                                                  int clientId,
                                                  Duration timeout) {
        CreateQueueRequest.Builder builder = CreateQueueRequest.newBuilder();
        if (ttl != null && !ttl.isZero()) {
            builder.setTtl(System.currentTimeMillis() + ttl.toMillis());
        }
        Key protoKey = KeyUtils.createKey(key, clientId);
        builder.setKey(protoKey);

        long currentChunkSize = protoKey.getSerializedSize();
        int splitIndex = 0;

        if (initialValue != null) {
            for (byte[] bytes : initialValue) {
                Value.Builder compressedValue = CompressionUtils.compressIfNeeded(bytes);
                int elemSize = compressedValue.build().getSerializedSize();

                if (currentChunkSize + elemSize > MAX_RPC_SIZE) {
                    break;
                }

                builder.addValue(compressedValue);
                currentChunkSize += elemSize;
                splitIndex++;
            }
        }

        CompletableFuture<KeyHint> createFuture = new CompletableFuture<>();
        getStub(timeout).createQueue(builder.build(), new CompletableFutureObserver<>(createFuture, KeyHintResponse::getKeyHint));

        List<byte[]> remainingTail = initialValue != null
                                     ? initialValue.subList(splitIndex, initialValue.size())
                                     : List.of();

        return createFuture.thenCompose(keyHint -> {
            if (remainingTail.isEmpty()) {
                return CompletableFuture.completedFuture(keyHint);
            } else {
                repDelay();
                return sendTailInChunks(protoKey, keyHint, remainingTail, timeout);
            }
        });
    }

    @Override
    public CompletableFuture<KeyHint> createList(byte[] key,
                                                 List<byte[]> initialValue,
                                                 Duration ttl,
                                                 int clientId,
                                                 Duration timeout) {
        CreateListRequest.Builder builder = CreateListRequest.newBuilder();
        if (ttl != null && !ttl.isZero()) {
            builder.setTtl(System.currentTimeMillis() + ttl.toMillis());
        }
        Key protoKey = KeyUtils.createKey(key, clientId);
        builder.setAsArray(false).setKey(protoKey);

        long currentChunkSize = protoKey.getSerializedSize();
        int splitIndex = 0;

        if (initialValue != null) {
            for (byte[] bytes : initialValue) {
                Value.Builder compressedValue = CompressionUtils.compressIfNeeded(bytes);
                int elemSize = compressedValue.build().getSerializedSize();

                if (currentChunkSize + elemSize > MAX_RPC_SIZE) {
                    break;
                }

                builder.addValue(compressedValue);
                currentChunkSize += elemSize;
                splitIndex++;
            }
        }

        CompletableFuture<KeyHint> createFuture = new CompletableFuture<>();
        getStub(timeout).createList(builder.build(), new CompletableFutureObserver<>(createFuture, KeyHintResponse::getKeyHint));

        List<byte[]> remainingTail = initialValue != null
                                     ? initialValue.subList(splitIndex, initialValue.size())
                                     : List.of();

        return createFuture.thenCompose(keyHint -> {
            if (remainingTail.isEmpty()) {
                return CompletableFuture.completedFuture(keyHint);
            } else {
                repDelay();
                return sendTailInChunks(protoKey, keyHint, remainingTail, timeout);
            }
        });
    }

    @Override
    public CompletableFuture<KeyHint> createVector(byte[] key,
                                                   List<byte[]> initialValue,
                                                   Duration ttl,
                                                   int clientId,
                                                   Duration timeout) {
        CreateListRequest.Builder builder = CreateListRequest.newBuilder();
        if (ttl != null && !ttl.isZero()) {
            builder.setTtl(System.currentTimeMillis() + ttl.toMillis());
        }
        Key protoKey = KeyUtils.createKey(key,  clientId);
        builder.setAsArray(true).setKey(protoKey);
        long currentChunkSize = protoKey.getSerializedSize();
        int splitIndex = getSplitIndex(initialValue, currentChunkSize, builder, 0);

        // 1. Собираем первый батч для createList (до 10 МБ)


        CompletableFuture<KeyHint> createFuture = new CompletableFuture<>();
        getStub(timeout).createList(builder.build(), new CompletableFutureObserver<>(createFuture,KeyHintResponse::getKeyHint));

        List<byte[]> remainingTail = initialValue.subList(splitIndex, initialValue.size());

        // 2. Связываем фьючерсы: берем Hint из первого ответа и фиксируем его для всего хвоста
        return createFuture.thenCompose(keyHint -> {
            if (remainingTail.isEmpty()) {
                return CompletableFuture.completedFuture(keyHint);
            } else {
                repDelay();
                return sendTailInChunks(protoKey,keyHint, remainingTail, timeout);
            }
        });
    }

    private static void repDelay() {
        try {Thread.sleep(150);} catch (InterruptedException e) {}//Нудно спать чтобы подождать репликации
    }

    private static int getSplitIndex(List<byte[]> initialValue,
                                     long currentChunkSize,
                                     CreateListRequest.Builder builder,
                                     int splitIndex) {
        for (byte[] bytes : initialValue) {
            Value.Builder compressedValue = CompressionUtils.compressIfNeeded(bytes);
            int elemSize = compressedValue.build().getSerializedSize();

            if (currentChunkSize + elemSize > MAX_RPC_SIZE) {
                break;
            }

            builder.addValue(compressedValue);
            currentChunkSize += elemSize;
            splitIndex++;
        }
        return splitIndex;
    }

    @Override
    public CompletableFuture<byte[]> getAndRemoveFront(byte[] key, KeyHint hint, int clientId, Duration timeout) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        getStub(timeout).getAndRemoveFront(GetRequest.newBuilder().setKey(KeyUtils.createKey(key, getKeyHint(key, hint), clientId)).build(),
                new DecompressingObserver(future));
        return future;
    }

    @Override
    public CompletableFuture<byte[]> getFront(byte[] key, KeyHint hint, int clientId, Duration timeout) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        getStub(timeout).getHead(buildGetReq(key, getKeyHint(key, hint), clientId), new DecompressingObserver(future));
        return future;
    }

    @Override
    public CompletableFuture<Boolean> addElementToTail(byte[] key,
                                                       KeyHint hint,
                                                       List<byte[]> data,
                                                       int clientId,
                                                       Duration timeout) {
        if (data == null || data.isEmpty()) {
            return CompletableFuture.completedFuture(true);
        }
        Key protoKey = buildKey(key, getKeyHint(key, hint), clientId);
        return sendAddRequestInChunks(protoKey, data, -1, AddType.TAIL, timeout);
    }

    @Override
    public CompletableFuture<byte[]> getElementAtPosition(byte[] key,
                                                          KeyHint hint,
                                                          int pos,
                                                          int clientId,
                                                          Duration timeout) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        KeyPositionRequest req = KeyPositionRequest.newBuilder()
                .setKey(buildKey(key, getKeyHint(key, hint), clientId))
                .setPos(pos)
                .build();
        getStub(timeout).getElementAtPosition(req, new DecompressingObserver(future));
        return future;
    }

    @Override
    public CompletableFuture<List<byte[]>> streamList(byte[] key, KeyHint hint, int clientId, Duration timeout) {
        CompletableFuture<List<byte[]>> future = new CompletableFuture<>();
        getStub(timeout).getList(buildGetReq(key, getKeyHint(key, hint), clientId), new StreamBatchObserver(future));
        return future;
    }

    @Override
    public CompletableFuture<LockStatus> lockObject(byte[] key,
                                                    KeyHint hint,
                                                    LockType type,
                                                    int clientId,
                                                    Duration duration,
                                                    Duration timeout) {
        CompletableFuture<LockStatus> future = new CompletableFuture<>();
        LockRequest req = LockRequest.newBuilder()
                .setKey(buildKey(key, getKeyHint(key, hint), clientId))
                .setLockType(type)
                .setClientId(clientId)
                .setLockDuration((int) duration.toSeconds())
                .build();
        getStub(timeout).lockObject(req, new CompletableFutureObserver<>(future, LockResponse::getResult));
        return future;
    }

    @Override
    public CompletableFuture<LockStatus> unlockObject(byte[] key, KeyHint hint, int clientId, Duration timeout) {
        CompletableFuture<LockStatus> future = new CompletableFuture<>();
        UnLockRequest req = UnLockRequest.newBuilder()
                .setKey(buildKey(key, getKeyHint(key, hint), clientId))
                .setClientId(clientId)
                .build();
        getStub(timeout).unlockObject(req, new CompletableFutureObserver<>(future, UnlockResponse::getResult));
        return future;
    }

    @Override
    public CompletableFuture<List<byte[]>> streamElementInRange(byte[] key,
                                                                KeyHint hint,
                                                                boolean isArray,
                                                                int start,
                                                                int end,
                                                                int clientId,
                                                                Duration timeout) {
        KeyPositionRequest request = KeyPositionRequest.newBuilder()
                .setKey(KeyUtils.createKey(key, getKeyHint(key, hint), clientId))
                .setPos(start)
                .setEnd(end)
                .build();
        CompletableFuture<List<byte[]>> future = new CompletableFuture<>();

        if (isArray) {
            getStub(timeout).getElementInRangeVector(request, new StreamBatchObserver(future));
        } else {
            getStub(timeout).getElementInRange(request, new StreamBatchObserver(future));
        }
        return future;
    }

    @Override
    public CompletableFuture<List<byte[]>> streamVector(byte[] key, KeyHint hint, int clientId, Duration timeout) {
        GetRequest request = GetRequest.newBuilder().setKey(KeyUtils.createKey(key, getKeyHint(key, hint), clientId)).build();
        CompletableFuture<List<byte[]>> future = new CompletableFuture<>();
        getStub(timeout).getVector(request, new StreamBatchObserver(future));
        return future;
    }


    @Override
    public CompletableFuture<byte[]> getAndRemoveElementAtPosition(byte[] key,
                                                                   KeyHint hint,
                                                                   int pos,
                                                                   int clientId,
                                                                   Duration timeout) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        KeyPositionRequest request = KeyPositionRequest.newBuilder()
                .setKey(KeyUtils.createKey(key, getKeyHint(key, hint), clientId))
                .setPos(pos)
                .build();
        getStub(timeout).getAndRemoveElementAtPosition(request, new DecompressingObserver(future));
        return future;
    }

    @Override
    public CompletableFuture<Boolean> addElementToHead(byte[] key,
                                                       KeyHint hint,
                                                       List<byte[]> data,
                                                       int clientId,
                                                       Duration timeout) {
        if (data == null || data.isEmpty()) {
            return CompletableFuture.completedFuture(true);
        }
        Key protoKey = KeyUtils.createKey(key, getKeyHint(key, hint), clientId);
        // Для сохранения исходного порядка при добавлении в HEAD чанками,
        // мы шлем пакеты по очереди.
        return sendAddRequestInChunks(protoKey, data, -1, AddType.HEAD, timeout);
    }

    @Override
    public CompletableFuture<Boolean> addElementToPosition(byte[] key,
                                                           KeyHint hint,
                                                           List<byte[]> data,
                                                           int pos,
                                                           int clientId,
                                                           Duration timeout) {
        if (data == null || data.isEmpty()) {
            return CompletableFuture.completedFuture(true);
        }
        Key protoKey = buildKey(key, getKeyHint(key, hint), clientId);
        return sendAddRequestInChunks(protoKey, data, pos, AddType.POSITION, timeout);
    }

    @Override
    public CompletableFuture<Boolean> removeTail(byte[] key, KeyHint hint, int clientId, Duration timeout) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        getStub(timeout).removeTail(GetRequest.newBuilder().setKey(KeyUtils.createKey(key, getKeyHint(key, hint), clientId)).build(),
                new CompletableFutureObserver<>(future, BoolResponse::getValue));
        return future;
    }

    @Override
    public CompletableFuture<Boolean> removeHead(byte[] key, KeyHint hint, int clientId, Duration timeout) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        getStub(timeout).removeHead(GetRequest.newBuilder().setKey(KeyUtils.createKey(key, getKeyHint(key, hint), clientId)).build(),
                new CompletableFutureObserver<>(future, BoolResponse::getValue));
        return future;
    }

    @Override
    public CompletableFuture<Boolean> removeElementAtPosition(byte[] key,
                                                              KeyHint hint,
                                                              int pos,
                                                              int endPos,
                                                              int clientId,
                                                              Duration timeout) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        KeyPositionRequest.Builder builder = KeyPositionRequest.newBuilder()
                .setKey(KeyUtils.createKey(key, getKeyHint(key, hint), clientId))
                .setPos(pos).setEnd(endPos);
        getStub(timeout).removeElementAtPosition(builder.build(), new CompletableFutureObserver<>(future, BoolResponse::getValue));
        return future;
    }

    // --- LIFECYCLE ---
    @Override
    public void shutdown() {
        channel.shutdown();
    }

    @Override
    public CompletableFuture<byte[]> getHead(byte[] key, KeyHint hint, int clientId, Duration timeout) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        getStub(timeout).getHead(GetRequest.newBuilder().setKey(KeyUtils.createKey(key, getKeyHint(key, hint), clientId)).build(),
                new DecompressingObserver(future));
        return future;
    }

    @Override
    public CompletableFuture<byte[]> getTail(byte[] key, KeyHint hint, int clientId, Duration timeout) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        getStub(timeout).getTail(GetRequest.newBuilder().setKey(KeyUtils.createKey(key, getKeyHint(key, hint), clientId)).build(),
                new DecompressingObserver(future));
        return future;
    }

    @Override
    public CompletableFuture<byte[]> getAndRemoveTail(byte[] key, KeyHint hint, int clientId, Duration timeout) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        getStub(timeout).getAndRemoveTail(GetRequest.newBuilder().setKey(KeyUtils.createKey(key, getKeyHint(key, hint), clientId)).build(),
                new DecompressingObserver(future));
        return future;
    }

    private Key buildKey(byte[] key, KeyHint hint, int clientId) {
        int cid = (clientId != 0) ? clientId : defaultClientId;
        return (hint == null)
                ? KeyUtils.createKey(key, cid)
                : KeyUtils.createKey(key, hint, cid);
    }

    private GetRequest buildGetReq(byte[] key, KeyHint hint, Integer clientId) {
        return GetRequest.newBuilder().setKey(buildKey(key, getKeyHint(key, hint), clientId)).build();
    }

    private HurriCacheGrpcServiceGrpc.HurriCacheGrpcServiceStub getStub(Duration timeout) {
        if (timeout == null || timeout.isZero()) {
            return asyncStub;
        }
        return asyncStub.withDeadlineAfter(timeout);
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

    private CompletableFuture<KeyHint> sendTailInChunks(Key protoKey, KeyHint originalHint,
                                                        List<byte[]> tail,
                                                        Duration timeout) {
        long currentChunkSize = originalHint.getSerializedSize() + 32;

        AddToRequest.Builder builder = AddToRequest.newBuilder()
                .setKey(protoKey.toBuilder().setKeyHint(originalHint).build());

        int splitIndex = 0;
        for (byte[] bytes : tail) {
            Value.Builder compressedValue = CompressionUtils.compressIfNeeded(bytes);
            int elemSize = compressedValue.build().getSerializedSize();

            if (currentChunkSize + elemSize > MAX_RPC_SIZE) {
                break;
            }

            builder.addValue(compressedValue);
            currentChunkSize += elemSize;
            splitIndex++;
        }

        CompletableFuture<BoolResponse> tailFuture = new CompletableFuture<>();
        getStub(timeout).addElementToTail(builder.build(), new CompletableFutureObserver<>(tailFuture));

        List<byte[]> nextTail = tail.subList(splitIndex, tail.size());

        // Рекурсивно склеиваем цепочку. В самом конце возвращаем именно оригинальный hint
        return tailFuture.thenCompose(response -> {
            if (nextTail.isEmpty() && response.getValue()) {
                return CompletableFuture.completedFuture(originalHint); // Возвращаем первый hint наружу
            } else {
                // Пробрасываем оригинальный hint на следующий шаг рекурсии
                return sendTailInChunks(protoKey, originalHint, nextTail, timeout);
            }
        });
    }

    private CompletableFuture<Boolean> sendAddRequestInChunks(Key protoKey,
                                                              List<byte[]> data,
                                                              int currentPos,
                                                              AddType type,
                                                              Duration timeout) {
        long currentChunkSize = protoKey.getSerializedSize() + 32;
        AddToRequest.Builder builder = AddToRequest.newBuilder().setKey(protoKey);

        if (type == AddType.POSITION) {
            builder.setPos(currentPos);
        }

        int splitIndex = 0;
        for (byte[] datum : data) {
            Value.Builder compressedValue = (type == AddType.TAIL)
                                            ? CompressionUtils.compressIfNeeded(datum)
                                            : KeyUtils.createValue(datum); // Исходная обертка для HEAD/POSITION

            int elemSize = compressedValue.build().getSerializedSize();
            if (currentChunkSize + elemSize > MAX_RPC_SIZE) {
                break;
            }
            builder.addValue(compressedValue);
            currentChunkSize += elemSize;
            splitIndex++;
        }

        CompletableFuture<Boolean> chunkFuture = new CompletableFuture<>();

        // Вызываем соответствующую RPC-процедуру
        switch (type) {
            case TAIL:
                getStub(timeout).addElementToTail(builder.build(), new CompletableFutureObserver<>(chunkFuture, BoolResponse::getValue));
                break;
            case HEAD:
                getStub(timeout).addElementToHead(builder.build(), new CompletableFutureObserver<>(chunkFuture, BoolResponse::getValue));
                break;
            case POSITION:
                getStub(timeout).addElementToPosition(builder.build(), new CompletableFutureObserver<>(chunkFuture, BoolResponse::getValue));
                break;
        }

        List<byte[]> nextTail = data.subList(splitIndex, data.size());

        int finalSplitIndex = splitIndex;
        return chunkFuture.thenCompose(success -> {
            if (!success) {
                return CompletableFuture.completedFuture(false);
            }
            if (nextTail.isEmpty()) {
                return CompletableFuture.completedFuture(true);
            }

            // Сдвигаем позицию для следующего куска (применяется только к инкрементальному добавлению по индексу)
            int nextPos = (type == AddType.POSITION) ? currentPos + finalSplitIndex
                                                     : currentPos;

            return sendAddRequestInChunks(protoKey, nextTail, nextPos, type, timeout);
        });
    }


    // =========================================================================
    // ATOMIC OPERATIONS IMPLEMENTATION (WITH ATOMIC_VALUE)
    // =========================================================================

    private AtomicCreate buildAtomicCreateReq(byte[] key, KeyHint hint, long value, Duration ttl, int clientId) {
        AtomicValue atomicVal = AtomicValue.newBuilder()
                .setVal(value)
                .build();

        AtomicCreate.Builder builder = AtomicCreate.newBuilder()
                .setKey(buildKey(key, getKeyHint(key, hint), clientId))
                .setVal(atomicVal); // Передаем структуру AtomicValue напрямую

        if (ttl != null && !ttl.isZero()) {
            builder.setTtl(System.currentTimeMillis() + ttl.toMillis());
        }
        return builder.build();
    }

    @Override
    public CompletableFuture<KeyHint> atomicCreate(byte[] key, KeyHint hint, long value, Duration ttl, int clientId, Duration timeout) {
        CompletableFuture<KeyHint> future = new CompletableFuture<>();
        AtomicCreate req = buildAtomicCreateReq(key, hint, value, ttl, clientId);
        getStub(timeout).atomicCreate(req, new CompletableFutureObserver<>(future, KeyHintResponse::getKeyHint));
        return future;
    }

    @Override
    public CompletableFuture<KeyHint> atomicStore(byte[] key, KeyHint hint, long value, Duration ttl, int clientId, Duration timeout) {
        CompletableFuture<KeyHint> future = new CompletableFuture<>();
        AtomicCreate req = buildAtomicCreateReq(key, hint, value, ttl, clientId);
        getStub(timeout).atomicStore(req, new CompletableFutureObserver<>(future, KeyHintResponse::getKeyHint));
        return future;
    }

    @Override
    public CompletableFuture<Long> atomicExchange(byte[] key, KeyHint hint, long value, Duration ttl, int clientId, Duration timeout) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        AtomicCreate req = buildAtomicCreateReq(key, hint, value, ttl, clientId);
        // Сервер возвращает AtomicValueResponse или сам AtomicValue, вытаскиваем через getVal()
        getStub(timeout).atomicExchange(req, new CompletableFutureObserver<>(future, AtomicValue::getVal));
        return future;
    }

    @Override
    public CompletableFuture<Long> atomicAdd(byte[] key, KeyHint hint, long delta, Duration ttl, int clientId, Duration timeout) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        AtomicCreate req = buildAtomicCreateReq(key, hint, delta, ttl, clientId);
        getStub(timeout).atomicAdd(req, new CompletableFutureObserver<>(future, AtomicValue::getVal));
        return future;
    }

    @Override
    public CompletableFuture<Long> atomicSub(byte[] key, KeyHint hint, long delta, Duration ttl, int clientId, Duration timeout) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        AtomicCreate req = buildAtomicCreateReq(key, hint, delta, ttl, clientId);
        getStub(timeout).atomicSub(req, new CompletableFutureObserver<>(future, AtomicValue::getVal));
        return future;
    }

    @Override
    public CompletableFuture<Long> atomicAnd(byte[] key, KeyHint hint, long mask, Duration ttl, int clientId, Duration timeout) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        AtomicCreate req = buildAtomicCreateReq(key, hint, mask, ttl, clientId);
        getStub(timeout).atomicAnd(req, new CompletableFutureObserver<>(future, AtomicValue::getVal));
        return future;
    }

    @Override
    public CompletableFuture<Long> atomicOr(byte[] key, KeyHint hint, long mask, Duration ttl, int clientId, Duration timeout) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        AtomicCreate req = buildAtomicCreateReq(key, hint, mask, ttl, clientId);
        getStub(timeout).atomicOr(req, new CompletableFutureObserver<>(future, AtomicValue::getVal));
        return future;
    }

    @Override
    public CompletableFuture<Long> atomicXor(byte[] key, KeyHint hint, long mask, Duration ttl, int clientId, Duration timeout) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        AtomicCreate req = buildAtomicCreateReq(key, hint, mask, ttl, clientId);
        getStub(timeout).atomicXor(req, new CompletableFutureObserver<>(future, AtomicValue::getVal));
        return future;
    }

    @Override
    public CompletableFuture<AtomicCasRes> atomicCompareAndSet(byte[] key, KeyHint hint, long expectedValue, long newValue, Duration ttl, int clientId, Duration timeout) {
        CompletableFuture<AtomicCasRes> future = new CompletableFuture<>();

        AtomicValue expectedAtomic = AtomicValue.newBuilder().setVal(expectedValue).build();
        AtomicValue newAtomic = AtomicValue.newBuilder().setVal(newValue).build();

        AtomicCas.Builder builder = AtomicCas.newBuilder()
                .setKey(buildKey(key, getKeyHint(key, hint), clientId))
                .setExpected(expectedAtomic)
                .setToSet(newAtomic);

        if (ttl != null && !ttl.isZero()) {
            builder.setTtl(System.currentTimeMillis() + ttl.toMillis());
        }

        getStub(timeout).atomicCompareAndSet(builder.build(), new CompletableFutureObserver<>(future));
        return future;
    }
    @Override
    public CompletableFuture<Boolean> addElementToPositionBefore(byte[] key, KeyHint hint, List<byte[]> data, byte[] pos, int clientId, Duration timeout){
        if (data == null || data.isEmpty()) {
            return CompletableFuture.completedFuture(true);
        }
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        AddToValRequest.Builder builder = AddToValRequest.newBuilder();
        builder.setKey(buildKey(key, getKeyHint(key, hint), clientId));
        builder.setIsBefore(true);
        data.forEach(element -> builder.addValue(KeyUtils.createValue(element)));
        builder.setPos(KeyUtils.createValue(pos));
        getStub(timeout).addElementToPositionByValue(builder.build(),new CompletableFutureObserver<>(future,BoolResponse::getValue));
        return future;
    }
    public CompletableFuture<Boolean> addElementToPositionAfter(byte[] key, KeyHint hint, List<byte[]> data, byte[] pos, int clientId, Duration timeout){
        if (data == null || data.isEmpty()) {
            return CompletableFuture.completedFuture(true);
        }
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        AddToValRequest.Builder builder = AddToValRequest.newBuilder();
        builder.setKey(buildKey(key, getKeyHint(key, hint), clientId));
        builder.setIsBefore(false);
        data.forEach(element -> builder.addValue(KeyUtils.createValue(element)));
        builder.setPos(KeyUtils.createValue(pos));
        getStub(timeout).addElementToPositionByValue(builder.build(),new CompletableFutureObserver<>(future,BoolResponse::getValue));
        return future;
    }

}