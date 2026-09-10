package com.hurricache.client;

import com.hurricache.client.intf.HurriCacheClientInterface;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.OrderedPayload;
import com.hurricache.client.intf.Payload;
import com.hurricache.grpc.AtomicCasRes;
import com.hurricache.grpc.ContainerType;
import com.hurricache.grpc.HurriCacheGrpcServiceGrpc;
import com.hurricache.grpc.Key;
import com.hurricache.grpc.LockStatus;
import com.hurricache.grpc.LockType;
import com.hurricache.grpc.OrderedKey;
import com.hurricache.grpc.OrderedValue;
import com.hurricache.grpc.Value;
import com.hurricache.utils.CompressionUtils;
import io.grpc.ManagedChannelBuilder;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class FastCacheAsyncStandaloneClient implements HurriCacheClientInterface {

    private final FastCacheAsyncSimpleClient delegate;

    public FastCacheAsyncStandaloneClient(FastCacheAsyncSimpleClient delegate) {
        this.delegate = delegate;
    }

    public FastCacheAsyncStandaloneClient(String host,
                                      int port,
                                      int defaultClientId,
                                      Duration timeout
                                      ) {
        delegate = new FastCacheAsyncSimpleClient(host, port, defaultClientId,timeout, getDefaultCompressionThreshold());
    }

    // =========================================================================
    // DELEGATE INTERFACE BASE METHODS
    // =========================================================================

    @Override
    public String getTarget() {
        return delegate.getTarget();
    }

    @Override
    public int getDefaultClientId() {
        return delegate.getDefaultClientId();
    }

    @Override
    public Duration getDefaultTimeout() {
        return delegate.getDefaultTimeout();
    }



    @Override
    public void shutdown() {
        delegate.shutdown();
    }

    // =========================================================================
    // TTL MANAGEMENT
    // =========================================================================

    @Override
    public CompletableFuture<Boolean> setTtl(byte[] key, KeyHintData hint, long ttl, int clientId, Duration timeout) {
        return delegate.setTtl(key, hint, ttl, clientId, timeout);
    }

    @Override
    public CompletableFuture<Long> getTtl(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return delegate.getTtl(key, hint, clientId, timeout);
    }

    // =========================================================================
    // KEY-VALUE OPERATIONS
    // =========================================================================

    @Override
    public CompletableFuture<byte[]> getAndDeleteValue(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return delegate.getAndDeleteValue(key, hint, clientId, timeout);
    }

    @Override
    public CompletableFuture<KeyHintData> createKeyValue(byte[] key, KeyHintData hint, byte[] value, Duration ttl, int clientId, Duration timeout) {
        return delegate.createKeyValue(key, hint, value, ttl, clientId, timeout);
    }

    @Override
    public CompletableFuture<byte[]> getValue(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return delegate.getValue(key, hint, clientId, timeout);
    }

    @Override
    public CompletableFuture<byte[]> updateKeyValue(byte[] key, KeyHintData hint, byte[] value, Duration ttl, int clientId, Duration timeout) {
        return delegate.updateKeyValue(key, hint, value, ttl, clientId, timeout);
    }

    @Override
    public CompletableFuture<Boolean> existKey(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return delegate.existKey(key, hint, clientId, timeout);
    }

    @Override
    public CompletableFuture<Boolean> remove(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return delegate.remove(key, hint, clientId, timeout);
    }

    // =========================================================================
    // CHUNKED CONTAINER CREATION
    // =========================================================================

    @Override
    public CompletableFuture<KeyHintData> createQueue(byte[] key, KeyHintData keyHint, List<Payload> initialValue, Duration ttl, int clientId, Duration timeout) {
        return createUnorderedContainerChunked(key, keyHint, initialValue, ContainerType.QUEUE, ttl, clientId, timeout);
    }

    @Override
    public CompletableFuture<KeyHintData> createList(byte[] key, KeyHintData keyHint, List<Payload> initialValue, Duration ttl, int clientId, Duration timeout) {
        return createUnorderedContainerChunked(key, keyHint, initialValue, ContainerType.LIST, ttl, clientId, timeout);
    }

    @Override
    public CompletableFuture<KeyHintData> createVector(byte[] key, KeyHintData keyHint, List<Payload> initialValue, Duration ttl, int clientId, Duration timeout) {
        return createUnorderedContainerChunked(key, keyHint, initialValue, ContainerType.VECTOR, ttl, clientId, timeout);
    }

    @Override
    public CompletableFuture<KeyHintData> createSet(byte[] key, KeyHintData keyHint, List<Payload> initialValue, Duration ttl, int clientId, Duration timeout) {
        return createUnorderedContainerChunked(key, keyHint, initialValue, ContainerType.SET, ttl, clientId, timeout);
    }

    private CompletableFuture<KeyHintData> createUnorderedContainerChunked(byte[] key,
                                                                           KeyHintData keyHint,
                                                                           List<Payload> initialValue,
                                                                           ContainerType type,
                                                                           Duration ttl,
                                                                           int clientId,
                                                                           Duration timeout) {
        if (initialValue == null || initialValue.isEmpty()) {
            return switch (type) {
                case QUEUE -> delegate.createQueue(key, keyHint, List.of(), ttl, clientId, timeout);
                case VECTOR -> delegate.createVector(key, keyHint, List.of(), ttl, clientId, timeout);
                case SET -> delegate.createSet(key, keyHint, List.of(), ttl, clientId, timeout);
                default ->  delegate.createList(key, keyHint, List.of(), ttl, clientId, timeout);
            };

        }

        Key protoKey = KeyValueUtils.createUnorderedKey(key, keyHint, clientId, getDefaultCompressionThreshold()).build();
        long currentChunkSize = protoKey.getSerializedSize();
        int splitIndex = 0;

        List<Payload> firstChunk = new ArrayList<>();
        for (Payload payload : initialValue) {
            Value.Builder compressedValue = CompressionUtils.compressIfNeeded(payload.getValue(), null);
            int elemSize = compressedValue.build().getSerializedSize();

            if (!firstChunk.isEmpty() && (currentChunkSize + elemSize > MAX_RPC_SIZE)) {
                break;
            }

            firstChunk.add(payload);
            currentChunkSize += elemSize;
            splitIndex++;
        }

        CompletableFuture<KeyHintData> createFuture;
        switch (type) {
            case QUEUE -> createFuture = delegate.createQueue(key, keyHint, firstChunk, ttl, clientId, timeout);
            case VECTOR -> createFuture = delegate.createVector(key, keyHint, firstChunk, ttl, clientId, timeout);
            case SET -> createFuture = delegate.createSet(key, keyHint, firstChunk, ttl, clientId, timeout);
            default -> createFuture = delegate.createList(key, keyHint, firstChunk, ttl, clientId, timeout);
        }

        List<Payload> remainingTail = initialValue.subList(splitIndex, initialValue.size());
        if (remainingTail.isEmpty()) {
            return createFuture;
        }

        return createFuture.thenCompose(returnedHint ->
                                                sendRemainingUnorderedChunks(key, returnedHint, remainingTail, type, clientId, timeout)
                                                        .thenApply(ignored -> returnedHint)
        );
    }

    private CompletableFuture<Void> sendRemainingUnorderedChunks(byte[] key,
                                                                 KeyHintData hint,
                                                                 List<Payload> tail,
                                                                 ContainerType type,
                                                                 int clientId,
                                                                 Duration timeout) {
        if (tail.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        Key protoKey = KeyValueUtils.createUnorderedKey(key, hint, clientId, getDefaultCompressionThreshold()).build();
        long currentChunkSize = protoKey.getSerializedSize() + 32;

        List<Payload> currentChunk = new ArrayList<>();
        int splitIndex = 0;

        for (Payload payload : tail) {
            Value.Builder compressedValue = CompressionUtils.compressIfNeeded(payload.getValue(), null);
            int elemSize = compressedValue.build().getSerializedSize();

            if (!currentChunk.isEmpty() && (currentChunkSize + elemSize > MAX_RPC_SIZE)) {
                break;
            }

            currentChunk.add(payload);
            currentChunkSize += elemSize;
            splitIndex++;
        }

        CompletableFuture<?> sendFuture;
        if (type == ContainerType.SET) {
            sendFuture = delegate.addElement(key, hint, currentChunk, clientId, timeout);
        } else {
            sendFuture = delegate.addElementToTail(key, hint, currentChunk, clientId, timeout);
        }

        List<Payload> nextTail = tail.subList(splitIndex, tail.size());
        return sendFuture.thenCompose(res -> sendRemainingUnorderedChunks(key, hint, nextTail, type, clientId, timeout));
    }

    @Override
    public CompletableFuture<KeyHintData> createOrderedSet(byte[] key, KeyHintData keyHint, List<OrderedPayload> initialValue, Duration ttl, int clientId, Duration timeout) {
        if (initialValue == null || initialValue.isEmpty()) {
            return delegate.createOrderedSet(key, keyHint, initialValue, ttl, clientId, timeout);
        }

        Key protoKey = KeyValueUtils.createUnorderedKey(key, keyHint, clientId, getDefaultCompressionThreshold()).build();
        long currentChunkSize = protoKey.getSerializedSize();
        int splitIndex = 0;

        List<OrderedPayload> firstChunk = new ArrayList<>();
        for (OrderedPayload payload : initialValue) {
            long order = payload.getOrder() != null ? payload.getOrder() : 0L;
            OrderedValue orderedValue = KeyValueUtils.createOrderedValue(payload.getValue(), order, ttl, getDefaultCompressionThreshold()).build();
            int elemSize = orderedValue.getSerializedSize();

            if (!firstChunk.isEmpty() && (currentChunkSize + elemSize > MAX_RPC_SIZE)) {
                break;
            }

            firstChunk.add(payload);
            currentChunkSize += elemSize;
            splitIndex++;
        }

        CompletableFuture<KeyHintData> createFuture = delegate.createOrderedSet(key, keyHint, firstChunk, ttl, clientId, timeout);

        List<OrderedPayload> remainingTail = initialValue.subList(splitIndex, initialValue.size());
        if (remainingTail.isEmpty()) {
            return createFuture;
        }

        return createFuture.thenCompose(returnedHint ->
                                                sendRemainingOrderedSetChunks(key, returnedHint, remainingTail, clientId, timeout)
                                                        .thenApply(ignored -> returnedHint)
        );
    }

    private CompletableFuture<Void> sendRemainingOrderedSetChunks(byte[] key,
                                                                  KeyHintData hint,
                                                                  List<OrderedPayload> tail,
                                                                  int clientId,
                                                                  Duration timeout) {
        if (tail.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        Key protoKey = KeyValueUtils.createUnorderedKey(key, hint, clientId, getDefaultCompressionThreshold()).build();
        long currentChunkSize = protoKey.getSerializedSize() + 32;

        List<OrderedPayload> currentChunk = new ArrayList<>();
        int splitIndex = 0;

        for (OrderedPayload payload : tail) {
            long order = payload.getOrder() != null ? payload.getOrder() : 0L;
            OrderedValue orderedValue = KeyValueUtils.createOrderedValue(payload.getValue(), order, null, getDefaultCompressionThreshold()).build();
            int elemSize = orderedValue.getSerializedSize();

            if (!currentChunk.isEmpty() && (currentChunkSize + elemSize > MAX_RPC_SIZE)) {
                break;
            }

            currentChunk.add(payload);
            currentChunkSize += elemSize;
            splitIndex++;
        }

        CompletableFuture<Integer> sendFuture = delegate.addElementWithWeight(key, hint, currentChunk, clientId, timeout);

        List<OrderedPayload> nextTail = tail.subList(splitIndex, tail.size());
        return sendFuture.thenCompose(res -> sendRemainingOrderedSetChunks(key, hint, nextTail, clientId, timeout));
    }

    @Override
    public CompletableFuture<KeyHintData> createMap(byte[] key, KeyHintData keyHint, Map<Payload, Payload> initialValue, Duration ttl, int clientId, Duration timeout) {
        if (initialValue == null || initialValue.isEmpty()) {
            return delegate.createMap(key, keyHint, initialValue, ttl, clientId, timeout);
        }

        Key protoKey = KeyValueUtils.createUnorderedKey(key, keyHint, clientId, getDefaultCompressionThreshold()).build();
        long currentChunkSize = protoKey.getSerializedSize();

        List<Payload> remainingKeys = new ArrayList<>();
        List<Payload> remainingValues = new ArrayList<>();
        Map<Payload, Payload> firstChunk = new java.util.LinkedHashMap<>();

        for (Map.Entry<Payload, Payload> entry : initialValue.entrySet()) {
            Key kVal = KeyValueUtils.createUnorderedKey(entry.getKey().getValue(), clientId, null).build();
            Value vVal = KeyValueUtils.createUnorderedValue(entry.getValue().getValue(), ttl, null).build();
            int pairSize = kVal.getSerializedSize() + vVal.getSerializedSize();

            if (remainingKeys.isEmpty() && (currentChunkSize + pairSize <= MAX_RPC_SIZE)) {
                firstChunk.put(entry.getKey(), entry.getValue());
                currentChunkSize += pairSize;
            } else {
                remainingKeys.add(entry.getKey());
                remainingValues.add(entry.getValue());
            }
        }

        CompletableFuture<KeyHintData> createFuture = delegate.createMap(key, keyHint, firstChunk, ttl, clientId, timeout);

        if (remainingKeys.isEmpty()) {
            return createFuture;
        }

        return createFuture.thenCompose(returnedHint ->
                                                sendRemainingMapChunks(key, returnedHint, remainingKeys, remainingValues, clientId, ttl, timeout)
                                                        .thenApply(ignored -> returnedHint)
        );
    }

    private CompletableFuture<Void> sendRemainingMapChunks(byte[] key,
                                                           KeyHintData hint,
                                                           List<Payload> keys,
                                                           List<Payload> values,
                                                           int clientId,
                                                           Duration ttl,
                                                           Duration timeout) {
        List<CompletableFuture<Integer>> chunkFutures = new ArrayList<>();

        List<Payload> currentChunkKeys = new ArrayList<>();
        List<Payload> currentChunkValues = new ArrayList<>();
        long currentChunkSize = 0;

        for (int i = 0; i < keys.size(); i++) {
            Payload k = keys.get(i);
            Payload v = values.get(i);

            Key kVal = KeyValueUtils.createUnorderedKey(k.getValue(), clientId, null).build();
            Value vVal = KeyValueUtils.createUnorderedValue(v.getValue(), ttl, null).build();
            int pairSize = kVal.getSerializedSize() + vVal.getSerializedSize();

            if (!currentChunkKeys.isEmpty() && (currentChunkSize + pairSize > MAX_RPC_SIZE)) {
                chunkFutures.add(delegate.addElementHashMap(key, hint, new ArrayList<>(currentChunkKeys), new ArrayList<>(currentChunkValues), clientId, timeout));
                currentChunkKeys.clear();
                currentChunkValues.clear();
                currentChunkSize = 0;
            }

            currentChunkKeys.add(k);
            currentChunkValues.add(v);
            currentChunkSize += pairSize;
        }

        if (!currentChunkKeys.isEmpty()) {
            chunkFutures.add(delegate.addElementHashMap(key, hint, currentChunkKeys, currentChunkValues, clientId, timeout));
        }

        return CompletableFuture.allOf(chunkFutures.toArray(new CompletableFuture[0]));
    }

    @Override
    public CompletableFuture<KeyHintData> createOrderedMap(byte[] key, KeyHintData keyHint, Map<OrderedPayload, Payload> initialValue, Duration ttl, int clientId, Duration timeout) {
        if (initialValue == null || initialValue.isEmpty()) {
            return delegate.createOrderedMap(key, keyHint, initialValue, ttl, clientId, timeout);
        }

        Key protoKey = KeyValueUtils.createUnorderedKey(key, keyHint, clientId, getDefaultCompressionThreshold()).build();
        long currentChunkSize = protoKey.getSerializedSize();

        List<OrderedPayload> remainingKeys = new ArrayList<>();
        List<Payload> remainingValues = new ArrayList<>();
        Map<OrderedPayload, Payload> firstChunk = new java.util.LinkedHashMap<>();

        for (Map.Entry<OrderedPayload, Payload> entry : initialValue.entrySet()) {
            long order = entry.getKey().getOrder() != null ? entry.getKey().getOrder() : 0L;
            OrderedKey kVal = KeyValueUtils.createOrderedKey(entry.getKey().getValue(), order, clientId,getDefaultCompressionThreshold()).build();
            Value vVal = KeyValueUtils.createUnorderedValue(entry.getValue().getValue(), ttl, getDefaultCompressionThreshold()).build();

            int pairSize = kVal.getSerializedSize() + vVal.getSerializedSize();

            if (remainingKeys.isEmpty() && (currentChunkSize + pairSize <= MAX_RPC_SIZE)) {
                firstChunk.put(entry.getKey(), entry.getValue());
                currentChunkSize += pairSize;
            } else {
                remainingKeys.add(entry.getKey());
                remainingValues.add(entry.getValue());
            }
        }

        CompletableFuture<KeyHintData> createFuture = delegate.createOrderedMap(key, keyHint, firstChunk, ttl, clientId, timeout);

        if (remainingKeys.isEmpty()) {
            return createFuture;
        }

        return createFuture.thenCompose(returnedHint ->
                                                sendRemainingOrderedChunks(
                                                        key, returnedHint, remainingKeys, remainingValues, clientId, timeout,
                                                        getDefaultCompressionThreshold(),
                                                        (chunkKeys, chunkValues) -> delegate.addElementOrderedMap(key, returnedHint, chunkKeys, chunkValues, clientId, timeout)
                                                ).thenApply(ignored -> returnedHint)
        );
    }

    private CompletableFuture<Void> sendRemainingOrderedChunks(
            byte[] key,
            KeyHintData hint,
            List<OrderedPayload> keys,
            List<Payload> values,
            int clientId,
            Duration timeout,
            int compressionThreshold,
            java.util.function.BiFunction<List<OrderedPayload>, List<Payload>, CompletableFuture<Integer>> chunkSender) {

        if (keys.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        List<CompletableFuture<Integer>> chunkFutures = new ArrayList<>();
        List<OrderedPayload> currentChunkKeys = new ArrayList<>();
        List<Payload> currentChunkValues = new ArrayList<>();
        long currentChunkSize = 0;

        for (int i = 0; i < keys.size(); i++) {
            OrderedPayload k = keys.get(i);
            Payload v = values.get(i);

            long order = k.getOrder() != null ? k.getOrder() : 0L;
            OrderedKey kVal = KeyValueUtils.createOrderedKey(k.getValue(), order, clientId,getDefaultCompressionThreshold()).build();
            Value vVal = KeyValueUtils.createUnorderedValue(v.getValue(), null, compressionThreshold).build();

            int pairSize = kVal.getSerializedSize() + vVal.getSerializedSize();

            if (!currentChunkKeys.isEmpty() && (currentChunkSize + pairSize > MAX_RPC_SIZE)) {
                chunkFutures.add(chunkSender.apply(new ArrayList<>(currentChunkKeys), new ArrayList<>(currentChunkValues)));
                currentChunkKeys.clear();
                currentChunkValues.clear();
                currentChunkSize = 0;
            }

            currentChunkKeys.add(k);
            currentChunkValues.add(v);
            currentChunkSize += pairSize;
        }

        if (!currentChunkKeys.isEmpty()) {
            chunkFutures.add(chunkSender.apply(currentChunkKeys, currentChunkValues));
        }

        return CompletableFuture.allOf(chunkFutures.toArray(new CompletableFuture[0]));
    }

    // =========================================================================
    // BOUNDARY & POSITIONAL READS / CONTAINER INFO
    // =========================================================================

    @Override
    public CompletableFuture<Integer> getSize(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return delegate.getSize(key, hint, clientId, timeout);
    }

    @Override
    public CompletableFuture<Payload> getAndRemoveFront(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return delegate.getAndRemoveFront(key, hint, clientId, timeout);
    }

    @Override
    public CompletableFuture<Payload> getFront(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return delegate.getFront(key, hint, clientId, timeout);
    }

    @Override
    public CompletableFuture<Payload> getHead(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return delegate.getHead(key, hint, clientId, timeout);
    }

    @Override
    public CompletableFuture<Payload> getTail(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return delegate.getTail(key, hint, clientId, timeout);
    }

    @Override
    public CompletableFuture<Payload> getElementAtPosition(byte[] key, KeyHintData hint, int pos, int clientId, Duration timeout) {
        return delegate.getElementAtPosition(key, hint, pos, clientId, timeout);
    }

    // =========================================================================
    // STREAMING READ OPERATIONS
    // =========================================================================

    @Override
    public CompletableFuture<List<Payload>> streamList(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return delegate.streamList(key, hint, clientId, timeout);
    }

    @Override
    public CompletableFuture<List<Payload>> streamVector(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return delegate.streamVector(key, hint, clientId, timeout);
    }

    @Override
    public CompletableFuture<Map<Payload, Payload>> streamMap(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return delegate.streamMap(key, hint, clientId, timeout);
    }

    @Override
    public CompletableFuture<Map<OrderedPayload, Payload>> streamOrderedMap(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return delegate.streamOrderedMap(key, hint, clientId, timeout);
    }

    @Override
    public CompletableFuture<List<Payload>> streamElementInRangeUnordered(byte[] key, KeyHintData hint, ContainerType containerType, int start, int end, int clientId, Duration timeout) {
        return delegate.streamElementInRangeUnordered(key, hint, containerType, start, end, clientId, timeout);
    }

    @Override
    public CompletableFuture<List<OrderedPayload>> streamElementInRangeOrderedSet(byte[] key, KeyHintData hint, long startWeight, long endWeight, boolean reverse, int clientId, Duration timeout) {
        return delegate.streamElementInRangeOrderedSet(key, hint, startWeight, endWeight, reverse, clientId, timeout);
    }

    // =========================================================================
    // INSERTION OPERATIONS (CHUNKED)
    // =========================================================================

    @Override
    public CompletableFuture<Integer> addElement(byte[] key, KeyHintData hint, List<Payload> data, int clientId, Duration timeout) {
        if (data == null || data.isEmpty()) {
            return CompletableFuture.completedFuture(0);
        }

        return sendUnorderedChunksInSequence(key, hint, data, clientId, timeout,
                                             (k, h, chunk) -> delegate.addElement(k, h, chunk, clientId, timeout)
        );
    }

    @Override
    public CompletableFuture<Integer> addElementToTail(byte[] key, KeyHintData hint, List<Payload> data, int clientId, Duration timeout) {
        if (data == null || data.isEmpty()) {
            return CompletableFuture.completedFuture(0);
        }

        return sendUnorderedChunksInSequence(key, hint, data, clientId, timeout,
                                             (k, h, chunk) -> delegate.addElementToTail(k, h, chunk, clientId, timeout)
        );
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
                                                delegate.addElementToHead(key, hint, chunk, clientId, timeout)
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

        return sendOrderedChunksInSequence(key, hint, data, clientId, timeout,
                                           (k, h, chunk) -> delegate.addElementOrdered(k, h, chunk, clientId, timeout)
        );
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
                                                delegate.addElementWithWeight(key, hint, chunk, clientId, timeout)
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
                                                delegate.addElementToPosition(key, hint, chunk, targetPos, clientId, timeout)
                                                        .thenApply(res -> addedCount + res)
            );
            currentPos += chunkSize;
        }

        return future;
    }

    @Override
    public CompletableFuture<Integer> addElementToPositionBefore(byte[] key, KeyHintData hint, List<Payload> data, Payload pivot, int clientId, Duration timeout) {
        if (data == null || data.isEmpty()) {
            return CompletableFuture.completedFuture(0);
        }

        List<List<Payload>> chunks = splitUnorderedPayloads(key, hint, data, clientId);

        CompletableFuture<Integer> future = delegate.addElementToPositionBefore(key, hint, chunks.get(0), pivot, clientId, timeout);
        Payload currentPivot = chunks.get(0).get(chunks.get(0).size() - 1);

        for (int i = 1; i < chunks.size(); i++) {
            List<Payload> chunk = chunks.get(i);
            Payload nextPivot = chunk.get(chunk.size() - 1);
            Payload anchor = currentPivot;

            future = future.thenCompose(addedCount ->
                                                delegate.addElementToPositionAfter(key, hint, chunk, anchor, clientId, timeout)
                                                        .thenApply(res -> addedCount + res)
            );
            currentPivot = nextPivot;
        }

        return future;
    }

    @Override
    public CompletableFuture<Integer> addElementToPositionAfter(byte[] key, KeyHintData hint, List<Payload> data, Payload pivot, int clientId, Duration timeout) {
        if (data == null || data.isEmpty()) {
            return CompletableFuture.completedFuture(0);
        }

        List<List<Payload>> chunks = splitUnorderedPayloads(key, hint, data, clientId);
        CompletableFuture<Integer> future = CompletableFuture.completedFuture(0);

        Payload currentPivot = pivot;
        for (List<Payload> chunk : chunks) {
            Payload anchor = currentPivot;
            Payload nextPivot = chunk.get(chunk.size() - 1);

            future = future.thenCompose(addedCount ->
                                                delegate.addElementToPositionAfter(key, hint, chunk, anchor, clientId, timeout)
                                                        .thenApply(res -> addedCount + res)
            );
            currentPivot = nextPivot;
        }

        return future;
    }

    // =========================================================================
    // HELPER METHODS FOR CHUNKING
    // =========================================================================

    @FunctionalInterface
    private interface UnorderedChunkConsumer {
        CompletableFuture<Integer> accept(byte[] key, KeyHintData hint, List<Payload> chunk);
    }

    @FunctionalInterface
    private interface OrderedChunkConsumer {
        CompletableFuture<Integer> accept(byte[] key, KeyHintData hint, List<OrderedPayload> chunk);
    }

    private List<List<Payload>> splitUnorderedPayloads(byte[] key, KeyHintData hint, List<Payload> data, int clientId) {
        List<List<Payload>> chunks = new ArrayList<>();
        Key protoKey = KeyValueUtils.createUnorderedKey(key, hint, clientId, getDefaultCompressionThreshold()).build();
        long currentChunkSize = protoKey.getSerializedSize() + 32;

        List<Payload> currentChunk = new ArrayList<>();

        for (Payload payload : data) {
            Value.Builder compressedValue = CompressionUtils.compressIfNeeded(payload.getValue(), null);
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
        Key protoKey = KeyValueUtils.createUnorderedKey(key, hint, clientId, getDefaultCompressionThreshold()).build();
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

    private CompletableFuture<Integer> sendUnorderedChunksInSequence(
            byte[] key, KeyHintData hint, List<Payload> data, int clientId, Duration timeout, UnorderedChunkConsumer consumer) {
        List<List<Payload>> chunks = splitUnorderedPayloads(key, hint, data, clientId);
        CompletableFuture<Integer> future = CompletableFuture.completedFuture(0);

        for (List<Payload> chunk : chunks) {
            future = future.thenCompose(addedCount ->
                                                consumer.accept(key, hint, chunk)
                                                        .thenApply(res -> addedCount + res)
            );
        }

        return future;
    }

    private CompletableFuture<Integer> sendOrderedChunksInSequence(
            byte[] key, KeyHintData hint, List<OrderedPayload> data, int clientId, Duration timeout, OrderedChunkConsumer consumer) {
        List<List<OrderedPayload>> chunks = splitOrderedPayloads(key, hint, data, clientId);
        CompletableFuture<Integer> future = CompletableFuture.completedFuture(0);

        for (List<OrderedPayload> chunk : chunks) {
            future = future.thenCompose(addedCount ->
                                                consumer.accept(key, hint, chunk)
                                                        .thenApply(res -> addedCount + res)
            );
        }

        return future;
    }

    // =========================================================================
    // POP & DELETION OPERATIONS
    // =========================================================================

    @Override
    public CompletableFuture<Payload> getAndRemoveTail(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return delegate.getAndRemoveTail(key, hint, clientId, timeout);
    }

    @Override
    public CompletableFuture<Payload> getAndRemoveElementAtPosition(byte[] key, KeyHintData hint, int pos, int clientId, Duration timeout) {
        return delegate.getAndRemoveElementAtPosition(key, hint, pos, clientId, timeout);
    }

    @Override
    public CompletableFuture<Boolean> removeTail(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return delegate.removeTail(key, hint, clientId, timeout);
    }

    @Override
    public CompletableFuture<Boolean> removeHead(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return delegate.removeHead(key, hint, clientId, timeout);
    }

    @Override
    public CompletableFuture<Boolean> removeElementAtPosition(byte[] key, KeyHintData hint, int pos, int endPos, int clientId, Duration timeout) {
        return delegate.removeElementAtPosition(key, hint, pos, endPos, clientId, timeout);
    }

    @Override
    public CompletableFuture<Integer> removeFromContainer(byte[] key, KeyHintData hint, ContainerType type, List<Payload> values, List<Payload> keys, int clientId, Duration timeout) {
        return delegate.removeFromContainer(key, hint, type, values, keys, clientId, timeout);
    }

    // =========================================================================
    // LOCKING OPERATIONS
    // =========================================================================

    @Override
    public CompletableFuture<LockStatus> lockObject(byte[] key, KeyHintData hint, LockType type, int clientId, Duration duration, Duration timeout) {
        return delegate.lockObject(key, hint, type, clientId, duration, timeout);
    }

    @Override
    public CompletableFuture<LockStatus> unlockObject(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return delegate.unlockObject(key, hint, clientId, timeout);
    }

    // =========================================================================
    // ATOMIC OPERATIONS
    // =========================================================================

    @Override
    public CompletableFuture<Long> atomicLoad(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return delegate.atomicLoad(key, hint, clientId, timeout);
    }

    @Override
    public CompletableFuture<Long> atomicLoadAndDelete(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return delegate.atomicLoadAndDelete(key, hint, clientId, timeout);
    }

    @Override
    public CompletableFuture<KeyHintData> atomicCreate(byte[] key, KeyHintData hint, long value, Duration ttl, int clientId, Duration timeout) {
        return delegate.atomicCreate(key, hint, value, ttl, clientId, timeout);
    }

    @Override
    public CompletableFuture<KeyHintData> atomicStore(byte[] key, KeyHintData hint, long value, Duration ttl, int clientId, Duration timeout) {
        return delegate.atomicStore(key, hint, value, ttl, clientId, timeout);
    }

    @Override
    public CompletableFuture<Long> atomicExchange(byte[] key, KeyHintData hint, long value, Duration ttl, int clientId, Duration timeout) {
        return delegate.atomicExchange(key, hint, value, ttl, clientId, timeout);
    }

    @Override
    public CompletableFuture<Long> atomicAdd(byte[] key, KeyHintData hint, long delta, Duration ttl, int clientId, Duration timeout) {
        return delegate.atomicAdd(key, hint, delta, ttl, clientId, timeout);
    }

    @Override
    public CompletableFuture<Long> atomicSub(byte[] key, KeyHintData hint, long delta, Duration ttl, int clientId, Duration timeout) {
        return delegate.atomicSub(key, hint, delta, ttl, clientId, timeout);
    }

    @Override
    public CompletableFuture<Long> atomicAnd(byte[] key, KeyHintData hint, long mask, Duration ttl, int clientId, Duration timeout) {
        return delegate.atomicAnd(key, hint, mask, ttl, clientId, timeout);
    }

    @Override
    public CompletableFuture<Long> atomicOr(byte[] key, KeyHintData hint, long mask, Duration ttl, int clientId, Duration timeout) {
        return delegate.atomicOr(key, hint, mask, ttl, clientId, timeout);
    }

    @Override
    public CompletableFuture<Long> atomicXor(byte[] key, KeyHintData hint, long mask, Duration ttl, int clientId, Duration timeout) {
        return delegate.atomicXor(key, hint, mask, ttl, clientId, timeout);
    }

    @Override
    public CompletableFuture<AtomicCasRes> atomicCompareAndSet(byte[] key, KeyHintData hint, long expectedValue, long newValue, Duration ttl, int clientId, Duration timeout) {
        return delegate.atomicCompareAndSet(key, hint, expectedValue, newValue, ttl, clientId, timeout);
    }

    // =========================================================================
    // CONTAINER ITEM OPERATIONS
    // =========================================================================

    @Override
    public CompletableFuture<byte[]> getContainerValue(byte[] key, KeyHintData hint, byte[] elementKey, int clientId, Duration timeout) {
        return delegate.getContainerValue(key, hint, elementKey, clientId, timeout);
    }

    @Override
    public CompletableFuture<byte[]> getAndRemoveContainerValue(byte[] key, KeyHintData hint, byte[] elementKey, int clientId, Duration timeout) {
        return delegate.getAndRemoveContainerValue(key, hint, elementKey, clientId, timeout);
    }

    @Override
    public CompletableFuture<Boolean> containsContainerKey(byte[] key, KeyHintData hint, byte[] elementKey, int clientId, Duration timeout) {
        return delegate.containsContainerKey(key, hint, elementKey, clientId, timeout);
    }

    @Override
    public CompletableFuture<byte[]> updateContainerValue(byte[] key, KeyHintData hint, byte[] elementKey, byte[] value, int clientId, Duration timeout) {
        return delegate.updateContainerValue(key, hint, elementKey, value, clientId, timeout);
    }

    @Override
    public CompletableFuture<Integer> removeFromContainer(byte[] key, KeyHintData hint, byte[] elementKey, int clientId, Duration timeout) {
        return delegate.removeFromContainer(key, hint, elementKey, clientId, timeout);
    }

    @Override
    public CompletableFuture<Integer> addElementHashMap(byte[] key, KeyHintData hint, List<Payload> container_keys, List<Payload> container_values, int clientId, Duration timeout) {
        return delegate.addElementHashMap(key, hint, container_keys, container_values, clientId, timeout);
    }

    @Override
    public CompletableFuture<Integer> addElementOrderedMap(byte[] key, KeyHintData hint, List<OrderedPayload> container_keys, List<Payload> container_values, int clientId, Duration timeout) {
        return delegate.addElementOrderedMap(key, hint, container_keys, container_values, clientId, timeout);
    }
}