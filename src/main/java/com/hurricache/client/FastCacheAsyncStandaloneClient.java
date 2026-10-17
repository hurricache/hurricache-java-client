package com.hurricache.client;

import com.hurricache.client.intf.HurriCacheClientInterface;
import com.hurricache.client.intf.KeyHintData;
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
import com.hurricache.utils.CompressionUtils;

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
    public CompletableFuture<Variant> pop(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return delegate.pop(key, hint, clientId, timeout);
    }

    @Override
    public CompletableFuture<KeyHintData> create(byte[] key, KeyHintData hint, Variant value, Duration ttl, int clientId, Duration timeout) {
        return delegate.create(key, hint, value, ttl, clientId, timeout);
    }

    @Override
    public CompletableFuture<Variant> get(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return delegate.get(key, hint, clientId, timeout);
    }

    @Override
    public CompletableFuture<Variant> replace(byte[] key, KeyHintData hint, Variant value, Duration ttl, int clientId, Duration timeout) {
        return delegate.replace(key, hint, value, ttl, clientId, timeout);
    }

    @Override
    public CompletableFuture<Boolean> containsKey(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return delegate.containsKey(key, hint, clientId, timeout);
    }

    @Override
    public CompletableFuture<Boolean> remove(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return delegate.remove(key, hint, clientId, timeout);
    }

    // =========================================================================
    // CHUNKED CONTAINER CREATION
    // =========================================================================

    @Override
    public CompletableFuture<KeyHintData> createQueue(byte[] key, KeyHintData keyHint, List<Variant> initialValue, Duration ttl, int clientId, Duration timeout) {
        return createUnorderedContainerChunked(key, keyHint, initialValue, ContainerType.QUEUE, ttl, clientId, timeout);
    }

    @Override
    public CompletableFuture<KeyHintData> createList(byte[] key, KeyHintData keyHint, List<Variant> initialValue, Duration ttl, int clientId, Duration timeout) {
        return createUnorderedContainerChunked(key, keyHint, initialValue, ContainerType.LIST, ttl, clientId, timeout);
    }

    @Override
    public CompletableFuture<KeyHintData> createVector(byte[] key, KeyHintData keyHint, List<Variant> initialValue, Duration ttl, int clientId, Duration timeout) {
        return createUnorderedContainerChunked(key, keyHint, initialValue, ContainerType.VECTOR, ttl, clientId, timeout);
    }

    @Override
    public CompletableFuture<KeyHintData> createSet(byte[] key, KeyHintData keyHint, List<Variant> initialValue, Duration ttl, int clientId, Duration timeout) {
        return createUnorderedContainerChunked(key, keyHint, initialValue, ContainerType.SET, ttl, clientId, timeout);
    }

    private CompletableFuture<KeyHintData> createUnorderedContainerChunked(byte[] key,
                                                                           KeyHintData keyHint,
                                                                           List<Variant> initialValue,
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

        List<Variant> firstChunk = new ArrayList<>();
        for (Variant payload : initialValue) {
            Value.Builder compressedValue = CompressionUtils.compressIfNeeded(payload.bytesValue(), null);
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

        List<Variant> remainingTail = initialValue.subList(splitIndex, initialValue.size());
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
                                                                 List<Variant> tail,
                                                                 ContainerType type,
                                                                 int clientId,
                                                                 Duration timeout) {
        if (tail.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        Key protoKey = KeyValueUtils.createUnorderedKey(key, hint, clientId, getDefaultCompressionThreshold()).build();
        long currentChunkSize = protoKey.getSerializedSize() + 32;

        List<Variant> currentChunk = new ArrayList<>();
        int splitIndex = 0;

        for (Variant payload : tail) {
            Value.Builder compressedValue = CompressionUtils.compressIfNeeded(payload.bytesValue(), null);
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
            sendFuture = delegate.addElementUnordered(key, hint, currentChunk, clientId, timeout);
        } else {
            sendFuture = delegate.pushTail(key, hint, currentChunk, clientId, timeout);
        }

        List<Variant> nextTail = tail.subList(splitIndex, tail.size());
        return sendFuture.thenCompose(res -> sendRemainingUnorderedChunks(key, hint, nextTail, type, clientId, timeout));
    }

    @Override
    public CompletableFuture<KeyHintData> createOrderedSet(byte[] key, KeyHintData keyHint, List<OrderedVariant> initialValue, Duration ttl, int clientId, Duration timeout) {
        if (initialValue == null || initialValue.isEmpty()) {
            return delegate.createOrderedSet(key, keyHint, initialValue, ttl, clientId, timeout);
        }

        Key protoKey = KeyValueUtils.createUnorderedKey(key, keyHint, clientId, getDefaultCompressionThreshold()).build();
        long currentChunkSize = protoKey.getSerializedSize();
        int splitIndex = 0;

        List<OrderedVariant> firstChunk = new ArrayList<>();
        for (OrderedVariant payload : initialValue) {
            OrderedValue orderedValue = KeyValueUtils.buildOrderedValueFromVariant(payload, ttl, getDefaultCompressionThreshold()).build();
            int elemSize = orderedValue.getSerializedSize();

            if (!firstChunk.isEmpty() && (currentChunkSize + elemSize > MAX_RPC_SIZE)) {
                break;
            }

            firstChunk.add(payload);
            currentChunkSize += elemSize;
            splitIndex++;
        }

        CompletableFuture<KeyHintData> createFuture = delegate.createOrderedSet(key, keyHint, firstChunk, ttl, clientId, timeout);

        List<OrderedVariant> remainingTail = initialValue.subList(splitIndex, initialValue.size());
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
                                                                  List<OrderedVariant> tail,
                                                                  int clientId,
                                                                  Duration timeout) {
        if (tail.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        Key protoKey = KeyValueUtils.createUnorderedKey(key, hint, clientId, getDefaultCompressionThreshold()).build();
        long currentChunkSize = protoKey.getSerializedSize() + 32;

        List<OrderedVariant> currentChunk = new ArrayList<>();
        int splitIndex = 0;

        for (OrderedVariant payload : tail) {
            long order = payload.getOrder() != null ? payload.getOrder() : 0L;
            OrderedValue orderedValue = KeyValueUtils.buildOrderedValueFromVariant(payload, null, getDefaultCompressionThreshold()).build();
            int elemSize = orderedValue.getSerializedSize();

            if (!currentChunk.isEmpty() && (currentChunkSize + elemSize > MAX_RPC_SIZE)) {
                break;
            }

            currentChunk.add(payload);
            currentChunkSize += elemSize;
            splitIndex++;
        }

        CompletableFuture<Integer> sendFuture = delegate.addElementWithWeight(key, hint, currentChunk, clientId, timeout);

        List<OrderedVariant> nextTail = tail.subList(splitIndex, tail.size());
        return sendFuture.thenCompose(res -> sendRemainingOrderedSetChunks(key, hint, nextTail, clientId, timeout));
    }

    @Override
    public CompletableFuture<KeyHintData> createMap(byte[] key, KeyHintData keyHint, Map<Payload, Variant> initialValue, Duration ttl, int clientId, Duration timeout) {
        if (initialValue == null || initialValue.isEmpty()) {
            return delegate.createMap(key, keyHint, initialValue, ttl, clientId, timeout);
        }

        Key protoKey = KeyValueUtils.createUnorderedKey(key, keyHint, clientId, getDefaultCompressionThreshold()).build();
        long currentChunkSize = protoKey.getSerializedSize();

        List<Payload> remainingKeys = new ArrayList<>();
        List<Variant> remainingValues = new ArrayList<>();
        Map<Payload, Variant> firstChunk = new java.util.LinkedHashMap<>();

        for (Map.Entry<Payload, Variant> entry : initialValue.entrySet()) {
            Key kVal = KeyValueUtils.createUnorderedKey(entry.getKey().getValue(), clientId, null).build();
            Value vVal = KeyValueUtils.buildValueFromVariantCompressed(entry.getValue(), getDefaultCompressionThreshold()).build();
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
                                                           List<Variant> values,
                                                           int clientId,
                                                           Duration ttl,
                                                           Duration timeout) {
        List<CompletableFuture<Integer>> chunkFutures = new ArrayList<>();

        List<Payload> currentChunkKeys = new ArrayList<>();
        List<Variant> currentChunkValues = new ArrayList<>();
        long currentChunkSize = 0;

        for (int i = 0; i < keys.size(); i++) {
            Payload k = keys.get(i);
            Variant v = values.get(i);

            Key kVal = KeyValueUtils.createUnorderedKey(k.getValue(), clientId, null).build();
            Value vVal = KeyValueUtils.buildValueFromVariantCompressed(v, getDefaultCompressionThreshold()).build();
            int pairSize = kVal.getSerializedSize() + vVal.getSerializedSize();

            if (!currentChunkKeys.isEmpty() && (currentChunkSize + pairSize > MAX_RPC_SIZE)) {
                chunkFutures.add(delegate.putAll(key, hint, new ArrayList<>(currentChunkKeys), new ArrayList<>(currentChunkValues), clientId, timeout));
                currentChunkKeys.clear();
                currentChunkValues.clear();
                currentChunkSize = 0;
            }

            currentChunkKeys.add(k);
            currentChunkValues.add(v);
            currentChunkSize += pairSize;
        }

        if (!currentChunkKeys.isEmpty()) {
            chunkFutures.add(delegate.putAll(key, hint, currentChunkKeys, currentChunkValues, clientId, timeout));
        }

        return CompletableFuture.allOf(chunkFutures.toArray(new CompletableFuture[0]));
    }

    @Override
    public CompletableFuture<KeyHintData> createOrderedMap(byte[] key, KeyHintData keyHint, Map<OrderedPayload, Variant> initialValue, Duration ttl, int clientId, Duration timeout) {
        if (initialValue == null || initialValue.isEmpty()) {
            return delegate.createOrderedMap(key, keyHint, initialValue, ttl, clientId, timeout);
        }

        Key protoKey = KeyValueUtils.createUnorderedKey(key, keyHint, clientId, getDefaultCompressionThreshold()).build();
        long currentChunkSize = protoKey.getSerializedSize();

        List<OrderedPayload> remainingKeys = new ArrayList<>();
        List<Variant> remainingValues = new ArrayList<>();
        Map<OrderedPayload, Variant> firstChunk = new java.util.LinkedHashMap<>();

        for (Map.Entry<OrderedPayload, Variant> entry : initialValue.entrySet()) {
            long order = entry.getKey().getOrder() != null ? entry.getKey().getOrder() : 0L;
            OrderedKey kVal = KeyValueUtils.createOrderedKey(entry.getKey().getValue(), order, clientId,getDefaultCompressionThreshold()).build();
            Value vVal = KeyValueUtils.buildValueFromVariantCompressed(entry.getValue(), getDefaultCompressionThreshold()).build();

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
            List<Variant> values,
            int clientId,
            Duration timeout,
            int compressionThreshold,
            java.util.function.BiFunction<List<OrderedPayload>, List<Variant>, CompletableFuture<Integer>> chunkSender) {

        if (keys.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        List<CompletableFuture<Integer>> chunkFutures = new ArrayList<>();
        List<OrderedPayload> currentChunkKeys = new ArrayList<>();
        List<Variant> currentChunkValues = new ArrayList<>();
        long currentChunkSize = 0;

        for (int i = 0; i < keys.size(); i++) {
            OrderedPayload k = keys.get(i);
            Variant v = values.get(i);

            long order = k.getOrder() != null ? k.getOrder() : 0L;
            OrderedKey kVal = KeyValueUtils.createOrderedKey(k.getValue(), order, clientId,getDefaultCompressionThreshold()).build();
            Value vVal = KeyValueUtils.buildValueFromVariantCompressed(v, compressionThreshold).build();

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
    public CompletableFuture<Variant> popHead(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return delegate.popHead(key, hint, clientId, timeout);
    }

    @Override
    public CompletableFuture<Variant> peekHead(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return delegate.peekHead(key, hint, clientId, timeout);
    }

    @Override
    public CompletableFuture<Variant> peekTail(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return delegate.peekTail(key, hint, clientId, timeout);
    }

    @Override
    public CompletableFuture<Variant> getAt(byte[] key, KeyHintData hint, int pos, int clientId, Duration timeout) {
        return delegate.getAt(key, hint, pos, clientId, timeout);
    }

    @Override
    public CompletableFuture<Variant> getElementWithWeight(byte[] key,
                                                           KeyHintData hint,
                                                           int pos,
                                                           int clientId,
                                                           Duration timeout) {
        return delegate.getElementWithWeight(key, hint, pos, clientId, timeout);
    }

    @Override
    public CompletableFuture<Variant> getAndRemoveElementWithWeight(byte[] key,
                                                                    KeyHintData hint,
                                                                    int pos,
                                                                    int clientId,
                                                                    Duration timeout) {
        return delegate.getAndRemoveElementWithWeight(key, hint, pos, clientId, timeout);
    }

    // =========================================================================
    // STREAMING READ OPERATIONS
    // =========================================================================

    @Override
    public CompletableFuture<List<Variant>> getList(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return delegate.getList(key, hint, clientId, timeout);
    }

    @Override
    public CompletableFuture<List<Variant>> getVector(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return delegate.getVector(key, hint, clientId, timeout);
    }

    @Override
    public CompletableFuture<List<OrderedVariant>> getOrderedSet(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return delegate.getOrderedSet(key, hint, clientId, timeout);
    }

    @Override
    public CompletableFuture<Map<Payload, Variant>> getMap(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return delegate.getMap(key, hint, clientId, timeout);
    }

    @Override
    public CompletableFuture<Map<OrderedPayload, Variant>> getOrderedMap(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return delegate.getOrderedMap(key, hint, clientId, timeout);
    }

    @Override
    public CompletableFuture<Map<OrderedPayload, Variant>> getElementInRangeOrderedMap(byte[] key,
                                                                                       KeyHintData hint,
                                                                                       long startWeight,
                                                                                       long endWeight,
                                                                                       boolean reverse,
                                                                                       int clientId,
                                                                                       Duration timeout) {
        return delegate.getElementInRangeOrderedMap(key, hint, startWeight, endWeight, reverse, clientId, timeout);
    }

    @Override
    public CompletableFuture<List<Variant>> getSet(byte[] key, KeyHintData hint, int clientId,
                                                   Duration timeout){
        return delegate.getSet(key, hint, clientId, timeout);
    }
    @Override
    public CompletableFuture<List<Variant>> getRange(byte[] key, KeyHintData hint, ContainerType containerType, int start, int end, int clientId, Duration timeout) {
        return delegate.getRange(key, hint, containerType, start, end, clientId, timeout);
    }

    @Override
    public CompletableFuture<List<OrderedVariant>> getElementInRangeOrderedSet(byte[] key, KeyHintData hint, long startWeight, long endWeight, boolean reverse, int clientId, Duration timeout) {
        return delegate.getElementInRangeOrderedSet(key, hint, startWeight, endWeight, reverse, clientId, timeout);
    }

    // =========================================================================
    // INSERTION OPERATIONS (CHUNKED)
    // =========================================================================

    @Override
    public CompletableFuture<Integer> addElementUnordered(byte[] key, KeyHintData hint, List<Variant> data, int clientId, Duration timeout) {
        if (data == null || data.isEmpty()) {
            return CompletableFuture.completedFuture(0);
        }

        return sendUnorderedChunksInSequence(key, hint, data, clientId, timeout,
                                             (k, h, chunk) -> delegate.addElementUnordered(k, h, chunk, clientId, timeout)
        );
    }

    @Override
    public CompletableFuture<Integer> pushTail(byte[] key, KeyHintData hint, List<Variant> data, int clientId, Duration timeout) {
        if (data == null || data.isEmpty()) {
            return CompletableFuture.completedFuture(0);
        }

        return sendUnorderedChunksInSequence(key, hint, data, clientId, timeout,
                                             (k, h, chunk) -> delegate.pushTail(k, h, chunk, clientId, timeout)
        );
    }

    @Override
    public CompletableFuture<Integer> pushHead(byte[] key, KeyHintData hint, List<Variant> data, int clientId, Duration timeout) {
        if (data == null || data.isEmpty()) {
            return CompletableFuture.completedFuture(0);
        }

        List<List<Variant>> chunks = splitUnorderedPayloads(key, hint, data, clientId);
        CompletableFuture<Integer> future = CompletableFuture.completedFuture(0);

        for (int i = chunks.size() - 1; i >= 0; i--) {
            List<Variant> chunk = chunks.get(i);
            future = future.thenCompose(addedCount ->
                                                delegate.pushHead(key, hint, chunk, clientId, timeout)
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

        return sendOrderedChunksInSequence(key, hint, data, clientId, timeout,
                                           (k, h, chunk) -> delegate.addElementWithWeight(k, h, chunk, clientId, timeout)
        );
    }



    @Override
    public CompletableFuture<Integer> insertAt(byte[] key, KeyHintData hint, List<Variant> data, int pos, int clientId, Duration timeout) {
        if (data == null || data.isEmpty()) {
            return CompletableFuture.completedFuture(0);
        }

        List<List<Variant>> chunks = splitUnorderedPayloads(key, hint, data, clientId);
        CompletableFuture<Integer> future = CompletableFuture.completedFuture(0);

        int currentPos = pos;
        for (List<Variant> chunk : chunks) {
            final int targetPos = currentPos;
            final int chunkSize = chunk.size();
            future = future.thenCompose(addedCount ->
                                                delegate.insertAt(key, hint, chunk, targetPos, clientId, timeout)
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

        List<List<Variant>> chunks = splitUnorderedPayloads(key, hint, data, clientId);

        CompletableFuture<Integer> future = delegate.insertBefore(key, hint, chunks.get(0), pivot, clientId, timeout);
        Variant currentPivot = chunks.get(0).get(chunks.get(0).size() - 1);

        for (int i = 1; i < chunks.size(); i++) {
            List<Variant> chunk = chunks.get(i);
            Variant nextPivot = chunk.get(chunk.size() - 1);
            Variant anchor = currentPivot;

            future = future.thenCompose(addedCount ->
                                                delegate.insertAfter(key, hint, chunk, anchor, clientId, timeout)
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

        List<List<Variant>> chunks = splitUnorderedPayloads(key, hint, data, clientId);
        CompletableFuture<Integer> future = CompletableFuture.completedFuture(0);

        Variant currentPivot = pivot;
        for (List<Variant> chunk : chunks) {
            Variant anchor = currentPivot;
            Variant nextPivot = chunk.get(chunk.size() - 1);

            future = future.thenCompose(addedCount ->
                                                delegate.insertAfter(key, hint, chunk, anchor, clientId, timeout)
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
        CompletableFuture<Integer> accept(byte[] key, KeyHintData hint, List<Variant> chunk);
    }

    @FunctionalInterface
    private interface OrderedChunkConsumer {
        CompletableFuture<Integer> accept(byte[] key, KeyHintData hint, List<OrderedVariant> chunk);
    }

    private List<List<Variant>> splitUnorderedPayloads(byte[] key, KeyHintData hint, List<Variant> data, int clientId) {
        List<List<Variant>> chunks = new ArrayList<>();
        Key protoKey = KeyValueUtils.createUnorderedKey(key, hint, clientId, getDefaultCompressionThreshold()).build();
        long currentChunkSize = protoKey.getSerializedSize() + 32;

        List<Variant> currentChunk = new ArrayList<>();

        for (Variant payload : data) {
            Value.Builder compressedValue = CompressionUtils.compressIfNeeded(payload.bytesValue(), null);
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

    private List<List<OrderedVariant>> splitOrderedPayloads(byte[] key, KeyHintData hint, List<OrderedVariant> data, int clientId) {
        List<List<OrderedVariant>> chunks = new ArrayList<>();
        Key protoKey = KeyValueUtils.createUnorderedKey(key, hint, clientId, getDefaultCompressionThreshold()).build();
        long currentChunkSize = protoKey.getSerializedSize() + 32;

        List<OrderedVariant> currentChunk = new ArrayList<>();

        for (OrderedVariant payload : data) {
            OrderedValue orderedValue = KeyValueUtils.buildOrderedValueFromVariant(payload, null, getDefaultCompressionThreshold()).build();
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
            byte[] key, KeyHintData hint, List<Variant> data, int clientId, Duration timeout, UnorderedChunkConsumer consumer) {
        List<List<Variant>> chunks = splitUnorderedPayloads(key, hint, data, clientId);
        CompletableFuture<Integer> future = CompletableFuture.completedFuture(0);

        for (List<Variant> chunk : chunks) {
            future = future.thenCompose(addedCount ->
                                                consumer.accept(key, hint, chunk)
                                                        .thenApply(res -> addedCount + res)
            );
        }

        return future;
    }

    private CompletableFuture<Integer> sendOrderedChunksInSequence(
            byte[] key, KeyHintData hint, List<OrderedVariant> data, int clientId, Duration timeout, OrderedChunkConsumer consumer) {
        List<List<OrderedVariant>> chunks = splitOrderedPayloads(key, hint, data, clientId);
        CompletableFuture<Integer> future = CompletableFuture.completedFuture(0);

        for (List<OrderedVariant> chunk : chunks) {
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
    public CompletableFuture<Variant> popTail(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return delegate.popTail(key, hint, clientId, timeout);
    }

    @Override
    public CompletableFuture<Variant> popAt(byte[] key, KeyHintData hint, int pos, int clientId, Duration timeout) {
        return delegate.popAt(key, hint, pos, clientId, timeout);
    }

    @Override
    public CompletableFuture<Integer> deleteTail(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return delegate.deleteTail(key, hint, clientId, timeout);
    }

    @Override
    public CompletableFuture<Integer> deleteHead(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return delegate.deleteHead(key, hint, clientId, timeout);
    }

    @Override
    public CompletableFuture<Integer> removeAt(byte[] key, KeyHintData hint, long pos, long endPos, int clientId, Duration timeout) {
        return delegate.removeAt(key, hint, pos, endPos, clientId, timeout);
    }



    @Override
    public CompletableFuture<Integer> removeFromContainerByKeyAndValue(byte[] key, KeyHintData hint,
                                                                       List<Payload> keys, List<Variant> values, int clientId, Duration timeout) {
        return delegate.removeFromContainerByKeyAndValue(key, hint, keys, values, clientId, timeout);
    }

    @Override
    public CompletableFuture<Integer> removeFromContainerByKey(byte[] key, KeyHintData hint,
                                                                       Payload keys, int clientId, Duration timeout) {
        return delegate.removeFromContainerByKey(key, hint, keys, clientId, timeout);
    }

    public CompletableFuture<Integer> removeFromContainerByKey(byte[] key,
                                                               KeyHintData hint,
                                                               Variant containerKey,
                                                               int clientId,
                                                               Duration timeout) {

        return delegate.removeFromContainerByKey(key, hint, containerKey, clientId, timeout);
    }
    // =========================================================================
    // LOCKING OPERATIONS
    // =========================================================================

    @Override
    public CompletableFuture<LockStatus> lock(byte[] key, KeyHintData hint, LockType type, int clientId, Duration duration, Duration timeout) {
        return delegate.lock(key, hint, type, clientId, duration, timeout);
    }

    @Override
    public CompletableFuture<LockStatus> unlock(byte[] key, KeyHintData hint, int clientId, Duration timeout) {
        return delegate.unlock(key, hint, clientId, timeout);
    }

    // =========================================================================
    // ATOMIC OPERATIONS
    // =========================================================================

    @Override
    public CompletableFuture<Number> exchange(byte[] key, KeyHintData hint, long value, Duration ttl, int clientId, Duration timeout) {
        return delegate.exchange(key, hint, value, ttl, clientId, timeout);
    }

    @Override
    public CompletableFuture<Number> exchange(byte[] key, KeyHintData hint, double value, Duration ttl, int clientId, Duration timeout) {
        return delegate.exchange(key, hint, value, ttl, clientId, timeout);
    }

    @Override
    public CompletableFuture<Number> add(byte[] key, KeyHintData hint, long delta, Duration ttl, int clientId, Duration timeout) {
        return delegate.add(key, hint, delta, ttl, clientId, timeout);
    }

    @Override
    public CompletableFuture<Number> add(byte[] key, KeyHintData hint, double delta, Duration ttl, int clientId, Duration timeout) {
        return delegate.add(key, hint, delta, ttl, clientId, timeout);
    }

    @Override
    public CompletableFuture<Number> atomicSub(byte[] key, KeyHintData hint, long delta, Duration ttl, int clientId, Duration timeout) {
        return delegate.atomicSub(key, hint, delta, ttl, clientId, timeout);
    }

    @Override
    public CompletableFuture<Number> atomicSub(byte[] key, KeyHintData hint, double delta, Duration ttl, int clientId, Duration timeout) {
        return delegate.atomicSub(key, hint, delta, ttl, clientId, timeout);
    }

    @Override
    public CompletableFuture<Long> and(byte[] key, KeyHintData hint, long mask, Duration ttl, int clientId, Duration timeout) {
        return delegate.and(key, hint, mask, ttl, clientId, timeout);
    }

    @Override
    public CompletableFuture<Long> or(byte[] key, KeyHintData hint, long mask, Duration ttl, int clientId, Duration timeout) {
        return delegate.or(key, hint, mask, ttl, clientId, timeout);
    }

    @Override
    public CompletableFuture<Long> xor(byte[] key, KeyHintData hint, long mask, Duration ttl, int clientId, Duration timeout) {
        return delegate.xor(key, hint, mask, ttl, clientId, timeout);
    }

    @Override
    public CompletableFuture<AtomicCasRes> compareAndSet(byte[] key, KeyHintData hint, long expectedValue, long newValue, Duration ttl, int clientId, Duration timeout) {
        return delegate.compareAndSet(key, hint, expectedValue, newValue, ttl, clientId, timeout);
    }

    @Override
    public CompletableFuture<AtomicCasRes> compareAndSet(byte[] key, KeyHintData hint, double expectedValue, double newValue, Duration ttl, int clientId, Duration timeout) {
        return delegate.compareAndSet(key, hint, expectedValue, newValue, ttl, clientId, timeout);
    }

    // =========================================================================
    // CONTAINER ITEM OPERATIONS
    // =========================================================================

    @Override
    public CompletableFuture<Variant> getInContainer(byte[] key, KeyHintData hint, byte[] elementKey, int clientId, Duration timeout) {
        return delegate.getInContainer(key, hint, elementKey, clientId, timeout);
    }

    @Override
    public CompletableFuture<Variant> popInContainer(byte[] key, KeyHintData hint, byte[] elementKey, int clientId, Duration timeout) {
        return delegate.popInContainer(key, hint, elementKey, clientId, timeout);
    }

    @Override
    public CompletableFuture<Boolean> containsKey(byte[] key, KeyHintData hint, byte[] elementKey, int clientId, Duration timeout) {
        return delegate.containsKey(key, hint, elementKey, clientId, timeout);
    }

    @Override
    public CompletableFuture<Variant> update(byte[] key, KeyHintData hint, byte[] elementKey, Variant value, int clientId, Duration timeout) {
        return delegate.update(key, hint, elementKey, value, clientId, timeout);
    }

    @Override
    public CompletableFuture<Integer> remove(byte[] key, KeyHintData hint, byte[] elementKey, int clientId, Duration timeout) {
        return delegate.remove(key, hint, elementKey, clientId, timeout);
    }

    @Override
    public CompletableFuture<Integer> putAll(byte[] key, KeyHintData hint, List<Payload> container_keys, List<Variant> container_values, int clientId, Duration timeout) {
        return delegate.putAll(key, hint, container_keys, container_values, clientId, timeout);
    }

    @Override
    public CompletableFuture<Integer> addElementOrderedMap(byte[] key, KeyHintData hint, List<OrderedPayload> container_keys, List<Variant> container_values, int clientId, Duration timeout) {
        return delegate.addElementOrderedMap(key, hint, container_keys, container_values, clientId, timeout);
    }
}