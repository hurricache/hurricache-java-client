package com.hurricache.utils;

import com.google.protobuf.ByteString;
import com.hurricache.client.intf.OrderedPayload;
import com.hurricache.client.intf.Payload;
import com.hurricache.grpc.BatchValueResponse;
import com.hurricache.grpc.BinaryPayload;
import com.hurricache.grpc.CompressedInfo;
import com.hurricache.grpc.KeyBinaryPayload;
import com.hurricache.grpc.OrderedKey;
import com.hurricache.grpc.Value;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.*;

class StreamBatchOrderedMapObserverTest {
    private static OrderedKey key(byte[] bytes, long order) {
        return OrderedKey.newBuilder().setOrder(order)
                .setPayload(KeyBinaryPayload.newBuilder().setPayload(ByteString.copyFrom(bytes)).setSize(bytes.length))
                .build();
    }

    private static Value value(byte[] bytes) {
        return Value.newBuilder()
                .setValue(BinaryPayload.newBuilder().setPayload(ByteString.copyFrom(bytes)).setSize(bytes.length))
                .build();
    }

    private static byte[] gzip(byte[] bytes) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (GZIPOutputStream stream = new GZIPOutputStream(output)) {
            stream.write(bytes);
        }
        return output.toByteArray();
    }

    @Test
    void returnsKeyPayloadWithoutProtobufFraming() {
        byte[] bytes = new byte[4096];
        Arrays.fill(bytes, (byte) 42);
        CompletableFuture<Map<OrderedPayload, Payload>> future = new CompletableFuture<>();
        StreamBatchOrderedMapObserver observer = new StreamBatchOrderedMapObserver(future);
        observer.onNext(BatchValueResponse.newBuilder().addKeyOrdered(key(bytes, 7))
                .addValueUnordered(value(new byte[]{0, -1, 3})).build());
        observer.onCompleted();
        Map<OrderedPayload, Payload> result = future.join();
        assertEquals(1, result.size());
        OrderedPayload actual = result.keySet().iterator().next();
        assertArrayEquals(bytes, actual.getValue());
        assertEquals(7L, actual.getOrder());
        assertArrayEquals(new byte[]{0, -1, 3}, result.get(actual).getValue());
    }

    @Test
    void decompressesKeysAndValues() throws IOException {
        byte[] keyBytes = new byte[4096];
        byte[] valueBytes = new byte[4097];
        Arrays.fill(keyBytes, (byte) 42);
        Arrays.fill(valueBytes, (byte) 73);
        OrderedKey compressedKey = key(gzip(keyBytes), 9).toBuilder()
                .setCompressionInfo(CompressedInfo.newBuilder().setEnabled(true).setRawSize(keyBytes.length)).build();
        Value compressedValue = value(gzip(valueBytes)).toBuilder()
                .setCompressionInfo(CompressedInfo.newBuilder().setEnabled(true).setRawSize(valueBytes.length)).build();
        CompletableFuture<Map<OrderedPayload, Payload>> future = new CompletableFuture<>();
        StreamBatchOrderedMapObserver observer = new StreamBatchOrderedMapObserver(future);
        observer.onNext(BatchValueResponse.newBuilder().addKeyOrdered(compressedKey)
                .addValueUnordered(compressedValue).build());
        observer.onCompleted();
        Map.Entry<OrderedPayload, Payload> entry = future.join().entrySet().iterator().next();
        assertArrayEquals(keyBytes, entry.getKey().getValue());
        assertArrayEquals(valueBytes, entry.getValue().getValue());
    }

    @Test
    void preservesForwardAndReverseOrderAcrossBatches() {
        for (List<Long> orders : List.of(List.of(1L, 2L, 3L), List.of(3L, 2L, 1L))) {
            CompletableFuture<Map<OrderedPayload, Payload>> future = new CompletableFuture<>();
            StreamBatchOrderedMapObserver observer = new StreamBatchOrderedMapObserver(future);
            BatchValueResponse.Builder first = BatchValueResponse.newBuilder();
            for (long order : orders.subList(0, 2)) {
                first.addKeyOrdered(key(new byte[]{(byte) order}, order)).addValueUnordered(value(new byte[]{7}));
            }
            observer.onNext(first.build());
            long last = orders.get(2);
            observer.onNext(BatchValueResponse.newBuilder().addKeyOrdered(key(new byte[]{(byte) last}, last))
                    .addValueUnordered(value(new byte[]{8})).build());
            observer.onCompleted();
            assertEquals(orders, future.join().keySet().stream().map(OrderedPayload::getOrder).toList());
        }
    }

    @Test
    void completesExceptionallyForUnpairedData() {
        CompletableFuture<Map<OrderedPayload, Payload>> future = new CompletableFuture<>();
        StreamBatchOrderedMapObserver observer = new StreamBatchOrderedMapObserver(future);
        observer.onNext(BatchValueResponse.newBuilder().addKeyOrdered(key(new byte[]{1}, 1)).build());
        observer.onCompleted();
        assertTrue(future.isCompletedExceptionally());
        assertInstanceOf(IllegalArgumentException.class, assertThrows(CompletionException.class, future::join).getCause());
    }

    @Test
    void completesExceptionallyForInvalidCompressedData() {
        CompletableFuture<Map<OrderedPayload, Payload>> future = new CompletableFuture<>();
        StreamBatchOrderedMapObserver observer = new StreamBatchOrderedMapObserver(future);
        OrderedKey invalid = key(new byte[]{1, 2, 3}, 1).toBuilder()
                .setCompressionInfo(CompressedInfo.newBuilder().setEnabled(true).setRawSize(10)).build();
        observer.onNext(BatchValueResponse.newBuilder().addKeyOrdered(invalid)
                .addValueUnordered(value(new byte[]{7})).build());
        observer.onCompleted();
        assertTrue(future.isCompletedExceptionally());
        assertThrows(CompletionException.class, future::join);
    }

    @Test
    void emptyStreamReturnsEmptyMap() {
        CompletableFuture<Map<OrderedPayload, Payload>> future = new CompletableFuture<>();
        new StreamBatchOrderedMapObserver(future).onCompleted();
        assertTrue(future.join().isEmpty());
    }
}
