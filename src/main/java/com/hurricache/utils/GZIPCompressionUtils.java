package com.hurricache.utils;

import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import com.hurricache.grpc.BinaryPayload;
import com.hurricache.grpc.CompressedInfo;
import com.hurricache.grpc.Key;
import com.hurricache.grpc.KeyBinaryPayload;
import com.hurricache.grpc.OrderedKey;
import com.hurricache.grpc.OrderedValue;
import com.hurricache.grpc.UpdateValueResponse;
import com.hurricache.grpc.Value;
import com.hurricache.grpc.ValueResponse;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public final class GZIPCompressionUtils {

    private GZIPCompressionUtils() {
        // Утилитный класс
    }

    private static byte[] compressGzip(byte[] data) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             GZIPOutputStream gzipOut = new GZIPOutputStream(baos)) {
            gzipOut.write(data);
            gzipOut.finish();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("GZIP compression failed", e);
        }
    }

    private static byte[] decompressGzip(ByteString data, int rawSize) {
        try (InputStream input = data.newInput();
             GZIPInputStream gzipIn = new GZIPInputStream(input)) {
            byte[] restored = new byte[rawSize];
            int totalRead = 0;
            while (totalRead < rawSize) {
                int read = gzipIn.read(restored, totalRead, rawSize - totalRead);
                if (read == -1) {
                    break;
                }
                totalRead += read;
            }
            return restored;
        } catch (IOException e) {
            throw new RuntimeException("GZIP decompression failed", e);
        }
    }

    private record PreparedPayload(ByteString payload, int size, CompressedInfo compressionInfo) {}

    private static PreparedPayload preparePayload(byte[] data, Integer compressionThreshold) {
        if (data != null && compressionThreshold != null && data.length > compressionThreshold) {
            byte[] compressed = compressGzip(data);
            ByteString payload = UnsafeByteOperations.unsafeWrap(compressed);
            CompressedInfo info = CompressedInfo.newBuilder()
                    .setEnabled(true)
                    .setRawSize(data.length)
                    .build();
            return new PreparedPayload(payload, compressed.length, info);
        } else {
            ByteString payload = data != null ? UnsafeByteOperations.unsafeWrap(data) : ByteString.EMPTY;
            return new PreparedPayload(payload, data != null ? data.length : 0, null);
        }
    }

    public static Key.Builder compressKeyIfNeeded(byte[] data, Integer clientId, Integer compressionThreshold) {
        PreparedPayload prepared = preparePayload(data, compressionThreshold);
        KeyBinaryPayload.Builder payloadBuilder = KeyBinaryPayload.newBuilder()
                .setPayload(prepared.payload())
                .setSize(prepared.size());

        Key.Builder keyBuilder = Key.newBuilder().setPayload(payloadBuilder.build());
        if (prepared.compressionInfo() != null) {
            keyBuilder.setCompressionInfo(prepared.compressionInfo());
        }
        if (clientId != null) {
            keyBuilder.setClientId(clientId);
        }
        return keyBuilder;
    }

    public static OrderedKey.Builder compressKeyIfNeeded(byte[] data,
                                                         long order,
                                                         Integer clientId,
                                                         Integer compressionThreshold) {
        PreparedPayload prepared = preparePayload(data, compressionThreshold);
        KeyBinaryPayload.Builder payloadBuilder = KeyBinaryPayload.newBuilder()
                .setPayload(prepared.payload())
                .setSize(prepared.size());

        OrderedKey.Builder keyBuilder = OrderedKey.newBuilder()
                .setOrder(order)
                .setPayload(payloadBuilder.build());

        if (prepared.compressionInfo() != null) {
            keyBuilder.setCompressionInfo(prepared.compressionInfo());
        }
        if (clientId != null) {
            keyBuilder.setClientId(clientId);
        }
        return keyBuilder;
    }

    public static Value.Builder compressIfNeeded(byte[] data, Integer compressionThreshold) {
        PreparedPayload prepared = preparePayload(data, compressionThreshold);
        BinaryPayload.Builder payloadBuilder = BinaryPayload.newBuilder()
                .setPayload(prepared.payload())
                .setSize(prepared.size());

        Value.Builder valueBuilder = Value.newBuilder().setValue(payloadBuilder.build());
        if (prepared.compressionInfo() != null) {
            valueBuilder.setCompressionInfo(prepared.compressionInfo());
        }
        return valueBuilder;
    }

    public static OrderedValue.Builder compressIfNeeded(byte[] data, long order, Integer compressionThreshold) {
        PreparedPayload prepared = preparePayload(data, compressionThreshold);
        BinaryPayload.Builder payloadBuilder = BinaryPayload.newBuilder()
                .setPayload(prepared.payload())
                .setSize(prepared.size());

        OrderedValue.Builder valueBuilder = OrderedValue.newBuilder()
                .setOrder(order)
                .setValue(payloadBuilder.build());

        if (prepared.compressionInfo() != null) {
            valueBuilder.setCompressionInfo(prepared.compressionInfo());
        }
        return valueBuilder;
    }

    public static byte[] decompressIfNeeded(ValueResponse responseValue) {
        return decompressIfNeeded(responseValue.getValueUnordered());
    }

    public static byte[] decompressIfNeeded(UpdateValueResponse responseValue) {
        return responseValue.getResult() ? decompressIfNeeded(responseValue.getValue()) : null;
    }

    public static byte[] decompressIfNeeded(Value responseValue) {
        BinaryPayload payload = responseValue.getValue();
        ByteString byteString = payload.getPayload();
        if (responseValue.hasCompressionInfo() && responseValue.getCompressionInfo().getEnabled()) {
            return decompressGzip(byteString, responseValue.getCompressionInfo().getRawSize());
        }

        // Zero-copy извлечение в массив байт без дублирования буфера, если это поддерживается
        if (byteString.isValidUtf8() || true) { // Напрямую через ByteBuffer
            ByteBuffer buffer = byteString.asReadOnlyByteBuffer();
            if (buffer.hasArray() && buffer.arrayOffset() == 0 && buffer.array().length == byteString.size()) {
                return buffer.array();
            }
        }
        return byteString.toByteArray();
    }

    public static byte[] decompressIfNeeded(OrderedValue responseValue) {
        BinaryPayload payload = responseValue.getValue();
        ByteString byteString = payload.getPayload();
        if (responseValue.hasCompressionInfo() && responseValue.getCompressionInfo().getEnabled()) {
            return decompressGzip(byteString, responseValue.getCompressionInfo().getRawSize());
        }

        ByteBuffer buffer = byteString.asReadOnlyByteBuffer();
        if (buffer.hasArray() && buffer.arrayOffset() == 0 && buffer.array().length == byteString.size()) {
            return buffer.array();
        }
        return byteString.toByteArray();
    }
}