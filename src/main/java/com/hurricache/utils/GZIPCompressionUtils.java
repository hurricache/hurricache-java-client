package com.hurricache.utils;

import com.google.protobuf.ByteString;
import com.hurricache.grpc.BinaryPayload;
import com.hurricache.grpc.CompressedInfo;
import com.hurricache.grpc.Key;
import com.hurricache.grpc.KeyBinaryPayload;
import com.hurricache.grpc.OrderedKey;
import com.hurricache.grpc.OrderedValue;
import com.hurricache.grpc.UpdateValueResponse;
import com.hurricache.grpc.Value;
import com.hurricache.grpc.ValueResponse;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class GZIPCompressionUtils {
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

    private static byte[] decompressGzip(byte[] data, int rawSize) {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(data);
             GZIPInputStream gzipIn = new GZIPInputStream(bais)) {
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

    public static Key.Builder compressKeyIfNeeded(byte[] data, Integer clientId, Integer compressionThreshold) {
        KeyBinaryPayload.Builder payloadBuilder = KeyBinaryPayload.newBuilder();
        Key.Builder keyBuilder = Key.newBuilder();
        if (data != null && compressionThreshold != null && data.length > compressionThreshold) {
            byte[] compressed = compressGzip(data);

            payloadBuilder.setPayload(ByteString.copyFrom(compressed, 0, compressed.length));
            payloadBuilder.setSize(compressed.length);

            keyBuilder.setCompressionInfo(CompressedInfo.newBuilder()
                                                  .setEnabled(true)
                                                  .setRawSize(data.length)
                                                  .build());
        } else {
            payloadBuilder.setPayload(data != null ? ByteString.copyFrom(data) : ByteString.EMPTY);
            payloadBuilder.setSize(data != null ? data.length : 0);
        }
        if (clientId != null) {
            keyBuilder.setClientId(clientId);
        }

        return keyBuilder.setPayload(payloadBuilder.build());
    }
    public static OrderedKey.Builder compressKeyIfNeeded(byte[] data,
                                                         long order,
                                                         Integer clientId,
                                                         Integer compressionThreshold) {
        KeyBinaryPayload.Builder payloadBuilder = KeyBinaryPayload.newBuilder();
        OrderedKey.Builder keyBuilder = OrderedKey.newBuilder();
        if (data != null && compressionThreshold != null && data.length > compressionThreshold) {
            byte[] compressed = compressGzip(data);

            payloadBuilder.setPayload(ByteString.copyFrom(compressed, 0, compressed.length));
            payloadBuilder.setSize(compressed.length);

            keyBuilder.setCompressionInfo(CompressedInfo.newBuilder()
                                                  .setEnabled(true)
                                                  .setRawSize(data.length)
                                                  .build());
        } else {
            payloadBuilder.setPayload(data != null ? ByteString.copyFrom(data) : ByteString.EMPTY);
            payloadBuilder.setSize(data != null ? data.length : 0);
        }
        if (clientId != null) {
            keyBuilder.setClientId(clientId);
        }

        return keyBuilder.setOrder(order).setPayload(payloadBuilder.build());
    }
    public static Value.Builder compressIfNeeded(byte[] data, Integer compressionThreshold) {
        BinaryPayload.Builder payloadBuilder = BinaryPayload.newBuilder();
        Value.Builder valueBuilder = Value.newBuilder();

        if (data != null && compressionThreshold != null && data.length > compressionThreshold) {
            byte[] compressed = compressGzip(data);

            payloadBuilder.setPayload(ByteString.copyFrom(compressed, 0, compressed.length));
            payloadBuilder.setSize(compressed.length);

            valueBuilder.setCompressionInfo(CompressedInfo.newBuilder()
                                                    .setEnabled(true)
                                                    .setRawSize(data.length)
                                                    .build());
        } else {
            payloadBuilder.setPayload(data != null ? ByteString.copyFrom(data) : ByteString.EMPTY);
            payloadBuilder.setSize(data != null ? data.length : 0);
        }

        return valueBuilder.setValue(payloadBuilder.build());
    }


    public static byte[] decompressIfNeeded(ValueResponse responseValue) {
        return decompressIfNeeded(responseValue.getValueUnordered());
    }

    public static byte[] decompressIfNeeded(UpdateValueResponse responseValue) {
        if (responseValue.getResult()) {
            return decompressIfNeeded(responseValue.getValue());
        } else {
            return null;
        }
    }

    public static byte[] decompressIfNeeded(Value responseValue) {
        BinaryPayload payload = responseValue.getValue();
        byte[] data = payload.getPayload().toByteArray();

        if (responseValue.hasCompressionInfo() && responseValue.getCompressionInfo().getEnabled()) {
            int rawSize = responseValue.getCompressionInfo().getRawSize();
            return decompressGzip(data, rawSize);
        }
        return data;
    }

    public static byte[] decompressIfNeeded(OrderedValue responseValue) {
        BinaryPayload payload = responseValue.getValue();
        byte[] data = payload.getPayload().toByteArray();

        if (responseValue.hasCompressionInfo() && responseValue.getCompressionInfo().getEnabled()) {
            int rawSize = responseValue.getCompressionInfo().getRawSize();
            return decompressGzip(data, rawSize);
        }
        return data;
    }

    public static OrderedValue.Builder compressIfNeeded(byte[] data, long order, Integer compressionThreshold) {
        BinaryPayload.Builder payloadBuilder = BinaryPayload.newBuilder();
        OrderedValue.Builder valueBuilder = OrderedValue.newBuilder();

        if (data != null && compressionThreshold != null && data.length > compressionThreshold) {
            byte[] compressed = compressGzip(data);

            payloadBuilder.setPayload(ByteString.copyFrom(compressed, 0, compressed.length));
            payloadBuilder.setSize(compressed.length);

            valueBuilder.setCompressionInfo(CompressedInfo.newBuilder()
                                                    .setEnabled(true)
                                                    .setRawSize(data.length)
                                                    .build());
        } else {
            payloadBuilder.setPayload(data != null ? ByteString.copyFrom(data) : ByteString.EMPTY);
            payloadBuilder.setSize(data != null ? data.length : 0);
        }
        valueBuilder.setOrder(order);
        return valueBuilder.setValue(payloadBuilder.build());
    }


}