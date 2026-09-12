package com.hurricache.utils;

import com.hurricache.grpc.BinaryPayload;
import com.hurricache.grpc.CompressedInfo;
import com.hurricache.grpc.Key;
import com.hurricache.grpc.KeyBinaryPayload;
import com.hurricache.grpc.OrderedKey;
import com.hurricache.grpc.OrderedValue;
import com.hurricache.grpc.UpdateValueResponse;
import com.hurricache.grpc.Value;
import com.hurricache.grpc.ValueResponse;
import com.google.protobuf.ByteString;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;


public class LZ4CompressionUtils {
    private static final LZ4Factory factory = LZ4Factory.fastestInstance();


    public static Key.Builder compressKeyIfNeeded(byte[] data, Integer clientId, Integer compressionThreshold) {
        KeyBinaryPayload.Builder payloadBuilder = KeyBinaryPayload.newBuilder();
        Key.Builder keyBuilder = Key.newBuilder();
        if (data != null && compressionThreshold != null &&data.length > compressionThreshold) {
            LZ4Compressor compressor = factory.fastCompressor();
            int maxCompressedLength = compressor.maxCompressedLength(data.length);
            byte[] compressed = new byte[maxCompressedLength];
            int compressedLength = compressor.compress(data, 0, data.length, compressed, 0, maxCompressedLength);

            payloadBuilder.setPayload(ByteString.copyFrom(compressed, 0, compressedLength));
            payloadBuilder.setSize(compressedLength);

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
        if (data != null && compressionThreshold != null &&data.length > compressionThreshold) {
            LZ4Compressor compressor = factory.fastCompressor();
            int maxCompressedLength = compressor.maxCompressedLength(data.length);
            byte[] compressed = new byte[maxCompressedLength];
            int compressedLength = compressor.compress(data, 0, data.length, compressed, 0, maxCompressedLength);

            payloadBuilder.setPayload(ByteString.copyFrom(compressed, 0, compressedLength));
            payloadBuilder.setSize(compressedLength);

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

        if (data != null && compressionThreshold != null &&data.length > compressionThreshold ) {
            LZ4Compressor compressor = factory.fastCompressor();
            int maxCompressedLength = compressor.maxCompressedLength(data.length);
            byte[] compressed = new byte[maxCompressedLength];
            int compressedLength = compressor.compress(data, 0, data.length, compressed, 0, maxCompressedLength);

            payloadBuilder.setPayload(ByteString.copyFrom(compressed, 0, compressedLength));
            payloadBuilder.setSize(compressedLength);

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
            LZ4SafeDecompressor decompressor = factory.safeDecompressor();
            byte[] restored = new byte[rawSize];
            decompressor.decompress(data, 0, data.length, restored, 0);
            return restored;
        }
        return data;
    }

    public static byte[] decompressIfNeeded(OrderedValue responseValue) {
        BinaryPayload payload = responseValue.getValue();
        byte[] data = payload.getPayload().toByteArray();

        if (responseValue.hasCompressionInfo() && responseValue.getCompressionInfo().getEnabled()) {
            int rawSize = responseValue.getCompressionInfo().getRawSize();
            LZ4SafeDecompressor decompressor = factory.safeDecompressor();
            byte[] restored = new byte[rawSize];
            decompressor.decompress(data, 0, data.length, restored, 0);
            return restored;
        }
        return data;
    }

    public static OrderedValue.Builder compressIfNeeded(byte[] data, long order, Integer compressionThreshold) {
        BinaryPayload.Builder payloadBuilder = BinaryPayload.newBuilder();
        OrderedValue.Builder valueBuilder = OrderedValue.newBuilder();

        if (data != null && compressionThreshold != null &&data.length > compressionThreshold ) {
            LZ4Compressor compressor = factory.fastCompressor();
            int maxCompressedLength = compressor.maxCompressedLength(data.length);
            byte[] compressed = new byte[maxCompressedLength];
            int compressedLength = compressor.compress(data, 0, data.length, compressed, 0, maxCompressedLength);

            payloadBuilder.setPayload(ByteString.copyFrom(compressed, 0, compressedLength));
            payloadBuilder.setSize(compressedLength);

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