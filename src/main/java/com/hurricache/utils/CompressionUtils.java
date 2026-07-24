package com.hurricache.utils;

import com.hurricache.grpc.BinaryPayload;
import com.hurricache.grpc.CompressedInfo;
import com.hurricache.grpc.Key;
import com.hurricache.grpc.KeyBinaryPayload;
import com.hurricache.grpc.UpdateValueResponse;
import com.hurricache.grpc.Value;
import com.hurricache.grpc.ValueResponse;
import com.google.protobuf.ByteString;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;

public class CompressionUtils {
    private static final LZ4Factory factory = LZ4Factory.fastestInstance();
    private static final int COMPRESSION_THRESHOLD = 1024; // 1KB


    public static Key.Builder compressKeyIfNeeded(byte[] data, Integer clientId) {
        KeyBinaryPayload.Builder payloadBuilder = KeyBinaryPayload.newBuilder();
        Key.Builder keyBuilder = Key.newBuilder();
        if (data.length > COMPRESSION_THRESHOLD) {
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
            payloadBuilder.setPayload(ByteString.copyFrom(data));
            payloadBuilder.setSize(data.length);
        }
        if (clientId != null) {
            keyBuilder.setClientId(clientId);
        }

        return keyBuilder.setPayload(payloadBuilder.build());
    }

    public static Value.Builder compressIfNeeded(byte[] data) {
        BinaryPayload.Builder payloadBuilder = BinaryPayload.newBuilder();
        Value.Builder valueBuilder = Value.newBuilder();

        if (data.length > COMPRESSION_THRESHOLD) {
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
            payloadBuilder.setPayload(ByteString.copyFrom(data));
            payloadBuilder.setSize(data.length);
        }

        return valueBuilder.setValue(payloadBuilder.build());
    }

    public static byte[] decompressIfNeeded(ValueResponse responseValue) {
        return decompressIfNeeded(responseValue.getValue());
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

}