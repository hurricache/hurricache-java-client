package com.hurricache.utils;

import com.hurricache.grpc.Key;
import com.hurricache.grpc.OrderedValue;
import com.hurricache.grpc.UpdateValueResponse;
import com.hurricache.grpc.Value;
import com.hurricache.grpc.ValueResponse;

public class CompressionUtils {

    private static final String PROPERTY_NAME = "hurricache.compression";
    private static final boolean IS_GZIP;

    static {
        // Читаем проперти, по умолчанию можно использовать lz4 (или gzip, как вам привычнее)
        String compressionType = System.getProperty(PROPERTY_NAME, "gzip");
        IS_GZIP = "gzip".equalsIgnoreCase(compressionType) || "deflate".equalsIgnoreCase(compressionType);
    }

    public static Key.Builder compressKeyIfNeeded(byte[] data, Integer clientId, Integer compressionThreshold) {
        if (IS_GZIP) {
            return GZIPCompressionUtils.compressKeyIfNeeded(data, clientId, compressionThreshold);
        } else {
            return LZ4CompressionUtils.compressKeyIfNeeded(data, clientId, compressionThreshold);
        }
    }

    public static Value.Builder compressIfNeeded(byte[] data, Integer compressionThreshold) {
        if (IS_GZIP) {
            return GZIPCompressionUtils.compressIfNeeded(data, compressionThreshold);
        } else {
            return LZ4CompressionUtils.compressIfNeeded(data, compressionThreshold);
        }
    }

    public static byte[] decompressIfNeeded(ValueResponse responseValue) {
        if (IS_GZIP) {
            return GZIPCompressionUtils.decompressIfNeeded(responseValue);
        } else {
            return LZ4CompressionUtils.decompressIfNeeded(responseValue);
        }
    }

    public static byte[] decompressIfNeeded(UpdateValueResponse responseValue) {
        if (IS_GZIP) {
            return GZIPCompressionUtils.decompressIfNeeded(responseValue);
        } else {
            return LZ4CompressionUtils.decompressIfNeeded(responseValue);
        }
    }

    public static byte[] decompressIfNeeded(Value responseValue) {
        if (IS_GZIP) {
            return GZIPCompressionUtils.decompressIfNeeded(responseValue);
        } else {
            return LZ4CompressionUtils.decompressIfNeeded(responseValue);
        }
    }

    public static byte[] decompressIfNeeded(OrderedValue responseValue) {
        if (IS_GZIP) {
            return GZIPCompressionUtils.decompressIfNeeded(responseValue);
        } else {
            return LZ4CompressionUtils.decompressIfNeeded(responseValue);
        }
    }
}