package com.hurricache.client;

import com.google.protobuf.UnsafeByteOperations;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.OrderedVariant;
import com.hurricache.client.intf.Variant;
import com.hurricache.grpc.BinaryPayload;
import com.hurricache.grpc.CreateRequest;
import com.hurricache.grpc.GetRequest;
import com.hurricache.grpc.Key;
import com.hurricache.grpc.KeyHint;
import com.hurricache.grpc.OrderedKey;
import com.hurricache.grpc.OrderedValue;
import com.hurricache.grpc.Value;
import com.hurricache.grpc.ValueResponse;
import com.hurricache.grpc.ValueType;
import com.hurricache.utils.CompressionUtils;

import java.time.Duration;

public class KeyValueUtils {

    public static  Variant toVariant(ValueResponse resp) {
        if (!resp.hasValueUnordered()) return Variant.none();
        Value val = resp.getValueUnordered();
        ValueType vt = val.hasValueType() ? val.getValueType() : ValueType.AUTO;
        BinaryPayload bp = val.getValue();
        switch (vt) {
            case INT: return Variant.ofInt(bp.getIntPayload());
            case DOUBLE: return Variant.ofDouble(bp.getDoublePayload());
            case BYTES: case AUTO: default:
                if (bp.hasIntPayload()) return Variant.ofInt(bp.getIntPayload());
                if (bp.hasDoublePayload()) return Variant.ofDouble(bp.getDoublePayload());
                if (bp.hasPayload()) return Variant.ofBytes(bp.getPayload().toByteArray());
                return Variant.none();
        }
    }

    public static  Key buildKey(byte[] key, KeyHintData hint, int clientId, int compressionThreshold ) {
        return KeyValueUtils.createUnorderedKey(key, hint, clientId, compressionThreshold).build();
    }

    public static  GetRequest buildGetReq(byte[] key, KeyHintData hint, int clientId, int compressionThreshold) {
        return GetRequest.newBuilder().setKey(buildKey(key, hint, clientId,compressionThreshold)).build();
    }

    public static   CreateRequest buildAtomicIntCreateRequest(byte[] key, KeyHintData hint, long value, Duration ttl, int clientId,
                                                              int compressionThreshold) {
        Value.Builder vb = Value.newBuilder()
                .setValue(BinaryPayload.newBuilder().setIntPayload(value))
                .setValueType(ValueType.INT);
        if (ttl != null && !ttl.isZero()) vb.setTtl(System.currentTimeMillis() + ttl.toMillis());
        return CreateRequest.newBuilder().setKey(buildKey(key, hint, clientId,compressionThreshold)).setValue(vb).build();
    }

    public static   CreateRequest buildAtomicIntCreateRequest(byte[] key, KeyHintData hint, double value, Duration ttl, int clientId,
                                                              int compressionThreshold) {
        Value.Builder vb = Value.newBuilder()
                .setValue(BinaryPayload.newBuilder().setDoublePayload(value))
                .setValueType(ValueType.DOUBLE);
        if (ttl != null && !ttl.isZero()) vb.setTtl(System.currentTimeMillis() + ttl.toMillis());
        return CreateRequest.newBuilder().setKey(buildKey(key, hint, clientId,compressionThreshold)).setValue(vb).build();
    }

    public static   Value buildValueFromVariant(Variant variant, Duration ttl) {
        Value.Builder b = switch (variant.type()) {
            case INT -> Value.newBuilder()
                    .setValue(BinaryPayload.newBuilder().setIntPayload(variant.longValue()))
                    .setValueType(ValueType.INT);
            case DOUBLE -> Value.newBuilder()
                    .setValue(BinaryPayload.newBuilder().setDoublePayload(variant.doubleValue()))
                    .setValueType(ValueType.DOUBLE);
            default -> Value.newBuilder()
                    .setValue(BinaryPayload.newBuilder()
                                      .setPayload(UnsafeByteOperations.unsafeWrap(variant.bytesValue())))
                    .setValueType(ValueType.BYTES);
        };
        if (ttl != null && !ttl.isZero()) b.setTtl(System.currentTimeMillis() + ttl.toMillis());
        return b.build();
    }

    /**
     * Builds a Value.Builder from a Variant.
     * Compression is only applied for BYTES type; INT/DOUBLE are stored directly.
     */
    public static Value.Builder buildValueFromVariantCompressed(Variant variant, Integer compressionThreshold) {
        Value.Builder builder;

        switch (variant.type()) {
            case INT:
                builder = Value.newBuilder()
                        .setValue(BinaryPayload.newBuilder().setIntPayload(variant.longValue()))
                        .setValueType(ValueType.INT);
                break;

            case DOUBLE:
                builder = Value.newBuilder()
                        .setValue(BinaryPayload.newBuilder().setDoublePayload(variant.doubleValue()))
                        .setValueType(ValueType.DOUBLE);
                break;

            case BYTES:
                // Apply compression only for BYTES type
                builder = CompressionUtils.compressIfNeeded(variant.bytesValue(), compressionThreshold);
                builder.setValueType(ValueType.BYTES);
                break;

            default:
                builder = Value.newBuilder()
                        .setValueType(ValueType.AUTO);
                break;
        }

        return builder;
    }
    public static Key.Builder createUnorderedKey(byte[] keyStr, int clientId, Integer compressionThreshold) {
        return createUnorderedKey(keyStr, null, clientId,compressionThreshold );
    }

    /**
     * Creates a Key using a pre-calculated KeyHint (Strong/Week hashes).
     */
    public static Key.Builder createUnorderedKey(byte[] key, KeyHintData hint, int clientId, Integer compressionThreshold) {
        Key.Builder builder = CompressionUtils.compressKeyIfNeeded(key, clientId,compressionThreshold);
            if (hint != null) {
                KeyHint.Builder khBuilder = KeyHint.newBuilder();
                if (hint.hasWeekHash()) khBuilder.setWeakHash(hint.getWeek_hash());
                if (hint.hasStrongHash()) khBuilder.setStrongHash(hint.getStrong_hash());
                builder.setKeyHint(khBuilder);
        }
        return builder;
    }

    public static OrderedKey.Builder createOrderedKey(byte[] keyStr,KeyHintData hint,long order, int clientId, Integer compressionThreshold) {
        OrderedKey.Builder builder = CompressionUtils.compressKeyIfNeeded(keyStr,order, clientId,compressionThreshold);
        if (hint != null) {
            KeyHint.Builder khBuilder = KeyHint.newBuilder();
            if (hint.hasWeekHash()) khBuilder.setWeakHash(hint.getWeek_hash());
            if (hint.hasStrongHash()) khBuilder.setStrongHash(hint.getStrong_hash());
            builder.setKeyHint(khBuilder);
        }
        return builder;
    }

    public static OrderedKey.Builder createOrderedKey(byte[] keyStr,long order, int clientId, Integer compressionThreshold) {
        return createOrderedKey(keyStr,null,order,clientId,compressionThreshold);
    }


    /**
     * Helper to wrap raw bytes into a Protobuf Value object.
     */
    public static Value.Builder createUnorderedValue(byte[] data, Duration ttl, Integer compressionThreshold) {
        Value.Builder builder = CompressionUtils.compressIfNeeded(data,compressionThreshold);
        if (ttl != null && !ttl.isZero()) builder.setTtl(System.currentTimeMillis() + ttl.toMillis());
        return builder;
    }

    public static OrderedValue.Builder createOrderedValue(byte[] data,long order,Duration ttl,Integer compressionThreshold) {
        OrderedValue.Builder builder = CompressionUtils.compressIfNeeded(data,order,compressionThreshold);
        if (ttl != null && !ttl.isZero()) builder.setTtl(System.currentTimeMillis() + ttl.toMillis());
        builder.setOrder(order);
        return builder;
    }

    /**
     * Builds an OrderedValue.Builder from an OrderedVariant.
     * Compression is only applied for BYTES type; INT/DOUBLE are stored directly.
     */
    public static OrderedValue.Builder buildOrderedValueFromVariant(OrderedVariant orderedVariant, Duration ttl, Integer compressionThreshold) {
        Variant variant = orderedVariant.variant();
        long order = orderedVariant.getOrder() != null ? orderedVariant.getOrder() : 0L;
        OrderedValue.Builder builder;

        switch (variant.type()) {
            case INT:
                builder = OrderedValue.newBuilder()
                        .setValue(BinaryPayload.newBuilder().setIntPayload(variant.longValue()))
                        .setValueType(ValueType.INT)
                        .setOrder(order);
                break;

            case DOUBLE:
                builder = OrderedValue.newBuilder()
                        .setValue(BinaryPayload.newBuilder().setDoublePayload(variant.doubleValue()))
                        .setValueType(ValueType.DOUBLE)
                        .setOrder(order);
                break;

            case BYTES:
                // Apply compression only for BYTES type
                builder = CompressionUtils.compressIfNeeded(variant.bytesValue(), order, compressionThreshold);
                builder.setValueType(ValueType.BYTES);
                break;

            default:
                builder = OrderedValue.newBuilder()
                        .setValueType(ValueType.AUTO)
                        .setOrder(order);
                break;
        }

        if (ttl != null && !ttl.isZero()) {
            builder.setTtl(System.currentTimeMillis() + ttl.toMillis());
        }

        return builder;
    }

    private static final long PRIME_0 = 0x9E3779B97F4A7C15L;
    private static final long PRIME_1 = 0xBF58476D1CE4E5B9L;

    public static int weakHash(byte[] data) {
        int seed = 0;
        int len = data.length;
        long a = seed ^ PRIME_0;
        long b = len ^ PRIME_1;
        int offset = 0;

        // Process 64-bit blocks
        while (len >= 8) {
            long val = readLongLE(data, offset);

            // Simulating mux64 (Multiply and get high/low 64 bits)
            long[] result = mux64(val ^ a, PRIME_0);
            long low = result[0];
            long high = result[1];

            a ^= high;
            b += low;

            offset += 8;
            len -= 8;
        }

        // Handle tails
        if (len > 4) {
            int tail = readIntLE(data, offset);
            b ^= (tail & 0xFFFFFFFFL); // Treat as unsigned
            offset += 4;
            len -= 4;
        }

        if (len > 2) {
            int tail = readShortLE(data, offset);
            b ^= (tail & 0xFFFFL);
            offset += 2;
            len -= 2;
        }

        if (len >= 1) {
            int tail = data[offset] & 0xFF;
            b ^= tail;
        }

        // Final mix
        long[] finalResult = mux64(a ^ Long.rotateLeft(b, 17), PRIME_1);
        long finalLow = finalResult[0];
        long finalHigh = finalResult[1];

        return (int) (finalLow ^ finalHigh);
    }

    /**
     * Simulates the mux64 (multiplication producing 128-bit result)
     */
    private static long[] mux64(long a, long b) {
        // In Java 9+, we can use Math.multiplyHigh for the high 64 bits.
        long low = a * b;
        long high = Math.multiplyHigh(a, b);
        return new long[]{low, high};
    }

    // Helper methods to read Little-Endian values from byte array
    private static long readLongLE(byte[] data, int offset) {
        return ((long) (data[offset] & 0xFF))
               | ((long) (data[offset + 1] & 0xFF) << 8)
               | ((long) (data[offset + 2]
                          & 0xFF) << 16)
               | ((long) (data[offset + 3] & 0xFF) << 24)
               | ((long) (data[offset + 4] & 0xFF) << 32)
               | ((long) (data[offset + 5] & 0xFF) << 40)
               | ((long) (data[offset + 6] & 0xFF) << 48)
               | ((long) (data[offset + 7] & 0xFF) << 56);
    }

    private static int readIntLE(byte[] data, int offset) {
        return (data[offset] & 0xFF)
               | ((data[offset + 1] & 0xFF) << 8)
               | ((data[offset + 2] & 0xFF) << 16)
               | ((data[offset + 3] & 0xFF) << 24);
    }

    private static int readShortLE(byte[] data, int offset) {
        return (data[offset] & 0xFF) | ((data[offset + 1] & 0xFF) << 8);
    }
}