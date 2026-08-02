package com.hurricache.client;

import com.google.protobuf.ByteString;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.grpc.BinaryPayload;
import com.hurricache.grpc.Key;
import com.hurricache.grpc.KeyBinaryPayload;
import com.hurricache.grpc.KeyHint;
import com.hurricache.grpc.OrderedKey;
import com.hurricache.grpc.OrderedValue;
import com.hurricache.grpc.Value;
import com.hurricache.utils.CompressionUtils;

import java.time.Duration;

public class KeyValueUtils {

    public static Key.Builder createUnorderedKey(byte[] keyStr, int clientId) {
        return createUnorderedKey(keyStr, null, clientId);
    }

    /**
     * Creates a Key using a pre-calculated KeyHint (Strong/Week hashes).
     */
    public static Key.Builder createUnorderedKey(byte[] key, KeyHintData hint, int clientId) {
        Key.Builder builder = CompressionUtils.compressKeyIfNeeded(key, clientId);
            if (hint != null) {
                KeyHint.Builder khBuilder = KeyHint.newBuilder();
                if (hint.hasWeekHash()) khBuilder.setWeekHash(hint.getWeek_hash());
                if (hint.hasStrongHash()) khBuilder.setStrongHash(hint.getStrong_hash());
                builder.setKeyHint(khBuilder);
        }
        return builder;
    }

    public static OrderedKey.Builder createOrderedKey(byte[] keyStr,KeyHintData hint,long order, int clientId) {
        OrderedKey.Builder builder = OrderedKey.newBuilder();
        if (hint != null) {
            KeyHint.Builder khBuilder = KeyHint.newBuilder();
            if (hint.hasWeekHash()) khBuilder.setWeekHash(hint.getWeek_hash());
            if (hint.hasStrongHash()) khBuilder.setStrongHash(hint.getStrong_hash());
            builder.setKeyHint(khBuilder);
        }
        builder.setOrder(order).setClientId(clientId);
        builder.setPayload(KeyBinaryPayload.newBuilder().setPayload(ByteString.copyFrom(keyStr)).setSize(keyStr.length));
        return builder;
    }

    public static OrderedKey.Builder createOrderedKey(byte[] keyStr,long order, int clientId) {
        return createOrderedKey(keyStr,null,order,clientId);
    }


    /**
     * Helper to wrap raw bytes into a Protobuf Value object.
     */
    public static Value.Builder createUnorderedValue(byte[] data, Duration ttl) {
        Value.Builder builder = CompressionUtils.compressIfNeeded(data);
        if (ttl != null && !ttl.isZero()) builder.setTtl(System.currentTimeMillis() + ttl.toMillis());
        return builder;
    }

    public static OrderedValue.Builder createOrderedValue(byte[] data,long order,Duration ttl) {
        OrderedValue.Builder builder = OrderedValue.newBuilder();
        builder.setValue(BinaryPayload.newBuilder().setSize(data.length).setPayload(ByteString.copyFrom(data)).build());
        if (ttl != null && !ttl.isZero()) builder.setTtl(System.currentTimeMillis() + ttl.toMillis());
        builder.setOrder(order);
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