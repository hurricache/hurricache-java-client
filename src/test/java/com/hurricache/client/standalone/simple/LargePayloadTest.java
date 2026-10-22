package com.hurricache.client.standalone.simple;

import com.hurricache.TestBase;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Variant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Large payload and binary boundary tests.
 */
public class LargePayloadTest extends TestBase {

    private static final int OWNER_CLIENT_ID = 100;

    // =========================================================================
    // LARGE KEYS
    // =========================================================================

    @Test
    @DisplayName("Very long key (1KB)")
    void testLongKey1KB() throws ExecutionException, InterruptedException {
        String longKey = "k".repeat(1024);
        String key = "edge_long_key_1kb_" + UUID.randomUUID() + "_" + longKey;

        KeyHintData hint = client.create(key, v("value")).get();
        // If server accepts it, verify we can read it back
        Variant value = client.get(key).get();
        assertNotNull(value);
        client.remove(key).get();
    }

    @Test
    @DisplayName("Very long key (10KB)")
    void testLongKey10KB() throws ExecutionException, InterruptedException {
        String longKey = "k".repeat(10 * 1024);
        String key = "edge_long_key_10kb_" + UUID.randomUUID() + "_" + longKey;

        KeyHintData hint = client.create(key, v("value")).get();
        Variant value = client.get(key).get();
        assertNotNull(value);
        client.remove(key).get();
    }

    // =========================================================================
    // BINARY KEY VALUES
    // =========================================================================

    @Test
    @DisplayName("Key with null bytes")
    void testKeyWithNullBytes() throws ExecutionException, InterruptedException {
        String key = "edge_null_byte_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, v("value")).get();
        Variant value = client.get(key).get();
        assertNotNull(value);
        client.remove(key).get();
    }

    @Test
    @DisplayName("Key with special UTF-8 characters")
    void testKeyWithUTF8() throws ExecutionException, InterruptedException {
        String key = "edge_utf8_" + UUID.randomUUID() + "_тест_🔥_中文";

        KeyHintData hint = client.create(key, v("value")).get();
        Variant value = client.get(key).get();
        assertNotNull(value);
        client.remove(key).get();
    }

    // =========================================================================
    // LARGE VALUES
    // =========================================================================

    @Test
    @DisplayName("Large bytes value (1KB)")
    void testLargeValue1KB() throws ExecutionException, InterruptedException {
        String key = "edge_large_value_1kb_" + UUID.randomUUID();
        byte[] largeData = new byte[1024];
        for (int i = 0; i < largeData.length; i++) {
            largeData[i] = (byte) (i % 256);
        }

        KeyHintData hint = client.create(key, Variant.ofBytes(largeData)).get();
        Variant value = client.get(key).get();
        assertNotNull(value);
        assertArrayEquals(largeData, value.bytesValue());
        client.remove(key).get();
    }

    @Test
    @DisplayName("Large bytes value (10KB)")
    void testLargeValue10KB() throws ExecutionException, InterruptedException {
        String key = "edge_large_value_10kb_" + UUID.randomUUID();
        byte[] largeData = new byte[10 * 1024];
        for (int i = 0; i < largeData.length; i++) {
            largeData[i] = (byte) (i % 256);
        }

        KeyHintData hint = client.create(key, Variant.ofBytes(largeData)).get();
        Variant value = client.get(key).get();
        assertNotNull(value);
        assertArrayEquals(largeData, value.bytesValue());
        client.remove(key).get();
    }

    @Test
    @DisplayName("Large bytes value (100KB)")
    void testLargeValue100KB() throws ExecutionException, InterruptedException {
        String key = "edge_large_value_100kb_" + UUID.randomUUID();
        byte[] largeData = new byte[100 * 1024];
        for (int i = 0; i < largeData.length; i++) {
            largeData[i] = (byte) (i % 256);
        }

        KeyHintData hint = client.create(key, Variant.ofBytes(largeData)).get();
        Variant value = client.get(key).get();
        assertNotNull(value);
        assertArrayEquals(largeData, value.bytesValue());
        client.remove(key).get();
    }

    // =========================================================================
    // BINARY BOUNDARY VALUES
    // =========================================================================

    @Test
    @DisplayName("Value with all byte values 0x00-0xFF")
    void testAllByteValues() throws ExecutionException, InterruptedException {
        String key = "edge_all_bytes_" + UUID.randomUUID();
        byte[] allBytes = new byte[256];
        for (int i = 0; i < 256; i++) {
            allBytes[i] = (byte) i;
        }

        KeyHintData hint = client.create(key, Variant.ofBytes(allBytes)).get();
        Variant value = client.get(key).get();
        assertNotNull(value);
        assertArrayEquals(allBytes, value.bytesValue());
        client.remove(key).get();
    }

    @Test
    @DisplayName("Value with high bytes (0x80-0xFF)")
    void testHighBytes() throws ExecutionException, InterruptedException {
        String key = "edge_high_bytes_" + UUID.randomUUID();
        byte[] highBytes = new byte[128];
        for (int i = 0; i < 128; i++) {
            highBytes[i] = (byte) (0x80 + i);
        }

        KeyHintData hint = client.create(key, Variant.ofBytes(highBytes)).get();
        Variant value = client.get(key).get();
        assertNotNull(value);
        assertArrayEquals(highBytes, value.bytesValue());
        client.remove(key).get();
    }

    @Test
    @DisplayName("Value with mixed null bytes")
    void testMixedNullBytes() throws ExecutionException, InterruptedException {
        String key = "edge_mixed_null_" + UUID.randomUUID();
        byte[] data = new byte[100];
        // Null bytes at various positions
        data[0] = 0;
        data[50] = 0;
        data[99] = 0;
        data[10] = 'A';
        data[60] = 'B';

        KeyHintData hint = client.create(key, Variant.ofBytes(data)).get();
        Variant value = client.get(key).get();
        assertNotNull(value);
        assertArrayEquals(data, value.bytesValue());
        client.remove(key).get();
    }

    // =========================================================================
    // EMPTY VALUES
    // =========================================================================

    @Test
    @DisplayName("Empty bytes value")
    void testEmptyBytesValue() throws ExecutionException, InterruptedException {
        String key = "edge_empty_value_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, Variant.ofBytes(new byte[0])).get();
        Variant value = client.get(key).get();
        assertNotNull(value);
        assertTrue(value.isBytes());
        assertArrayEquals(new byte[0], value.bytesValue());
        client.remove(key).get();
    }

    // =========================================================================
    // LARGE CONTAINER ELEMENTS
    // =========================================================================

    @Test
    @DisplayName("List with large elements")
    void testListWithLargeElements() throws ExecutionException, InterruptedException {
        String key = "edge_list_large_" + UUID.randomUUID();
        byte[] largeData = new byte[1024];
        for (int i = 0; i < largeData.length; i++) {
            largeData[i] = (byte) (i % 256);
        }

        List<Variant> data = List.of(
                Variant.ofBytes(largeData),
                Variant.ofBytes(largeData),
                Variant.ofBytes(largeData)
        );

        KeyHintData hint = client.createList(key, data).get();
        assertNotNull(hint);

        // Verify by reading head
        Variant head = client.peekHead(key).get();
        assertNotNull(head);
        assertArrayEquals(largeData, head.bytesValue());
        client.remove(key).get();
    }

    @Test
    @DisplayName("Vector with large elements")
    void testVectorWithLargeElements() throws ExecutionException, InterruptedException {
        String key = "edge_vector_large_" + UUID.randomUUID();
        byte[] largeData = new byte[512];
        for (int i = 0; i < largeData.length; i++) {
            largeData[i] = (byte) (i % 256);
        }

        List<Variant> data = List.of(
                Variant.ofBytes(largeData),
                Variant.ofBytes(largeData)
        );

        KeyHintData hint = client.createVector(key, data).get();
        assertNotNull(hint);

        Variant element = client.getAt(key, 0).get();
        assertNotNull(element);
        assertArrayEquals(largeData, element.bytesValue());
        client.remove(key).get();
    }

    // =========================================================================
    // SPECIAL FLOATING POINT VALUES
    // =========================================================================

    @Test
    @DisplayName("Double with NaN")
    void testDoubleNaN() throws ExecutionException, InterruptedException {
        String key = "edge_double_nan_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, Variant.ofDouble(Double.NaN)).get();
        Variant value = client.get(key).get();
        assertNotNull(value);
        assertTrue(Double.isNaN(value.doubleValue()));
        client.remove(key).get();
    }

    @Test
    @DisplayName("Double with positive infinity")
    void testDoublePositiveInfinity() throws ExecutionException, InterruptedException {
        String key = "edge_double_pos_inf_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, Variant.ofDouble(Double.POSITIVE_INFINITY)).get();
        Variant value = client.get(key).get();
        assertNotNull(value);
        assertTrue(Double.isInfinite(value.doubleValue()));
        client.remove(key).get();
    }

    @Test
    @DisplayName("Double with negative infinity")
    void testDoubleNegativeInfinity() throws ExecutionException, InterruptedException {
        String key = "edge_double_neg_inf_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, Variant.ofDouble(Double.NEGATIVE_INFINITY)).get();
        Variant value = client.get(key).get();
        assertNotNull(value);
        assertTrue(Double.isInfinite(value.doubleValue()));
        client.remove(key).get();
    }

    @Test
    @DisplayName("Double with max value")
    void testDoubleMaxValue() throws ExecutionException, InterruptedException {
        String key = "edge_double_max_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, Variant.ofDouble(Double.MAX_VALUE)).get();
        Variant value = client.get(key).get();
        assertNotNull(value);
        assertEquals(Double.MAX_VALUE, value.doubleValue());
        client.remove(key).get();
    }

    @Test
    @DisplayName("Double with min positive value")
    void testDoubleMinValue() throws ExecutionException, InterruptedException {
        String key = "edge_double_min_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, Variant.ofDouble(Double.MIN_VALUE)).get();
        Variant value = client.get(key).get();
        assertNotNull(value);
        assertEquals(Double.MIN_VALUE, value.doubleValue(), 0);
        client.remove(key).get();
    }

    // =========================================================================
    // EXTREME INTEGER VALUES
    // =========================================================================

    @Test
    @DisplayName("Long MAX_VALUE")
    void testLongMaxValue() throws ExecutionException, InterruptedException {
        String key = "edge_long_max_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, Variant.ofLong(Long.MAX_VALUE)).get();
        Variant value = client.get(key).get();
        assertNotNull(value);
        assertEquals(Long.MAX_VALUE, value.longValue());
        client.remove(key).get();
    }

    @Test
    @DisplayName("Long MIN_VALUE")
    void testLongMinValue() throws ExecutionException, InterruptedException {
        String key = "edge_long_min_" + UUID.randomUUID();

        KeyHintData hint = client.create(key, Variant.ofLong(Long.MIN_VALUE)).get();
        Variant value = client.get(key).get();
        assertNotNull(value);
        assertEquals(Long.MIN_VALUE, value.longValue());
        client.remove(key).get();
    }
}