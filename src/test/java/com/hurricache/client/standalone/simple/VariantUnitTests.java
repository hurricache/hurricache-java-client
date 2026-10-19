package com.hurricache.client.standalone.simple;

import com.hurricache.client.intf.OrderedPayload;
import com.hurricache.client.intf.OrderedVariant;
import com.hurricache.client.intf.Payload;
import com.hurricache.client.intf.Variant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for Variant, Payload, OrderedPayload, OrderedVariant classes.
 * No server required - pure unit tests.
 */
public class VariantUnitTests {

    // =========================================================================
    // VARIANT - Type Detection
    // =========================================================================

    @Test
    @DisplayName("Variant.ofBytes - type detection")
    void testVariantBytesType() {
        Variant v = Variant.ofBytes("hello".getBytes(StandardCharsets.UTF_8));

        assertTrue(v.isBytes());
        assertFalse(v.isInt());
        assertFalse(v.isDouble());
        assertFalse(v.isNone());
        assertEquals(Variant.Type.BYTES, v.type());
    }

    @Test
    @DisplayName("Variant.ofLong - type detection")
    void testVariantLongType() {
        Variant v = Variant.ofLong(42);

        assertTrue(v.isInt());
        assertFalse(v.isBytes());
        assertFalse(v.isDouble());
        assertEquals(Variant.Type.INT, v.type());
    }

    @Test
    @DisplayName("Variant.ofDouble - type detection")
    void testVariantDoubleType() {
        Variant v = Variant.ofDouble(3.14);

        assertTrue(v.isDouble());
        assertFalse(v.isInt());
        assertFalse(v.isBytes());
        assertEquals(Variant.Type.DOUBLE, v.type());
    }

    @Test
    @DisplayName("Variant.none - type detection")
    void testVariantNoneType() {
        Variant v = Variant.none();

        assertTrue(v.isNone());
        assertFalse(v.isBytes());
        assertFalse(v.isInt());
        assertFalse(v.isDouble());
        assertEquals(Variant.Type.NONE, v.type());
    }

    // =========================================================================
    // VARIANT - Value Access
    // =========================================================================

    @Test
    @DisplayName("Variant bytesValue returns correct bytes")
    void testVariantBytesValue() {
        byte[] data = "test_value".getBytes(StandardCharsets.UTF_8);
        Variant v = Variant.ofBytes(data);

        assertArrayEquals(data, v.bytesValue());
    }

    @Test
    @DisplayName("Variant bytesValue on int returns null")
    void testVariantBytesValueOnInt() {
        Variant v = Variant.ofLong(100);
        assertNull(v.bytesValue());
    }

    @Test
    @DisplayName("Variant bytesValue on double returns null")
    void testVariantBytesValueOnDouble() {
        Variant v = Variant.ofDouble(2.71);
        assertNull(v.bytesValue());
    }

    @Test
    @DisplayName("Variant longValue returns correct value")
    void testVariantLongValue() {
        Variant v = Variant.ofLong(999);
        assertEquals(999L, v.longValue());
    }

    @Test
    @DisplayName("Variant longValue on double returns 0")
    void testVariantLongValueOnDouble() {
        Variant v = Variant.ofDouble(3.14);
        assertEquals(0L, v.longValue());
    }

    @Test
    @DisplayName("Variant doubleValue returns correct value")
    void testVariantDoubleValue() {
        Variant v = Variant.ofDouble(42.42);
        assertEquals(42.42, v.doubleValue(), 0.0001);
    }

    @Test
    @DisplayName("Variant doubleValue on int returns 0.0")
    void testVariantDoubleValueOnInt() {
        Variant v = Variant.ofLong(100);
        assertEquals(0.0, v.doubleValue());
    }

    @Test
    @DisplayName("Variant numberValue for INT returns Long")
    void testVariantNumberValueInt() {
        Variant v = Variant.ofLong(42);
        Number n = v.numberValue();

        assertNotNull(n);
        assertEquals(42L, n.longValue());
    }

    @Test
    @DisplayName("Variant numberValue for DOUBLE returns Double")
    void testVariantNumberValueDouble() {
        Variant v = Variant.ofDouble(3.14);
        Number n = v.numberValue();

        assertNotNull(n);
        assertTrue(n instanceof Double);
        assertEquals(3.14, n.doubleValue(), 0.0001);
    }

    @Test
    @DisplayName("Variant requireBytes throws on wrong type")
    void testVariantRequireBytesThrows() {
        Variant v = Variant.ofLong(42);

        assertThrows(IllegalStateException.class, v::requireBytes);
    }

    @Test
    @DisplayName("Variant requireLong throws on wrong type")
    void testVariantRequireLongThrows() {
        Variant v = Variant.ofDouble(3.14);

        assertThrows(IllegalStateException.class, v::requireLong);
    }

    @Test
    @DisplayName("Variant requireDouble throws on wrong type")
    void testVariantRequireDoubleThrows() {
        Variant v = Variant.ofBytes("test".getBytes());

        assertThrows(IllegalStateException.class, v::requireDouble);
    }

    // =========================================================================
    // VARIANT - Equals & HashCode
    // =========================================================================

    @Test
    @DisplayName("Variant equals - same bytes")
    void testVariantEqualsBytes() {
        byte[] data = "test".getBytes(StandardCharsets.UTF_8);
        Variant v1 = Variant.ofBytes(data);
        Variant v2 = Variant.ofBytes(data);

        assertEquals(v1, v2);
        assertEquals(v1.hashCode(), v2.hashCode());
    }

    @Test
    @DisplayName("Variant equals - same long")
    void testVariantEqualsLong() {
        Variant v1 = Variant.ofLong(42);
        Variant v2 = Variant.ofLong(42);

        assertEquals(v1, v2);
        assertEquals(v1.hashCode(), v2.hashCode());
    }

    @Test
    @DisplayName("Variant equals - same double")
    void testVariantEqualsDouble() {
        Variant v1 = Variant.ofDouble(3.14);
        Variant v2 = Variant.ofDouble(3.14);

        assertEquals(v1, v2);
        assertEquals(v1.hashCode(), v2.hashCode());
    }

    @Test
    @DisplayName("Variant equals - different types")
    void testVariantEqualsDifferentTypes() {
        Variant v1 = Variant.ofBytes("test".getBytes());
        Variant v2 = Variant.ofLong(42);

        assertNotEquals(v1, v2);
    }

    @Test
    @DisplayName("Variant equals - null")
    void testVariantEqualsNull() {
        Variant v = Variant.ofLong(42);

        assertNotEquals(v, null);
    }

    @Test
    @DisplayName("Variant equals - different class")
    void testVariantEqualsDifferentClass() {
        Variant v = Variant.ofLong(42);

        assertNotEquals(v, "string");
    }

    // =========================================================================
    // VARIANT - toString
    // =========================================================================

    @Test
    @DisplayName("Variant toString for BYTES")
    void testVariantToStringBytes() {
        Variant v = Variant.ofBytes("hello".getBytes());
        String str = v.toString();

        assertTrue(str.contains("Variant{bytes="));
        assertTrue(str.contains("5B}"));
    }

    @Test
    @DisplayName("Variant toString for INT")
    void testVariantToStringInt() {
        Variant v = Variant.ofLong(42);
        String str = v.toString();

        assertTrue(str.contains("Variant{int=42}"));
    }

    @Test
    @DisplayName("Variant toString for DOUBLE")
    void testVariantToStringDouble() {
        Variant v = Variant.ofDouble(3.14);
        String str = v.toString();

        assertTrue(str.contains("Variant{double="));
        assertTrue(str.contains("3.14"));
    }

    @Test
    @DisplayName("Variant toString for NONE")
    void testVariantToStringNone() {
        Variant v = Variant.none();
        String str = v.toString();

        assertEquals("Variant{none}", str);
    }

    // =========================================================================
    // VARIANT - Empty & Zero Values
    // =========================================================================

    @Test
    @DisplayName("Variant ofBytes with empty array")
    void testVariantEmptyBytes() {
        Variant v = Variant.ofBytes(new byte[0]);

        assertTrue(v.isBytes());
        assertArrayEquals(new byte[0], v.bytesValue());
        assertEquals(0, v.bytesValue().length);
    }

    @Test
    @DisplayName("Variant ofBytes with null bytes - handles gracefully")
    void testVariantBytesWithNull() {
        // Variant should handle null gracefully
        Variant v = Variant.ofBytes(null);
        assertTrue(v.isBytes());
        assertNull(v.bytesValue());
    }

    @Test
    @DisplayName("Variant ofLong with zero")
    void testVariantZeroLong() {
        Variant v = Variant.ofLong(0);

        assertTrue(v.isInt());
        assertEquals(0L, v.longValue());
    }

    @Test
    @DisplayName("Variant ofDouble with zero")
    void testVariantZeroDouble() {
        Variant v = Variant.ofDouble(0.0);

        assertTrue(v.isDouble());
        assertEquals(0.0, v.doubleValue());
    }

    @Test
    @DisplayName("Variant ofLong with negative value")
    void testVariantNegativeLong() {
        Variant v = Variant.ofLong(-999);

        assertTrue(v.isInt());
        assertEquals(-999L, v.longValue());
    }

    @Test
    @DisplayName("Variant ofDouble with negative value")
    void testVariantNegativeDouble() {
        Variant v = Variant.ofDouble(-3.14);

        assertTrue(v.isDouble());
        assertEquals(-3.14, v.doubleValue(), 0.0001);
    }

    @Test
    @DisplayName("Variant ofDouble with NaN")
    void testVariantNaN() {
        Variant v = Variant.ofDouble(Double.NaN);

        assertTrue(v.isDouble());
        assertTrue(Double.isNaN(v.doubleValue()));
    }

    @Test
    @DisplayName("Variant ofDouble with Infinity")
    void testVariantInfinity() {
        Variant v = Variant.ofDouble(Double.POSITIVE_INFINITY);

        assertTrue(v.isDouble());
        assertTrue(Double.isInfinite(v.doubleValue()));
    }

    // =========================================================================
    // PAYLOAD - Unit Tests
    // =========================================================================

    @Test
    @DisplayName("Payload.of with bytes")
    void testPayloadOfBytes() {
        byte[] data = "key_data".getBytes(StandardCharsets.UTF_8);
        Payload p = Payload.of(data);

        assertArrayEquals(data, p.getValue());
    }

    @Test
    @DisplayName("Payload equals and hashCode")
    void testPayloadEquals() {
        byte[] data = "test_key".getBytes(StandardCharsets.UTF_8);
        Payload p1 = Payload.of(data);
        Payload p2 = Payload.of(data);

        assertEquals(p1, p2);
        assertEquals(p1.hashCode(), p2.hashCode());
    }

    @Test
    @DisplayName("Payload toString")
    void testPayloadToString() {
        byte[] data = "test".getBytes(StandardCharsets.UTF_8);
        Payload p = Payload.of(data);

        String str = p.toString();
        assertTrue(str.contains("Payload"));
        assertTrue(str.contains("value="));
    }

    // =========================================================================
    // ORDEREDPAYLOAD - Unit Tests
    // =========================================================================

    @Test
    @DisplayName("OrderedPayload.of with order")
    void testOrderedPayloadOf() {
        byte[] data = "key".getBytes(StandardCharsets.UTF_8);
        OrderedPayload op = OrderedPayload.of(data, 100L);

        assertArrayEquals(data, op.getValue());
        assertEquals(100L, op.getOrder());
    }

    @Test
    @DisplayName("OrderedPayload equals and hashCode")
    void testOrderedPayloadEquals() {
        byte[] data = "key".getBytes(StandardCharsets.UTF_8);
        OrderedPayload op1 = OrderedPayload.of(data, 42L);
        OrderedPayload op2 = OrderedPayload.of(data, 42L);

        assertEquals(op1, op2);
        assertEquals(op1.hashCode(), op2.hashCode());
    }

    @Test
    @DisplayName("OrderedPayload different order not equal")
    void testOrderedPayloadDifferentOrder() {
        byte[] data = "key".getBytes(StandardCharsets.UTF_8);
        OrderedPayload op1 = OrderedPayload.of(data, 42L);
        OrderedPayload op2 = OrderedPayload.of(data, 43L);

        assertNotEquals(op1, op2);
    }

    // =========================================================================
    // ORDEREDVARIANT - Unit Tests
    // =========================================================================

    @Test
    @DisplayName("OrderedVariant.of with Variant and order")
    void testOrderedVariantOf() {
        Variant v = Variant.ofLong(100);
        OrderedVariant ov = OrderedVariant.of(v, 50L);

        assertEquals(v, ov.variant());
        assertEquals(50L, ov.getOrder());
    }

    @Test
    @DisplayName("OrderedVariant of bytes with order")
    void testOrderedVariantBytes() {
        Variant v = Variant.ofBytes("test".getBytes());
        OrderedVariant ov = OrderedVariant.of(v, 1L);

        assertTrue(ov.variant().isBytes());
        assertEquals(1L, ov.getOrder());
    }

    @Test
    @DisplayName("OrderedVariant of double with order")
    void testOrderedVariantDouble() {
        Variant v = Variant.ofDouble(3.14);
        OrderedVariant ov = OrderedVariant.of(v, 2L);

        assertTrue(ov.variant().isDouble());
        assertEquals(2L, ov.getOrder());
    }

    @Test
    @DisplayName("OrderedVariant equals and hashCode")
    void testOrderedVariantEquals() {
        Variant v = Variant.ofLong(42);
        OrderedVariant ov1 = OrderedVariant.of(v, 10L);
        OrderedVariant ov2 = OrderedVariant.of(v, 10L);

        assertEquals(ov1, ov2);
        assertEquals(ov1.hashCode(), ov2.hashCode());
    }

    @Test
    @DisplayName("OrderedVariant different order not equal")
    void testOrderedVariantDifferentOrder() {
        Variant v = Variant.ofLong(42);
        OrderedVariant ov1 = OrderedVariant.of(v, 10L);
        OrderedVariant ov2 = OrderedVariant.of(v, 11L);

        assertNotEquals(ov1, ov2);
    }
}