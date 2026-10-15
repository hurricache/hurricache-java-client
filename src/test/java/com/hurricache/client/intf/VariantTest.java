package com.hurricache.client.intf;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

class VariantTest {

    // =========================================================================
    // Factory Methods
    // =========================================================================

    @Test
    @DisplayName("ofBytes should create Variant with BYTES type")
    void testOfBytes() {
        byte[] data = "hello".getBytes();
        Variant variant = Variant.ofBytes(data);

        assertEquals(Variant.Type.BYTES, variant.type());
        assertTrue(variant.isBytes());
        assertArrayEquals(data, variant.bytesValue());
    }

    @Test
    @DisplayName("ofInt should create Variant with INT type")
    void testOfInt() {
        Variant variant = Variant.ofInt(42L);

        assertEquals(Variant.Type.INT, variant.type());
        assertTrue(variant.isInt());
        assertEquals(42L, variant.longValue());
        assertEquals(42L, variant.longValue());
    }

    @Test
    @DisplayName("ofDouble should create Variant with DOUBLE type")
    void testOfDouble() {
        Variant variant = Variant.ofDouble(3.14);

        assertEquals(Variant.Type.DOUBLE, variant.type());
        assertTrue(variant.isDouble());
        assertEquals(3.14, variant.doubleValue(), 0.001);
    }

    @Test
    @DisplayName("none should create Variant with NONE type")
    void testNone() {
        Variant variant = Variant.none();

        assertEquals(Variant.Type.NONE, variant.type());
        assertTrue(variant.isNone());
        assertNull(variant.bytesValue());
        assertEquals(0L, variant.longValue());
        assertEquals(0.0, variant.doubleValue());
    }

    // =========================================================================
    // Type Checking
    // =========================================================================

    @Test
    @DisplayName("type() should return correct type for each variant")
    void testType() {
        assertEquals(Variant.Type.BYTES, Variant.ofBytes(new byte[0]).type());
        assertEquals(Variant.Type.INT, Variant.ofInt(0).type());
        assertEquals(Variant.Type.DOUBLE, Variant.ofDouble(0.0).type());
        assertEquals(Variant.Type.NONE, Variant.none().type());
    }

    @Test
    @DisplayName("isNone/isBytes/isInt/isDouble should work correctly")
    void testTypeChecks() {
        Variant bytes = Variant.ofBytes(new byte[1]);
        Variant integer = Variant.ofInt(1);
        Variant doubleV = Variant.ofDouble(1.0);
        Variant none = Variant.none();

        assertTrue(bytes.isBytes());
        assertFalse(bytes.isInt());
        assertFalse(bytes.isDouble());
        assertFalse(bytes.isNone());

        assertTrue(integer.isInt());
        assertFalse(integer.isBytes());
        assertFalse(integer.isDouble());
        assertFalse(integer.isNone());

        assertTrue(doubleV.isDouble());
        assertFalse(doubleV.isBytes());
        assertFalse(doubleV.isInt());
        assertFalse(doubleV.isNone());

        assertTrue(none.isNone());
        assertFalse(none.isBytes());
        assertFalse(none.isInt());
        assertFalse(none.isDouble());
    }

    // =========================================================================
    // Value Accessors
    // =========================================================================

    @Test
    @DisplayName("bytesValue should return correct bytes")
    void testBytesValue() {
        byte[] data = {1, 2, 3, 4, 5};
        Variant variant = Variant.ofBytes(data);

        assertArrayEquals(data, variant.bytesValue());
    }

    @Test
    @DisplayName("intValue should return correct long value")
    void testIntValue() {
        Variant variant = Variant.ofInt(12345L);
        assertEquals(12345L, variant.longValue());
    }

    @Test
    @DisplayName("doubleValue should return correct double value")
    void testDoubleValue() {
        Variant variant = Variant.ofDouble(123.456);
        assertEquals(123.456, variant.doubleValue(), 0.0001);
    }

    // =========================================================================
    // Number Value
    // =========================================================================

    @Test
    @DisplayName("numberValue should return Long for INT type")
    void testNumberValueInt() {
        Variant variant = Variant.ofInt(42L);
        Number number = variant.numberValue();

        assertNotNull(number);
        assertEquals(42L, number.longValue());
        assertInstanceOf(Long.class, number);
    }

    @Test
    @DisplayName("numberValue should return Double for DOUBLE type")
    void testNumberValueDouble() {
        Variant variant = Variant.ofDouble(3.14);
        Number number = variant.numberValue();

        assertNotNull(number);
        assertEquals(3.14, number.doubleValue(), 0.001);
        assertInstanceOf(Double.class, number);
    }

    @Test
    @DisplayName("numberValue should return null for NONE type")
    void testNumberValueNone() {
        Variant variant = Variant.none();
        Number number = variant.numberValue();

        assertNull(number);
    }

    // =========================================================================
    // Require Methods
    // =========================================================================

    @Test
    @DisplayName("requireBytes should return bytes for BYTES type")
    void testRequireBytes() {
        byte[] data = "test".getBytes();
        Variant variant = Variant.ofBytes(data);

        assertArrayEquals(data, variant.requireBytes());
    }

    @Test
    @DisplayName("requireBytes should throw for non-BYTES type")
    void testRequireBytesThrows() {
        Variant integer = Variant.ofInt(42);
        Variant none = Variant.none();

        assertThrows(IllegalStateException.class, () -> integer.requireBytes());
        assertThrows(IllegalStateException.class, () -> none.requireBytes());
    }

    @Test
    @DisplayName("requireLong should return long for INT type")
    void testRequireLong() {
        Variant variant = Variant.ofInt(99L);
        assertEquals(99L, variant.requireLong());
    }

    @Test
    @DisplayName("requireLong should throw for non-INT type")
    void testRequireLongThrows() {
        Variant bytes = Variant.ofBytes(new byte[1]);
        Variant none = Variant.none();

        assertThrows(IllegalStateException.class, () -> bytes.requireLong());
        assertThrows(IllegalStateException.class, () -> none.requireLong());
    }

    @Test
    @DisplayName("requireDouble should return double for DOUBLE type")
    void testRequireDouble() {
        Variant variant = Variant.ofDouble(2.718);
        assertEquals(2.718, variant.requireDouble(), 0.001);
    }

    @Test
    @DisplayName("requireDouble should throw for non-DOUBLE type")
    void testRequireDoubleThrows() {
        Variant bytes = Variant.ofBytes(new byte[1]);
        Variant integer = Variant.ofInt(42);
        Variant none = Variant.none();

        assertThrows(IllegalStateException.class, () -> bytes.requireDouble());
        assertThrows(IllegalStateException.class, () -> integer.requireDouble());
        assertThrows(IllegalStateException.class, () -> none.requireDouble());
    }

    // =========================================================================
    // Edge Cases
    // =========================================================================

    @Test
    @DisplayName("should handle empty bytes")
    void testEmptyBytes() {
        byte[] empty = new byte[0];
        Variant variant = Variant.ofBytes(empty);

        assertEquals(Variant.Type.BYTES, variant.type());
        assertArrayEquals(empty, variant.bytesValue());
    }

    @Test
    @DisplayName("should handle null bytes in none variant")
    void testNoneHasNullBytes() {
        Variant variant = Variant.none();
        assertNull(variant.bytesValue());
    }

    @Test
    @DisplayName("should handle negative long values")
    void testNegativeLong() {
        Variant variant = Variant.ofInt(-12345L);
        assertEquals(-12345L, variant.longValue());
        assertEquals(-12345L, variant.requireLong());
    }

    @Test
    @DisplayName("should handle zero values for all types")
    void testZeroValues() {
        Variant bytes = Variant.ofBytes(new byte[]{0});
        Variant integer = Variant.ofInt(0L);
        Variant doubleV = Variant.ofDouble(0.0);

        assertEquals(Variant.Type.BYTES, bytes.type());
        assertEquals(0L, integer.longValue());
        assertEquals(0.0, doubleV.doubleValue(), 0.0001);
    }

    // =========================================================================
    // toString
    // =========================================================================

    @Test
    @DisplayName("toString should include type information")
    void testToString() {
        Variant bytes = Variant.ofBytes(new byte[]{1, 2, 3});
        Variant integer = Variant.ofInt(42);
        Variant doubleV = Variant.ofDouble(3.14);
        Variant none = Variant.none();

        assertTrue(bytes.toString().contains("bytes"));
        assertTrue(integer.toString().contains("int"));
        assertTrue(doubleV.toString().contains("double"));
        assertTrue(none.toString().contains("none"));
    }
}