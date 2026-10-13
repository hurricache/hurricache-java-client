package com.hurricache.client.intf;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

class OrderedVariantTest {

    // =========================================================================
    // Factory Methods
    // =========================================================================

    @Test
    @DisplayName("of(Variant, long) should create OrderedVariant with order")
    void testOfWithOrder() {
        Variant variant = Variant.ofBytes(new byte[]{1, 2, 3});
        OrderedVariant ordered = OrderedVariant.of(variant, 42L);

        assertEquals(variant, ordered.variant());
        assertEquals(42L, ordered.getOrder());
        assertTrue(ordered.hasOrder());
    }

    @Test
    @DisplayName("of(Variant) should create OrderedVariant without order (null)")
    void testOfWithoutOrder() {
        Variant variant = Variant.ofInt(100);
        OrderedVariant ordered = OrderedVariant.of(variant);

        assertEquals(variant, ordered.variant());
        assertNull(ordered.getOrder());
        assertFalse(ordered.hasOrder());
    }

    @Test
    @DisplayName("ofBytes with order should create OrderedVariant")
    void testOfBytesWithOrder() {
        OrderedVariant ordered = OrderedVariant.ofBytes(new byte[]{1, 2, 3}, 10L);

        assertTrue(ordered.isBytes());
        assertArrayEquals(new byte[]{1, 2, 3}, ordered.bytesValue());
        assertEquals(10L, ordered.getOrder());
    }

    @Test
    @DisplayName("ofInt with order should create OrderedVariant")
    void testOfIntWithOrder() {
        OrderedVariant ordered = OrderedVariant.ofInt(42L, 100L);

        assertTrue(ordered.isInt());
        assertEquals(42L, ordered.intValue());
        assertEquals(100L, ordered.getOrder());
    }

    @Test
    @DisplayName("ofDouble with order should create OrderedVariant")
    void testOfDoubleWithOrder() {
        OrderedVariant ordered = OrderedVariant.ofDouble(3.14, 200L);

        assertTrue(ordered.isDouble());
        assertEquals(3.14, ordered.doubleValue(), 0.001);
        assertEquals(200L, ordered.getOrder());
    }

    // =========================================================================
    // Variant Delegation
    // =========================================================================

    @Test
    @DisplayName("should delegate type() to variant")
    void testDelegateType() {
        Variant variant = Variant.ofInt(42);
        OrderedVariant ordered = OrderedVariant.of(variant, 1L);

        assertEquals(variant.type(), ordered.type());
    }

    @Test
    @DisplayName("should delegate isNone/isBytes/isInt/isDouble to variant")
    void testDelegateTypeChecks() {
        Variant bytes = Variant.ofBytes(new byte[1]);
        Variant integer = Variant.ofInt(1);
        Variant doubleV = Variant.ofDouble(1.0);
        Variant none = Variant.none();

        OrderedVariant ob = OrderedVariant.of(bytes, 1L);
        OrderedVariant oi = OrderedVariant.of(integer, 1L);
        OrderedVariant od = OrderedVariant.of(doubleV, 1L);
        OrderedVariant on = OrderedVariant.of(none, 1L);

        assertTrue(ob.isBytes());
        assertTrue(oi.isInt());
        assertTrue(od.isDouble());
        assertTrue(on.isNone());
    }

    @Test
    @DisplayName("should delegate bytesValue to variant")
    void testDelegateBytesValue() {
        byte[] data = {10, 20, 30};
        Variant variant = Variant.ofBytes(data);
        OrderedVariant ordered = OrderedVariant.of(variant, 5L);

        assertArrayEquals(data, ordered.bytesValue());
    }

    @Test
    @DisplayName("should delegate intValue to variant")
    void testDelegateIntValue() {
        Variant variant = Variant.ofInt(999L);
        OrderedVariant ordered = OrderedVariant.of(variant, 10L);

        assertEquals(999L, ordered.intValue());
    }

    @Test
    @DisplayName("should delegate doubleValue to variant")
    void testDelegateDoubleValue() {
        Variant variant = Variant.ofDouble(123.456);
        OrderedVariant ordered = OrderedVariant.of(variant, 20L);

        assertEquals(123.456, ordered.doubleValue(), 0.0001);
    }

    @Test
    @DisplayName("should delegate numberValue to variant")
    void testDelegateNumberValue() {
        Variant intVariant = Variant.ofInt(42L);
        OrderedVariant intOrdered = OrderedVariant.of(intVariant, 1L);
        assertEquals(42L, intOrdered.numberValue().longValue());

        Variant doubleVariant = Variant.ofDouble(3.14);
        OrderedVariant doubleOrdered = OrderedVariant.of(doubleVariant, 2L);
        assertEquals(3.14, doubleOrdered.numberValue().doubleValue(), 0.001);
    }

    @Test
    @DisplayName("should delegate requireBytes to variant")
    void testDelegateRequireBytes() {
        byte[] data = "test".getBytes();
        Variant variant = Variant.ofBytes(data);
        OrderedVariant ordered = OrderedVariant.of(variant, 1L);

        assertArrayEquals(data, ordered.requireBytes());
    }

    @Test
    @DisplayName("should delegate requireLong to variant")
    void testDelegateRequireLong() {
        Variant variant = Variant.ofInt(777L);
        OrderedVariant ordered = OrderedVariant.of(variant, 1L);

        assertEquals(777L, ordered.requireLong());
    }

    @Test
    @DisplayName("should delegate requireDouble to variant")
    void testDelegateRequireDouble() {
        Variant variant = Variant.ofDouble(2.718);
        OrderedVariant ordered = OrderedVariant.of(variant, 1L);

        assertEquals(2.718, ordered.requireDouble(), 0.001);
    }

    // =========================================================================
    // Edge Cases
    // =========================================================================

    @Test
    @DisplayName("should handle zero order")
    void testZeroOrder() {
        Variant variant = Variant.ofInt(0);
        OrderedVariant ordered = OrderedVariant.of(variant, 0L);

        assertTrue(ordered.hasOrder());
        assertEquals(0L, ordered.getOrder());
    }

    @Test
    @DisplayName("should handle negative order")
    void testNegativeOrder() {
        Variant variant = Variant.ofInt(1);
        OrderedVariant ordered = OrderedVariant.of(variant, -100L);

        assertEquals(-100L, ordered.getOrder());
    }

    @Test
    @DisplayName("should handle large order values")
    void testLargeOrder() {
        Variant variant = Variant.ofInt(1);
        long largeOrder = Long.MAX_VALUE;
        OrderedVariant ordered = OrderedVariant.of(variant, largeOrder);

        assertEquals(largeOrder, ordered.getOrder());
    }

    @Test
    @DisplayName("should handle NONE variant with order")
    void testNoneWithOrder() {
        OrderedVariant ordered = OrderedVariant.of(Variant.none(), 42L);

        assertTrue(ordered.isNone());
        assertEquals(42L, ordered.getOrder());
    }

    // =========================================================================
    // toString
    // =========================================================================

    @Test
    @DisplayName("toString should include variant and order information")
    void testToString() {
        Variant variant = Variant.ofInt(42);
        OrderedVariant ordered = OrderedVariant.of(variant, 100L);

        String str = ordered.toString();
        assertTrue(str.contains("OrderedVariant"));
        assertTrue(str.contains("variant="));
        assertTrue(str.contains("order="));
    }

    @Test
    @DisplayName("toString should show null order when not set")
    void testToStringNullOrder() {
        OrderedVariant ordered = OrderedVariant.of(Variant.ofInt(42));

        String str = ordered.toString();
        assertTrue(str.contains("order=null"));
    }
}
