package com.hurricache.client;

import com.hurricache.client.intf.OrderedVariant;
import com.hurricache.client.intf.Variant;
import com.hurricache.grpc.BinaryPayload;
import com.hurricache.grpc.OrderedValue;
import com.hurricache.grpc.Value;
import com.hurricache.grpc.ValueType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class KeyValueUtilsVariantTest {

    // =========================================================================
    // buildValueFromVariant
    // =========================================================================

    @Test
    @DisplayName("buildValueFromVariant should create Value from BYTES Variant")
    void testBuildValueFromVariantBytes() {
        byte[] data = "hello".getBytes();
        Variant variant = Variant.ofBytes(data);
        Duration ttl = Duration.ofSeconds(60);

        Value value = KeyValueUtils.buildValueFromVariant(variant, ttl);

        assertEquals(ValueType.BYTES, value.getValueType());
        assertArrayEquals(data, value.getValue().getPayload().toByteArray());
        assertTrue(value.hasTtl());
    }

    @Test
    @DisplayName("buildValueFromVariant should create Value from INT Variant")
    void testBuildValueFromVariantInt() {
        Variant variant = Variant.ofInt(42L);
        Duration ttl = Duration.ofSeconds(60);

        Value value = KeyValueUtils.buildValueFromVariant(variant, ttl);

        assertEquals(ValueType.INT, value.getValueType());
        assertEquals(42L, value.getValue().getIntPayload());
    }

    @Test
    @DisplayName("buildValueFromVariant should create Value from DOUBLE Variant")
    void testBuildValueFromVariantDouble() {
        Variant variant = Variant.ofDouble(3.14);
        Duration ttl = Duration.ofSeconds(60);

        Value value = KeyValueUtils.buildValueFromVariant(variant, ttl);

        assertEquals(ValueType.DOUBLE, value.getValueType());
        assertEquals(3.14, value.getValue().getDoublePayload(), 0.001);
    }

    // =========================================================================
    // buildValueFromVariantCompressed
    // =========================================================================

    @Test
    @DisplayName("buildValueFromVariantCompressed should handle BYTES Variant")
    void testBuildValueFromVariantCompressedBytes() {
        byte[] data = "test data for compression".getBytes();
        Variant variant = Variant.ofBytes(data);

        Value.Builder builder = KeyValueUtils.buildValueFromVariantCompressed(variant, 10);

        Value value = builder.build();
        assertEquals(ValueType.BYTES, value.getValueType());
    }

    @Test
    @DisplayName("buildValueFromVariantCompressed should handle INT Variant without compression")
    void testBuildValueFromVariantCompressedInt() {
        Variant variant = Variant.ofInt(12345L);

        Value.Builder builder = KeyValueUtils.buildValueFromVariantCompressed(variant, 10);

        Value value = builder.build();
        assertEquals(ValueType.INT, value.getValueType());
        assertEquals(12345L, value.getValue().getIntPayload());
    }

    @Test
    @DisplayName("buildValueFromVariantCompressed should handle DOUBLE Variant without compression")
    void testBuildValueFromVariantCompressedDouble() {
        Variant variant = Variant.ofDouble(2.71828);

        Value.Builder builder = KeyValueUtils.buildValueFromVariantCompressed(variant, 10);

        Value value = builder.build();
        assertEquals(ValueType.DOUBLE, value.getValueType());
        assertEquals(2.71828, value.getValue().getDoublePayload(), 0.0001);
    }

    // =========================================================================
    // buildOrderedValueFromVariant
    // =========================================================================

    @Test
    @DisplayName("buildOrderedValueFromVariant should handle BYTES with order")
    void testBuildOrderedValueFromVariantBytes() {
        Variant variant = Variant.ofBytes(new byte[]{1, 2, 3});
        OrderedVariant ordered = OrderedVariant.of(variant, 100L);
        Duration ttl = Duration.ofSeconds(30);

        OrderedValue value = KeyValueUtils.buildOrderedValueFromVariant(ordered, ttl, 10).build();

        assertEquals(100L, value.getOrder());
        assertEquals(ValueType.BYTES, value.getValueType());
    }

    @Test
    @DisplayName("buildOrderedValueFromVariant should handle INT with order")
    void testBuildOrderedValueFromVariantInt() {
        Variant variant = Variant.ofInt(999L);
        OrderedVariant ordered = OrderedVariant.of(variant, 200L);

        OrderedValue value = KeyValueUtils.buildOrderedValueFromVariant(ordered, null, 10).build();

        assertEquals(200L, value.getOrder());
        assertEquals(ValueType.INT, value.getValueType());
        assertEquals(999L, value.getValue().getIntPayload());
    }

    @Test
    @DisplayName("buildOrderedValueFromVariant should handle DOUBLE with order")
    void testBuildOrderedValueFromVariantDouble() {
        Variant variant = Variant.ofDouble(1.414);
        OrderedVariant ordered = OrderedVariant.of(variant, 300L);

        OrderedValue value = KeyValueUtils.buildOrderedValueFromVariant(ordered, null, 10).build();

        assertEquals(300L, value.getOrder());
        assertEquals(ValueType.DOUBLE, value.getValueType());
        assertEquals(1.414, value.getValue().getDoublePayload(), 0.001);
    }

    @Test
    @DisplayName("buildOrderedValueFromVariant should handle null order")
    void testBuildOrderedValueFromVariantNullOrder() {
        Variant variant = Variant.ofInt(42);
        OrderedVariant ordered = OrderedVariant.of(variant);

        OrderedValue value = KeyValueUtils.buildOrderedValueFromVariant(ordered, null, 10).build();

        assertEquals(0L, value.getOrder()); // null order becomes 0
    }
}
