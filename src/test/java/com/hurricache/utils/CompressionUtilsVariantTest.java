package com.hurricache.utils;

import com.hurricache.client.intf.OrderedVariant;
import com.hurricache.client.intf.Variant;
import com.hurricache.grpc.OrderedValue;
import com.hurricache.grpc.Value;
import com.hurricache.grpc.ValueType;
import com.hurricache.grpc.ValueResponse;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

class CompressionUtilsVariantTest {

    // =========================================================================
    // decompressIfNeededToVariant (from ValueResponse)
    // =========================================================================

    @Test
    @DisplayName("decompressIfNeededToVariant should handle plain bytes")
    void testDecompressIfNeededToVariantPlainBytes() {
        byte[] data = "test data".getBytes();
        Value value = Value.newBuilder()
                .setValue(com.hurricache.grpc.BinaryPayload.newBuilder()
                        .setPayload(com.google.protobuf.ByteString.copyFrom(data)))
                .setValueType(ValueType.BYTES)
                .build();

        ValueResponse response = ValueResponse.newBuilder()
                .setValueUnordered(value)
                .build();

        Variant variant = CompressionUtils.decompressIfNeededToVariant(response);

        assertEquals(Variant.Type.BYTES, variant.type());
        assertArrayEquals(data, variant.bytesValue());
    }

    @Test
    @DisplayName("decompressIfNeededToVariant should handle INT")
    void testDecompressIfNeededToVariantInt() {
        Value value = Value.newBuilder()
                .setValue(com.hurricache.grpc.BinaryPayload.newBuilder()
                        .setIntPayload(42L))
                .setValueType(ValueType.INT)
                .build();

        ValueResponse response = ValueResponse.newBuilder()
                .setValueUnordered(value)
                .build();

        Variant variant = CompressionUtils.decompressIfNeededToVariant(response);

        assertEquals(Variant.Type.INT, variant.type());
        assertEquals(42L, variant.longValue());
    }

    @Test
    @DisplayName("decompressIfNeededToVariant should handle DOUBLE")
    void testDecompressIfNeededToVariantDouble() {
        Value value = Value.newBuilder()
                .setValue(com.hurricache.grpc.BinaryPayload.newBuilder()
                        .setDoublePayload(3.14))
                .setValueType(ValueType.DOUBLE)
                .build();

        ValueResponse response = ValueResponse.newBuilder()
                .setValueUnordered(value)
                .build();

        Variant variant = CompressionUtils.decompressIfNeededToVariant(response);

        assertEquals(Variant.Type.DOUBLE, variant.type());
        assertEquals(3.14, variant.doubleValue(), 0.001);
    }

    // =========================================================================
    // decompressIfNeededToVariant (from OrderedValue)
    // =========================================================================

    @Test
    @DisplayName("decompressIfNeededToVariant should handle OrderedValue bytes")
    void testDecompressIfNeededToVariantOrderedBytes() {
        byte[] data = "ordered data".getBytes();
        OrderedValue orderedValue = OrderedValue.newBuilder()
                .setValue(com.hurricache.grpc.BinaryPayload.newBuilder()
                        .setPayload(com.google.protobuf.ByteString.copyFrom(data)))
                .setValueType(ValueType.BYTES)
                .setOrder(100L)
                .build();

        Variant variant = CompressionUtils.decompressIfNeededToVariant(orderedValue);

        assertEquals(Variant.Type.BYTES, variant.type());
        assertArrayEquals(data, variant.bytesValue());
    }

    @Test
    @DisplayName("decompressIfNeededToVariant should handle OrderedValue INT")
    void testDecompressIfNeededToVariantOrderedInt() {
        OrderedValue orderedValue = OrderedValue.newBuilder()
                .setValue(com.hurricache.grpc.BinaryPayload.newBuilder()
                        .setIntPayload(999L))
                .setValueType(ValueType.INT)
                .setOrder(200L)
                .build();

        Variant variant = CompressionUtils.decompressIfNeededToVariant(orderedValue);

        assertEquals(Variant.Type.INT, variant.type());
        assertEquals(999L, variant.longValue());
    }

    @Test
    @DisplayName("decompressIfNeededToVariant should handle OrderedValue DOUBLE")
    void testDecompressIfNeededToVariantOrderedDouble() {
        OrderedValue orderedValue = OrderedValue.newBuilder()
                .setValue(com.hurricache.grpc.BinaryPayload.newBuilder()
                        .setDoublePayload(2.718))
                .setValueType(ValueType.DOUBLE)
                .setOrder(300L)
                .build();

        Variant variant = CompressionUtils.decompressIfNeededToVariant(orderedValue);

        assertEquals(Variant.Type.DOUBLE, variant.type());
        assertEquals(2.718, variant.doubleValue(), 0.001);
    }

    // =========================================================================
    // decompressIfNeededToOrderedVariant
    // =========================================================================

    @Test
    @DisplayName("decompressIfNeededToOrderedVariant should preserve order for bytes")
    void testDecompressIfNeededToOrderedVariantBytes() {
        byte[] data = "test".getBytes();
        OrderedValue orderedValue = OrderedValue.newBuilder()
                .setValue(com.hurricache.grpc.BinaryPayload.newBuilder()
                        .setPayload(com.google.protobuf.ByteString.copyFrom(data)))
                .setValueType(ValueType.BYTES)
                .setOrder(42L)
                .build();

        OrderedVariant orderedVariant = CompressionUtils.decompressIfNeededToOrderedVariant(orderedValue);

        assertEquals(Variant.Type.BYTES, orderedVariant.variant().type());
        assertArrayEquals(data, orderedVariant.variant().bytesValue());
        assertEquals(42L, orderedVariant.getOrder());
    }

    @Test
    @DisplayName("decompressIfNeededToOrderedVariant should preserve order for INT")
    void testDecompressIfNeededToOrderedVariantInt() {
        OrderedValue orderedValue = OrderedValue.newBuilder()
                .setValue(com.hurricache.grpc.BinaryPayload.newBuilder()
                        .setIntPayload(123L))
                .setValueType(ValueType.INT)
                .setOrder(456L)
                .build();

        OrderedVariant orderedVariant = CompressionUtils.decompressIfNeededToOrderedVariant(orderedValue);

        assertEquals(Variant.Type.INT, orderedVariant.variant().type());
        assertEquals(123L, orderedVariant.variant().longValue());
        assertEquals(456L, orderedVariant.getOrder());
    }

    @Test
    @DisplayName("decompressIfNeededToOrderedVariant should preserve order for DOUBLE")
    void testDecompressIfNeededToOrderedVariantDouble() {
        OrderedValue orderedValue = OrderedValue.newBuilder()
                .setValue(com.hurricache.grpc.BinaryPayload.newBuilder()
                        .setDoublePayload(1.414))
                .setValueType(ValueType.DOUBLE)
                .setOrder(789L)
                .build();

        OrderedVariant orderedVariant = CompressionUtils.decompressIfNeededToOrderedVariant(orderedValue);

        assertEquals(Variant.Type.DOUBLE, orderedVariant.variant().type());
        assertEquals(1.414, orderedVariant.variant().doubleValue(), 0.001);
        assertEquals(789L, orderedVariant.getOrder());
    }

    @Test
    @DisplayName("decompressIfNeededToOrderedVariant should handle AUTO type as bytes")
    void testDecompressIfNeededToOrderedVariantAuto() {
        byte[] data = "auto type".getBytes();
        OrderedValue orderedValue = OrderedValue.newBuilder()
                .setValue(com.hurricache.grpc.BinaryPayload.newBuilder()
                        .setPayload(com.google.protobuf.ByteString.copyFrom(data)))
                .setOrder(10L)
                .build();

        OrderedVariant orderedVariant = CompressionUtils.decompressIfNeededToOrderedVariant(orderedValue);

        assertEquals(Variant.Type.BYTES, orderedVariant.variant().type());
        assertArrayEquals(data, orderedVariant.variant().bytesValue());
        assertEquals(10L, orderedVariant.getOrder());
    }

    // =========================================================================
    // Edge Cases
    // =========================================================================

    @Test
    @DisplayName("should handle empty bytes")
    void testEmptyBytes() {
        byte[] empty = new byte[0];
        Value value = Value.newBuilder()
                .setValue(com.hurricache.grpc.BinaryPayload.newBuilder()
                        .setPayload(com.google.protobuf.ByteString.copyFrom(empty)))
                .setValueType(ValueType.BYTES)
                .build();

        ValueResponse response = ValueResponse.newBuilder()
                .setValueUnordered(value)
                .build();

        Variant variant = CompressionUtils.decompressIfNeededToVariant(response);

        assertEquals(Variant.Type.BYTES, variant.type());
        assertArrayEquals(empty, variant.bytesValue());
    }

    @Test
    @DisplayName("should handle zero values for all types")
    void testZeroValues() {
        // INT zero
        Value intVal = Value.newBuilder()
                .setValue(com.hurricache.grpc.BinaryPayload.newBuilder().setIntPayload(0L))
                .setValueType(ValueType.INT)
                .build();
        Variant intVariant = CompressionUtils.decompressIfNeededToVariant(
                ValueResponse.newBuilder().setValueUnordered(intVal).build());
        assertEquals(0L, intVariant.longValue());

        // DOUBLE zero
        Value doubleVal = Value.newBuilder()
                .setValue(com.hurricache.grpc.BinaryPayload.newBuilder().setDoublePayload(0.0))
                .setValueType(ValueType.DOUBLE)
                .build();
        Variant doubleVariant = CompressionUtils.decompressIfNeededToVariant(
                ValueResponse.newBuilder().setValueUnordered(doubleVal).build());
        assertEquals(0.0, doubleVariant.doubleValue(), 0.0001);
    }

    @Test
    @DisplayName("should handle negative values")
    void testNegativeValues() {
        // Negative INT
        Value intVal = Value.newBuilder()
                .setValue(com.hurricache.grpc.BinaryPayload.newBuilder().setIntPayload(-42L))
                .setValueType(ValueType.INT)
                .build();
        Variant intVariant = CompressionUtils.decompressIfNeededToVariant(
                ValueResponse.newBuilder().setValueUnordered(intVal).build());
        assertEquals(-42L, intVariant.longValue());

        // Negative DOUBLE
        Value doubleVal = Value.newBuilder()
                .setValue(com.hurricache.grpc.BinaryPayload.newBuilder().setDoublePayload(-3.14))
                .setValueType(ValueType.DOUBLE)
                .build();
        Variant doubleVariant = CompressionUtils.decompressIfNeededToVariant(
                ValueResponse.newBuilder().setValueUnordered(doubleVal).build());
        assertEquals(-3.14, doubleVariant.doubleValue(), 0.001);
    }
}