package com.hurricache.client.intf;

import java.util.Objects;

/**
 * OrderedVariant — Variant с порядковым весом (order) для сортированных структур.
 * Наследует всю функциональность Variant и добавляет long order.
 */
public final class OrderedVariant {

    private final Variant variant;
    private final Long order;

    public OrderedVariant(Variant variant, Long order) {
        this.variant = variant;
        this.order = order;
    }

    /**
     * Creates an OrderedVariant from a Variant with a specific order.
     */
    public static OrderedVariant of(Variant variant, long order) {
        return new OrderedVariant(variant, order);
    }

    /**
     * Creates an OrderedVariant from a Variant with no explicit order (null).
     */
    public static OrderedVariant of(Variant variant) {
        return new OrderedVariant(variant, null);
    }

    /**
     * Creates an OrderedVariant from raw bytes with a specific order.
     */
    public static OrderedVariant ofBytes(byte[] value, long order) {
        return new OrderedVariant(Variant.ofBytes(value), order);
    }

    /**
     * Creates an OrderedVariant from an int value with a specific order.
     */
    public static OrderedVariant ofInt(long value, long order) {
        return new OrderedVariant(Variant.ofLong(value), order);
    }

    /**
     * Creates an OrderedVariant from a double value with a specific order.
     */
    public static OrderedVariant ofDouble(double value, long order) {
        return new OrderedVariant(Variant.ofDouble(value), order);
    }

    public Variant variant() {
        return variant;
    }

    public Long getOrder() {
        return order;
    }

    public boolean hasOrder() {
        return order != null;
    }

    // Delegate Variant methods for convenience

    public Variant.Type type() {
        return variant.type();
    }

    public boolean isNone() {
        return variant.isNone();
    }

    public boolean isBytes() {
        return variant.isBytes();
    }

    public boolean isInt() {
        return variant.isInt();
    }

    public boolean isDouble() {
        return variant.isDouble();
    }

    public byte[] bytesValue() {
        return variant.bytesValue();
    }

    public long intValue() {
        return variant.longValue();
    }

    public double doubleValue() {
        return variant.doubleValue();
    }

    public Number numberValue() {
        return variant.numberValue();
    }

    public byte[] requireBytes() {
        return variant.requireBytes();
    }

    public long requireLong() {
        return variant.requireLong();
    }

    public double requireDouble() {
        return variant.requireDouble();
    }

    @Override
    public String toString() {
        return "OrderedVariant{variant=" + variant + ", order=" + order + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        OrderedVariant that = (OrderedVariant) o;
        return Objects.equals(variant, that.variant) && Objects.equals(order, that.order);
    }

    @Override
    public int hashCode() {
        return Objects.hash(variant, order);
    }
}