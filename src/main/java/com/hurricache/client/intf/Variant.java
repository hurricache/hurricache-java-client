package com.hurricache.client.intf;

/**
 * Variant — универсальный тип возврата, может содержать int, double или byte[].
 * Тип определяется на основе ValueType из протокола.
 */
public final class Variant {

    public enum Type {
        NONE, BYTES, INT, DOUBLE
    }

    private final Type type;
    private final byte[] bytesValue;
    private final long intValue;
    private final double doubleValue;

    private Variant(Type type, byte[] bytesValue, long intValue, double doubleValue) {
        this.type = type;
        this.bytesValue = bytesValue;
        this.intValue = intValue;
        this.doubleValue = doubleValue;
    }

    public static Variant ofBytes(byte[] value) {
        return new Variant(Type.BYTES, value, 0L, 0.0);
    }

    public static Variant ofInt(long value) {
        return new Variant(Type.INT, null, value, 0.0);
    }

    public static Variant ofDouble(double value) {
        return new Variant(Type.DOUBLE, null, 0L, value);
    }

    public static Variant none() {
        return new Variant(Type.NONE, null, 0L, 0.0);
    }

    public Type type() {
        return type;
    }

    public boolean isNone() {
        return type == Type.NONE;
    }

    public boolean isBytes() {
        return type == Type.BYTES;
    }

    public boolean isInt() {
        return type == Type.INT;
    }

    public boolean isDouble() {
        return type == Type.DOUBLE;
    }

    public byte[] bytesValue() {
        return bytesValue;
    }

    public long intValue() {
        return intValue;
    }

    public double doubleValue() {
        return doubleValue;
    }

    /**
     * Returns the value as a Number (Long for INT, Double for DOUBLE).
     */
    public Number numberValue() {
        return (type == Type.DOUBLE) ? doubleValue : intValue;
    }

    public byte[] requireBytes() {
        if (type != Type.BYTES) {
            throw new IllegalStateException("Expected BYTES, got " + type);
        }
        return bytesValue;
    }

    public long requireLong() {
        if (type != Type.INT) {
            throw new IllegalStateException("Expected INT, got " + type);
        }
        return intValue;
    }

    public double requireDouble() {
        if (type != Type.DOUBLE) {
            throw new IllegalStateException("Expected DOUBLE, got " + type);
        }
        return doubleValue;
    }

    @Override
    public String toString() {
        switch (type) {
            case NONE:
                return "Variant{none}";
            case BYTES:
                return "Variant{bytes=" + (bytesValue != null ? bytesValue.length : 0) + "B}";
            case INT:
                return "Variant{int=" + intValue + "}";
            case DOUBLE:
                return "Variant{double=" + doubleValue + "}";
            default:
                return "Variant{unknown}";
        }
    }
}
