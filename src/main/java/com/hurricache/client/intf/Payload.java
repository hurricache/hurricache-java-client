package com.hurricache.client.intf;

import java.util.Arrays;
import java.util.Objects;

public class Payload {
    final protected byte[] value;


    protected Payload(byte[] value) {
        this.value = value;
    }

    public static Payload of(byte[] value) {
        return new Payload( value);
    }

    public byte[] getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "Payload{" + "value=" + Arrays.toString(value) + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Payload payload = (Payload) o;
        return Objects.deepEquals(value, payload.value);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(value);
    }
}