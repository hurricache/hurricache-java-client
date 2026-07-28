package com.hurricache.client.intf;

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

}