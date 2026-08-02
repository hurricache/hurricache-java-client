package com.hurricache.client.intf;

public class OrderedPayload extends Payload {
    private final Long order;

    public OrderedPayload(byte[] value) {
        super(value);
        this.order = null;
    }
    public OrderedPayload(byte[] value, Long order) {
        super(value);
        this.order = order;
    }

    public static  OrderedPayload of(Long order, byte[] value) {
        return new OrderedPayload(value,order);
    }
    public static  OrderedPayload of(long order, byte[] value) {
        return new OrderedPayload(value,order);
    }

    public static  OrderedPayload of(byte[] value,Long order ) {
        return new OrderedPayload(value,order);
    }

    public static  OrderedPayload of(byte[] value,long order ) {
        return new OrderedPayload(value,order);
    }

    public Long getOrder() {
        return order;
    }
    boolean isOrdered() {
        return order != null;
    }

}