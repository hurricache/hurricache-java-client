package com.hurricache.utils;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;

/**
 * Прикрепляет к каждому вызову заголовок x-hc-client-ts — время отправки клиента
 * в миллисекундах от Unix epoch (UTC/GMT-0). Сервер использует его для штампа
 * версий и честного отклонения записей, задержанных сетью (stale write).
 */
public final class ClientSendTimeInterceptor implements ClientInterceptor {

    private static final Metadata.BinaryMarshaller<Long> LONG_LE_MARSHALLER =
            new Metadata.BinaryMarshaller<>() {
                @Override
                public byte[] toBytes(Long value) {
                    long v = value;
                    return new byte[] {
                            (byte) v,
                            (byte) (v >>> 8),
                            (byte) (v >>> 16),
                            (byte) (v >>> 24),
                            (byte) (v >>> 32),
                            (byte) (v >>> 40),
                            (byte) (v >>> 48),
                            (byte) (v >>> 56)
                    };
                }

                @Override
                public Long parseBytes(byte[] serialized) {
                    if (serialized == null || serialized.length != Long.BYTES) {
                        return 0L;
                    }
                    return ((long) serialized[0] & 0xFFL)
                           | (((long) serialized[1] & 0xFFL) << 8)
                           | (((long) serialized[2] & 0xFFL) << 16)
                           | (((long) serialized[3] & 0xFFL) << 24)
                           | (((long) serialized[4] & 0xFFL) << 32)
                           | (((long) serialized[5] & 0xFFL) << 40)
                           | (((long) serialized[6] & 0xFFL) << 48)
                           | (((long) serialized[7] & 0xFFL) << 56);
                }
            };

    public static final Metadata.Key<Long> CLIENT_TS_KEY =
            Metadata.Key.of("x-hc-client-ts-bin", LONG_LE_MARSHALLER);

    public static final ClientSendTimeInterceptor INSTANCE = new ClientSendTimeInterceptor();

    private ClientSendTimeInterceptor() {
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
                                                               CallOptions callOptions,
                                                               Channel next) {
        return new ForwardingClientCall.SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {
            @Override
            public void start(Listener<RespT> responseListener, Metadata headers) {
                headers.put(CLIENT_TS_KEY, Instant.now().toEpochMilli());
                super.start(responseListener, headers);
            }
        };
    }
}