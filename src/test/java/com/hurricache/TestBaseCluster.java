package com.hurricache;

import com.hurricache.client.FastCacheAsyncSmartClient;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.OrderedPayload;
import com.hurricache.client.intf.OrderedVariant;
import com.hurricache.client.intf.Payload;
import com.hurricache.client.intf.Variant;
import com.hurricache.grpc.AtomicCasRes;
import com.hurricache.grpc.LockStatus;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Base class for in-memory gRPC tests.
 * Provides a mock FastCache server implementation for testing the client without a physical server.
 */
public abstract class TestBaseCluster {

    protected FastCacheAsyncSmartClient client;
    private static final String ALPHA_NUMERIC_POOL = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

    private static final int KEY_SIZE = 1024;
    private static final int VALUE_SIZE = 2048;

    protected static String createRandomString(int size) {
        if (size <= 0) {
            return "";
        }

        StringBuilder sb = new StringBuilder(size);
        ThreadLocalRandom random = ThreadLocalRandom.current();

        for (int i = 0; i < size; i++) {
            int randomIndex = random.nextInt(ALPHA_NUMERIC_POOL.length());
            char randomChar = ALPHA_NUMERIC_POOL.charAt(randomIndex);
            sb.append(randomChar);
        }

        return sb.toString();
    }

    protected Variant createLargePayload(int size) {
        byte[] bytes = UUID.randomUUID().toString().getBytes();
        byte[] payload = new byte[size];
        System.arraycopy(bytes, 0, payload, 0, bytes.length);
        for (int i = bytes.length; i < size; i++) {
            payload[i] = (byte) (i % 128);
        }
        return Variant.ofBytes(payload);
    }
    protected byte[] createLargePayloadKey(int size) {
        byte[] bytes = UUID.randomUUID().toString().getBytes();
        byte[] payload = new byte[size];
        System.arraycopy(bytes, 0, payload, 0, bytes.length);
        for (int i = bytes.length; i < size; i++) {
            payload[i] = (byte) (i % 128);
        }
        return payload;
    }


    @BeforeEach
    void setUp() throws IOException {
        client = new FastCacheAsyncSmartClient(List.of("192.168.0.123:51000","192.168.0.123:61000","192.168.0.123:21000"), 0, Duration.ofSeconds(10)) {
            @Override
            public Duration getDefaultTtl() {
                return getTestTtl();
            }

            @Override
            public int getDefaultClientId() {
                return 0;
            }


        };

    }

    protected Duration getTestTtl(){
        return Duration.ofMinutes(1);
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        if (client != null) {
            client.shutdown();
        }
    }
    protected byte[] bytes(String val) {
        return val.getBytes(StandardCharsets.UTF_8);
    }

    protected void assertDenied(java.util.concurrent.CompletableFuture<?> future) {
        try {
            Object result = future.get();
            if (result instanceof LockStatus lockStatus) {
                Assertions.assertNotEquals(LockStatus.OK.name(), lockStatus.name(), "Expected Not OK: " + lockStatus);
                if (lockStatus == LockStatus.OK) {
                    Assertions.fail("Expected PERMISSION_DENIED - Access Denied by Lock details: " + lockStatus.name());
                }
            } else {
                Assertions.fail("Expected PERMISSION_DENIED - Access Denied by Lock details: " + result );
            }

        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.PERMISSION_DENIED, cause.getStatus().getCode());
            Assertions.assertTrue(cause.getStatus().getDescription().contains("Access Denied by Lock"),
                                  "Expected 'Access Denied by Lock' but got: " + cause.getStatus().getDescription());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            Assertions.fail(e.getMessage());
        }
    }
    protected Variant v(String val) {
        return Variant.ofBytes(val.getBytes(StandardCharsets.UTF_8));
    }

    protected Payload p(String val) {
        return Payload.of(val.getBytes(StandardCharsets.UTF_8));
    }

    protected Payload p(byte[] val) {
        return Payload.of(val);
    }

    protected String str(Variant payload) {
        return new String(payload.bytesValue(), StandardCharsets.UTF_8);
    }
    protected String str(Payload payload) {
        return new String(payload.getValue(), StandardCharsets.UTF_8);
    }

    protected OrderedPayload op(Long order, String val) {
        return OrderedPayload.of(order, val.getBytes(StandardCharsets.UTF_8));
    }


    protected Variant v(byte[] val) {
        return Variant.ofBytes(val);
    }

    protected Variant vi(int val) {
        return Variant.ofLong(val);
    }

    protected Variant vd(double val) {
        return Variant.ofDouble(val);
    }



    protected OrderedPayload op(byte[] value, Long order) {
        return OrderedPayload.of(value, order);
    }

    protected OrderedPayload op(Long order,byte[] value) {
        return OrderedPayload.of(value, order);
    }
    protected OrderedVariant ov(byte[] value, Long order) {
        return OrderedVariant.of(v(value), order);
    }

    protected void assertNotFound(CompletableFuture<?> future) {
        try {
            future.get();
            Assertions.fail("Expected NOT_FOUND after TTL expiry");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            Assertions.fail(e.getMessage());
        }
    }

    protected void assertInvalidArgument(CompletableFuture<?> future) {
        try {
            future.get();
            Assertions.fail("Expected INVALID_ARGUMENT");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.INVALID_ARGUMENT, cause.getStatus().getCode());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            Assertions.fail(e.getMessage());
        }
    }
}