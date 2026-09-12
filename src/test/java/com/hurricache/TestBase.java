package com.hurricache;

import com.hurricache.client.FastCacheAsyncStandaloneClient;
import com.hurricache.grpc.LockStatus;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Base class for in-memory gRPC tests.
 * Provides a mock FastCache server implementation for testing the client without a physical server.
 */
public abstract class TestBase {

    protected FastCacheAsyncStandaloneClient client;

    protected Duration getTestTtl() {
        return Duration.ofMinutes(3);
    }

    protected byte[] createLargePayload(int size) {
        byte[] bytes = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        byte[] payload = new byte[size];
        System.arraycopy(bytes, 0, payload, 0, bytes.length);
        for (int i = bytes.length; i < size; i++) {
            payload[i] = (byte) (i % 128);
        }
        return payload;
    }

    @BeforeEach
    void setUp() throws IOException {
        client = new FastCacheAsyncStandaloneClient("127.0.0.1",50000,0,  Duration.ofSeconds(3600)){
            @Override
            public Duration getDefaultTtl() {
                return Duration.ofMinutes(10);
            }
        };
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        if (client != null) {
            client.shutdown();
        }
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

    protected void assertUnsupportedMethod(CompletableFuture<?> future) {
        try {
            future.get();
            Assertions.fail("Expected INTERNAL error for unsupported method");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.INTERNAL, cause.getStatus().getCode());
            Assertions.assertTrue(cause.getStatus().getDescription().contains("Key not found or type is not correct"),
                                  "Expected 'Key not found or type is not correct' but got: " + cause.getStatus().getDescription());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            Assertions.fail(e.getMessage());
        }
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
}