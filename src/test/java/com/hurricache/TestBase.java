package com.hurricache;

import com.hurricache.client.FastCacheAsyncStandaloneClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;

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
        client = new FastCacheAsyncStandaloneClient("127.0.0.1",50000,0,  Duration.ofSeconds(3600));
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        if (client != null) {
            client.shutdown();
        }
    }
}