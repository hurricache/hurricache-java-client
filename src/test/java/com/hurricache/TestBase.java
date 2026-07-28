package com.hurricache;

import com.hurricache.client.FastCacheAsyncSimpleClient;
import io.grpc.Server;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.time.Duration;

/**
 * Base class for in-memory gRPC tests.
 * Provides a mock FastCache server implementation for testing the client without a physical server.
 */
public abstract class TestBase {

    protected FastCacheAsyncSimpleClient client;
    private Server server;

    @BeforeEach
    void setUp() throws IOException {
        client = new FastCacheAsyncSimpleClient("127.0.0.1", 50000, Duration.ofSeconds(3600));
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        if (client != null) {
            client.shutdown();
        }
        if (server != null) {
            server.shutdown().awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS);
        }
    }
}