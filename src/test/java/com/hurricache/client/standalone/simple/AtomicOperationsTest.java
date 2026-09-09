package com.hurricache.client.standalone.simple;

import com.hurricache.TestBase;
import com.hurricache.grpc.AtomicCasRes;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class AtomicOperationsTest extends TestBase {

    @Test
    void atomicCreateAndStoreTest() throws ExecutionException, InterruptedException {
        String testKey = "atomicCreateStoreKey" + UUID.randomUUID();
        byte[] keyBytes = testKey.getBytes(StandardCharsets.UTF_8);

        Assertions.assertNotNull(client.atomicCreate(keyBytes, 42L).get());
        Thread.sleep(150);

        Assertions.assertNotNull(client.atomicStore(keyBytes, 100L).get());

        Thread.sleep(150);
        long current = client.atomicOr(keyBytes, null, 0L).get();
        Assertions.assertEquals(100L, current);
    }

    @Test
    void atomicExchangeTest() throws ExecutionException, InterruptedException {
        String testKey = "atomicExchangeKey" + UUID.randomUUID();
        byte[] keyBytes = testKey.getBytes(StandardCharsets.UTF_8);

        Assertions.assertNotNull(client.atomicCreate(keyBytes, 10L).get());
        Thread.sleep(150);

        long oldValue = client.atomicExchange(keyBytes, 20L).get();
        Assertions.assertEquals(10L, oldValue);

        Thread.sleep(150);
        long current = client.atomicOr(keyBytes, null, 0L).get();
        Assertions.assertEquals(20L, current);
    }

    @Test
    void atomicAddAndSubTest() throws ExecutionException, InterruptedException {
        String testKey = "atomicAddSubKey" + UUID.randomUUID();
        byte[] keyBytes = testKey.getBytes(StandardCharsets.UTF_8);

        Assertions.assertNotNull(client.atomicCreate(keyBytes, 50L).get());
        Thread.sleep(150);

        long afterAdd = client.atomicAdd(keyBytes, 25L).get();
        Assertions.assertEquals(50L, afterAdd);

        long afterSub = client.atomicSub(keyBytes, 10L).get();
        Assertions.assertEquals(75L, afterSub);

        Thread.sleep(150);
        long backupValue = client.atomicOr(keyBytes, null, 0L).get();
        Assertions.assertEquals(65L, backupValue);
    }

    @Test
    void atomicBitwiseOpsTest() throws ExecutionException, InterruptedException {
        String testKey = "atomicBitwiseKey" + UUID.randomUUID();
        byte[] keyBytes = testKey.getBytes(StandardCharsets.UTF_8);

        Assertions.assertNotNull(client.atomicCreate(keyBytes, 12L).get());
        Thread.sleep(150);

        long afterAnd = client.atomicAnd(keyBytes, null, 10L).get();
        Assertions.assertEquals(12L, afterAnd);

        long afterOr = client.atomicOr(keyBytes, null, 3L).get();
        Assertions.assertEquals(8L, afterOr);

        long afterXor = client.atomicXor(keyBytes, null, 15L).get();
        Assertions.assertEquals(11L, afterXor);

        Thread.sleep(150);
        long backupValue = client.atomicOr(keyBytes, null, 0L).get();
        Assertions.assertEquals(4L, backupValue);
    }

    @Test
    void atomicCompareAndSetSuccessTest() throws ExecutionException, InterruptedException {
        String testKey = "atomicCasSuccessKey" + UUID.randomUUID();
        byte[] keyBytes = testKey.getBytes(StandardCharsets.UTF_8);

        Assertions.assertNotNull(client.atomicCreate(keyBytes, 500L).get());
        Thread.sleep(150);

        AtomicCasRes res = client.atomicCompareAndSet(keyBytes, null, 500L, 600L).get();

        Assertions.assertTrue(res.getResult());
        Assertions.assertFalse(res.hasExpected());

        Thread.sleep(150);
        long backupValue = client.atomicOr(keyBytes, null, 0L).get();
        Assertions.assertEquals(600L, backupValue);
    }

    @Test
    void atomicCompareAndSetFailureTest() throws ExecutionException, InterruptedException {
        String testKey = "atomicCasFailKey" + UUID.randomUUID();
        byte[] keyBytes = testKey.getBytes(StandardCharsets.UTF_8);

        Assertions.assertNotNull(client.atomicCreate(keyBytes, 500L).get());
        Thread.sleep(150);

        AtomicCasRes res = client.atomicCompareAndSet(keyBytes, null, 999L, 600L).get();

        Assertions.assertFalse(res.getResult());
        Assertions.assertEquals(500L, res.getExpected().getVal());

        Thread.sleep(500);
        long backupValue = client.atomicOr(keyBytes, null, 0L).get();
        Assertions.assertEquals(500L, backupValue);
    }

    @Test
    void atomicNonExistKeyTest() {
        String testKey = "atomicNonExistKey" + UUID.randomUUID();
        byte[] keyBytes = testKey.getBytes(StandardCharsets.UTF_8);

        try {
            client.atomicAdd(keyBytes, null, 10L).get();
            Assertions.fail("Expected ExecutionException caused by NOT_FOUND status");
        } catch (Exception e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }
}