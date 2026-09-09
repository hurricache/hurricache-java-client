package com.hurricache.client.standalone.simple;

import com.hurricache.TestBase;
import com.hurricache.grpc.LockStatus;
import com.hurricache.grpc.LockType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class ControlOperationsTest extends TestBase {

    @Test
    void testTtlMethods() throws ExecutionException, InterruptedException {
        String key = "ttlKey" + UUID.randomUUID();

        client.createKeyValue(key, "data".getBytes()).get();
        Thread.sleep(500);

        Boolean success = client.setTtl(key, null, 100).get();
        Assertions.assertTrue(success);

        Long res = client.getTtl(key).get();
        Assertions.assertTrue(res > 0 && res <= 100, () -> res.toString() + " " + key);
    }

    @Test
    void testLockingMechanism() throws ExecutionException, InterruptedException {
        String lockKey = "resourceKey" + UUID.randomUUID();

        client.createKeyValue(lockKey, "secure_data".getBytes()).get();
        Thread.sleep(500);

        LockStatus lockRes = client.lockObject(lockKey, LockType.WRITE_LOCK, 101, Duration.ofSeconds(30)).get();
        Assertions.assertEquals(LockStatus.OK, lockRes);

        LockStatus lockResConflict = client.lockObject(lockKey, LockType.WRITE_LOCK, 102, Duration.ofSeconds(30)).get();
        Assertions.assertNotEquals(LockStatus.OK, lockResConflict);
    }
}