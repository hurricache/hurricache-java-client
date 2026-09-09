package com.hurricache.client.standalone.simple;

import com.hurricache.TestBase;
import com.hurricache.client.intf.KeyHintData;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class RawValuesTest extends TestBase {

    @Test
    void singleCreateValue() throws ExecutionException, InterruptedException {
        String testKey = "singleCreateValueKey" + UUID.randomUUID();
        String testValue = "singleCreateValueValue" + UUID.randomUUID();
        KeyHintData KeyHint = client.createKeyValue(testKey, testValue.getBytes(StandardCharsets.UTF_8)).get();
        byte[] bytes = client.getValue(testKey).get();
        // Key doesn't exist yet, will throw NOT_FOUND
    }

    @Test
    void singleCreateAndGet() throws ExecutionException, InterruptedException {
        String testKey = "singleCreateValueKey" + UUID.randomUUID();
        String testValue = "singleCreateValueValue" + UUID.randomUUID();
        client.createKeyValue(testKey, testValue.getBytes(StandardCharsets.UTF_8)).get();
        Thread.sleep(150);
        byte[] bytes = client.getValue(testKey).get();
        Assertions.assertNotNull(bytes);
        Assertions.assertEquals(testValue, new String(bytes));
    }

    @Test
    void singleCreateExistValue() throws ExecutionException, InterruptedException {
        String testKey = "singleCreateExistValue" + UUID.randomUUID();
        String testValue = "singleCreateExistValue123" + UUID.randomUUID();
        client.createKeyValue(testKey, testValue.getBytes(StandardCharsets.UTF_8)).get();
        Thread.sleep(150);
        byte[] bytes = client.getValue(testKey).get();
        Boolean isExist = client.existKey(testKey).get();
        Assertions.assertNotNull(bytes);
        Assertions.assertEquals(testValue, new String(bytes));
        Assertions.assertTrue(isExist);
    }

    @Test
    void singleCreateGetAndDeleteValue() throws ExecutionException, InterruptedException {
        String testKey = "singleCreateGetAndDeleteValue" + UUID.randomUUID();
        String testValue = "singleCreateGetAndDeleteValue" + UUID.randomUUID();
        client.createKeyValue(testKey, testValue.getBytes(StandardCharsets.UTF_8)).get();
        Thread.sleep(100);
        byte[] bytes = client.getAndDeleteValue(testKey).get();
        Assertions.assertNotNull(bytes);
        Assertions.assertEquals(testValue, new String(bytes));
        try {
            client.getAndDeleteValue(testKey).get();
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    @Test
    void singleGenNonExistValue() throws InterruptedException {
        String testKey = "singleGenNonExistValue" + UUID.randomUUID();
        try {
            client.getValue(testKey).get();
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    @Test
    void singleNonExistValue() throws InterruptedException {
        String testKey = "singleNonExistValue" + UUID.randomUUID();
        try {
            client.existKey(testKey).get();
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    @Test
    void singleCreateUpdateValue() throws ExecutionException, InterruptedException {
        String testKey = "singleCreateUpdateValue" + UUID.randomUUID();
        String testValue = "singleCreateUpdateValueValue" + UUID.randomUUID();
        String testValueUpdate = "singleCreateUpdateValueValue123" + UUID.randomUUID();
        client.createKeyValue(testKey, testValue.getBytes(StandardCharsets.UTF_8)).get();
        Thread.sleep(150);
        byte[] bytes = client.getValue(testKey).get();
        Assertions.assertNotNull(bytes);
        Assertions.assertEquals(testValue, new String(bytes));
        client.updateKeyValue(testKey, testValueUpdate.getBytes(StandardCharsets.UTF_8)).get();
        byte[] newVal = client.getValue(testKey).get();
        Assertions.assertEquals(testValueUpdate, new String(newVal));
    }

    @Test
    void singleCreateDelete() throws ExecutionException, InterruptedException {
        String testKey = "singleCreateUpdateValue" + UUID.randomUUID();
        String testValue = "singleCreateUpdateValueValue" + UUID.randomUUID();
        client.createKeyValue(testKey, testValue.getBytes(StandardCharsets.UTF_8)).get();
        Thread.sleep(150);
        byte[] bytes = client.getValue(testKey).get();
        Assertions.assertNotNull(bytes);
        Assertions.assertEquals(testValue, new String(bytes));
        Boolean b = client.remove(testKey).get();
        Assertions.assertTrue(b);
    }

    @Test
    void singleCreateGetDelete() throws ExecutionException, InterruptedException {
        String testKey = "singleCreateUpdateValue" + UUID.randomUUID();
        String testValue = "singleCreateUpdateValueValue" + UUID.randomUUID();
        client.createKeyValue(testKey, testValue.getBytes(StandardCharsets.UTF_8)).get();
        Thread.sleep(150);
        byte[] bytes = client.getValue(testKey).get();
        Assertions.assertNotNull(bytes);
        Assertions.assertEquals(testValue, new String(bytes));
        byte[] bytes1 = client.getAndDeleteValue(testKey).get();
        Assertions.assertEquals(testValue, new String(bytes1));
    }

    @Test
    void singleCreateGetDeleteNoKeyHint() throws ExecutionException, InterruptedException {
        String testKey = "singleCreateUpdateValue" + UUID.randomUUID();
        String testValue = "singleCreateUpdateValueValue" + UUID.randomUUID();
        client.createKeyValue(testKey, testValue.getBytes(StandardCharsets.UTF_8)).get();
        Thread.sleep(150);
        byte[] bytes = client.getValue(testKey).get();
        Assertions.assertNotNull(bytes);
        Assertions.assertEquals(testValue, new String(bytes));
        byte[] bytes1 = client.getAndDeleteValue(testKey).get();
        Assertions.assertEquals(testValue, new String(bytes1));
        Thread.sleep(150);
        assertThrows(ExecutionException.class, () -> {
            client.getValue(testKey).get();
        });
    }
}