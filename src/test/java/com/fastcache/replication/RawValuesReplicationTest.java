package com.fastcache.replication;

import com.fastcache.client.FastCacheAsyncSimpleClient;
import com.fastcache.grpc.KeyHint;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class RawValuesReplicationTest {

    private FastCacheAsyncSimpleClient client;
    private FastCacheAsyncSimpleClient clientReplica;

    @BeforeEach
    void init() throws IOException {
        client = new FastCacheAsyncSimpleClient("127.0.0.1", 50000);
        clientReplica = new FastCacheAsyncSimpleClient("127.0.0.1", 60000);
    }

    @Test
    void singleCreateValue() throws ExecutionException, InterruptedException {
        String testKey = "singleCreateValueKey";
        String testValue = "singleCreateValueValue";
        KeyHint KeyHint = client.createKeyValue(testKey, testValue.getBytes(StandardCharsets.UTF_8)).get();
        byte[] bytes = client.getValue(testKey).get();
        byte[] bytes1 = client.getValue(testKey, KeyHint).get();
        Thread.sleep(TimeUnit.SECONDS.toMillis(1));
        byte[] clientReplicaD = clientReplica.getValue(testKey).get();
        byte[] clientReplicaD1 = clientReplica.getValue(testKey, KeyHint).get();

        Assertions.assertNotNull(KeyHint);
        Assertions.assertEquals(testValue, new String(bytes1));
        Assertions.assertEquals(testValue, new String(bytes));
        Assertions.assertEquals(new String(bytes), new String(bytes1));
        Assertions.assertEquals(new String(bytes), new String(clientReplicaD));
        Assertions.assertEquals(new String(clientReplicaD), new String(clientReplicaD1));

    }

    @Test
    void singleCreateExistValue() throws ExecutionException, InterruptedException {
        String testKey = "singleCreateExistValue";
        String testValue = "singleCreateExistValue123";
        KeyHint KeyHint = client.createKeyValue(testKey, testValue.getBytes(StandardCharsets.UTF_8)).get();
        byte[] bytes = client.getValue(testKey).get();
        Boolean isExist = client.existKey(testKey).get();
        Thread.sleep(TimeUnit.SECONDS.toMillis(1));
        byte[] bytesreplica = clientReplica.getValue(testKey).get();
        Boolean isExistreplica = clientReplica.existKey(testKey).get();
        Assertions.assertNotNull(KeyHint);
        Assertions.assertEquals(testValue, new String(bytes));
        Assertions.assertEquals(testValue, new String(bytesreplica));
        Assertions.assertTrue(isExist);
        Assertions.assertTrue(isExistreplica);
    }

    @Test
    void singleCreateExistHintValue() throws ExecutionException, InterruptedException {
        String testKey = "singleCreateExistHintValue";
        String testValue = "singleCreateExistHintValue123";
        KeyHint KeyHint = client.createKeyValue(testKey, testValue.getBytes(StandardCharsets.UTF_8)).get();
        byte[] bytes = client.getValue(testKey).get();
        Boolean isExist = client.existKey(testKey, KeyHint).get();
        Thread.sleep(TimeUnit.SECONDS.toMillis(1));
        Boolean isExistReplica = clientReplica.existKey(testKey, KeyHint).get();
        Assertions.assertNotNull(KeyHint);
        Assertions.assertEquals(testValue, new String(bytes));
        Assertions.assertTrue(isExist);
        Assertions.assertTrue(isExistReplica);
    }

    @Test
    void singleCreateGetAndDeleteValue() throws ExecutionException, InterruptedException {
        String testKey = "singleCreateGetAndDeleteValue";
        String testValue = "singleCreateGetAndDeleteValue";
        KeyHint KeyHint = client.createKeyValue(testKey, testValue.getBytes(StandardCharsets.UTF_8)).get();
        byte[] bytes = client.getAndDeleteValue(testKey).get();
        Assertions.assertNotNull(KeyHint);
        Assertions.assertEquals(testValue, new String(bytes));
        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(1));
            clientReplica.getValue(testKey).get();
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    @Test
    void singleGenNonExistValue() throws InterruptedException {
        String testKey = "singleGenNonExistValue";
        try {
            client.getValue(testKey).get();
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
        Thread.sleep(TimeUnit.SECONDS.toMillis(1));
        try {
            clientReplica.getValue(testKey).get();
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    @Test
    void singleNonExistValue() throws InterruptedException {
        String testKey = "singleNonExistValue";
        try {
            client.existKey(testKey).get();
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
        Thread.sleep(TimeUnit.SECONDS.toMillis(1));
        try {
            clientReplica.existKey(testKey).get();
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    @Test
    void singleCreateUpdateValue() throws ExecutionException, InterruptedException {
        String testKey = "singleCreateUpdateValue";
        String testValue = "singleCreateUpdateValueValue";
        String testValueUpdate = "singleCreateUpdateValueValue123";
        KeyHint KeyHint = client.createKeyValue(testKey, testValue.getBytes(StandardCharsets.UTF_8)).get();
        byte[] bytes = client.getValue(testKey).get();
        Assertions.assertNotNull(KeyHint);
        Assertions.assertEquals(testValue, new String(bytes));
        byte[] oldVal = client.updateKeyValue(testKey, testValueUpdate.getBytes(StandardCharsets.UTF_8)).get();
        Thread.sleep(TimeUnit.SECONDS.toMillis(1));
        byte[] newVal = clientReplica.getValue(testKey).get();
        Assertions.assertEquals(testValue, new String(oldVal));
        Assertions.assertEquals(testValueUpdate, new String(newVal));
    }

    @Test
    void singleCreateUpdateKeyHintValue() throws ExecutionException, InterruptedException {
        String testKey = "singleCreateUpdateKeyHintValue";
        String testValue = "singleCreateUpdateKeyHintValue123";
        String testValueUpdate = "singleCreateUpdateKeyHintValue56543";
        KeyHint KeyHint = client.createKeyValue(testKey, testValue.getBytes(StandardCharsets.UTF_8)).get();
        byte[] bytes = client.getValue(testKey).get();
        Assertions.assertNotNull(KeyHint);
        Assertions.assertEquals(testValue, new String(bytes));
        byte[] oldVal = client.updateKeyValue(testKey, KeyHint, testValueUpdate.getBytes(StandardCharsets.UTF_8)).get();
        Thread.sleep(TimeUnit.SECONDS.toMillis(1));
        byte[] newVal = clientReplica.getValue(testKey).get();
        Assertions.assertEquals(testValue, new String(oldVal));
        Assertions.assertEquals(testValueUpdate, new String(newVal));
    }
}