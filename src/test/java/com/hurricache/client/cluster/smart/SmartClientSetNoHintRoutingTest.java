package com.hurricache.client.cluster.smart;

import com.hurricache.TestBaseCluster;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Mode;
import com.hurricache.client.intf.Variant;
import com.hurricache.grpc.ContainerType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * SmartClient routing tests for Set operations WITHOUT keyHint.
 */
public class SmartClientSetNoHintRoutingTest extends TestBaseCluster {

    @Test
    @DisplayName("Create empty set, read on MASTER without keyHint")
    void testCreateEmptySetWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "set_no_hint_empty_" + UUID.randomUUID();

        KeyHintData hint = client.createSet(key, List.of()).get();
        assertNotNull(hint);
        Thread.sleep(500);

        Integer size = client.setMode(Mode.MASTER).getSize(key).get();
        assertEquals(0, size);
    }

    @Test
    @DisplayName("Create empty set, read on BACKUP without keyHint")
    void testCreateEmptySetWithoutKeyHintOnBackup() throws ExecutionException, InterruptedException {
        String key = "set_no_hint_empty_b_" + UUID.randomUUID();

        KeyHintData hint = client.createSet(key, List.of()).get();
        assertNotNull(hint);
        Thread.sleep(500);

        Integer size = client.setMode(Mode.BACKUP).getSize(key).get();
        assertEquals(0, size);
    }

    @Test
    @DisplayName("Create set with bytes, read on MASTER without keyHint")
    void testCreateSetWithBytesWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "set_no_hint_bytes_" + UUID.randomUUID();
        List<Variant> data = List.of(v("item1"), v("item2"), v("item3"));

        KeyHintData hint = client.createSet(key, data).get();
        assertNotNull(hint);
        Thread.sleep(500);

        List<Variant> stream = client.setMode(Mode.MASTER).getSet(key).get();
        assertEquals(3, stream.size());
    }

    @Test
    @DisplayName("Create set with bytes, read on BACKUP without keyHint")
    void testCreateSetWithBytesWithoutKeyHintOnBackup() throws ExecutionException, InterruptedException {
        String key = "set_no_hint_bytes_b_" + UUID.randomUUID();
        List<Variant> data = List.of(v("item1"), v("item2"), v("item3"));

        KeyHintData hint = client.createSet(key, data).get();
        assertNotNull(hint);
        Thread.sleep(500);

        List<Variant> stream = client.setMode(Mode.BACKUP).getSet(key).get();
        assertEquals(3, stream.size());
    }

    @Test
    @DisplayName("Create set with int, read on MASTER without keyHint")
    void testCreateSetWithIntWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "set_no_hint_int_" + UUID.randomUUID();
        List<Variant> data = List.of(vi(10), vi(20), vi(30));

        KeyHintData hint = client.createSet(key, data).get();
        assertNotNull(hint);
        Thread.sleep(500);

        List<Variant> stream = client.setMode(Mode.MASTER).getSet(key).get();
        assertEquals(3, stream.size());
    }

    @Test
    @DisplayName("Create set with int, read on BACKUP without keyHint")
    void testCreateSetWithIntWithoutKeyHintOnBackup() throws ExecutionException, InterruptedException {
        String key = "set_no_hint_int_b_" + UUID.randomUUID();
        List<Variant> data = List.of(vi(10), vi(20), vi(30));

        KeyHintData hint = client.createSet(key, data).get();
        assertNotNull(hint);
        Thread.sleep(500);

        List<Variant> stream = client.setMode(Mode.BACKUP).getSet(key).get();
        assertEquals(3, stream.size());
    }

    @Test
    @DisplayName("Add element to set on MASTER without keyHint")
    void testAddElementToSetWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "set_no_hint_add_" + UUID.randomUUID();

        KeyHintData hint = client.createSet(key, List.of()).get();
        Thread.sleep(500);

        client.setMode(Mode.MASTER);
        Integer added = client.addElementUnordered(key, null, List.of(v("elem1"))).get();
        assertEquals(1, added);

        Thread.sleep(500);
        List<Variant> stream = client.setMode(Mode.BACKUP).getSet(key).get();
        assertEquals(1, stream.size());
    }

    @Test
    @DisplayName("Add element to set on BACKUP without keyHint")
    void testAddElementToSetWithoutKeyHintOnBackup() throws ExecutionException, InterruptedException {
        String key = "set_no_hint_add_b_" + UUID.randomUUID();

        KeyHintData hint = client.createSet(key, List.of()).get();
        Thread.sleep(500);

        client.setMode(Mode.MASTER);
        client.addElementUnordered(key, null, List.of(v("elem1"))).get();
        Thread.sleep(500);

        Integer added = client.setMode(Mode.BACKUP).addElementUnordered(key, null, List.of(v("elem2"))).get();
        assertEquals(1, added);

        Thread.sleep(500);
        Integer size = client.setMode(Mode.MASTER).getSize(key, null).get();
        assertEquals(2, size);
    }

    @Test
    @DisplayName("Remove element from set on MASTER without keyHint")
    void testRemoveElementFromSetWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "set_no_hint_remove_" + UUID.randomUUID();
        List<Variant> data = List.of(v("elem1"), v("elem2"));

        KeyHintData hint = client.createSet(key, data).get();
        Thread.sleep(500);

        client.setMode(Mode.MASTER);
        Integer removed = client.removeFromContainerByKeyAndValue(key, ContainerType.SET, List.of(), List.of(v("elem1"))).get();
        assertEquals(1, removed);

        Thread.sleep(500);
        Integer size = client.setMode(Mode.BACKUP).getSize(key).get();
        assertEquals(1, size);
    }

    @Test
    @DisplayName("Remove element from set on BACKUP without keyHint")
    void testRemoveElementFromSetWithoutKeyHintOnBackup() throws ExecutionException, InterruptedException {
        String key = "set_no_hint_remove_b_" + UUID.randomUUID();
        List<Variant> data = List.of(v("elem1"), v("elem2"));

        KeyHintData hint = client.createSet(key, data).get();
        Thread.sleep(500);

        client.setMode(Mode.MASTER);
        client.removeFromContainerByKeyAndValue(key, ContainerType.SET, List.of(), List.of(v("elem1"))).get();
        Thread.sleep(500);

        Integer removed = client.setMode(Mode.BACKUP).removeFromContainerByKeyAndValue(key, ContainerType.SET, List.of(), List.of(v("elem2"))).get();
        assertEquals(1, removed);
    }

    @Test
    @DisplayName("Check set contains on MASTER without keyHint")
    void testSetContainsOnMasterWithoutKeyHint() throws ExecutionException, InterruptedException {
        String key = "set_no_hint_contains_" + UUID.randomUUID();
        List<Variant> data = List.of(v("elem1"), v("elem2"));

        KeyHintData hint = client.createSet(key, data).get();
        Thread.sleep(500);

        Boolean contains = client.setMode(Mode.MASTER).containsKey(key, null, "elem1".getBytes()).get();
        assertTrue(contains);
    }

    @Test
    @DisplayName("Check set contains on BACKUP without keyHint")
    void testSetContainsOnBackupWithoutKeyHint() throws ExecutionException, InterruptedException {
        String key = "set_no_hint_contains_b_" + UUID.randomUUID();
        List<Variant> data = List.of(v("elem1"), v("elem2"));

        KeyHintData hint = client.createSet(key, data).get();
        Thread.sleep(500);

        client.setMode(Mode.MASTER);
        client.removeFromContainerByKeyAndValue(key, ContainerType.SET, List.of(), List.of(v("elem1"))).get();
        Thread.sleep(500);

        Boolean contains = client.setMode(Mode.BACKUP).containsKey(key, null, "elem1".getBytes()).get();
        assertFalse(contains);
    }

    @Test
    @DisplayName("Get set size on MASTER without keyHint")
    void testSetSizeOnMasterWithoutKeyHint() throws ExecutionException, InterruptedException {
        String key = "set_no_hint_size_" + UUID.randomUUID();
        List<Variant> data = List.of(v("elem1"), v("elem2"), v("elem3"));

        KeyHintData hint = client.createSet(key, data).get();
        Thread.sleep(500);

        Integer size = client.setMode(Mode.MASTER).getSize(key).get();
        assertEquals(3, size);
    }

    @Test
    @DisplayName("Get set size on BACKUP without keyHint")
    void testSetSizeOnBackupWithoutKeyHint() throws ExecutionException, InterruptedException {
        String key = "set_no_hint_size_b_" + UUID.randomUUID();
        List<Variant> data = List.of(v("elem1"), v("elem2"), v("elem3"));

        KeyHintData hint = client.createSet(key, data).get();
        Thread.sleep(500);

        Integer size = client.setMode(Mode.BACKUP).getSize(key).get();
        assertEquals(3, size);
    }

    @Test
    @DisplayName("Stream set on MASTER without keyHint")
    void testStreamSetOnMasterWithoutKeyHint() throws ExecutionException, InterruptedException {
        String key = "set_no_hint_stream_" + UUID.randomUUID();
        List<Variant> data = List.of(v("a"), v("b"), v("c"));

        KeyHintData hint = client.createSet(key, data).get();
        Thread.sleep(500);

        List<Variant> stream = client.setMode(Mode.MASTER).getSet(key).get();
        assertEquals(3, stream.size());
    }

    @Test
    @DisplayName("Stream set on BACKUP without keyHint")
    void testStreamSetOnBackupWithoutKeyHint() throws ExecutionException, InterruptedException {
        String key = "set_no_hint_stream_b_" + UUID.randomUUID();
        List<Variant> data = List.of(v("a"), v("b"), v("c"));

        KeyHintData hint = client.createSet(key, data).get();
        Thread.sleep(500);

        List<Variant> stream = client.setMode(Mode.BACKUP).getSet(key).get();
        assertEquals(3, stream.size());
    }

    @Test
    @DisplayName("Remove by variant from set on MASTER without keyHint")
    void testRemoveByVariantFromSetOnMasterWithoutKeyHint() throws ExecutionException, InterruptedException {
        String key = "set_no_hint_remove_var_" + UUID.randomUUID();
        List<Variant> data = List.of(v("elem1"), v("elem2"));

        KeyHintData hint = client.createSet(key, data).get();
        Thread.sleep(500);

        client.setMode(Mode.MASTER);
        Integer removed = client.removeFromContainerByKeyAndValue(key, ContainerType.SET, List.of(), List.of(v("elem1"))).get();
        assertEquals(1, removed);

        Thread.sleep(500);
        Integer size = client.setMode(Mode.BACKUP).getSize(key).get();
        assertEquals(1, size);
    }

    @Test
    @DisplayName("Create set with duplicate ints, validate uniqueness on MASTER and BACKUP without keyHint")
    void testCreateSetWithDuplicateIntsUniquenessWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "set_no_hint_unique_int_" + UUID.randomUUID();
        List<Variant> data = List.of(vi(10), vi(20), vi(10));

        KeyHintData hint = client.createSet(key, data).get();
        assertNotNull(hint);
        Thread.sleep(500);

        // On MASTER - should have 2 unique elements
        List<Variant> stream = client.setMode(Mode.MASTER).getSet(key).get();
        assertEquals(2, stream.size());

        // On BACKUP - should also have 2 unique elements
        stream = client.setMode(Mode.BACKUP).getSet(key).get();
        assertEquals(2, stream.size());
    }

    @Test
    @DisplayName("addElementUnordered ignores duplicates on MASTER without keyHint")
    void testAddElementUnorderedDuplicateOnMasterWithoutKeyHint() throws ExecutionException, InterruptedException {
        String key = "set_no_hint_add_dup_" + UUID.randomUUID();
        List<Variant> data = List.of(v("item1"));

        KeyHintData hint = client.createSet(key, data).get();
        Thread.sleep(500);

        // Try to add duplicate on MASTER
        client.setMode(Mode.MASTER);
        Integer added = client.addElementUnordered(key, null, List.of(v("item1"))).get();
        assertEquals(0, added);
        Thread.sleep(500);

        // Verify on BACKUP - should still have 1 element
        List<Variant> stream = client.setMode(Mode.BACKUP).getSet(key).get();
        assertEquals(1, stream.size());
    }
}