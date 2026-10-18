package com.hurricache.client.cluster.smart;

import com.hurricache.TestBaseCluster;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.Mode;
import com.hurricache.client.intf.OrderedVariant;
import com.hurricache.client.intf.Variant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

public class SmartClientOrderedSetNoHintRoutingTest extends TestBaseCluster {

    @Test
    @DisplayName("Create empty ordered set, read on MASTER without keyHint")
    void testCreateEmptyOrderedSetWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "oset_no_hint_empty_" + UUID.randomUUID();
        KeyHintData hint = client.createOrderedSet(key, new ArrayList<>()).get();
        assertNotNull(hint);
        Thread.sleep(500);
        Integer size = client.setMode(Mode.MASTER).getSize(key).get();
        assertEquals(0, size);
    }

    @Test
    @DisplayName("Create empty ordered set, read on BACKUP without keyHint")
    void testCreateEmptyOrderedSetWithoutKeyHintOnBackup() throws ExecutionException, InterruptedException {
        String key = "oset_no_hint_empty_b_" + UUID.randomUUID();
        KeyHintData hint = client.createOrderedSet(key, new ArrayList<>()).get();
        assertNotNull(hint);
        Thread.sleep(500);
        Integer size = client.setMode(Mode.BACKUP).getSize(key).get();
        assertEquals(0, size);
    }

    @Test
    @DisplayName("Create ordered set with bytes, read on MASTER without keyHint")
    void testCreateOrderedSetWithBytesWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "oset_no_hint_bytes_" + UUID.randomUUID();
        List<OrderedVariant> data = List.of(
                OrderedVariant.ofBytes("item1".getBytes(), 1L),
                OrderedVariant.ofBytes("item2".getBytes(), 2L),
                OrderedVariant.ofBytes("item3".getBytes(), 3L)
        );
        KeyHintData hint = client.createOrderedSet(key, data).get();
        assertNotNull(hint);
        Thread.sleep(500);
        List<OrderedVariant> stream = client.setMode(Mode.MASTER).getOrderedSet(key).get();
        assertEquals(3, stream.size());
    }

    @Test
    @DisplayName("Create ordered set with bytes, read on BACKUP without keyHint")
    void testCreateOrderedSetWithBytesWithoutKeyHintOnBackup() throws ExecutionException, InterruptedException {
        String key = "oset_no_hint_bytes_b_" + UUID.randomUUID();
        List<OrderedVariant> data = List.of(
                OrderedVariant.ofBytes("item1".getBytes(), 1L),
                OrderedVariant.ofBytes("item2".getBytes(), 2L),
                OrderedVariant.ofBytes("item3".getBytes(), 3L)
        );
        KeyHintData hint = client.createOrderedSet(key, data).get();
        assertNotNull(hint);
        Thread.sleep(500);
        List<OrderedVariant> stream = client.setMode(Mode.BACKUP).getOrderedSet(key).get();
        assertEquals(3, stream.size());
    }

    @Test
    @DisplayName("Create ordered set with int, read on MASTER without keyHint")
    void testCreateOrderedSetWithIntWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "oset_no_hint_int_" + UUID.randomUUID();
        List<OrderedVariant> data = List.of(
                OrderedVariant.ofInt(10, 1L),
                OrderedVariant.ofInt(20, 2L),
                OrderedVariant.ofInt(30, 3L)
        );
        KeyHintData hint = client.createOrderedSet(key, data).get();
        assertNotNull(hint);
        Thread.sleep(500);
        List<OrderedVariant> stream = client.setMode(Mode.MASTER).getOrderedSet(key).get();
        assertEquals(3, stream.size());
    }

    @Test
    @DisplayName("Create ordered set with int, read on BACKUP without keyHint")
    void testCreateOrderedSetWithIntWithoutKeyHintOnBackup() throws ExecutionException, InterruptedException {
        String key = "oset_no_hint_int_b_" + UUID.randomUUID();
        List<OrderedVariant> data = List.of(
                OrderedVariant.ofInt(10, 1L),
                OrderedVariant.ofInt(20, 2L),
                OrderedVariant.ofInt(30, 3L)
        );
        KeyHintData hint = client.createOrderedSet(key, data).get();
        assertNotNull(hint);
        Thread.sleep(500);
        List<OrderedVariant> stream = client.setMode(Mode.BACKUP).getOrderedSet(key).get();
        assertEquals(3, stream.size());
    }

    @Test
    @DisplayName("Add element to ordered set on MASTER without keyHint")
    void testAddElementToOrderedSetWithoutKeyHintOnMaster() throws ExecutionException, InterruptedException {
        String key = "oset_no_hint_add_" + UUID.randomUUID();
        KeyHintData hint = client.createOrderedSet(key, new ArrayList<>()).get();
        Thread.sleep(500);
        client.setMode(Mode.MASTER);
        Integer added = client.addElementWithWeight(key, null, List.of(OrderedVariant.ofBytes("elem1".getBytes(), 1L))).get();
        assertEquals(1, added);
        Thread.sleep(500);
        List<OrderedVariant> stream = client.setMode(Mode.BACKUP).getOrderedSet(key).get();
        assertEquals(1, stream.size());
    }

    @Test
    @DisplayName("Add element to ordered set on BACKUP without keyHint")
    void testAddElementToOrderedSetWithoutKeyHintOnBackup() throws ExecutionException, InterruptedException {
        String key = "oset_no_hint_add_b_" + UUID.randomUUID();
        KeyHintData hint = client.createOrderedSet(key, new ArrayList<>()).get();
        Thread.sleep(500);
        client.setMode(Mode.MASTER);
        client.addElementWithWeight(key, null, List.of(OrderedVariant.ofBytes("elem1".getBytes(), 1L))).get();
        Thread.sleep(500);
        Integer added = client.setMode(Mode.BACKUP).addElementWithWeight(key, null, List.of(OrderedVariant.ofBytes("elem2".getBytes(), 2L))).get();
        assertEquals(1, added);
        Thread.sleep(500);
        Integer sizeb = client.setMode(Mode.BACKUP).getSize(key).get();
        Integer sizem = client.setMode(Mode.MASTER).getSize(key).get();
        assertEquals(2, sizeb);
        assertEquals(2, sizem);
    }

    @Test
    @DisplayName("Check ordered set contains on MASTER without keyHint")
    void testOrderedSetContainsOnMasterWithoutKeyHint() throws ExecutionException, InterruptedException {
        String key = "oset_no_hint_contains_" + UUID.randomUUID();
        List<OrderedVariant> data = List.of(
                OrderedVariant.ofBytes("elem1".getBytes(), 1L),
                OrderedVariant.ofBytes("elem2".getBytes(), 2L)
        );
        KeyHintData hint = client.createOrderedSet(key, data).get();
        Thread.sleep(500);
        Boolean contains = client.setMode(Mode.MASTER).containsKey(key, null, "elem1".getBytes()).get();
        assertTrue(contains);
    }

    @Test
    @DisplayName("Check ordered set contains on BACKUP without keyHint")
    void testOrderedSetContainsOnBackupWithoutKeyHint() throws ExecutionException, InterruptedException {
        String key = "oset_no_hint_contains_b_" + UUID.randomUUID();
        List<OrderedVariant> data = List.of(
                OrderedVariant.ofBytes("elem1".getBytes(), 1L),
                OrderedVariant.ofBytes("elem2".getBytes(), 2L)
        );
        KeyHintData hint = client.createOrderedSet(key, data).get();
        Thread.sleep(500);
        Boolean contains = client.setMode(Mode.BACKUP).containsKey(key, null, "elem1".getBytes()).get();
        assertTrue(contains);
        contains = client.setMode(Mode.MASTER).containsKey(key, null, "missing".getBytes()).get();
        assertFalse(contains);
    }

    @Test
    @DisplayName("Get ordered set size on MASTER without keyHint")
    void testOrderedSetSizeOnMasterWithoutKeyHint() throws ExecutionException, InterruptedException {
        String key = "oset_no_hint_size_" + UUID.randomUUID();
        List<OrderedVariant> data = List.of(
                OrderedVariant.ofBytes("elem1".getBytes(), 1L),
                OrderedVariant.ofBytes("elem2".getBytes(), 2L),
                OrderedVariant.ofBytes("elem3".getBytes(), 3L)
        );
        KeyHintData hint = client.createOrderedSet(key, data).get();
        Thread.sleep(500);
        Integer size = client.setMode(Mode.MASTER).getSize(key).get();
        assertEquals(3, size);
    }

    @Test
    @DisplayName("Get ordered set size on BACKUP without keyHint")
    void testOrderedSetSizeOnBackupWithoutKeyHint() throws ExecutionException, InterruptedException {
        String key = "oset_no_hint_size_b_" + UUID.randomUUID();
        List<OrderedVariant> data = List.of(
                OrderedVariant.ofBytes("elem1".getBytes(), 1L),
                OrderedVariant.ofBytes("elem2".getBytes(), 2L),
                OrderedVariant.ofBytes("elem3".getBytes(), 3L)
        );
        KeyHintData hint = client.createOrderedSet(key, data).get();
        Thread.sleep(500);
        Integer size = client.setMode(Mode.BACKUP).getSize(key).get();
        assertEquals(3, size);
    }

    @Test
    @DisplayName("Stream ordered set on MASTER without keyHint")
    void testStreamOrderedSetOnMasterWithoutKeyHint() throws ExecutionException, InterruptedException {
        String key = "oset_no_hint_stream_" + UUID.randomUUID();
        List<OrderedVariant> data = List.of(
                OrderedVariant.ofBytes("a".getBytes(), 1L),
                OrderedVariant.ofBytes("b".getBytes(), 2L),
                OrderedVariant.ofBytes("c".getBytes(), 3L)
        );
        KeyHintData hint = client.createOrderedSet(key, data).get();
        Thread.sleep(500);
        List<OrderedVariant> stream = client.setMode(Mode.MASTER).getOrderedSet(key).get();
        assertEquals(3, stream.size());
    }

    @Test
    @DisplayName("Stream ordered set on BACKUP without keyHint")
    void testStreamOrderedSetOnBackupWithoutKeyHint() throws ExecutionException, InterruptedException {
        String key = "oset_no_hint_stream_b_" + UUID.randomUUID();
        List<OrderedVariant> data = List.of(
                OrderedVariant.ofBytes("a".getBytes(), 1L),
                OrderedVariant.ofBytes("b".getBytes(), 2L),
                OrderedVariant.ofBytes("c".getBytes(), 3L)
        );
        KeyHintData hint = client.createOrderedSet(key, data).get();
        Thread.sleep(500);
        List<OrderedVariant> stream = client.setMode(Mode.BACKUP).getOrderedSet(key).get();
        assertEquals(3, stream.size());
    }

    @Test
    @DisplayName("Get element at position on MASTER without keyHint")
    void testGetElementAtPositionOnMasterWithoutKeyHint() throws ExecutionException, InterruptedException {
        String key = "oset_no_hint_position_" + UUID.randomUUID();
        List<OrderedVariant> data = List.of(
                OrderedVariant.ofBytes("first".getBytes(), 1L),
                OrderedVariant.ofBytes("second".getBytes(), 2L),
                OrderedVariant.ofBytes("third".getBytes(), 3L)
        );
        KeyHintData hint = client.createOrderedSet(key, data).get();
        Thread.sleep(500);
        Variant element = client.setMode(Mode.MASTER).getAt(key, null, 1).get();
        assertNotNull(element);
        assertEquals("first", new String(element.bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("Get element at position on BACKUP without keyHint")
    void testGetElementAtPositionOnBackupWithoutKeyHint() throws ExecutionException, InterruptedException {
        String key = "oset_no_hint_position_b_" + UUID.randomUUID();
        List<OrderedVariant> data = List.of(
                OrderedVariant.ofBytes("first".getBytes(), 1L),
                OrderedVariant.ofBytes("second".getBytes(), 2L),
                OrderedVariant.ofBytes("third".getBytes(), 3L)
        );
        KeyHintData hint = client.createOrderedSet(key, data).get();
        Thread.sleep(500);
        Variant element = client.setMode(Mode.BACKUP).getAt(key, null, 2).get();
        assertNotNull(element);
        assertEquals("second", new String(element.bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("Get and delete element at position on MASTER without keyHint")
    void testGetAndDeleteElementAtPositionOnMasterWithoutKeyHint() throws ExecutionException, InterruptedException {
        String key = "oset_no_hint_get_delete_" + UUID.randomUUID();
        List<OrderedVariant> data = List.of(
                OrderedVariant.ofBytes("1".getBytes(), 1L),
                OrderedVariant.ofBytes("2".getBytes(), 2L),
                OrderedVariant.ofBytes("3".getBytes(), 3L)
        );
        KeyHintData hint = client.createOrderedSet(key, data).get();
        Thread.sleep(500);
        client.setMode(Mode.MASTER);
        Variant deleted = client.popAt(key, null, 1).get();
        assertNotNull(deleted);
        assertEquals("1", new String(deleted.bytesValue(), StandardCharsets.UTF_8));
        Thread.sleep(500);
        Integer size = client.setMode(Mode.BACKUP).getSize(key).get();
        assertEquals(2, size);
    }

    @Test
    @DisplayName("Delete element at position on MASTER without keyHint")
    void testDeleteElementAtPositionOnMasterWithoutKeyHint() throws ExecutionException, InterruptedException {
        String key = "oset_no_hint_delete_pos_" + UUID.randomUUID();
        List<OrderedVariant> data = List.of(
                OrderedVariant.ofBytes("1".getBytes(), 1L),
                OrderedVariant.ofBytes("2".getBytes(), 2L),
                OrderedVariant.ofBytes("3".getBytes(), 3L)
        );
        KeyHintData hint = client.createOrderedSet(key, data).get();
        Thread.sleep(500);
        client.setMode(Mode.MASTER);
        Integer deleted = client.removeAt(key, null, 1).get();
        assertEquals(1, deleted);
        Thread.sleep(500);
        Integer size = client.setMode(Mode.BACKUP).getSize(key).get();
        assertEquals(2, size);
    }

    @Test
    @DisplayName("Stream element range on MASTER without keyHint")
    void testStreamElementRangeOnMasterWithoutKeyHint() throws ExecutionException, InterruptedException {
        String key = "oset_no_hint_range_" + UUID.randomUUID();
        List<OrderedVariant> data = List.of(
                OrderedVariant.ofBytes("1".getBytes(), 1L),
                OrderedVariant.ofBytes("2".getBytes(), 2L),
                OrderedVariant.ofBytes("3".getBytes(), 3L),
                OrderedVariant.ofBytes("4".getBytes(), 4L),
                OrderedVariant.ofBytes("5".getBytes(), 5L)
        );
        KeyHintData hint = client.createOrderedSet(key, data).get();
        Thread.sleep(500);
        List<OrderedVariant> range = client.setMode(Mode.MASTER).getElementInRangeOrderedSet(key, null, 2L, 4L, false).get();
        assertEquals(3, range.size());
        assertEquals("2", new String(range.get(0).bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("Stream element range with reverse on BACKUP without keyHint")
    void testStreamElementRangeReverseOnBackupWithoutKeyHint() throws ExecutionException, InterruptedException {
        String key = "oset_no_hint_range_reverse_b_" + UUID.randomUUID();
        List<OrderedVariant> data = List.of(
                OrderedVariant.ofBytes("1".getBytes(), 1L),
                OrderedVariant.ofBytes("2".getBytes(), 2L),
                OrderedVariant.ofBytes("3".getBytes(), 3L)
        );
        KeyHintData hint = client.createOrderedSet(key, data).get();
        Thread.sleep(500);
        List<OrderedVariant> range = client.setMode(Mode.BACKUP).getElementInRangeOrderedSet(key, null, 1L, 3L, true).get();
        assertEquals(3, range.size());
        assertEquals("3", new String(range.get(0).bytesValue(), StandardCharsets.UTF_8));
        assertEquals("1", new String(range.get(2).bytesValue(), StandardCharsets.UTF_8));
    }
}