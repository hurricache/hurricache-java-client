package hurricache.clients.jedis;

import com.hurricache.client.intf.HurriCacheClientInterface;
import com.hurricache.client.intf.KeyHintData;
import com.hurricache.client.intf.OrderedPayload;
import com.hurricache.client.intf.Payload;
import com.hurricache.grpc.ContainerType;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.jspecify.annotations.NonNull;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

import static com.hurricache.client.KeyValueUtils.weakHash;

/**
 * Full Jedis API drop-in replacement routing requests to HurriCache backend.
 * Supporting KV, List, Hash (Map), Set, and Sorted Set (Ordered Set/Map) operations.
 */
public class Jedis implements AutoCloseable {

    private final HurriCacheClientInterface cacheClient;

    public enum ListPosition {
        BEFORE,
        AFTER
    }

    public Jedis(HurriCacheClientInterface cacheClient) {
        this.cacheClient = Objects.requireNonNull(cacheClient, "Cache client cannot be null");
    }

    // =========================================================================
    // Basic Key-Value Operations
    // =========================================================================

    public String set(String key, String value) {
        byte[] valBytes = value == null ? new byte[0] : value.getBytes(StandardCharsets.UTF_8);
        cacheClient.createKeyValue(key, valBytes).join(); //
        return "OK";
    }

    public String set(byte[] key, byte[] value) {
        cacheClient.createKeyValue(key, value).join(); //
        return "OK";
    }

    public String setex(String key, long seconds, String value) {
        byte[] keyBytes = cacheClient.serializeKey(key); //[cite: 4]
        byte[] valBytes = value == null ? new byte[0] : value.getBytes(StandardCharsets.UTF_8);
        KeyHintData hint = getKeyHintData(keyBytes);
        cacheClient.createKeyValue(keyBytes, hint, valBytes, Duration.ofSeconds(seconds),
                                   cacheClient.getDefaultClientId(), cacheClient.getDefaultTimeout()).join(); //[cite: 4]
        return "OK";
    }

    public String get(String key) {
        byte[] res = cacheClient.getValue(key).join(); //[cite: 4]
        return res != null ? new String(res, StandardCharsets.UTF_8) : null;
    }

    public byte[] get(byte[] key) {
        return cacheClient.getValue(key).join(); //[cite: 4]
    }

    public String getSet(String key, String value) {
        byte[] valBytes = value == null ? new byte[0] : value.getBytes(StandardCharsets.UTF_8);
        byte[] oldVal = cacheClient.updateKeyValue(key, valBytes).join(); //[cite: 4]
        return oldVal != null ? new String(oldVal, StandardCharsets.UTF_8) : null;
    }

    public String getDel(String key) {
        byte[] res = cacheClient.getAndDeleteValue(key).join(); //[cite: 4]
        return res != null ? new String(res, StandardCharsets.UTF_8) : null;
    }

    public Boolean exists(String key) {
        return cacheClient.existKey(key).join(); //[cite: 4]
    }

    public Long del(String... keys) {
        long count = 0;
        for (String key : keys) {
            if (Boolean.TRUE.equals(cacheClient.remove(key).join())) { //[cite: 4]
                count++;
            }
        }
        return count;
    }

    public Long expire(String key, long seconds) {
        byte[] keyBytes = cacheClient.serializeKey(key); //[cite: 4]
        KeyHintData hint = getKeyHintData(keyBytes);
        Boolean ok = cacheClient.setTtl(keyBytes, hint, seconds, cacheClient.getDefaultClientId(), cacheClient.getDefaultTimeout()).join(); //[cite: 4]
        return Boolean.TRUE.equals(ok) ? 1L : 0L;
    }

    public Long ttl(String key) {
        return cacheClient.getTtl(key).join(); //[cite: 4]
    }

    // =========================================================================
    // List / Queue Operations
    // =========================================================================

    public Long rpush(String key, String... strings) {
        List<Payload> dataList = convertToPayloadList(strings);
        byte[] keyBytes = cacheClient.serializeKey(key); //[cite: 4]
        KeyHintData hint = getKeyHintData(keyBytes);

        if (!cacheClient.existKey(keyBytes, hint).join()) { //[cite: 4]
            cacheClient.createList(keyBytes, hint, dataList).join(); //[cite: 4]
        } else {
            cacheClient.addElementToTail(keyBytes, hint, dataList).join(); //[cite: 4]
        }
        return (long) cacheClient.streamList(keyBytes, hint).join().size(); //[cite: 4]
    }

    public Long lpush(String key, String... strings) {
        List<Payload> dataList = convertToPayloadList(strings);
        byte[] keyBytes = cacheClient.serializeKey(key); //[cite: 4]
        KeyHintData hint = getKeyHintData(keyBytes);

        if (!cacheClient.existKey(keyBytes, hint).join()) { //[cite: 4]
            cacheClient.createList(keyBytes, hint, dataList).join(); //[cite: 4]
        } else {
            cacheClient.addElementToHead(keyBytes, hint, dataList).join(); //[cite: 4]
        }
        return (long) cacheClient.streamList(keyBytes, hint).join().size(); //[cite: 4]
    }

    public String lpop(String key) {
        Payload payload = cacheClient.getAndRemoveFront(key).join(); //[cite: 4]
        if (payload == null || payload.getValue() == null) return null;
        return new String(payload.getValue(), StandardCharsets.UTF_8);
    }

    public String rpop(String key) {
        Payload payload = cacheClient.getAndRemoveTail(key).join(); //[cite: 4]
        if (payload == null || payload.getValue() == null) return null;
        return new String(payload.getValue(), StandardCharsets.UTF_8);
    }

    public String lindex(String key, long index) {
        Payload payload = cacheClient.getElementAtPosition(key, (int) index).join(); //[cite: 4]
        if (payload == null || payload.getValue() == null) return null;
        return new String(payload.getValue(), StandardCharsets.UTF_8);
    }

    public List<String> lrange(String key, long start, long stop) {
        byte[] keyBytes = cacheClient.serializeKey(key); //[cite: 4]
        KeyHintData hint = getKeyHintData(keyBytes);
        List<Payload> res = cacheClient.streamElementInRangeUnordered(keyBytes, hint,
                                                                      ContainerType.VECTOR, (int) start, (int) stop).join(); //[cite: 4]
        return convertToStringList(res);
    }

    public Long linsert(String key, ListPosition where, String pivot, String value) {
        byte[] keyBytes = cacheClient.serializeKey(key); //[cite: 4]
        byte[] pivotBytes = pivot == null ? new byte[0] : pivot.getBytes(StandardCharsets.UTF_8);
        byte[] valBytes = value == null ? new byte[0] : value.getBytes(StandardCharsets.UTF_8);

        return linsert(keyBytes, where, pivotBytes, valBytes);
    }

    public Long linsert(byte[] key, ListPosition where, byte[] pivot, byte[] value) {
        KeyHintData hint = getKeyHintData(key);
        List<Payload> valuesToInsert = Collections.singletonList(Payload.of(value));

        try {
            Boolean ok;
            if (where == ListPosition.BEFORE) {
                ok = cacheClient.addElementToPositionBefore(key, hint, valuesToInsert, Payload.of(pivot)).join(); //[cite: 4]
            } else if (where == ListPosition.AFTER) {
                ok = cacheClient.addElementToPositionAfter(key, hint, valuesToInsert, Payload.of(pivot)).join(); //[cite: 4]
            } else {
                throw new IllegalArgumentException("Unknown ListPosition: " + where);
            }

            if (Boolean.TRUE.equals(ok)) {
                return (long) cacheClient.streamList(key, hint).join().size(); //[cite: 4]
            }
            return -1L;
        } catch (CompletionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof StatusRuntimeException statusEx) {
                if (statusEx.getStatus().getCode() == Status.Code.NOT_FOUND) {
                    return -1L;
                }
            }
            throw e;
        }
    }

    // =========================================================================
    // Hashes / Maps (Unordered & Ordered)
    // =========================================================================

    public Long hset(String key, String field, String value) {
        return hset(key, Map.of(field, value));
    }

    public Long hset(String key, Map<String, String> hash) {
        byte[] keyBytes = cacheClient.serializeKey(key); //[cite: 4]
        KeyHintData hint = getKeyHintData(keyBytes);

        List<Payload> keys = new ArrayList<>();
        List<Payload> values = new ArrayList<>();

        for (var entry : hash.entrySet()) {
            keys.add(Payload.of(cacheClient.serializeKey(entry.getKey()))); //[cite: 4]
            values.add(Payload.of(entry.getValue() == null ? new byte[0] : entry.getValue().getBytes(StandardCharsets.UTF_8)));
        }

        if (!cacheClient.existKey(keyBytes, hint).join()) { //[cite: 4]
            Map<Payload, Payload> initialMap = new HashMap<>();
            for (int i = 0; i < keys.size(); i++) {
                initialMap.put(keys.get(i), values.get(i));
            }
            cacheClient.createMap(keyBytes, initialMap).join(); //[cite: 4]
            return (long) keys.size();
        }

        Integer added = cacheClient.addElementHashMap(keyBytes, hint, keys, values,
                                                      cacheClient.getDefaultClientId(),
                                                      cacheClient.getDefaultTimeout()).join(); //[cite: 4]
        return added != null ? added.longValue() : 0L;
    }

    public String hget(String key, String field) {
        byte[] keyBytes = cacheClient.serializeKey(key); //[cite: 4]
        KeyHintData hint = getKeyHintData(keyBytes);
        byte[] fieldBytes = cacheClient.serializeKey(field); //[cite: 4]

        byte[] res = cacheClient.getContainerValue(keyBytes, hint, fieldBytes).join(); //[cite: 4]
        return res != null ? new String(res, StandardCharsets.UTF_8) : null;
    }

    public Long hdel(String key, String... fields) {
        byte[] keyBytes = cacheClient.serializeKey(key); //[cite: 4]
        KeyHintData hint = getKeyHintData(keyBytes);

        long deletedCount = 0;
        for (String field : fields) {
            byte[] fieldBytes = cacheClient.serializeKey(field); //[cite: 4]
            Integer count = cacheClient.removeFromContainer(keyBytes, hint, fieldBytes).join(); //[cite: 4]
            if (count != null && count > 0) {
                deletedCount += count;
            }
        }
        return deletedCount;
    }

    public Boolean hexists(String key, String field) {
        byte[] keyBytes = cacheClient.serializeKey(key); //[cite: 4]
        KeyHintData hint = getKeyHintData(keyBytes);
        byte[] fieldBytes = cacheClient.serializeKey(field); //[cite: 4]
        return cacheClient.containsContainerKey(keyBytes, hint, fieldBytes).join(); //[cite: 4]
    }

    public Map<String, String> hgetAll(String key) {
        byte[] keyBytes = cacheClient.serializeKey(key); //[cite: 4]
        KeyHintData hint = getKeyHintData(keyBytes);
        Map<Payload, Payload> res = cacheClient.streamMap(keyBytes, hint).join(); //[cite: 4]

        if (res == null || res.isEmpty()) return Collections.emptyMap();
        Map<String, String> resultMap = new HashMap<>();
        for (var entry : res.entrySet()) {
            resultMap.put(new String(entry.getKey().getValue(), StandardCharsets.UTF_8),
                          new String(entry.getValue().getValue(), StandardCharsets.UTF_8));
        }
        return resultMap;
    }

    // =========================================================================
    // Sets (Unordered Sets)
    // =========================================================================

    public Long sadd(String key, String... members) {
        byte[] keyBytes = cacheClient.serializeKey(key); //[cite: 4]
        KeyHintData hint = getKeyHintData(keyBytes);

        List<Payload> memberList = convertToPayloadList(members);

        if (!cacheClient.existKey(keyBytes, hint).join()) { //[cite: 4]
            cacheClient.createSet(keyBytes, memberList).join(); //[cite: 4]
            return (long) memberList.size();
        }

        Boolean added = cacheClient.addElement(keyBytes, hint, memberList).join(); //[cite: 4]
        return Boolean.TRUE.equals(added) ? (long) memberList.size() : 0L;
    }

    public Long srem(String key, String... members) {
        byte[] keyBytes = cacheClient.serializeKey(key); //[cite: 4]
        KeyHintData hint = getKeyHintData(keyBytes);

        List<Payload> valuesToRemove = convertToPayloadList(members);
        Integer removed = cacheClient.removeFromContainer(keyBytes, hint, ContainerType.SET, valuesToRemove).join(); //[cite: 4]
        return removed != null ? removed.longValue() : 0L;
    }

    public Boolean sismember(String key, String member) {
        byte[] keyBytes = cacheClient.serializeKey(key); //[cite: 4]
        KeyHintData hint = getKeyHintData(keyBytes);
        byte[] mBytes = cacheClient.serializeKey(member); //[cite: 4]
        return cacheClient.containsContainerKey(keyBytes, hint, mBytes).join(); //[cite: 4]
    }

    public Set<String> smembers(String key) {
        byte[] keyBytes = cacheClient.serializeKey(key); //[cite: 4]
        KeyHintData hint = getKeyHintData(keyBytes);
        List<Payload> members = cacheClient.streamElementInRangeUnordered(keyBytes, hint, ContainerType.SET, 0, -1).join(); //[cite: 4]

        if (members == null) return Collections.emptySet();
        return members.stream()
                .map(m -> new String(m.getValue(), StandardCharsets.UTF_8))
                .collect(Collectors.toSet());
    }

    // =========================================================================
    // Ordered Sets / Sorted Sets (ZSet / OrderedSet / OrderedMap)
    // =========================================================================

    public Long zadd(String key, double score, String member) {
        return zadd(key, Map.of(member, score));
    }

    public Long zadd(String key, Map<String, Double> scoreMembers) {
        byte[] keyBytes = cacheClient.serializeKey(key); //[cite: 4]
        KeyHintData hint = getKeyHintData(keyBytes);

        List<OrderedPayload> items = new ArrayList<>();
        for (var entry : scoreMembers.entrySet()) {
            byte[] memberBytes = cacheClient.serializeKey(entry.getKey()); //[cite: 4]
            long weight = Double.doubleToRawLongBits(entry.getValue());
            items.add(OrderedPayload.of(memberBytes, weight));
        }

        if (!cacheClient.existKey(keyBytes, hint).join()) { //[cite: 4]
            cacheClient.createOrderedSet(keyBytes, items).join(); //[cite: 4]
            return (long) items.size();
        }

        Integer addedCount = cacheClient.addElementWithWeight(keyBytes, hint, items).join(); //[cite: 4]
        return addedCount != null ? addedCount.longValue() : 0L;
    }

    public Long zrem(String key, String... members) {
        byte[] keyBytes = cacheClient.serializeKey(key); //[cite: 4]
        KeyHintData hint = getKeyHintData(keyBytes);

        List<Payload> valuesToRemove = convertToPayloadList(members);
        Integer count = cacheClient.removeFromContainer(keyBytes, hint, ContainerType.ORDERED_SET, valuesToRemove).join(); //[cite: 4]
        return count != null ? count.longValue() : 0L;
    }

    public List<String> zrange(String key, long start, long stop) {
        byte[] keyBytes = cacheClient.serializeKey(key); //[cite: 4]
        KeyHintData hint = getKeyHintData(keyBytes);

        List<Payload> res = cacheClient.streamElementInRangeUnordered(keyBytes, hint,
                                                                      ContainerType.ORDERED_SET, (int) start, (int) stop).join(); //[cite: 4]
        return convertToStringList(res);
    }

    // =========================================================================
    // Numeric & Atomic Operations
    // =========================================================================

    public Long incr(String key) {
        return incrBy(key, 1L);
    }

    public Long incrBy(String key, long increment) {
        byte[] keyBytes = cacheClient.serializeKey(key); //[cite: 4]
        KeyHintData hint = getKeyHintData(keyBytes);

        if (!cacheClient.existKey(keyBytes, hint).join()) { //[cite: 4]
            cacheClient.atomicCreate(keyBytes, hint, increment).join(); //[cite: 4]
            return increment;
        }
        return cacheClient.atomicAdd(keyBytes, hint, increment).join(); //[cite: 4]
    }

    public Long decr(String key) {
        return decrBy(key, 1L);
    }

    public Long decrBy(String key, long decrement) {
        byte[] keyBytes = cacheClient.serializeKey(key); //[cite: 4]
        KeyHintData hint = getKeyHintData(keyBytes);

        if (!cacheClient.existKey(keyBytes, hint).join()) { //[cite: 4]
            cacheClient.atomicCreate(keyBytes, hint, -decrement).join(); //[cite: 4]
            return -decrement;
        }
        return cacheClient.atomicSub(keyBytes, hint, decrement).join(); //[cite: 4]
    }

    // =========================================================================
    // Connection Lifecycle
    // =========================================================================

    @Override
    public void close() {
        cacheClient.shutdown(); //[cite: 4]
    }

    public String ping() {
        return "PONG";
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    private @NonNull KeyHintData getKeyHintData(byte[] keyBytes) {
        return KeyHintData.of(null, weakHash(keyBytes));
    }

    private List<Payload> convertToPayloadList(String... values) {
        if (values == null || values.length == 0) return Collections.emptyList();
        List<Payload> list = new ArrayList<>(values.length);
        for (String v : values) {
            list.add(Payload.of(v == null ? new byte[0] : cacheClient.serializeKey(v))); //[cite: 4]
        }
        return list;
    }

    private List<String> convertToStringList(List<Payload> payloadList) {
        if (payloadList == null) return Collections.emptyList();
        return payloadList.stream()
                .map(p -> p == null || p.getValue() == null ? null : new String(p.getValue(), StandardCharsets.UTF_8))
                .collect(Collectors.toList());
    }
}