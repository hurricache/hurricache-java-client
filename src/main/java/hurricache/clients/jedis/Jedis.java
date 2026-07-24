package hurricache.clients.jedis;

import com.hurricache.client.intf.HurriCacheClientInterface;
import com.hurricache.grpc.KeyHint;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

/**
 * Full Jedis API drop-in replacement routing requests to HurriCache backend.
 * Replace 'import redis.clients.jedis.Jedis;' with this implementation or use
 * as a binary-compatible bridge.
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
        cacheClient.createKeyValue(key, valBytes).join();
        return "OK";
    }

    public String set(byte[] key, byte[] value) {
        cacheClient.createKeyValue(key, value).join();
        return "OK";
    }

    public String setex(String key, long seconds, String value) {
        byte[] keyBytes = cacheClient.serializeKey(key);
        byte[] valBytes = value == null ? new byte[0] : value.getBytes(StandardCharsets.UTF_8);
        KeyHint hint = cacheClient.getKeyHint(keyBytes);
        cacheClient.createKeyValue(keyBytes, hint, valBytes, Duration.ofSeconds(seconds),
                                   cacheClient.getDefaultClientId(), cacheClient.getDefaultTimeout()).join();
        return "OK";
    }

    public String get(String key) {
        byte[] res = cacheClient.getValue(key).join();
        return res != null ? new String(res, StandardCharsets.UTF_8) : null;
    }

    public byte[] get(byte[] key) {
        return cacheClient.getValue(key).join();
    }

    public String getSet(String key, String value) {
        byte[] valBytes = value == null ? new byte[0] : value.getBytes(StandardCharsets.UTF_8);
        byte[] oldVal = cacheClient.updateKeyValue(key, valBytes).join();
        return oldVal != null ? new String(oldVal, StandardCharsets.UTF_8) : null;
    }

    public String getDel(String key) {
        byte[] res = cacheClient.getAndDeleteValue(key).join();
        return res != null ? new String(res, StandardCharsets.UTF_8) : null;
    }

    public Boolean exists(String key) {
        return cacheClient.existKey(key).join();
    }

    public Long del(String... keys) {
        long count = 0;
        for (String key : keys) {
            if (Boolean.TRUE.equals(cacheClient.remove(key).join())) {
                count++;
            }
        }
        return count;
    }

    public Long expire(String key, long seconds) {
        byte[] keyBytes = cacheClient.serializeKey(key);
        KeyHint hint = cacheClient.getKeyHint(keyBytes);
        Boolean ok = cacheClient.setTtl(keyBytes, hint, seconds, cacheClient.getDefaultClientId(), cacheClient.getDefaultTimeout()).join();
        return Boolean.TRUE.equals(ok) ? 1L : 0L;
    }

    public Long ttl(String key) {
        return cacheClient.getTtl(key).join();
    }

    // =========================================================================
    // List / Queue Operations
    // =========================================================================

    public Long rpush(String key, String... strings) {
        List<byte[]> dataList = convertToByteList(strings);
        byte[] keyBytes = cacheClient.serializeKey(key);
        KeyHint hint = cacheClient.getKeyHint(keyBytes);

        if (!cacheClient.existKey(keyBytes, hint).join()) {
            cacheClient.createList(keyBytes, dataList, cacheClient.getDefaultTtl()).join();
        } else {
            cacheClient.addElementToTail(keyBytes, hint, dataList).join();
        }
        return (long) cacheClient.streamList(keyBytes, hint).join().size();
    }

    public Long lpush(String key, String... strings) {
        List<byte[]> dataList = convertToByteList(strings);
        byte[] keyBytes = cacheClient.serializeKey(key);
        KeyHint hint = cacheClient.getKeyHint(keyBytes);

        if (!cacheClient.existKey(keyBytes, hint).join()) {
            cacheClient.createList(keyBytes, dataList, cacheClient.getDefaultTtl()).join();
        } else {
            cacheClient.addElementToHead(keyBytes, hint, dataList).join();
        }
        return (long) cacheClient.streamList(keyBytes, hint).join().size();
    }

    public String lpop(String key) {
        byte[] res = cacheClient.getAndRemoveFront(key).join();
        return res != null ? new String(res, StandardCharsets.UTF_8) : null;
    }

    public String rpop(String key) {
        byte[] res = cacheClient.getAndRemoveTail(key).join();
        return res != null ? new String(res, StandardCharsets.UTF_8) : null;
    }

    public String lindex(String key, long index) {
        byte[] res = cacheClient.getElementAtPosition(key, (int) index).join();
        return res != null ? new String(res, StandardCharsets.UTF_8) : null;
    }

    public List<String> lrange(String key, long start, long stop) {
        byte[] keyBytes = cacheClient.serializeKey(key);
        KeyHint hint = cacheClient.getKeyHint(keyBytes);
        List<byte[]> res = cacheClient.streamElementInRange(keyBytes, hint, false, (int) start, (int) stop).join();
        return convertToStringList(res);
    }

    public Long linsert(String key, ListPosition where, String pivot, String value) {
        byte[] keyBytes = cacheClient.serializeKey(key);
        byte[] pivotBytes = pivot == null ? new byte[0] : pivot.getBytes(StandardCharsets.UTF_8);
        byte[] valBytes = value == null ? new byte[0] : value.getBytes(StandardCharsets.UTF_8);

        return linsert(keyBytes, where, pivotBytes, valBytes);
    }

    public Long linsert(byte[] key, ListPosition where, byte[] pivot, byte[] value) {
        KeyHint hint = cacheClient.getKeyHint(key);
        List<byte[]> valuesToInsert = Collections.singletonList(value);

        try {
            Boolean ok;
            if (where == ListPosition.BEFORE) {
                ok = cacheClient.addElementToPositionBefore(key, hint, valuesToInsert, pivot).join();
            } else if (where == ListPosition.AFTER) {
                ok = cacheClient.addElementToPositionAfter(key, hint, valuesToInsert, pivot).join();
            } else {
                throw new IllegalArgumentException("Unknown ListPosition: " + where);
            }

            if (Boolean.TRUE.equals(ok)) {
                // Redis LINSERT возвращает размер списка после успешеного встраивания
                return (long) cacheClient.streamList(key, hint).join().size();
            }
            return -1L;
        } catch (CompletionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof StatusRuntimeException statusEx) {
                if (statusEx.getStatus().getCode() == Status.Code.NOT_FOUND) {
                    // Пивот или список не найдены -> возвращаем -1 по спецификации Redis
                    return -1L;
                }
            }
            throw e;
        }
    }

    // =========================================================================
    // Numeric & Atomic Operations
    // =========================================================================

    public Long incr(String key) {
        return incrBy(key, 1L);
    }

    public Long incrBy(String key, long increment) {
        byte[] keyBytes = cacheClient.serializeKey(key);
        KeyHint hint = cacheClient.getKeyHint(keyBytes);

        // Если ключа нет, создаем атомик
        if (!cacheClient.existKey(keyBytes, hint).join()) {
            cacheClient.atomicCreate(keyBytes, hint, increment).join();
            return increment;
        }
        return cacheClient.atomicAdd(keyBytes, hint, increment).join();
    }

    public Long decr(String key) {
        return decrBy(key, 1L);
    }

    public Long decrBy(String key, long decrement) {
        byte[] keyBytes = cacheClient.serializeKey(key);
        KeyHint hint = cacheClient.getKeyHint(keyBytes);

        if (!cacheClient.existKey(keyBytes, hint).join()) {
            cacheClient.atomicCreate(keyBytes, hint, -decrement).join();
            return -decrement;
        }
        return cacheClient.atomicSub(keyBytes, hint, decrement).join();
    }

    // =========================================================================
    // Connection Lifecycle
    // =========================================================================

    @Override
    public void close() {
        cacheClient.shutdown();
    }

    public String ping() {
        return "PONG";
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    private List<byte[]> convertToByteList(String... values) {
        if (values == null || values.length == 0) return Collections.emptyList();
        List<byte[]> list = new ArrayList<>(values.length);
        for (String v : values) {
            list.add(v == null ? new byte[0] : v.getBytes(StandardCharsets.UTF_8));
        }
        return list;
    }

    private List<String> convertToStringList(List<byte[]> byteList) {
        if (byteList == null) return Collections.emptyList();
        return byteList.stream()
                .map(b -> b == null ? null : new String(b, StandardCharsets.UTF_8))
                .collect(Collectors.toList());
    }
}