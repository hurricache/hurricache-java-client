package hurricache.clients.jedis;

import com.hurricache.client.intf.HurriCacheClientInterface;

public class JedisPool implements AutoCloseable {
    private final HurriCacheClientInterface cacheClient;

    public JedisPool(HurriCacheClientInterface cacheClient) {
        this.cacheClient = cacheClient;
    }

    public Jedis getResource() {
        return new Jedis(cacheClient);
    }

    @Override
    public void close() {
        cacheClient.shutdown();
    }
}