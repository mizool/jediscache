package com.github.mizool.jediscache;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import javax.cache.Cache;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.integration.CompletionListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;

import lombok.Data;
import lombok.NonNull;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

@Slf4j
@Data
@ToString(of = { "name" })
class JedisCache<K, V> implements Cache<K, V>
{
    private static final String POOL_SIZE_WARN_THRESHOLD_PROPERTY_NAME = "jediscache.poolsize.warn.threshold";
    private static final int POOL_SIZE_WARN_THRESHOLD = Integer.parseInt(System.getProperty(
        POOL_SIZE_WARN_THRESHOLD_PROPERTY_NAME,
        "100"));

    private final JedisCacheManager cacheManager;
    private final MutableConfiguration<K, V> configuration;
    private final JedisPool jedisPool;
    private final String name;
    private final JedisCacheJavaSerializationConverter<K, V> converter;
    private final Class<K> keyClass;
    private final Class<V> valueClass;
    private final byte[] jedisCacheName;
    private final AtomicBoolean closed;

    public JedisCache(
        @NonNull JedisCacheManager cacheManager,
        @NonNull String name,
        @NonNull Configuration<K, V> configuration,
        @NonNull JedisPool jedisPool)
    {
        this.cacheManager = cacheManager;
        this.name = name;
        this.jedisPool = jedisPool;

        this.keyClass = configuration.getKeyType();
        this.valueClass = configuration.getValueType();

        //we make a copy of the configuration here so that the provided one
        //may be changed and/or used independently for other caches. we do this
        //as we don't know if the provided configuration is mutable
        if (configuration instanceof CompleteConfiguration)
        {
            //support use of CompleteConfiguration
            this.configuration = new MutableConfiguration<>((CompleteConfiguration<K, V>) configuration);
        }
        else
        {
            //support use of Basic Configuration
            MutableConfiguration<K, V> mutableConfiguration = new MutableConfiguration<>();
            mutableConfiguration.setStoreByValue(configuration.isStoreByValue());
            mutableConfiguration.setTypes(keyClass, valueClass);
            this.configuration = mutableConfiguration;
        }

        converter = new JedisCacheJavaSerializationConverter<>();
        jedisCacheName = name.getBytes(Charsets.UTF_8);

        closed = new AtomicBoolean(false);
    }

    @Override
    public V get(K key)
    {
        try (Jedis jedis = obtainJedis())
        {
            byte[] serializedKey = converter.serialize(key);
            byte[] serializedValue = jedis.hget(jedisCacheName, serializedKey);
            return converter.deserialize(serializedValue, valueClass);
        }
    }

    @Override
    public Map<K, V> getAll(Set<? extends K> keys)
    {
        Map<byte[], byte[]> allEntries;
        try (Jedis jedis = obtainJedis())
        {
            allEntries = jedis.hgetAll(jedisCacheName);
        }
        ImmutableMap.Builder<K, V> builder = ImmutableMap.builder();
        keys.forEach(key -> {
            byte[] serializedKey = converter.serialize(key);
            if (allEntries.containsKey(serializedKey))
            {
                builder.put(key, converter.deserialize(allEntries.get(serializedKey), valueClass));
            }
        });
        return builder.build();
    }

    @Override
    public boolean containsKey(K key)
    {
        try (Jedis jedis = obtainJedis())
        {
            return jedis.hexists(jedisCacheName, converter.serialize(key));
        }
    }

    @Override
    public void loadAll(
        Set<? extends K> keys, boolean replaceExistingValues, CompletionListener completionListener)
    {
        throw new UnsupportedOperationException("listener support is not implemented");
    }

    @Override
    public void put(K key, V value)
    {
        try (Jedis jedis = obtainJedis())
        {
            byte[] serializedKey = converter.serialize(key);
            byte[] serializedValue = converter.serialize(value);
            jedis.hset(jedisCacheName, serializedKey, serializedValue);
        }
    }

    @Override
    public V getAndPut(K key, V value)
    {
        V result = get(key);
        put(key, value);
        return result;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map)
    {
        map.forEach(this::put);
    }

    @Override
    public boolean putIfAbsent(K key, V value)
    {
        try (Jedis jedis = obtainJedis())
        {
            byte[] serializedKey = converter.serialize(key);
            byte[] serializedValue = converter.serialize(value);
            return jedis.hsetnx(jedisCacheName, serializedKey, serializedValue) == 1;
        }
    }

    @Override
    public boolean remove(K key)
    {
        try (Jedis jedis = obtainJedis())
        {
            byte[] serializedKey = converter.serialize(key);
            return jedis.hdel(jedisCacheName, serializedKey) == 1;
        }
    }

    @Override
    public boolean remove(K key, V oldValue)
    {
        return get(key).equals(oldValue) && remove(key);
    }

    @Override
    public V getAndRemove(K key)
    {
        V result = get(key);
        remove(key);
        return result;
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue)
    {
        boolean result = get(key).equals(oldValue);
        if (result)
        {
            put(key, newValue);
        }
        return result;
    }

    @Override
    public boolean replace(K key, V value)
    {
        boolean result = containsKey(key);
        if (result)
        {
            put(key, value);
        }
        return result;
    }

    @Override
    public V getAndReplace(K key, V value)
    {
        V result = get(key);
        replace(key, value);
        return result;
    }

    @Override
    public void removeAll(Set<? extends K> keys)
    {
        keys.forEach(this::remove);
    }

    @Override
    public void removeAll()
    {
        Set<K> keys;
        try (Jedis jedis = obtainJedis())
        {
            keys = jedis.hkeys(jedisCacheName)
                .stream()
                .map(bytes -> converter.deserialize(bytes, keyClass))
                .collect(Collectors.toSet());
        }
        removeAll(keys);
    }

    @Override
    public void clear()
    {
        try (Jedis jedis = obtainJedis())
        {
            jedis.del(jedisCacheName);
        }
    }

    @Override
    public <C extends Configuration<K, V>> C getConfiguration(Class<C> configurationClass)
    {
        return configurationClass.cast(configuration);
    }

    @Override
    public <T> T invoke(K key, EntryProcessor<K, V, T> entryProcessor, Object... arguments)
        throws EntryProcessorException
    {
        throw new UnsupportedOperationException("processor support is not implemented");
    }

    @Override
    public <T> Map<K, EntryProcessorResult<T>> invokeAll(
        Set<? extends K> keys, EntryProcessor<K, V, T> entryProcessor, Object... arguments)
    {
        throw new UnsupportedOperationException("processor support is not implemented");
    }

    @Override
    public void close()
    {
        if (!closed.getAndSet(true))
        {
            cacheManager.releaseCache(name);
        }
    }

    @Override
    public boolean isClosed()
    {
        return closed.get();
    }

    @Override
    public <T> T unwrap(Class<T> classOfT)
    {
        if (classOfT.isAssignableFrom(getClass()))
        {
            return classOfT.cast(this);
        }

        throw new IllegalArgumentException("Unwapping to " + classOfT + " is not a supported by this implementation");
    }

    @Override
    public void registerCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration)
    {
        throw new UnsupportedOperationException("listener support is not implemented");
    }

    @Override
    public void deregisterCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration)
    {
        throw new UnsupportedOperationException("listener support is not implemented");
    }

    @Override
    public Iterator<Entry<K, V>> iterator()
    {
        ScanParams scanParams = new ScanParams();
        byte[] cursor = ScanParams.SCAN_POINTER_START_BINARY;
        ScanResult<Map.Entry<byte[], byte[]>> scanResult;
        try (Jedis jedis = obtainJedis())
        {
            scanResult = jedis.hscan(jedisCacheName, cursor, scanParams);
        }
        return scanResult.getResult().stream().map(this::deserialize).iterator();
    }

    private Entry<K, V> deserialize(Map.Entry<byte[], byte[]> entry)
    {
        return new Entry<K, V>()
        {
            @Override
            public K getKey()
            {
                return converter.deserialize(entry.getKey(), keyClass);
            }

            @Override
            public V getValue()
            {
                return converter.deserialize(entry.getValue(), valueClass);
            }

            @Override
            public <T> T unwrap(Class<T> classOfT)
            {
                if (classOfT.isAssignableFrom(getClass()))
                {
                    return classOfT.cast(this);
                }

                throw new IllegalArgumentException("Unwrapping to " +
                    classOfT +
                    " is not a supported by this implementation");
            }
        };
    }

    private Jedis obtainJedis()
    {
        verifyOpen();
        int numActive = jedisPool.getNumActive();
        int numIdle = jedisPool.getNumIdle();
        if (numActive + numIdle >= POOL_SIZE_WARN_THRESHOLD)
        {
            log.warn(
                "Pool size exceeds warning threshold, numActive is {}, numIdle is {} ({} in total)",
                numActive,
                numIdle,
                numActive + numIdle);
        }
        else
        {
            log.debug("numActive is {}, numIdle is {} ({} in total)", numActive, numIdle, numActive + numIdle);
        }
        return jedisPool.getResource();
    }

    private void verifyOpen()
    {
        if (isClosed())
        {
            throw new IllegalStateException("cache is closed");
        }
    }
}
