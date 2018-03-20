package com.github.mizool.jediscache;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

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
import lombok.extern.slf4j.Slf4j;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

@Slf4j
@Data
class JedisCache<K, V> implements Cache<K, V>
{
    private final JedisCacheManager cacheManager;
    private final MutableConfiguration<K, V> configuration;
    private final JedisPool jedisPool;
    private final String name;
    private final JedisCacheKeyConverter<K> keyConverter;
    private final JedisCacheValueConverter<V> valueConverter;
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
            mutableConfiguration.setTypes(configuration.getKeyType(), configuration.getValueType());
            this.configuration = mutableConfiguration;
        }

        JedisCacheJavaSerializationConverter<K, V>
            converter
            = new JedisCacheJavaSerializationConverter<>(configuration.getKeyType(), configuration.getValueType());
        keyConverter = converter;
        valueConverter = converter;
        jedisCacheName = name.getBytes(Charsets.UTF_8);

        closed = new AtomicBoolean(false);
    }

    @Override
    public V get(K key)
    {
        verifyOpen();
        try (Jedis jedis = jedisPool.getResource())
        {
            return valueConverter.toValue(jedis.hget(jedisCacheName, keyConverter.fromKey(key)));
        }
    }

    @Override
    public Map<K, V> getAll(Set<? extends K> keys)
    {
        verifyOpen();
        Map<byte[], byte[]> allEntries;
        try (Jedis jedis = jedisPool.getResource())
        {
            allEntries = jedis.hgetAll(jedisCacheName);
        }
        ImmutableMap.Builder<K, V> builder = ImmutableMap.builder();
        keys.forEach(key -> {
            byte[] serializedKey = keyConverter.fromKey(key);
            if (allEntries.containsKey(serializedKey))
            {
                builder.put(key, valueConverter.toValue(allEntries.get(serializedKey)));
            }
        });
        return builder.build();
    }

    @Override
    public boolean containsKey(K key)
    {
        verifyOpen();
        try (Jedis jedis = jedisPool.getResource())
        {
            return jedis.hexists(jedisCacheName, keyConverter.fromKey(key));
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
        verifyOpen();
        try (Jedis jedis = jedisPool.getResource())
        {
            jedis.hset(jedisCacheName, keyConverter.fromKey(key), valueConverter.fromValue(value));
        }
    }

    @Override
    public V getAndPut(K key, V value)
    {
        verifyOpen();
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
        verifyOpen();
        try (Jedis jedis = jedisPool.getResource())
        {
            return jedis.hsetnx(jedisCacheName, keyConverter.fromKey(key), valueConverter.fromValue(value)) == 1;
        }
    }

    @Override
    public boolean remove(K key)
    {
        verifyOpen();
        try (Jedis jedis = jedisPool.getResource())
        {
            return jedis.hdel(jedisCacheName, keyConverter.fromKey(key)) == 1;
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
        boolean result = containsKey(key);
        if (result)
        {
            put(key, newValue);
        }
        return result;
    }

    @Override
    public boolean replace(K key, V value)
    {
        boolean result = remove(key);
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
        verifyOpen();
        Set<byte[]> keys;
        try (Jedis jedis = jedisPool.getResource())
        {
            keys = jedis.hkeys(jedisCacheName);
        }
        keys.stream().map(keyConverter::toKey).forEach(this::remove);
    }

    @Override
    public void clear()
    {
        verifyOpen();
        try (Jedis jedis = jedisPool.getResource())
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
        verifyOpen();
        ScanParams scanParams = new ScanParams();
        byte[] cursor = ScanParams.SCAN_POINTER_START_BINARY;
        ScanResult<Map.Entry<byte[], byte[]>> scanResult;
        try (Jedis jedis = jedisPool.getResource())
        {
            scanResult = jedis.hscan(jedisCacheName, cursor, scanParams);
        }
        return scanResult.getResult().stream().map(this::turnIntoEntries).iterator();
    }

    private Entry<K, V> turnIntoEntries(Map.Entry<byte[], byte[]> entry)
    {
        return new Entry<K, V>()
        {
            @Override
            public K getKey()
            {
                return keyConverter.toKey(entry.getKey());
            }

            @Override
            public V getValue()
            {
                return valueConverter.toValue(entry.getValue());
            }

            @Override
            public <T> T unwrap(Class<T> classOfT)
            {
                if (classOfT.isAssignableFrom(getClass()))
                {
                    return classOfT.cast(this);
                }

                throw new IllegalArgumentException("Unwapping to " +
                    classOfT +
                    " is not a supported by this implementation");
            }
        };
    }

    private void verifyOpen()
    {
        if (isClosed())
        {
            throw new IllegalStateException("cache is closed");
        }
    }
}
