/**
 * Copyright 2017-2018 incub8 Software Labs GmbH
 * Copyright 2017-2018 protel Hotelsoftware GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CompletionListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

@Slf4j
@Getter
@EqualsAndHashCode
@ToString
class JedisCache<K, V> implements Cache<K, V>
{
    private final JedisCacheManager cacheManager;
    private final ClassLoader classLoader;
    private final MutableConfiguration<K, V> configuration;
    private final JedisPool jedisPool;
    private final String name;
    private final JedisCacheKeyConverter<K> keyConverter;
    private final JedisCacheValueConverter<V> valueConverter;
    private final byte[] jedisCacheName;
    private final ExpiryPolicy expiryPolicy;

    private AtomicBoolean closed;

    public JedisCache(
        @NonNull JedisCacheManager cacheManager,
        @NonNull String name,
        @NonNull ClassLoader classLoader,
        @NonNull Configuration<K, V> configuration,
        @NonNull JedisPool jedisPool)
    {
        this.cacheManager = cacheManager;
        this.classLoader = classLoader;
        this.name = name;
        this.jedisPool = jedisPool;

        //we make a copy of the configuration here so that the provided one
        //may be changed and or used independently for other caches.  we do this
        //as we don't know if the provided configuration is mutable
        if (configuration instanceof CompleteConfiguration)
        {
            //support use of CompleteConfiguration
            this.configuration = new MutableConfiguration<>((MutableConfiguration<K, V>) configuration);
        }
        else
        {
            //support use of Basic Configuration
            MutableConfiguration<K, V> mutableConfiguration = new MutableConfiguration<>();
            mutableConfiguration.setStoreByValue(configuration.isStoreByValue());
            mutableConfiguration.setTypes(configuration.getKeyType(), configuration.getValueType());
            this.configuration = new MutableConfiguration<>(mutableConfiguration);
        }

        JedisCacheJavaSerializationConverter<K, V>
            converter
            = new JedisCacheJavaSerializationConverter<>(configuration.getKeyType(), configuration.getValueType());
        keyConverter = converter;
        valueConverter = converter;
        jedisCacheName = name.getBytes(Charsets.UTF_8);

        expiryPolicy = this.configuration.getExpiryPolicyFactory().create();

        closed = new AtomicBoolean(false);
    }

    @Override
    public V get(K key)
    {
        ensureOpen();
        try (Jedis jedis = jedisPool.getResource())
        {
            return valueConverter.toValue(jedis.hget(jedisCacheName, keyConverter.fromKey(key)));
        }
    }

    @Override
    public Map<K, V> getAll(Set<? extends K> keys)
    {
        ensureOpen();
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
        ensureOpen();
        try (Jedis jedis = jedisPool.getResource())
        {
            return jedis.hget(jedisCacheName, keyConverter.fromKey(key)) != null;
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
        ensureOpen();
        try (Jedis jedis = jedisPool.getResource())
        {
            jedis.hset(jedisCacheName, keyConverter.fromKey(key), valueConverter.fromValue(value));
        }
    }

    @Override
    public V getAndPut(K key, V value)
    {
        ensureOpen();
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
        ensureOpen();
        boolean result = false;
        if (!containsKey(key))
        {
            result = true;
            put(key, value);
        }
        return result;
    }

    @Override
    public boolean remove(K key)
    {
        ensureOpen();
        try (Jedis jedis = jedisPool.getResource())
        {
            return jedis.hdel(jedisCacheName, keyConverter.fromKey(key)) > 0;
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
        boolean result = remove(key, oldValue);
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
        ensureOpen();
        try (Jedis jedis = jedisPool.getResource())
        {
            jedis.hgetAll(jedisCacheName).keySet().forEach(bytes -> remove(keyConverter.toKey(bytes)));
        }
    }

    @Override
    public void clear()
    {
        ensureOpen();
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
        ensureOpen();
        Map<byte[], byte[]> allEntries;
        try (Jedis jedis = jedisPool.getResource())
        {
            allEntries = jedis.hgetAll(jedisCacheName);
        }
        ImmutableMap.Builder<K, V> builder = ImmutableMap.builder();
        allEntries.forEach((key, value) -> builder.put(keyConverter.toKey(key), valueConverter.toValue(value)));
        ImmutableMap<K, V> entriesMap = builder.build();
        return entriesMap.entrySet().stream().map(entry -> (Entry<K, V>) new Entry<K, V>()
        {
            @Override
            public K getKey()
            {
                return entry.getKey();
            }

            @Override
            public V getValue()
            {
                return entry.getValue();
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
        }).iterator();
    }

    private void ensureOpen()
    {
        if (isClosed())
        {
            throw new IllegalStateException("cache is closed");
        }
    }
}
