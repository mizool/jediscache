package com.github.mizool.jediscache;

import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.Configuration;

import lombok.Data;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import redis.clients.jedis.JedisPool;

@Slf4j
@Data
class JedisCacheManager implements CacheManager
{
    @NonNull
    private final JedisCachingProvider cachingProvider;

    @NonNull
    private final URI uri;

    @NonNull
    private final ClassLoader classLoader;

    @NonNull
    private final Properties properties;

    @NonNull
    private final JedisPool jedisPool;

    private final Map<String, Cache<?, ?>> caches = new HashMap<>();

    private final AtomicBoolean closed = new AtomicBoolean(false);

    @Override
    public boolean isClosed()
    {
        return closed.get();
    }

    @Override
    public URI getURI()
    {
        return uri;
    }

    @Override
    public <K, V, C extends Configuration<K, V>> Cache<K, V> createCache(
        @NonNull String cacheName, @NonNull C configuration) throws IllegalArgumentException
    {
        synchronized (caches)
        {
            verifyOpen();
            Cache<K, V> cache = getCacheInstance(cacheName);
            if (cache != null)
            {
                throw new CacheException("A cache named " + cacheName + " already exists.");
            }
            cache = new JedisCache<>(this, cacheName, configuration, jedisPool);
            caches.put(cache.getName(), cache);

            return cache;
        }
    }

    @Override
    public <K, V> Cache<K, V> getCache(
        @NonNull String cacheName, @NonNull Class<K> keyType, @NonNull Class<V> valueType)
    {
        verifyOpen();
        Cache<K, V> cache;
        synchronized (caches)
        {
            cache = getCacheInstance(cacheName);
        }
        if (cache != null)
        {
            Configuration<K, V> configuration = cache.getConfiguration(CompleteConfiguration.class);

            if (configuration.getKeyType() == null || !configuration.getKeyType().equals(keyType))
            {
                throw new ClassCastException("Incompatible cache key types specified, expected " +
                    configuration.getKeyType() +
                    " but " +
                    keyType +
                    " was specified");
            }

            if (configuration.getValueType() == null || !configuration.getValueType().equals(valueType))
            {
                throw new ClassCastException("Incompatible cache value types specified, expected " +
                    configuration.getValueType() +
                    " but " +
                    valueType +
                    " was specified");
            }
        }

        return cache;
    }

    @Override
    public <K, V> Cache<K, V> getCache(String cacheName)
    {
        synchronized (caches)
        {
            verifyOpen();
            return getCacheInstance(cacheName);
        }
    }

    @Override
    public Iterable<String> getCacheNames()
    {
        synchronized (caches)
        {
            verifyOpen();
            return ImmutableSet.copyOf(caches.keySet());
        }
    }

    @Override
    public void destroyCache(@NonNull String cacheName)
    {
        synchronized (caches)
        {
            verifyOpen();
            Cache<?, ?> cache = caches.get(cacheName);
            if (cache != null)
            {
                cache.close();
            }
        }
    }

    @Override
    public void enableManagement(@NonNull String cacheName, boolean enabled)
    {
        throw new UnsupportedOperationException("JMX features are not supported");
    }

    @Override
    public void enableStatistics(@NonNull String cacheName, boolean enabled)
    {
        throw new UnsupportedOperationException("JMX features are not supported");
    }

    @Override
    public void close()
    {
        if (!closed.getAndSet(true))
        {
            cachingProvider.releaseCacheManager(getURI(), getClassLoader());

            Collection<Cache<?, ?>> cachesToClose;
            synchronized (caches)
            {
                cachesToClose = Lists.newArrayList(caches.values());
                caches.clear();
            }
            for (Cache<?, ?> cache : cachesToClose)
            {
                try
                {
                    cache.close();
                }
                catch (RuntimeException e)
                {
                    log.warn("Error stopping cache: " + cache, e);
                }
            }
        }
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

    void releaseCache(@NonNull String cacheName)
    {
        synchronized (caches)
        {
            caches.remove(cacheName);
        }
    }

    private void verifyOpen()
    {
        if (isClosed())
        {
            throw new IllegalStateException("cacheManager is closed");
        }
    }

    @SuppressWarnings("unchecked")
    private <K, V> Cache<K, V> getCacheInstance(String cacheName)
    {
        return (Cache<K, V>) caches.get(cacheName);
    }
}
