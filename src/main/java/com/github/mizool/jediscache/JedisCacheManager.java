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

import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.Configuration;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import redis.clients.jedis.JedisPool;

@Slf4j
@Getter
@EqualsAndHashCode
@ToString
@RequiredArgsConstructor
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

    private final HashMap<String, JedisCache<?, ?>> caches = new HashMap<>();

    private AtomicBoolean closed = new AtomicBoolean(false);

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
        ensureOpen();
        synchronized (caches)
        {
            JedisCache<K, V> cache = getCacheInstance(cacheName);
            if (cache == null)
            {
                cache = new JedisCache<>(this, cacheName, getClassLoader(), configuration, jedisPool);
                caches.put(cache.getName(), cache);

                return cache;
            }
            else
            {
                throw new CacheException("A cache named " + cacheName + " already exists.");
            }
        }
    }

    @Override
    public <K, V> Cache<K, V> getCache(
        @NonNull String cacheName, @NonNull Class<K> keyType, @NonNull Class<V> valueType)
    {
        ensureOpen();
        JedisCache<K, V> cache;
        synchronized (caches)
        {
            cache = getCacheInstance(cacheName);
        }
        if (cache == null)
        {
            return null;
        }
        else
        {
            Configuration<?, ?> configuration = cache.getConfiguration(CompleteConfiguration.class);

            if (configuration.getKeyType() != null && configuration.getKeyType().equals(keyType))
            {
                if (configuration.getValueType() != null && configuration.getValueType().equals(valueType))
                {
                    return cache;
                }
                else
                {
                    throw new ClassCastException("Incompatible cache value types specified, expected " +
                        configuration.getValueType() +
                        " but " +
                        valueType +
                        " was specified");
                }
            }
            else
            {
                throw new ClassCastException("Incompatible cache key types specified, expected " +
                    configuration.getKeyType() +
                    " but " +
                    keyType +
                    " was specified");
            }
        }
    }

    @Override
    public <K, V> JedisCache<K, V> getCache(String cacheName)
    {
        ensureOpen();
        synchronized (caches)
        {
            return getCacheInstance(cacheName);
        }
    }

    @Override
    public Iterable<String> getCacheNames()
    {
        ensureOpen();
        synchronized (caches)
        {
            return ImmutableSet.<String>builder().addAll(caches.keySet()).build();
        }
    }

    @Override
    public void destroyCache(@NonNull String cacheName)
    {
        ensureOpen();
        Cache<?, ?> cache;
        synchronized (caches)
        {
            cache = caches.get(cacheName);
        }
        if (cache != null)
        {
            cache.close();
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
                catch (Exception e)
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

    private void ensureOpen()
    {
        if (isClosed())
        {
            throw new IllegalStateException("cacheManager is closed");
        }
    }

    @SuppressWarnings("unchecked")
    private <K, V> JedisCache<K, V> getCacheInstance(String cacheName)
    {
        return (JedisCache<K, V>) caches.get(cacheName);
    }
}
