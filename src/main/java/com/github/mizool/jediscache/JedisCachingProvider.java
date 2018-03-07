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
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Properties;
import java.util.WeakHashMap;

import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.configuration.OptionalFeature;
import javax.cache.spi.CachingProvider;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class JedisCachingProvider implements CachingProvider
{
    private final JedisPool jedisPool;
    private WeakHashMap<ClassLoader, HashMap<URI, CacheManager>> cacheManagersByClassLoader;

    public JedisCachingProvider()
    {
        this.jedisPool = new JedisPool(new JedisPoolConfig(), "localhost"); // TODO
        this.cacheManagersByClassLoader = new WeakHashMap<>();
    }

    @Override
    public synchronized CacheManager getCacheManager(URI uri, ClassLoader classLoader, Properties properties)
    {
        if (uri == null)
        {
            uri = getDefaultURI();
        }
        if (classLoader == null)
        {
            classLoader = getDefaultClassLoader();
        }
        if (properties == null)
        {
            properties = new Properties();
        }

        HashMap<URI, CacheManager> cacheManagersByURI = cacheManagersByClassLoader.get(classLoader);

        if (cacheManagersByURI == null)
        {
            cacheManagersByURI = new HashMap<>();
        }

        CacheManager cacheManager = cacheManagersByURI.get(uri);

        if (cacheManager == null)
        {
            cacheManager = new JedisCacheManager(this, uri, classLoader, properties, jedisPool);
            cacheManagersByURI.put(uri, cacheManager);
        }

        if (!cacheManagersByClassLoader.containsKey(classLoader))
        {
            cacheManagersByClassLoader.put(classLoader, cacheManagersByURI);
        }

        return cacheManager;
    }

    @Override
    public CacheManager getCacheManager(URI uri, ClassLoader classLoader)
    {
        return getCacheManager(uri, classLoader, getDefaultProperties());
    }

    @Override
    public CacheManager getCacheManager()
    {
        return getCacheManager(getDefaultURI(), getDefaultClassLoader(), null);
    }

    @Override
    public ClassLoader getDefaultClassLoader()
    {
        return getClass().getClassLoader();
    }

    @Override
    public URI getDefaultURI()
    {
        try
        {
            return new URI(this.getClass().getName());
        }
        catch (URISyntaxException e)
        {
            throw new CacheException("Failed to create the default URI for jedis cache", e);
        }
    }

    @Override
    public Properties getDefaultProperties()
    {
        return null;
    }

    @Override
    public synchronized void close()
    {
        WeakHashMap<ClassLoader, HashMap<URI, CacheManager>> managersByClassLoader = this.cacheManagersByClassLoader;
        this.cacheManagersByClassLoader = new WeakHashMap<>();

        for (ClassLoader classLoader : managersByClassLoader.keySet())
        {
            for (CacheManager cacheManager : managersByClassLoader.get(classLoader).values())
            {
                cacheManager.close();
            }
        }
        this.jedisPool.close();
    }

    @Override
    public synchronized void close(ClassLoader classLoader)
    {
        ClassLoader managerClassLoader = classLoader == null ? getDefaultClassLoader() : classLoader;

        HashMap<URI, CacheManager> cacheManagersByURI = cacheManagersByClassLoader.remove(managerClassLoader);

        if (cacheManagersByURI != null)
        {
            for (CacheManager cacheManager : cacheManagersByURI.values())
            {
                cacheManager.close();
            }
        }
    }

    @Override
    public synchronized void close(URI uri, ClassLoader classLoader)
    {
        URI managerURI = uri == null ? getDefaultURI() : uri;
        ClassLoader managerClassLoader = classLoader == null ? getDefaultClassLoader() : classLoader;

        HashMap<URI, CacheManager> cacheManagersByURI = cacheManagersByClassLoader.get(managerClassLoader);
        if (cacheManagersByURI != null)
        {
            CacheManager cacheManager = cacheManagersByURI.remove(managerURI);

            if (cacheManager != null)
            {
                cacheManager.close();
            }

            if (cacheManagersByURI.size() == 0)
            {
                cacheManagersByClassLoader.remove(managerClassLoader);
            }
        }
    }

    synchronized void releaseCacheManager(URI uri, ClassLoader classLoader)
    {
        URI managerURI = uri == null ? getDefaultURI() : uri;
        ClassLoader managerClassLoader = classLoader == null ? getDefaultClassLoader() : classLoader;

        HashMap<URI, CacheManager> cacheManagersByURI = cacheManagersByClassLoader.get(managerClassLoader);
        if (cacheManagersByURI != null)
        {
            cacheManagersByURI.remove(managerURI);

            if (cacheManagersByURI.size() == 0)
            {
                cacheManagersByClassLoader.remove(managerClassLoader);
            }
        }
    }

    @Override
    public boolean isSupported(OptionalFeature optionalFeature)
    {
        switch (optionalFeature)
        {
            case STORE_BY_REFERENCE:
                return true;
            default:
                return false;
        }
    }
}
