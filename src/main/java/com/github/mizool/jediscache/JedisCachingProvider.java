package com.github.mizool.jediscache;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.WeakHashMap;

import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.configuration.OptionalFeature;
import javax.cache.spi.CachingProvider;

import lombok.extern.slf4j.Slf4j;

import org.kohsuke.MetaInfServices;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

@Slf4j
@MetaInfServices(CachingProvider.class)
public class JedisCachingProvider implements CachingProvider
{
    private static final String JEDISCACHE_HOST = "jediscache.host";
    private final JedisPool jedisPool;
    private Map<ClassLoader, Map<URI, CacheManager>> cacheManagersByClassLoader;

    public JedisCachingProvider()
    {
        String host = System.getProperty(JEDISCACHE_HOST, "127.0.0.1");
        log.info("JedisCache active, connecting to {}", host);
        jedisPool = new JedisPool(new JedisPoolConfig(), host);
        cacheManagersByClassLoader = new WeakHashMap<>();
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

        Map<URI, CacheManager> cacheManagersByURI = cacheManagersByClassLoader.get(classLoader);

        if (cacheManagersByURI == null)
        {
            cacheManagersByURI = new HashMap<>();
            cacheManagersByClassLoader.put(classLoader, cacheManagersByURI);
        }

        CacheManager cacheManager = cacheManagersByURI.get(uri);

        if (cacheManager == null)
        {
            cacheManager = new JedisCacheManager(this, uri, classLoader, properties, jedisPool);
            cacheManagersByURI.put(uri, cacheManager);
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
        Map<ClassLoader, Map<URI, CacheManager>> cacheManagersByClassLoader = this.cacheManagersByClassLoader;
        this.cacheManagersByClassLoader = new WeakHashMap<>();

        for (ClassLoader classLoader : cacheManagersByClassLoader.keySet())
        {
            for (CacheManager cacheManager : cacheManagersByClassLoader.get(classLoader).values())
            {
                cacheManager.close();
            }
        }
        jedisPool.close();
    }

    @Override
    public synchronized void close(ClassLoader classLoader)
    {
        ClassLoader managerClassLoader = classLoader == null ? getDefaultClassLoader() : classLoader;

        Map<URI, CacheManager> cacheManagersByURI = cacheManagersByClassLoader.remove(managerClassLoader);

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

        Map<URI, CacheManager> cacheManagersByURI = cacheManagersByClassLoader.get(managerClassLoader);
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

        Map<URI, CacheManager> cacheManagersByURI = cacheManagersByClassLoader.get(managerClassLoader);
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
