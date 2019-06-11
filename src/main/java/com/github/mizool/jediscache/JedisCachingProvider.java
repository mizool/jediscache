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

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisException;

@Slf4j
@MetaInfServices(CachingProvider.class)
public class JedisCachingProvider implements CachingProvider
{
    private static final String HOST_PROPERTY_NAME = "jediscache.host";
    private static final String HOST = System.getProperty(HOST_PROPERTY_NAME, "localhost");
    private static final String PORT_PROPERTY_NAME = "jediscache.port";
    private static final int PORT = Integer.parseInt(System.getProperty(PORT_PROPERTY_NAME, "6379"));
    private static final String POOL_SIZE_PROPERTY_NAME = "jediscache.poolsize";
    private static final int POOL_SIZE = Integer.parseInt(System.getProperty(POOL_SIZE_PROPERTY_NAME, "-1"));

    private final JedisPool jedisPool;
    private Map<ClassLoader, Map<URI, CacheManager>> cacheManagersByClassLoader;

    public JedisCachingProvider()
    {
        log.info("JedisCache active, connecting to {}:{}", HOST, PORT);
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(POOL_SIZE);
        jedisPool = new JedisPool(poolConfig, HOST, PORT);
        runConnectivityCheck();
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

        return getOrCreateCacheManager(uri, classLoader, properties);
    }

    private CacheManager getOrCreateCacheManager(URI uri, ClassLoader classLoader, Properties properties)
    {
        Map<URI, CacheManager> cacheManagersByUri = getOrCreateCacheManagersByUri(classLoader);

        CacheManager cacheManager = cacheManagersByUri.get(uri);

        if (cacheManager == null)
        {
            cacheManager = new JedisCacheManager(this, uri, classLoader, properties, jedisPool);
            cacheManagersByUri.put(uri, cacheManager);
        }

        return cacheManager;
    }

    private Map<URI, CacheManager> getOrCreateCacheManagersByUri(ClassLoader classLoader)
    {
        Map<URI, CacheManager> cacheManagersByUri = cacheManagersByClassLoader.get(classLoader);

        if (cacheManagersByUri == null)
        {
            cacheManagersByUri = new HashMap<>();
            cacheManagersByClassLoader.put(classLoader, cacheManagersByUri);
        }
        return cacheManagersByUri;
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
            throw new CacheException("Failed to create the default uri for jedis cache", e);
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
        if (classLoader == null)
        {
            classLoader = getDefaultClassLoader();
        }

        Map<URI, CacheManager> cacheManagersByUri = cacheManagersByClassLoader.remove(classLoader);

        if (cacheManagersByUri != null)
        {
            for (CacheManager cacheManager : cacheManagersByUri.values())
            {
                cacheManager.close();
            }
        }
    }

    @Override
    public synchronized void close(URI uri, ClassLoader classLoader)
    {
        if (uri == null)
        {
            uri = getDefaultURI();
        }
        if (classLoader == null)
        {
            classLoader = getDefaultClassLoader();
        }

        Map<URI, CacheManager> cacheManagersByUri = cacheManagersByClassLoader.get(classLoader);
        if (cacheManagersByUri != null)
        {
            CacheManager cacheManager = cacheManagersByUri.remove(uri);

            if (cacheManager != null)
            {
                cacheManager.close();
            }

            if (cacheManagersByUri.size() == 0)
            {
                cacheManagersByClassLoader.remove(classLoader);
            }
        }
    }

    synchronized void releaseCacheManager(URI uri, ClassLoader classLoader)
    {
        if (uri == null)
        {
            uri = getDefaultURI();
        }
        if (classLoader == null)
        {
            classLoader = getDefaultClassLoader();
        }

        Map<URI, CacheManager> cacheManagersByUri = cacheManagersByClassLoader.get(classLoader);
        if (cacheManagersByUri != null)
        {
            cacheManagersByUri.remove(uri);

            if (cacheManagersByUri.size() == 0)
            {
                cacheManagersByClassLoader.remove(classLoader);
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

    private void runConnectivityCheck()
    {
        try (Jedis jedis = jedisPool.getResource())
        {
            jedis.ping();
        }
        catch (JedisException e)
        {
            log.error("JedisCache could not reach {}:{}", HOST, PORT);
            throw e;
        }
    }
}
