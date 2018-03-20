package com.github.mizool.jediscache;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import javax.cache.CacheException;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
class JedisCacheJavaSerializationConverter<K, V> implements JedisCacheKeyConverter<K>, JedisCacheValueConverter<V>
{
    private final Class<K> keyClass;
    private final Class<V> valueClass;

    @Override
    public byte[] fromKey(K key)
    {
        return serialize(key);
    }

    @Override
    public byte[] fromValue(V value)
    {
        return serialize(value);
    }

    @Override
    public V toValue(byte[] bytes)
    {
        return deserialize(bytes, valueClass);
    }

    @Override
    public K toKey(byte[] bytes)
    {
        return deserialize(bytes, keyClass);
    }

    private byte[] serialize(Object object)
    {
        if (object == null)
        {
            return null;
        }
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream))
        {
            objectOutputStream.writeObject(object);
            return byteArrayOutputStream.toByteArray();
        }
        catch (IOException e)
        {
            throw new CacheException(e);
        }
    }

    private <T> T deserialize(byte[] bytes, Class<T> classOfT)
    {
        if (bytes == null)
        {
            return null;
        }
        try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
            ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream))
        {
            Object object = objectInputStream.readObject();
            return classOfT.cast(object);
        }
        catch (IOException e)
        {
            throw new CacheException(e);
        }
        catch (ClassNotFoundException e)
        {
            throw new CacheException("object to read is of unknown class", e);
        }
    }
}
