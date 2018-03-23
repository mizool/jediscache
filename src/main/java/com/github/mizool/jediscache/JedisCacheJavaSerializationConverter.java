package com.github.mizool.jediscache;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import javax.cache.CacheException;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
class JedisCacheJavaSerializationConverter<K, V>
{
    public <T> byte[] serialize(T object)
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

    public <T> T deserialize(byte[] bytes, Class<T> classOfT)
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
