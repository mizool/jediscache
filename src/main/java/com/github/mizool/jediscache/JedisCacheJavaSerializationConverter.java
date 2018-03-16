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
