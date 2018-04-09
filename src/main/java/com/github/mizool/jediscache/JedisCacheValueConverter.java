package com.github.mizool.jediscache;

public interface JedisCacheValueConverter<V>
{
    byte[] fromValue(V value);

    V toValue(byte[] bytes);
}
