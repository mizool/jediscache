package com.github.mizool.jediscache;

public interface JedisCacheKeyConverter<K>
{
    byte[] fromKey(K key);

    K toKey(byte[] bytes);
}
