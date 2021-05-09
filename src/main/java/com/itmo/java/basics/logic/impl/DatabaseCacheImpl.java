package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.logic.DatabaseCache;

import java.util.LinkedHashMap;
import java.util.Map;

public class DatabaseCacheImpl implements DatabaseCache {
    private static final int CAPACITY = 5_000;

    private final Map<String, byte[]> cacheMap;

    public DatabaseCacheImpl(int N) {
        cacheMap = new LRUMap<>(N);
    }

    @Override
    public byte[] get(String key) {
        return cacheMap.get(key);
    }

    @Override
    public void set(String key, byte[] value) {
        cacheMap.put(key, value);
    }

    @Override
    public void delete(String key) {
        cacheMap.remove(key);
    }

    private class LRUMap<K, V> extends LinkedHashMap<K, V> {
        private final int capacity;

        LRUMap(int capacity) {
            super(capacity, 1f, true);
            this.capacity = capacity;
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
            return size() > capacity;
        }
    }
}
