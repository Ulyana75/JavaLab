package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.DatabaseCache;
import com.itmo.java.basics.logic.Table;

import java.util.Optional;

/**
 * Декоратор для таблицы. Кэширует данные
 */
public class CachingTable implements Table {
    static final int CACHE_SIZE = 5_000;

    private final Table table;
    private final DatabaseCache databaseCache;

    public CachingTable(Table table) {
        this.table = table;
        databaseCache = new DatabaseCacheImpl(CACHE_SIZE);
    }

    @Override
    public String getName() {
        return table.getName();
    }

    @Override
    public void write(String objectKey, byte[] objectValue) throws DatabaseException {
        table.write(objectKey, objectValue);
        databaseCache.set(objectKey, objectValue);
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws DatabaseException {
        var fromCache = databaseCache.get(objectKey);
        if (fromCache != null) {
            return Optional.of(fromCache);
        }
        return table.read(objectKey);
    }

    @Override
    public void delete(String objectKey) throws DatabaseException {
        table.delete(objectKey);
        databaseCache.delete(objectKey);
    }
}
