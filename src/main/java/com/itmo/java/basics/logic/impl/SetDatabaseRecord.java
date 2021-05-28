package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.logic.WritableDatabaseRecord;

/**
 * Запись в БД, означающая добавление значения по ключу
 */
public class SetDatabaseRecord implements WritableDatabaseRecord {

    private final byte[] key;
    private final byte[] value;

    public SetDatabaseRecord(byte[] key, byte[] value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public byte[] getKey() {
        return key;
    }

    @Override
    public byte[] getValue() {
        return value;
    }

    @Override
    public long size() {
        return getKeySize() + getValueSize() + 8;
    }

    @Override
    public boolean isValuePresented() {
        return true;
    }

    @Override
    public int getKeySize() {
        return key.length;
    }

    @Override
    public int getValueSize() {
        return value.length;
    }
}
