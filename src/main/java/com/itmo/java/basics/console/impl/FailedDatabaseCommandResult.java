package com.itmo.java.basics.console.impl;

import com.itmo.java.basics.console.DatabaseCommandResult;
import com.itmo.java.protocol.model.RespError;
import com.itmo.java.protocol.model.RespObject;

import java.nio.charset.StandardCharsets;

/**
 * Зафейленная команда
 */
public class FailedDatabaseCommandResult implements DatabaseCommandResult {

    private final byte[] data;

    public FailedDatabaseCommandResult(String payload) {
        if (payload == null) {
            data = null;
        } else {
            data = payload.getBytes();
        }
    }

    /**
     * Сообщение об ошибке
     */
    @Override
    public String getPayLoad() {
        if (data == null) {
            return null;
        }
        return new String(data);
    }

    @Override
    public boolean isSuccess() {
        return false;
    }

    /**
     * Сериализуется в {@link RespError}
     */
    @Override
    public RespObject serialize() {
        return new RespError(data);
    }
}
