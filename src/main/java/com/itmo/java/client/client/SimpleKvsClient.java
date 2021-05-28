package com.itmo.java.client.client;

import com.itmo.java.client.command.*;
import com.itmo.java.client.connection.KvsConnection;
import com.itmo.java.client.exception.ConnectionException;
import com.itmo.java.client.exception.DatabaseExecutionException;
import com.itmo.java.protocol.model.RespObject;

import java.util.function.Supplier;

public class SimpleKvsClient implements KvsClient {

    private final String databaseName;
    private final Supplier<KvsConnection> connectionSupplier;

    /**
     * Конструктор
     *
     * @param databaseName       имя базы, с которой работает
     * @param connectionSupplier метод создания подключения к базе
     */
    public SimpleKvsClient(String databaseName, Supplier<KvsConnection> connectionSupplier) {
        this.databaseName = databaseName;
        this.connectionSupplier = connectionSupplier;
    }

    @Override
    public String createDatabase() throws DatabaseExecutionException {
        KvsCommand command = new CreateDatabaseKvsCommand(databaseName);
        return getResult(command);
    }

    @Override
    public String createTable(String tableName) throws DatabaseExecutionException {
        KvsCommand command = new CreateTableKvsCommand(databaseName, tableName);
        return getResult(command);
    }

    @Override
    public String get(String tableName, String key) throws DatabaseExecutionException {
        KvsCommand command = new GetKvsCommand(databaseName, tableName, key);
        return getResult(command);
    }

    @Override
    public String set(String tableName, String key, String value) throws DatabaseExecutionException {
        KvsCommand command = new SetKvsCommand(databaseName, tableName, key, value);
        return getResult(command);
    }

    @Override
    public String delete(String tableName, String key) throws DatabaseExecutionException {
        KvsCommand command = new DeleteKvsCommand(databaseName, tableName, key);
        return getResult(command);
    }

    private String getResult(KvsCommand command) throws DatabaseExecutionException {
        try {
            RespObject result = connectionSupplier.get().send(command.getCommandId(), command.serialize());
            if (result.isError()) {
                throw new DatabaseExecutionException("Command was failed: " + result.asString());
            }
            return result.asString();
        } catch (ConnectionException e) {
            throw new DatabaseExecutionException("Something gone wrong while working with database " + databaseName);
        }
    }
}
