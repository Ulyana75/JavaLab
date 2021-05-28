package com.itmo.java.basics;

import com.itmo.java.basics.console.*;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.initialization.impl.DatabaseServerInitializer;
import com.itmo.java.basics.initialization.impl.InitializationContextImpl;
import com.itmo.java.protocol.model.RespArray;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DatabaseServer {

    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    private final ExecutionEnvironment env;

    private DatabaseServer(ExecutionEnvironment env) {
        this.env = env;
    }

    /**
     * Конструктор
     *
     * @param env         env для инициализации. Далее работа происходит с заполненным объектом
     * @param initializer готовый чейн инициализации
     * @throws DatabaseException если произошла ошибка инициализации
     */
    public static DatabaseServer initialize(ExecutionEnvironment env, DatabaseServerInitializer initializer) throws DatabaseException {
        DatabaseServer dbServer = new DatabaseServer(env);
        InitializationContextImpl context = InitializationContextImpl.builder()
                .executionEnvironment(env)
                .build();
        initializer.perform(context);
        return dbServer;
    }

    public CompletableFuture<DatabaseCommandResult> executeNextCommand(RespArray message) {
        String commandName = message.getObjects().get(
                DatabaseCommandArgPositions.COMMAND_NAME.getPositionIndex()
        ).asString();
        return executeNextCommand(DatabaseCommands.valueOf(commandName).getCommand(env, message.getObjects()));
    }

    public CompletableFuture<DatabaseCommandResult> executeNextCommand(DatabaseCommand command) {
        return CompletableFuture.supplyAsync(command::execute, executorService);
    }

    public ExecutionEnvironment getEnv() {
        //TODO implement
        return null;
    }
}