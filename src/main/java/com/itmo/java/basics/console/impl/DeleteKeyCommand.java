package com.itmo.java.basics.console.impl;

import com.itmo.java.basics.console.DatabaseCommand;
import com.itmo.java.basics.console.DatabaseCommandArgPositions;
import com.itmo.java.basics.console.DatabaseCommandResult;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.protocol.model.RespObject;

import java.util.List;
import java.util.Optional;

/**
 * Команда для создания удаления значения по ключу
 */
public class DeleteKeyCommand implements DatabaseCommand {

    private static final int ARGUMENTS_QUANTITY = 5;

    private final ExecutionEnvironment env;
    private final List<RespObject> commandArgs;

    /**
     * Создает команду.
     * <br/>
     * Обратите внимание, что в конструкторе нет логики проверки валидности данных. Не проверяется, можно ли исполнить команду. Только формальные признаки (например, количество переданных значений или ненуловость объектов
     *
     * @param env         env
     * @param commandArgs аргументы для создания (порядок - {@link DatabaseCommandArgPositions}.
     *                    Id команды, имя команды, имя бд, таблицы, ключ
     * @throws IllegalArgumentException если передано неправильное количество аргументов
     */
    public DeleteKeyCommand(ExecutionEnvironment env, List<RespObject> commandArgs) {
        if (commandArgs.size() !=ARGUMENTS_QUANTITY) {
            throw new IllegalArgumentException("Wrong quantity of command's arguments!");
        }
        this.env = env;
        this.commandArgs = commandArgs;
    }

    /**
     * Удаляет значение по ключу
     *
     * @return {@link DatabaseCommandResult#success(byte[])} с удаленным значением. Например, "previous"
     */
    @Override
    public DatabaseCommandResult execute() {
        try {
            String databaseName = commandArgs.get(DatabaseCommandArgPositions.DATABASE_NAME.getPositionIndex()).asString();
            Optional<Database> optionalDatabase = env.getDatabase(databaseName);
            if (optionalDatabase.isEmpty()) {
                throw new DatabaseException("No such database with name " + databaseName);
            }
            Database database = optionalDatabase.get();
            String tableName = commandArgs.get(DatabaseCommandArgPositions.TABLE_NAME.getPositionIndex()).asString();
            String key = commandArgs.get(DatabaseCommandArgPositions.KEY.getPositionIndex()).asString();
            Optional<byte[]> previousValue = database.read(tableName, key);
            database.delete(tableName, key);
            return previousValue.map(DatabaseCommandResult::success).orElseGet(() ->
                    DatabaseCommandResult.error("Nothing to delete: no such key " + key));
        } catch (Exception e) {
            return DatabaseCommandResult.error(e);
        }
    }
}
