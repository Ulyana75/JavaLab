package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.logic.impl.DatabaseImpl;

import java.io.File;

public class DatabaseInitializer implements Initializer {

    private final TableInitializer tableInitializer;

    public DatabaseInitializer(TableInitializer tableInitializer) {
        this.tableInitializer = tableInitializer;
    }

    /**
     * Добавляет в контекст информацию об инициализируемой бд.
     * Запускает инициализацию всех таблиц это базы
     *
     * @param initialContext контекст с информацией об инициализируемой бд и об окружении
     * @throws DatabaseException если в контексте лежит неправильный путь к базе, невозможно прочитать содержимого папки,
     *                           или если возникла ошибка дочерних инициализаторов
     */
    @Override
    public void perform(InitializationContext initialContext) throws DatabaseException {

        var dbContext = initialContext.currentDbContext();
        var file = new File(dbContext.getDatabasePath().toString());

        if (!file.exists()) {
            throw new DatabaseException(String.format("No directory with name \"%s\"", dbContext.getDbName()));
        }

        var listFiles = file.listFiles();
        if (listFiles == null) {
            throw new DatabaseException(String.format("Found a file, not a directory on path %s", file.toString()));
        }

        for (File i : listFiles) {
            var tableContext = new TableInitializationContextImpl(i.getName(),
                    dbContext.getDatabasePath(), new TableIndex());

            tableInitializer.perform(InitializationContextImpl.builder()
                    .currentDatabaseContext(dbContext)
                    .currentTableContext(tableContext)
                    .build()
            );
        }

        var database = DatabaseImpl.initializeFromContext(dbContext);
        initialContext.executionEnvironment().addDatabase(database);
    }
}
