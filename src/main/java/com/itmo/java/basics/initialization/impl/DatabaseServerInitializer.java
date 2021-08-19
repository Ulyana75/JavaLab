package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class DatabaseServerInitializer implements Initializer {

    private final DatabaseInitializer databaseInitializer;

    public DatabaseServerInitializer(DatabaseInitializer databaseInitializer) {
        this.databaseInitializer = databaseInitializer;
    }

    /**
     * Если заданная в окружении директория не существует - создает ее
     * Добавляет информацию о существующих в директории базах, начинает их инициализацию
     *
     * @param context контекст, содержащий информацию об окружении
     * @throws DatabaseException если произошла ошибка при создании директории, ее обходе или ошибка инициализации бд
     */
    @Override
    public void perform(InitializationContext context) throws DatabaseException {

        var execEnv = context.executionEnvironment();
        var path = execEnv.getWorkingPath();

        if (!Files.exists(path)) {
            try {
                Files.createDirectory(path);
            } catch (IOException e) {
                throw new DatabaseException(String.format("Something gone wrong while creating directory %s!",
                        path.toString()), e);
            }
        }

        var file = new File(path.toString());
        var listFiles = file.listFiles();

        if (listFiles == null) {
            return;
        }

        for (File i : listFiles) {
            var databaseContext = new DatabaseInitializationContextImpl(i.getName(), path);

            databaseInitializer.perform(InitializationContextImpl.builder()
                    .executionEnvironment(execEnv)
                    .currentDatabaseContext(databaseContext)
                    .build());
        }
    }
}
