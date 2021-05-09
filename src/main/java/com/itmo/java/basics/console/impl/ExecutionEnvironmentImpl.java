package com.itmo.java.basics.console.impl;

import com.itmo.java.basics.config.DatabaseConfig;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.logic.Database;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class ExecutionEnvironmentImpl implements ExecutionEnvironment {

    private final Map<String, Database> dataBases = new HashMap<>();
    private final DatabaseConfig databaseConfig;

    public ExecutionEnvironmentImpl(DatabaseConfig config) {
        databaseConfig = config;
    }

    @Override
    public Optional<Database> getDatabase(String name) {
        return Optional.ofNullable(dataBases.get(name));
    }

    @Override
    public void addDatabase(Database db) {
        dataBases.put(db.getName(), db);
    }

    @Override
    public Path getWorkingPath() {
        return Paths.get(databaseConfig.getWorkingPath());
    }
}
