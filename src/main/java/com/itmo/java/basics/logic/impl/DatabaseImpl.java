package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.initialization.DatabaseInitializationContext;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.basics.logic.Table;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class DatabaseImpl implements Database {

    private Path rootPath;
    private String name;
    private final Map<String, Table> tables = new HashMap<>();

    /**
     * @param databaseRoot путь к директории, которая может содержать несколько БД,
     *                     поэтому при создании БД необходимо создать директорию внутри databaseRoot.
     */
    public static Database create(String dbName, Path databaseRoot) throws DatabaseException {

        if (dbName == null) {
            throw new DatabaseException("Your database name is null!");
        }

        Path path = Paths.get(databaseRoot.toString(), dbName);

        if (Files.exists(path)) {
            throw new DatabaseException(String.format("Database with name \"%s\" already exists!", dbName));
        }

        try {
            Files.createDirectory(path);
        } catch (IOException e) {
            throw new DatabaseException(String.format("Something gone wrong while creating directory %s!",
                    path.toString()), e);
        }

        DatabaseImpl db = new DatabaseImpl();
        db.name = dbName;
        db.rootPath = databaseRoot;

        return db;
    }

    public static Database initializeFromContext(DatabaseInitializationContext context) {
        return null;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void createTableIfNotExists(String tableName) throws DatabaseException {
        if (tableName == null) {
            throw new DatabaseException("Your table name is null!");
        }

        Table t = TableImpl.create(tableName, Paths.get(rootPath.toString(), name), new TableIndex());
        tables.put(tableName, t);
    }

    @Override
    public void write(String tableName, String objectKey, byte[] objectValue) throws DatabaseException {
        Table t = checkTable(tableName);
        t.write(objectKey, objectValue);
    }

    @Override
    public Optional<byte[]> read(String tableName, String objectKey) throws DatabaseException {
        Table t = checkTable(tableName);
        return t.read(objectKey);
    }

    @Override
    public void delete(String tableName, String objectKey) throws DatabaseException {
        Table t = checkTable(tableName);
        t.delete(objectKey);
    }

    private Table checkTable(String tableName) throws DatabaseException {

        Table t = tables.get(tableName);

        if (t == null) {
            throw new DatabaseException(String.format("Table with name \"%s\" doesn't exists!", tableName));
        }

        return t;
    }
}
