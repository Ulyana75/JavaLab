package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.initialization.DatabaseInitializationContext;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.basics.logic.Table;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Optional;

public class DatabaseImpl implements Database {

    private Path mRootPath;
    private String mName;
    private final HashMap<String, Table> mTables = new HashMap<>();

    /**
     * @param databaseRoot путь к директории, которая может содержать несколько БД,
     *                     поэтому при создании БД необходимо создать директорию внутри databaseRoot.
     */
    public static Database create(String dbName, Path databaseRoot) throws DatabaseException {
        Path path = Paths.get(databaseRoot.toString() + '/' + dbName);
        if(Files.exists(path)) {
            throw new DatabaseException("Database with this name already exists!");
        }
        else {
            try {
                Files.createDirectory(path);
            }
            catch(IOException e) {
                throw new DatabaseException("Something gone wrong!");
            }
        }
        DatabaseImpl db = new DatabaseImpl();
        db.mName = dbName;
        db.mRootPath = databaseRoot;
        return db;
    }

    public static Database initializeFromContext(DatabaseInitializationContext context) {
        return null;
    }

    @Override
    public String getName() {
        return mName;
    }

    @Override
    public void createTableIfNotExists(String tableName) throws DatabaseException {
        Table t = TableImpl.create(tableName, Paths.get(mRootPath.toString() + '/' + mName), new TableIndex());
        mTables.put(tableName, t);
    }

    @Override
    public void write(String tableName, String objectKey, byte[] objectValue) throws DatabaseException {
        mTables.get(tableName).write(objectKey, objectValue);
    }

    @Override
    public Optional<byte[]> read(String tableName, String objectKey) throws DatabaseException {
        return mTables.get(tableName).read(objectKey);
    }

    @Override
    public void delete(String tableName, String objectKey) throws DatabaseException {
        mTables.get(tableName).delete(objectKey);
    }
}
