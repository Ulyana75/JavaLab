package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.initialization.TableInitializationContext;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.logic.Table;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

/**
 * Таблица - логическая сущность, представляющая собой набор файлов-сегментов, которые объединены одним
 * именем и используются для хранения однотипных данных (данных, представляющих собой одну и ту же сущность,
 * например, таблица "Пользователи")
 * <p>
 * - имеет единый размер сегмента
 * - представляет из себя директорию в файловой системе, именованную как таблица
 * и хранящую файлы-сегменты данной таблицы
 */
public class TableImpl implements Table {

    private TableIndex mIndex;
    private String mName;
    private Path mRootPath;
    private Segment mCurrentSegment = null;

    static Table create(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex) throws DatabaseException {
        Path path = Paths.get(pathToDatabaseRoot.toString() + '/' + tableName);
        if(Files.exists(path)) {
            throw new DatabaseException("Table with this name already exists!");
        }
        else {
            try {
                Files.createDirectory(path);
            }
            catch(IOException e) {
                throw new DatabaseException("Something gone wrong!");
            }
        }
        TableImpl t = new TableImpl();
        t.mName = tableName;
        t.mRootPath = pathToDatabaseRoot;
        t.mIndex = tableIndex;
        t.mCurrentSegment = SegmentImpl.create(SegmentImpl.createSegmentName(t.mName), path);
        return t;
    }

    public static Table initializeFromContext(TableInitializationContext context) {
        return null;
    }

    @Override
    public String getName() {
        return mName;
    }

    @Override
    public void write(String objectKey, byte[] objectValue) throws DatabaseException {
        try {
            boolean success = mCurrentSegment.write(objectKey, objectValue);
            if(!success) {
                mCurrentSegment = SegmentImpl.create(SegmentImpl.createSegmentName(mName),
                        Paths.get(mRootPath.toString() + '/' + mName));
                mCurrentSegment.write(objectKey, objectValue);
            }
            mIndex.onIndexedEntityUpdated(objectKey, mCurrentSegment);
        }
        catch (IOException e) {
            throw new DatabaseException("Something gone wrong!");
        }
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws DatabaseException {
        Optional<Segment> s = mIndex.searchForKey(objectKey);
        Optional<byte[]> value = Optional.empty();
        if(s.isPresent()) {
            try {
                value = s.get().read(objectKey);
            }
            catch (IOException e) {
                throw new DatabaseException("Something gone wrong!");
            }
        }
        else {
            throw new DatabaseException("This key doesn't exists!");
        }
        return value;
    }

    @Override
    public void delete(String objectKey) throws DatabaseException {
        Optional<Segment> s = mIndex.searchForKey(objectKey);
        if(s.isPresent()) {
            try {
                s.get().delete(objectKey);
            }
            catch (IOException e) {
                throw new DatabaseException("Something gone wrong!");
            }
        }
        else {
            throw new DatabaseException("This key doesn't exists!");
        }
        mIndex.onIndexedEntityUpdated(objectKey, null);
    }
}
