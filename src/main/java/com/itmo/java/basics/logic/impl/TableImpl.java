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

    private TableIndex index;
    private String name;
    private Path rootPath;
    private Segment currentSegment;

    private TableImpl(String name, Path rootPath, TableIndex index, Segment currentSegment) {
        this.name = name;
        this.rootPath = rootPath;
        this.index = index;
        this.currentSegment = currentSegment;
    }

    public static Table create(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex) throws DatabaseException {

        Path path = Paths.get(pathToDatabaseRoot.toString(), tableName);

        if (Files.exists(path)) {
            throw new DatabaseException(String.format("Table with name \"%s\" already exists!", tableName));
        }

        try {
            Files.createDirectory(path);
        } catch (IOException e) {
            throw new DatabaseException(String.format("Something gone wrong while creating directory %s!",
                    path.toString()), e);
        }

        TableImpl t = new TableImpl(
                tableName,
                pathToDatabaseRoot,
                tableIndex,
                SegmentImpl.create(SegmentImpl.createSegmentName(tableName), path)
        );

        return new CachingTable(t);
    }

    public static Table initializeFromContext(TableInitializationContext context) {
        var table = new TableImpl(
                context.getTableName(),
                context.getTablePath().getParent(),
                context.getTableIndex(),
                context.getCurrentSegment()
        );

        return new CachingTable(table);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void write(String objectKey, byte[] objectValue) throws DatabaseException {

        try {
            boolean success = currentSegment.write(objectKey, objectValue);

            if (!success) {
                currentSegment = SegmentImpl.create(SegmentImpl.createSegmentName(name),
                        Paths.get(rootPath.toString(), name));
                currentSegment.write(objectKey, objectValue);
            }

            index.onIndexedEntityUpdated(objectKey, currentSegment);
        } catch (IOException e) {
            throw new DatabaseException("Something gone wrong while writing!", e);
        }
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws DatabaseException {

        Optional<Segment> s = index.searchForKey(objectKey);
        Optional<byte[]> value = Optional.empty();

        if (s.isPresent()) {
            try {
                value = s.get().read(objectKey);
            } catch (IOException e) {
                throw new DatabaseException("Something gone wrong while reading!", e);
            }
        }

        return value;
    }

    @Override
    public void delete(String objectKey) throws DatabaseException {

        try {
            boolean success = currentSegment.delete(objectKey);

            if (!success) {
                currentSegment = SegmentImpl.create(SegmentImpl.createSegmentName(name),
                        Paths.get(rootPath.toString(), name));
                currentSegment.delete(objectKey);
            }

            index.onIndexedEntityUpdated(objectKey, null);
        } catch (IOException e) {
            throw new DatabaseException("Something gone wrong while deleting!", e);
        }
    }
}
