package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.SegmentOffsetInfo;
import com.itmo.java.basics.index.impl.SegmentIndex;
import com.itmo.java.basics.index.impl.SegmentOffsetInfoImpl;
import com.itmo.java.basics.initialization.SegmentInitializationContext;
import com.itmo.java.basics.logic.DatabaseRecord;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.logic.WritableDatabaseRecord;
import com.itmo.java.basics.logic.io.DatabaseInputStream;
import com.itmo.java.basics.logic.io.DatabaseOutputStream;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;


/**
 * Сегмент - append-only файл, хранящий пары ключ-значение, разделенные специальным символом.
 * - имеет ограниченный размер, большие значения (>100000) записываются в последний сегмент, если он не read-only
 * - при превышении размера сегмента создается новый сегмент и дальнейшие операции записи производятся в него
 * - именование файла-сегмента должно позволять установить очередность их появления
 * - является неизменяемым после появления более нового сегмента
 */

public class SegmentImpl implements Segment {

    private String name;
    private long freeSize;
    private Path rootPath;
    private boolean isReadOnly = false;
    private SegmentIndex index;
    private static final long SEGMENT_SIZE = 100_000;

    static Segment create(String segmentName, Path tableRootPath) throws DatabaseException {

        Path path = Paths.get(tableRootPath.toString(), segmentName);

        if (Files.exists(path)) {
            throw new DatabaseException(String.format("Segment with name \"%s\" already exists!", segmentName));
        }

        try {
            Files.createFile(path);
        } catch (IOException e) {
            throw new DatabaseException(String.format("Something gone wrong while creating file %s!",
                    path.toString()), e);
        }

        SegmentImpl s = new SegmentImpl();
        s.name = segmentName;
        s.rootPath = tableRootPath;
        s.freeSize = SEGMENT_SIZE;
        s.index = new SegmentIndex();

        return s;
    }

    public static Segment initializeFromContext(SegmentInitializationContext context) {
        return null;
    }

    static String createSegmentName(String tableName) {
        return tableName + "_" + System.currentTimeMillis();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean write(String objectKey, byte[] objectValue) throws IOException {

        if (objectValue == null) {
            return delete(objectKey);
        }

        SetDatabaseRecord dbr = new SetDatabaseRecord(objectKey.getBytes(), objectValue);

        return writeToFile(dbr, new SegmentOffsetInfoImpl(SEGMENT_SIZE - freeSize));
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws IOException {

        Optional<SegmentOffsetInfo> offsetInfo = index.searchForKey(objectKey);

        if (offsetInfo.isEmpty()) {
            return Optional.empty();
        }

        long offset = offsetInfo.get().getOffset();

        try (DatabaseInputStream dbs = new DatabaseInputStream(new FileInputStream(
                Paths.get(rootPath.toString(), name).toString()))) {
            var skipBytes = dbs.skip(offset);
            if (skipBytes != offset) {
                throw new IOException(String.format("Couldn't skip %d bytes", offset));
            }
            Optional<DatabaseRecord> dbr = dbs.readDbUnit();
            dbs.close();


            return dbr.map(e -> e.getValue());
        }
    }

    @Override
    public boolean isReadOnly() {
        return isReadOnly;
    }

    @Override
    public boolean delete(String objectKey) throws IOException {

        RemoveDatabaseRecord dbr = new RemoveDatabaseRecord(objectKey.getBytes());

        return writeToFile(dbr, null);
    }

    private boolean writeToFile(WritableDatabaseRecord dbr, SegmentOffsetInfoImpl soi) throws IOException {

        if (isReadOnly) {
            return false;
        }

        long recordSize = dbr.size();

        if (recordSize >= freeSize) {
            isReadOnly = true;
        }

        try (DatabaseOutputStream dbs = new DatabaseOutputStream(new FileOutputStream(
                Paths.get(rootPath.toString(), name).toString(), true))) {
            dbs.write(dbr);
        }

        index.onIndexedEntityUpdated(new String(dbr.getKey()), soi);
        freeSize -= recordSize;

        return true;

    }

}
