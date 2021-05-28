package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.SegmentOffsetInfo;
import com.itmo.java.basics.index.impl.SegmentIndex;
import com.itmo.java.basics.index.impl.SegmentOffsetInfoImpl;
import com.itmo.java.basics.initialization.SegmentInitializationContext;
import com.itmo.java.basics.logic.Segment;

import java.io.*;
import java.nio.ByteBuffer;
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

    private String mName;
    private int mFreeSize;
    private Path mRootPath;
    private boolean mIsReadOnly = false;
    private SegmentIndex mIndex;
    private static final int SEGMENT_SIZE = 100_000;

    static Segment create(String segmentName, Path tableRootPath) throws DatabaseException {
        Path path = Paths.get(tableRootPath.toString() + '/' + segmentName);
        if(Files.exists(path)) {
            throw new DatabaseException("Segment with this name already exists!");
        }
        else {
            try {
                Files.createFile(path);
            }
            catch(IOException e) {
                throw new DatabaseException("Something gone wrong!");
            }
        }
        SegmentImpl s = new SegmentImpl();
        s.mName = segmentName;
        s.mRootPath = tableRootPath;
        s.mFreeSize = SEGMENT_SIZE;
        s.mIndex = new SegmentIndex();
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
        return mName;
    }

    @Override
    public boolean write(String objectKey, byte[] objectValue) throws IOException, DatabaseException {
        int recordSize = 4 + objectKey.getBytes().length + 4 + objectValue.length;
        if(recordSize > mFreeSize) {
            mIsReadOnly = true;
            return false;
        }
        DataOutputStream fos = new DataOutputStream(new FileOutputStream(
                Paths.get(mRootPath.toString() + '/' + mName).toString(), true)
        );
        fos.writeInt(objectKey.length());
        fos.write(objectKey.getBytes());
        fos.writeInt(objectValue.length);
        fos.write(objectValue);
        mIndex.onIndexedEntityUpdated(objectKey, new SegmentOffsetInfoImpl(SEGMENT_SIZE - mFreeSize));
        mFreeSize -= recordSize;
        fos.close();
        return true;
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws IOException {
        Optional<SegmentOffsetInfo> offsetInfo = mIndex.searchForKey(objectKey);
        if(offsetInfo.isEmpty()) {
            return Optional.empty();
        }
        else {
            long offset = offsetInfo.get().getOffset();
            RandomAccessFile rf = new RandomAccessFile(
                    Paths.get(mRootPath.toString() + '/' + mName).toString(), "r");
            rf.seek(offset);
            byte[] sizeInfo = new byte[4];
            rf.read(sizeInfo);
            ByteBuffer bb = ByteBuffer.wrap(sizeInfo);
            int keySize = bb.getInt();
            byte[] key = new byte[keySize];
            rf.read(key);
            rf.read(sizeInfo);
            bb = ByteBuffer.wrap(sizeInfo);
            int valueSize = bb.getInt();
            byte[] value = new byte[valueSize];
            rf.read(value);
            rf.close();
            return Optional.of(value);
        }
    }

    @Override
    public boolean isReadOnly() {
        return mIsReadOnly;
    }

    @Override
    public boolean delete(String objectKey) throws IOException {
        int recordSize = 4 + objectKey.getBytes().length + 4;
        if(recordSize > mFreeSize) {
            mIsReadOnly = true;
            return false;
        }
        FileOutputStream fos = new FileOutputStream(mRootPath.toString() + File.pathSeparator + mName, true);
        fos.write(objectKey.getBytes().length);
        fos.write(objectKey.getBytes());
        fos.write(0);
        mIndex.onIndexedEntityUpdated(objectKey, null);
        mFreeSize -= recordSize;
        fos.close();
        return true;
    }

}
