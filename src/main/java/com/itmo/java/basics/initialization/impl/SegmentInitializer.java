package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.SegmentIndex;
import com.itmo.java.basics.index.impl.SegmentOffsetInfoImpl;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.logic.DatabaseRecord;
import com.itmo.java.basics.logic.WritableDatabaseRecord;
import com.itmo.java.basics.logic.impl.SegmentImpl;
import com.itmo.java.basics.logic.io.DatabaseInputStream;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

public class SegmentInitializer implements Initializer {

    /**
     * Добавляет в контекст информацию об инициализируемом сегменте.
     * Составляет индекс сегмента
     * Обновляет инфу в индексе таблицы
     *
     * @param context контекст с информацией об инициализируемой бд и об окружении
     * @throws DatabaseException если в контексте лежит неправильный путь к сегменту, невозможно прочитать содержимое. Ошибка в содержании
     */
    @Override
    public void perform(InitializationContext context) throws DatabaseException {

        var segmentContext = context.currentSegmentContext();
        var path = segmentContext.getSegmentPath();
        var segmentIndex = segmentContext.getIndex();
        long currentSize = 0;

        if (!Files.exists(path)) {
            throw new DatabaseException(String.format("No segment with name \"%s\"",
                    segmentContext.getSegmentName()));
        }

        List<String> listKeys = new LinkedList<>();

        try (DatabaseInputStream dis = new DatabaseInputStream(new FileInputStream(path.toString()))) {
            Optional<DatabaseRecord> dbUnit = dis.readDbUnit();
            while (dbUnit.isPresent()) {
                var dbr = dbUnit.get();

                segmentIndex.onIndexedEntityUpdated(new String(dbr.getKey()), new SegmentOffsetInfoImpl(currentSize));

                currentSize += dbr.size();
                listKeys.add(new String(dbr.getKey()));
                dbUnit = dis.readDbUnit();
            }

        } catch (IOException e) {
            throw new DatabaseException("Something gone wrong while initialising segment", e);
        }

        var segment = SegmentImpl.initializeFromContext(new SegmentInitializationContextImpl(
                segmentContext.getSegmentName(),
                segmentContext.getSegmentPath(),
                currentSize,
                segmentIndex
        ));
        for (String s : listKeys) {
            context.currentTableContext().getTableIndex().onIndexedEntityUpdated(s, segment);
        }
        context.currentTableContext().updateCurrentSegment(segment);

    }

}
