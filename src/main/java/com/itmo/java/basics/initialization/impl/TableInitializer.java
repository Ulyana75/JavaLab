package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.logic.impl.TableImpl;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;

public class TableInitializer implements Initializer {

    private final SegmentInitializer segmentInitializer;

    public TableInitializer(SegmentInitializer segmentInitializer) {
        this.segmentInitializer = segmentInitializer;
    }

    /**
     * Добавляет в контекст информацию об инициализируемой таблице.
     * Запускает инициализацию всех сегментов в порядке их создания (из имени)
     *
     * @param context контекст с информацией об инициализируемой бд, окружении, таблицы
     * @throws DatabaseException если в контексте лежит неправильный путь к таблице, невозможно прочитать содержимого папки,
     *                           или если возникла ошибка ошибка дочерних инициализаторов
     */
    @Override
    public void perform(InitializationContext context) throws DatabaseException {

        var tableContext = context.currentTableContext();
        var file = new File(tableContext.getTablePath().toString());

        if (!file.exists()) {
            throw new DatabaseException(String.format("No directory with name \"%s\"", tableContext.getTableName()));
        }

        var listFiles = file.listFiles();
        if (listFiles == null) {
            throw new DatabaseException(String.format("Found a file, not a directory on path %s", file.toString()));
        }

        var list = Arrays.asList(listFiles);
        Collections.sort(list);

        for (File i : list) {
            var segmentContext = new SegmentInitializationContextImpl(i.getName(),
                    tableContext.getTablePath(), 0);

            segmentInitializer.perform(InitializationContextImpl.builder()
                    .currentTableContext(tableContext)
                    .currentSegmentContext(segmentContext)
                    .build()
            );
        }

        var table = TableImpl.initializeFromContext(tableContext);
        context.currentDbContext().addTable(table);
    }
}
