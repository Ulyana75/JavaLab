package com.itmo.java.protocol.model;

import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedList;
import java.util.List;

/**
 * Массив RESP объектов
 */
public class RespArray implements RespObject {

    /**
     * Код объекта
     */
    public static final byte CODE = '*';

    private final RespObject[] objects;

    public RespArray(RespObject... objects) {
        this.objects = objects;
    }

    /**
     * Ошибка ли это? Ответ - нет
     *
     * @return false
     */
    @Override
    public boolean isError() {
        return false;
    }

    /**
     * Строковое представление
     *
     * @return результаты метода {@link RespObject#asString()} для всех хранимых объектов, разделенные пробелом
     */
    @Override
    public String asString() {
        List<String> str = new LinkedList<>();
        for (RespObject i : objects) {
            str.add(i.asString());
        }
        return String.join(" ", str);
    }

    @Override
    public void write(OutputStream os) throws IOException {
        os.write(CODE);
        os.write(String.valueOf(objects.length).getBytes());
        os.write(CRLF);
        for (RespObject i : objects) {
            i.write(os);
        }
    }

    public List<RespObject> getObjects() {
        return List.of(objects);
    }
}
