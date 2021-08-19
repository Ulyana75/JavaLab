package com.itmo.java.protocol;

import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespBulkString;
import com.itmo.java.protocol.model.RespCommandId;
import com.itmo.java.protocol.model.RespError;
import com.itmo.java.protocol.model.RespObject;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

public class RespReader implements AutoCloseable {

    /**
     * Специальные символы окончания элемента
     */
    private static final byte CR = '\r';
    private static final byte LF = '\n';

    private final PushbackInputStream pis;

    public RespReader(InputStream is) {
        pis = new PushbackInputStream(is);
    }

    /**
     * Есть ли следующий массив в стриме?
     */
    public boolean hasArray() throws IOException {
        return getFirstByte() == RespArray.CODE;
    }

    /**
     * Считывает из input stream следующий объект. Может прочитать любой объект, сам определит его тип на основе кода объекта.
     * Например, если первый элемент "-", то вернет ошибку. Если "$" - bulk строку
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespObject readObject() throws IOException {
        byte start = getFirstByte();

        switch (start) {
            case RespError.CODE:
                return readError();
            case RespBulkString.CODE:
                return readBulkString();
            case RespArray.CODE:
                return readArray();
            case RespCommandId.CODE:
                return readCommandId();
            case -1:
                throw new EOFException("Stream finished");
            default:
                throw new IOException("Wrong first symbol");
        }
    }

    /**
     * Считывает объект ошибки
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespError readError() throws IOException {
        checkEqualsWithObjectCode(RespError.CODE);
        return new RespError(readUntilCRLF());
    }

    /**
     * Читает bulk строку
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespBulkString readBulkString() throws IOException {
        checkEqualsWithObjectCode(RespBulkString.CODE);

        String data = new String(readUntilCRLF());
        int size = Integer.parseInt(data);
        if (size == RespBulkString.NULL_STRING_SIZE) {
            return RespBulkString.NULL_STRING;
        }

        byte[] payload = pis.readNBytes(size);
        byte[] restData = readUntilCRLF();
        if (payload.length != size || restData.length != 0) {
            throw new IOException("Wrong object in the stream");
        }

        return new RespBulkString(payload);
    }

    /**
     * Считывает массив RESP элементов
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespArray readArray() throws IOException {
        checkEqualsWithObjectCode(RespArray.CODE);

        List<RespObject> objects = new LinkedList<>();
        String data = new String(readUntilCRLF());
        int size = Integer.parseInt(data);

        for (int i = 0; i < size; i++) {
            objects.add(readObject());
        }

        return new RespArray(objects.toArray(new RespObject[0]));
    }

    /**
     * Считывает id команды
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespCommandId readCommandId() throws IOException {
        checkEqualsWithObjectCode(RespCommandId.CODE);
        byte[] data = pis.readNBytes(4);
        if (data.length != 4) {
            throw new IOException("Wrong object in the stream");
        }
        ByteBuffer bb = ByteBuffer.wrap(data);
        int id = bb.getInt();
        byte[] restData = readUntilCRLF();
        if (restData.length != 0) {
            throw new IOException("Wrong object in the stream");
        }
        return new RespCommandId(id);
    }


    @Override
    public void close() throws IOException {
        pis.close();
    }

    private byte[] readUntilCRLF() throws IOException {
        List<Byte> bytes = new LinkedList<>();
        byte b1 = 0;
        byte b2 = 0;
        while (b1 != CR || b2 != LF) {
            b1 = b2;
            b2 = (byte) pis.read();
            if (b2 == -1) {
                throw new IOException("Wrong object in the stream");
            }
            bytes.add(b2);
        }
        byte[] data = new byte[bytes.size() - 2];
        for (int i = 0; i < bytes.size() - 2; i++) {
            data[i] = bytes.get(i);
        }
        return data;
    }

    private void checkEqualsWithObjectCode(byte code) throws IOException {
        byte start = (byte) pis.read();
        if (start != code) {
            throw new IOException("Wrong object in the stream");
        }
    }

    private byte getFirstByte() throws IOException {
        byte firstByte = (byte) pis.read();
        pis.unread(firstByte);
        return firstByte;
    }
}