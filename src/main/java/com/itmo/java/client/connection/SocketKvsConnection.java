package com.itmo.java.client.connection;

import com.itmo.java.client.exception.ConnectionException;
import com.itmo.java.protocol.RespReader;
import com.itmo.java.protocol.RespWriter;
import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespBulkString;
import com.itmo.java.protocol.model.RespError;
import com.itmo.java.protocol.model.RespObject;

import java.io.*;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 * С помощью {@link RespWriter} и {@link RespReader} читает/пишет в сокет
 */
public class SocketKvsConnection implements KvsConnection {

    private final Socket socket;
    private final RespWriter writer;
    private final RespReader reader;

    public SocketKvsConnection(ConnectionConfig config) {
        try {
            socket = new Socket(config.getHost(), config.getPort());
            reader = new RespReader(new BufferedInputStream(socket.getInputStream()));
            writer = new RespWriter(new BufferedOutputStream(socket.getOutputStream()));
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    /**
     * Отправляет с помощью сокета команду и получает результат.
     *
     * @param commandId id команды (номер)
     * @param command   команда
     * @throws ConnectionException если сокет закрыт или если произошла другая ошибка соединения
     */
    @Override
    public synchronized RespObject send(int commandId, RespArray command) throws ConnectionException {
        try {
            writer.write(command);
            return reader.readObject();
        } catch (Exception e) {
            throw new ConnectionException("Something gone wrong while connecting", e);
        }
    }

    /**
     * Закрывает сокет (и другие использованные ресурсы)
     */
    @Override
    public void close() {
        try {
            reader.close();
            writer.close();
            socket.close();
        } catch (IOException e) {
            throw new RuntimeException("Cannot close something", e);
        }
    }
}
