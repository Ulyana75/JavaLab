package com.itmo.java.basics.connector;

import com.itmo.java.basics.DatabaseServer;
import com.itmo.java.basics.config.ConfigLoader;
import com.itmo.java.basics.config.DatabaseConfig;
import com.itmo.java.basics.config.DatabaseServerConfig;
import com.itmo.java.basics.config.ServerConfig;
import com.itmo.java.basics.console.DatabaseCommand;
import com.itmo.java.basics.console.DatabaseCommandResult;
import com.itmo.java.basics.console.impl.ExecutionEnvironmentImpl;
import com.itmo.java.basics.initialization.impl.DatabaseInitializer;
import com.itmo.java.basics.initialization.impl.DatabaseServerInitializer;
import com.itmo.java.basics.initialization.impl.SegmentInitializer;
import com.itmo.java.basics.initialization.impl.TableInitializer;
import com.itmo.java.basics.resp.CommandReader;
import com.itmo.java.protocol.RespReader;
import com.itmo.java.protocol.RespWriter;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Класс, который предоставляет доступ к серверу через сокеты
 */
public class JavaSocketServerConnector implements Closeable {

    /**
     * Экзекьютор для выполнения ClientTask
     */
    private final ExecutorService clientIOWorkers = Executors.newSingleThreadExecutor();

    private final ServerSocket serverSocket;
    private final ExecutorService connectionAcceptorExecutor = Executors.newSingleThreadExecutor();

    private final DatabaseServer databaseServer;

    /**
     * Стартует сервер. По аналогии с сокетом открывает коннекшн в конструкторе.
     */
    public JavaSocketServerConnector(DatabaseServer databaseServer, ServerConfig config) throws IOException {
        serverSocket = new ServerSocket(config.getPort());
        this.databaseServer = databaseServer;
    }

    /**
     * Начинает слушать заданный порт, начинает аксептить клиентские сокеты. На каждый из них начинает клиентскую таску
     */
    public void start() {
        connectionAcceptorExecutor.submit(() -> {
            try {
                while (!serverSocket.isClosed()) {
                    Socket clientSocket = serverSocket.accept();
                    clientIOWorkers.submit(new ClientTask(clientSocket, databaseServer));
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    /**
     * Закрывает все, что нужно ¯\_(ツ)_/¯
     */
    @Override
    public void close() {
        System.out.println("Stopping socket connector");
        clientIOWorkers.shutdownNow();
        connectionAcceptorExecutor.shutdownNow();
        try {
            serverSocket.close();
        } catch (IOException e) {
            throw new RuntimeException("Cannot close something", e);
        }
    }


    public static void main(String[] args) throws Exception {
        DatabaseServerConfig config = new ConfigLoader().readConfig();
        DatabaseServer server = DatabaseServer.initialize(
                new ExecutionEnvironmentImpl(config.getDbConfig()),
                new DatabaseServerInitializer(
                        new DatabaseInitializer(
                                new TableInitializer(
                                        new SegmentInitializer())))
        );
        JavaSocketServerConnector connector = new JavaSocketServerConnector(server, config.getServerConfig());
        connector.start();
    }

    /**
     * Runnable, описывающий исполнение клиентской команды.
     */
    static class ClientTask implements Runnable, Closeable {

        Socket socket;
        DatabaseServer server;
        CommandReader reader;
        RespWriter writer;

        /**
         * @param client клиентский сокет
         * @param server сервер, на котором исполняется задача
         */
        public ClientTask(Socket client, DatabaseServer server) {
            socket = client;
            this.server = server;
            try {
                reader = new CommandReader(
                        new RespReader(new BufferedInputStream(socket.getInputStream())),
                        server.getEnv()
                );
                writer = new RespWriter(new BufferedOutputStream(socket.getOutputStream()));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        /**
         * Исполняет задачи из одного клиентского сокета, пока клиент не отсоединился или текущий поток не был прерван (interrupted).
         * Для кажной из задач:
         * 1. Читает из сокета команду с помощью {@link CommandReader}
         * 2. Исполняет ее на сервере
         * 3. Записывает результат в сокет с помощью {@link RespWriter}
         */
        @Override
        public void run() {
            while (!socket.isClosed()) {
                try {
                    DatabaseCommand command = reader.readCommand();
                    CompletableFuture<DatabaseCommandResult> result = server.executeNextCommand(command);

                    writer.write(result.get().serialize());
                } catch (Exception ignored) {
                    close();
                    return;
                }
            }
        }

        /**
         * Закрывает клиентский сокет
         */
        @Override
        public void close() {
            try {
                reader.close();
                writer.close();
                socket.close();
            } catch (Exception e) {
                throw new RuntimeException("Cannot close something", e);
            }
        }
    }
}
