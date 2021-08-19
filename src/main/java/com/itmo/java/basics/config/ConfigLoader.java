package com.itmo.java.basics.config;

import java.io.*;
import java.util.Properties;

/**
 * Класс, отвечающий за подгрузку данных из конфигурационного файла формата .properties
 */
public class ConfigLoader {

    public static final String DEFAULT_FILENAME = "server.properties";

    private final String filename;

    /**
     * По умолчанию читает из server.properties
     */
    public ConfigLoader() {
        filename = DEFAULT_FILENAME;
    }

    /**
     * @param name Имя конфикурационного файла, откуда читать
     */
    public ConfigLoader(String name) {
        filename = name;
    }

    /**
     * Считывает конфиг из указанного в конструкторе файла.
     * Если не удалось считать из заданного файла, или какого-то конкретно значения не оказалось,
     * то используют дефолтные значения из {@link DatabaseConfig} и {@link ServerConfig}
     * <br/>
     * Читаются: "kvs.workingPath", "kvs.host", "kvs.port" (но в конфигурационном файле допустимы и другие проперти)
     */
    public DatabaseServerConfig readConfig() {
        Properties properties = new Properties();

        try (FileInputStream file = new FileInputStream(filename)) {
            properties.load(file);
        } catch (Exception e) {
            e.printStackTrace();
        }

        try (InputStream file = this.getClass().getClassLoader().getResourceAsStream(filename)) {
            properties.load(file);
        } catch (Exception e) {
            e.printStackTrace();
        }

        String workingPath = properties.getProperty("kvs.workingPath", DatabaseConfig.DEFAULT_WORKING_PATH);
        String host = properties.getProperty("kvs.host", ServerConfig.DEFAULT_HOST);
        int port = Integer.parseInt(properties.getProperty("kvs.port", String.valueOf(ServerConfig.DEFAULT_PORT)));

        return new DatabaseServerConfig(
                new ServerConfig(host, port),
                new DatabaseConfig(workingPath)
        );
    }
}
