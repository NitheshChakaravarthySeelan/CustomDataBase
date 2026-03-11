package com.minidb;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class MiniDbConfig {
    private final Properties properties = new Properties();

    public MiniDbConfig() {
        try (InputStream input = getClass().getClassLoader().getResourceAsStream("application.properties")) {
            if (input == null) {
                System.out.println("No application.properties found, using default values.");
                return;
            }
            properties.load(input);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public int getInt(String key, int defaultValue) {
        String value = properties.getProperty(key);
        return (value != null) ? Integer.parseInt(value) : defaultValue;
    }

    public int getBPlusTreeOrder() {
        return getInt("minidb.bPlusTreeOrder", 5);
    }

    public int getBufferPoolSize() {
        return getInt("minidb.bufferPoolSize", 10);
    }

    public int getPageSize() {
        return getInt("minidb.pageSize", 4096);
    }
}
