package gew.filesystem.common.util;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author Jason/GeW
 * @since 2022-10-22
 */
@Slf4j
public class DefaultPropertyReader {

    private Properties properties;

    public static final String VERSION_KEY = "client.version";


    public DefaultPropertyReader(String propertyFileName) {
        try (InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(propertyFileName)) {
            this.properties = new Properties();
            this.properties.load(is);

        } catch (IOException ioe) {
            log.error(String.format("Load Default Properties From Pom Failed: %s", ioe.getMessage()), ioe);
            // Generator Default Empty Properties to Avoid Null Pointer
            this.properties = new Properties();
        }
    }

    public String getVersion() {
        return this.properties.getProperty(VERSION_KEY);
    }

    public String get(String key) {
        return this.properties.getProperty(key);
    }

    public String get(String key, String defaultValue) {
        return this.properties.getProperty(key, defaultValue);
    }

}
