package gew.filesystem.client.hdfs;

import gew.filesystem.client.common.FileSystemConfig;
import org.apache.hadoop.conf.Configuration;

import java.io.Serializable;
import java.net.URI;

/**
 * @author Jason/GeW
 * @since 2019-03-24
 */
public class HadoopFsClientConfig implements FileSystemConfig, Serializable {

    private URI nameNodeUri;

    private String username;

    private String password;

    private Configuration configuration;


    public HadoopFsClientConfig() {

    }

    public HadoopFsClientConfig(URI nameNodeUri, String username, String password) {
        this.nameNodeUri = nameNodeUri;
        this.username = username;
        this.password = password;
    }

    public URI getNameNodeUri() {
        return nameNodeUri;
    }

    public void setNameNodeUri(URI nameNodeUri) {
        this.nameNodeUri = nameNodeUri;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }
}
