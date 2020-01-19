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

    private String fsDefaultName;

    private Integer replication;

    private String username;

    private boolean useCustomConfig;

    private Configuration configuration;


    public HadoopFsClientConfig() {

    }

    public HadoopFsClientConfig(URI nameNodeUri, String username) {
        this.nameNodeUri = nameNodeUri;
        this.username = username;
    }


    public URI getNameNodeUri() {
        return nameNodeUri;
    }

    public void setNameNodeUri(URI nameNodeUri) {
        this.nameNodeUri = nameNodeUri;
    }

    public String getFsDefaultName() {
        return fsDefaultName;
    }

    public void setFsDefaultName(String fsDefaultName) {
        this.fsDefaultName = fsDefaultName;
    }

    public Integer getReplication() {
        return replication;
    }

    public void setReplication(Integer replication) {
        this.replication = replication;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public boolean isUseCustomConfig() {
        return useCustomConfig;
    }

    public void setUseCustomConfig(boolean useCustomConfig) {
        this.useCustomConfig = useCustomConfig;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }
}
