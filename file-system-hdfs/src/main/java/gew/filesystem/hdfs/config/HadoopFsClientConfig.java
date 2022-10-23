package gew.filesystem.hdfs.config;

import gew.filesystem.common.config.FileSystemConfig;
import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.conf.Configuration;

import java.io.Serializable;
import java.net.URI;

/**
 * Hadoop Distributed File System Config
 *
 * @author Jason/GeW
 * @since 2019-03-24
 */
@Getter
@Setter
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

}
