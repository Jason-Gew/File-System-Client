package gew.filesystem.oss.config;

import com.aliyun.oss.ClientConfiguration;
import com.aliyun.oss.common.auth.CredentialsProvider;
import gew.filesystem.common.config.FileSystemConfig;
import lombok.Getter;
import lombok.Setter;

/**
 * AliYun OSS Config
 *
 * @author Jason/GeW
 * @since 2022-10-23
 */
@Getter
@Setter
public class AliOssConfig implements FileSystemConfig {

    private String endpoint;

    private String accessKeyId;

    private String accessKeySecret;

    private String securityToken;

    private CredentialsProvider credentialsProvider;


    public AliOssConfig() {
        // Default
    }

    public AliOssConfig(String endpoint) {
        this.endpoint = endpoint;
    }

    public AliOssConfig(String endpoint, String accessKeyId, String accessKeySecret) {
        this.endpoint = endpoint;
        this.accessKeyId = accessKeyId;
        this.accessKeySecret = accessKeySecret;
    }
}
