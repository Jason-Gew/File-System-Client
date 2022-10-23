package gew.filesystem.s3.config;


import gew.filesystem.common.config.FileSystemConfig;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

/**
 * AWS S3 Config
 * If empty settings, then SDK will try to use default
 * [Windows: C:\Users\<yourUserName>\.aws\credentials]
 * [Linux, macOS: ~/.aws/credentials]
 *
 * @author Jason/GeW
 * @since 2019-03-24
 */
@Getter
@Setter
public class AwsS3ClientConfig implements FileSystemConfig, Serializable {

    private String region;

    private String accessKeyId;

    private String accessKeySecret;


    public AwsS3ClientConfig() {

    }

    public AwsS3ClientConfig(String accessKeyId, String accessKeySecret) {
        this.accessKeyId = accessKeyId;
        this.accessKeySecret = accessKeySecret;
    }

    public AwsS3ClientConfig(String accessKeyId, String accessKeySecret, String region) {
        this.region = region;
        this.accessKeyId = accessKeyId;
        this.accessKeySecret = accessKeySecret;
    }

}
