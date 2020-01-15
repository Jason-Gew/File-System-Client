package gew.filesystem.client.aws;

import gew.filesystem.client.common.FileSystemConfig;

import java.io.Serializable;

/**
 * @author Jason/GeW
 * @since 2019-03-24
 */
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


    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getAccessKeyId() {
        return accessKeyId;
    }

    public void setAccessKeyId(String accessKeyId) {
        this.accessKeyId = accessKeyId;
    }

    public String getAccessKeySecret() {
        return accessKeySecret;
    }

    public void setAccessKeySecret(String accessKeySecret) {
        this.accessKeySecret = accessKeySecret;
    }


    @Override
    public String toString() {
        return "{" +
                "region='" + region + '\'' +
                ", accessKeyId='" + accessKeyId + '\'' +
                ", accessKeySecret='" + accessKeySecret + '\'' +
                '}';
    }
}
