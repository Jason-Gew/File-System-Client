package gew.filesystem.client.aws;

import gew.filesystem.client.common.FileSystemConfig;

/**
 * @author Jason/GeW
 * @since 2019-03-24
 */
public class AwsS3Config implements FileSystemConfig {

    private String region;

    private String accessKeyId;

    private String accessKeySecret;


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
}
