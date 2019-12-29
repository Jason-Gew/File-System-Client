package gew.filesystem.client.aws;

import gew.filesystem.client.common.CloudFileSystemClient;
import gew.filesystem.client.common.FileSystemConfig;
import gew.filesystem.client.model.FileOperation;
import gew.filesystem.client.model.FileSystemType;
import gew.filesystem.client.model.ObjectMetaInfo;
import gew.filesystem.client.model.ObjectProperty;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * AWS S3 File System Client Implementation based on AWS SDKv2
 * @author Jason/GeW
 * @since 2019-03-24
 */
public class AwsS3FileSystemClientImpl implements CloudFileSystemClient {

    private String region;
    private String defaultBucket;

    private S3Client s3Client;

    private boolean existenceCheck;


    private static final Logger log = LogManager.getLogger(AwsS3FileSystemClientImpl.class);


    public AwsS3FileSystemClientImpl() {
        // Default Constructor
    }

    public AwsS3FileSystemClientImpl(String defaultBucket) {
        this.defaultBucket = defaultBucket;
    }

    public AwsS3FileSystemClientImpl(String defaultBucket, String region) {
        this.region = region;
        this.defaultBucket = defaultBucket;
    }


    @Override
    public void init(final FileSystemConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("Invalid Initialization Properties");
        }
        AwsS3Config awsS3Config = (AwsS3Config) config;
        this.region = awsS3Config.getRegion();
        init(awsS3Config.getAccessKeyId(), awsS3Config.getAccessKeySecret());
    }

    @Override
    public void init(final String accessKey, final String accessSecret) {
        if (StringUtils.isAnyBlank(accessKey, accessSecret)) {
            this.s3Client = S3Client.builder().build();
        } else {
            if (StringUtils.isBlank(this.region)) {
                log.warn("Empty AWS S3 Service Region!");
            }
            this.s3Client = S3Client.builder()
                    .credentialsProvider(() -> new AwsCredentials() {
                        @Override
                        public String accessKeyId() {
                            return accessKey;
                        }
                        @Override
                        public String secretAccessKey() {
                            return accessSecret;
                        }
                    })
                    .region(Region.of(this.region))
                    .build();
        }
    }

    @Override
    public FileSystemType getFileSystemType() {
        return FileSystemType.S3;
    }

    @Override
    public Optional<ObjectMetaInfo> getObjectMetaInfo(String path) {
        return getObjectMetaInfo(this.defaultBucket, path);
    }

    @Override
    public Optional<ObjectMetaInfo> getObjectMetaInfo(String bucket, String path) {
        checkParameter(bucket, path);
        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(bucket)
                .key(path)
                .build();
//        try {
//
//
//        }
        return Optional.empty();
    }


    @Override
    public List<ObjectProperty> listPath(String path) throws IOException {
        return listPath(this.defaultBucket, path);
    }

    @Override
    public List<ObjectProperty> listPath(String bucket, String path) throws IOException {
        checkParameter(bucket, path);
        ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(bucket)
                .prefix(path)
                .build();
        try {
            ListObjectsV2Response response = s3Client.listObjectsV2(request);
            List<S3Object> s3Objects = response.contents();
            List<ObjectProperty> objects = s3Objects.stream()
                    .filter(o -> !o.key().equals(path))
                    .map(o -> o.key().endsWith("/") ? new ObjectProperty(o.key(), true)
                            : new ObjectProperty(o.key(), false))
                    .collect(Collectors.toList());
            log.debug("List Object(s) on Path [{}] in Bucket [{}] Success, Found [{}] Item(s)",
                    path, bucket, objects.size());
            return objects;

        } catch (Exception err) {
            if (err instanceof SdkServiceException) {
                throw new IOException(err.getMessage(), err.getCause());
            } else {
                log.error("List Object(s) on [{}] in Bucket [{}] Failed: {}", path, bucket, err.getMessage());
                throw err;
            }
        }
    }


    @Override
    public String mkdir(String path) throws IOException {
        return mkdir(this.defaultBucket, path);
    }

    @Override
    public String mkdir(String bucket, String path) throws IOException {
        checkParameter(bucket, path);
        String directory = path.endsWith("/") ? path : path + '/';
        PutObjectRequest request = PutObjectRequest.builder()
                .bucket(bucket)
                .key(directory)
                .tagging("Directory")
                .build();
        try {
            PutObjectResponse response = s3Client.putObject(request, RequestBody.empty());
            if (StringUtils.isNotBlank(response.eTag())) {
                return path;
            } else {
                return null;
            }
        } catch (Exception err) {
            if (err instanceof SdkServiceException) {
                throw new IOException(err.getMessage(), err.getCause());
            } else {
                log.error("Create Directory [{}] in Bucket [{}] Failed: {}", path, bucket, err.getMessage());
                throw err;
            }
        }
    }


    @Override
    public boolean exist(String path) throws IOException {
        return exist(this.defaultBucket, path);
    }

    @Override
    public boolean exist(String bucket, String path) throws IOException {
        checkParameter(bucket, path);
        ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(bucket)
                .prefix(path)
                .maxKeys(3)
                .build();
        try {
            ListObjectsV2Response response = s3Client.listObjectsV2(request);
            return response.contents().stream()
                    .anyMatch(object -> path.equals(object.key()));

        } catch (Exception err) {
            if (err instanceof SdkServiceException) {
                throw new IOException(err.getMessage(), err.getCause());
            } else {
                log.error("Check Object [{}] Existence in Bucket [{}] Failed: {}",
                        path, bucket, err.getMessage());
                throw err;
            }
        }
    }


    @Override
    public InputStream download(String source) throws IOException {
        return null;
    }

    @Override
    public Boolean download(String source, File localFile, FileOperation... localFileOperation) throws IOException {
        return null;
    }

    @Override
    public Boolean upload(String destination, InputStream in, FileOperation... destFileOperation) throws IOException {
        return null;
    }

    @Override
    public String upload(String destination, File localFile) throws IOException {
        return null;
    }

    @Override
    public Boolean delete(String path, FileOperation... deleteFileOperation) throws IOException {
        return delete(this.defaultBucket, path, deleteFileOperation);
    }

    @Override
    public Boolean delete(String bucket, String path, FileOperation... deleteFileOperation) throws IOException {
        checkParameter(bucket, path);
        DeleteObjectRequest request = DeleteObjectRequest.builder()
                .bucket(bucket)
                .key(path)
                .build();
        try {
            DeleteObjectResponse response = s3Client.deleteObject(request);
            log.info("Delete Object [{}] in Bucket [{}]: {}", path, bucket, response.deleteMarker());
            return true;

        } catch (Exception err) {
            if (err instanceof SdkServiceException) {
                throw new IOException(err.getMessage(), err.getCause());
            } else {
                log.error("Delete Object [{}] in Bucket [{}] Failed: {}", path, bucket, err.getMessage());
                throw err;
            }
        }
    }


    @Override
    public void close() {
        if (this.s3Client != null) {
            s3Client.close();
            log.debug("AWS S3 Client Has Been Closed");
        }
    }

    private void checkParameter(final String bucket, final String path) {
        if (StringUtils.isBlank(bucket)) {
            throw new IllegalArgumentException("Invalid Bucket Name");
        } else if (StringUtils.isBlank(path)) {
            throw new IllegalArgumentException("Invalid Remote Path");
        } else if (s3Client == null) {
            throw new IllegalStateException("Client Has Not Been Initialized");
        }
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getDefaultBucket() {
        return defaultBucket;
    }

    public void setDefaultBucket(String defaultBucket) {
        if (StringUtils.isBlank(defaultBucket)) {
            throw new IllegalArgumentException("Invalid Default Bucket Name");
        }
        this.defaultBucket = defaultBucket;
    }

    public boolean isExistenceCheck() {
        return existenceCheck;
    }

    public void setExistenceCheck(boolean existenceCheck) {
        this.existenceCheck = existenceCheck;
    }
}
