package gew.filesystem.s3.service;

import gew.filesystem.common.config.FileSystemConfig;
import gew.filesystem.common.model.FileOperation;
import gew.filesystem.common.model.FileSystemType;
import gew.filesystem.common.model.MetaDataPair;
import gew.filesystem.common.model.ObjectMetaInfo;
import gew.filesystem.common.model.ObjectProperty;
import gew.filesystem.common.service.CloudFileSystemClient;
import gew.filesystem.s3.config.AwsS3ClientConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.BufferedInputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * AWS S3 File System Client Implementation based on AWS SDK v2
 * @author Jason/GeW
 * @since 2019-03-24
 */
@Slf4j
public class AwsS3FileSystemClientImpl implements CloudFileSystemClient {

    /**
     * AWS S3 Specific Service Region
     */
    private String region;

    /**
     * Default Bucket for Single Bucket System
     */
    private String defaultBucket;

    /**
     * AWS S3 Client
     */
    private S3Client s3Client;

    /**
     * When list object, the max number | Default Max = 1000
     */
    private int maxListObjects = 1000;

    private boolean useTempFile;



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
        if (!(config instanceof AwsS3ClientConfig)) {
            throw new IllegalArgumentException("Invalid AWS S3 Client Config");
        }
        if (this.s3Client != null) {
            log.info("AWS S3 Client Has Been Initialized");
            return;
        }
        AwsS3ClientConfig awsS3ClientConfig = (AwsS3ClientConfig) config;
        if (StringUtils.isBlank(this.region)) {
            this.region = awsS3ClientConfig.getRegion();
        }
        init(awsS3ClientConfig.getAccessKeyId(), awsS3ClientConfig.getAccessKeySecret(),
                awsS3ClientConfig.getCredentialsProvider());
    }

    public void init(final String accessKey, final String accessSecret, AwsCredentialsProvider credentialsProvider) {
        if (credentialsProvider != null) {
            this.s3Client = S3Client.builder().credentialsProvider(credentialsProvider)
                    .region(Region.of(this.region))
                    .build();
        } else if (StringUtils.isAnyBlank(accessKey, accessSecret)) {
            this.s3Client = S3Client.builder().build();
            log.debug("Default AWS S3 Client Initialized");
        } else {
            if (StringUtils.isBlank(this.region)) {
                throw new IllegalArgumentException("Empty AWS S3 Service Region!");
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
            log.debug("AWS S3 Client Initialized");
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
        HeadObjectRequest request = HeadObjectRequest.builder()
                .bucket(bucket)
                .key(path)
                .build();
        try {
            HeadObjectResponse response = s3Client.headObject(request);
            if (response != null) {
                ObjectMetaInfo metaInfo = new ObjectMetaInfo(response.metadata());
                metaInfo.setLastModified(response.lastModified());
                metaInfo.setContentType(response.contentType());
                metaInfo.setSize(response.contentLength());

                metaInfo.getMetaData().put("versionId", response.versionId());
                metaInfo.getMetaDataValueType().put("versionId", String.class);
                metaInfo.getMetaData().put("expires", response.expires());
                metaInfo.getMetaDataValueType().put("expires", Instant.class);
                metaInfo.getMetaData().put("expiration", response.expiration());
                metaInfo.getMetaDataValueType().put("expiration", String.class);
                metaInfo.getMetaData().put("eTag", response.eTag());
                metaInfo.getMetaDataValueType().put("eTag", String.class);

                return Optional.of(metaInfo);

            } else {
                return Optional.empty();
            }
        } catch (Exception err) {
            if (err instanceof NoSuchKeyException) {
                return Optional.empty();
            } else {
                throw new RuntimeException(err.getMessage(), err.getCause());
            }
        }
    }


    @Override
    public List<ObjectProperty> list(String path) throws IOException {
        return list(this.defaultBucket, path);
    }

    @Override
    public List<ObjectProperty> list(String bucket, String path) throws IOException {
        checkParameter(bucket, path);
        ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(bucket)
                .prefix(path)
                .maxKeys(this.maxListObjects)
                .build();
        try {
            ListObjectsV2Response response = s3Client.listObjectsV2(request);
            List<S3Object> s3Objects = response.contents();
            List<ObjectProperty> objects = s3Objects.stream()
                    .filter(o -> !o.key().equals(path))
                    .map(o -> o.key().endsWith("/") ? new ObjectProperty(o.key(), true)
                            : new ObjectProperty(o.key(), false, o.size()))
                    .collect(Collectors.toList());
            log.debug("List Object(s) on Path [{}] in Bucket [{}] Success, Found [{}] Item(s)",
                    path, bucket, objects.size());
            return objects;

        } catch (Exception err) {
            if (err instanceof NoSuchKeyException) {
                return new ArrayList<>(0);
            } else if (err instanceof SdkServiceException) {
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
        HeadObjectRequest request = HeadObjectRequest.builder()
                .bucket(bucket)
                .key(path)
                .build();
        try {
            HeadObjectResponse response = s3Client.headObject(request);
            return response != null;

        } catch (Exception err) {
            if (err instanceof NoSuchKeyException) {
                return false;
            } else if (err instanceof SdkServiceException) {
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
        return download(this.defaultBucket, source);
    }

    @Override
    public InputStream download(String bucket, String source) throws IOException {
        checkParameter(bucket, source);
        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(bucket)
                .key(source)
                .build();
        try {
            InputStream response = s3Client.getObject(request, ResponseTransformer.toInputStream());
            log.debug("Prepare Downloading Object [{}] From Bucket [{}]: {}",
                    source, bucket, response == null ? "Failed" : "Success");
            return response;

        } catch (Exception err) {
            if (err instanceof SdkServiceException) {
                throw new IOException(err.getMessage(), err.getCause());
            } else {
                log.error("Download Object [{}] From Bucket [{}] Failed: {}",
                        source, bucket, err.getMessage());
                throw err;
            }
        }
    }

    @Override
    public Boolean download(String source, File localFile, FileOperation... localFileOperation) throws IOException {
        return download(this.defaultBucket, source, localFile, localFileOperation);
    }

    @Override
    public Boolean download(String bucket, String source, File localFile, FileOperation... operations) throws IOException {
        checkParameter(this.defaultBucket, source);
        if (localFile == null) {
            throw new IllegalArgumentException("Invalid Local File");
        }
        boolean append = operations != null && operations.length > 0
                && FileOperation.APPEND.equals(operations[0]);
        try (BufferedInputStream bufferedInputStream = new BufferedInputStream(download(source));
             FileOutputStream fos = FileUtils.openOutputStream(localFile, append)) {
            long bytes = IOUtils.copyLarge(bufferedInputStream, fos);
            log.debug("Download Object [{}] From Bucket [{}] to File [{}], {} Bytes", source, bucket, localFile.getName(), bytes);
            fos.flush();
            return true;

        } catch (Exception err) {
            if (err instanceof SdkServiceException) {
                throw new IOException(err.getMessage(), err.getCause());
            } else {
                log.error("Download Object [{}] From Bucket [{}] to File [{}] Failed: {}", source, bucket,
                        localFile.getName(), err.getMessage());
                throw err;
            }
        }
    }

    @Override
    public Boolean upload(String destination, InputStream in, FileOperation... destFileOperation) throws IOException {
        return upload(this.defaultBucket, destination, in);
    }

    @Override
    public Boolean upload(String bucket, String destination, InputStream in, MetaDataPair... metaDataPairs) throws IOException {
        checkParameter(this.defaultBucket, destination);
        if (in == null) {
            throw new IllegalArgumentException("Invalid InputStream");
        }
        PutObjectRequest request = genPutObjectRequest(bucket, destination, metaDataPairs);
        try {
            long start = System.currentTimeMillis();
            log.debug("Start Uploading Streaming File to Bucket [{}] Path={}", bucket, destination);
            PutObjectResponse response = s3Client.putObject(request, RequestBody.fromByteBuffer(
                    ByteBuffer.wrap(IOUtils.toByteArray(in))));
            long end = System.currentTimeMillis();
            log.debug("Finish Uploading Streaming File to Bucket [{}] Path={}, Time Utilized: {}ms", bucket, destination,
                    (end - start));
            return StringUtils.isNotEmpty(response.eTag());

        } catch (Exception err) {
            if (err instanceof SdkServiceException) {
                throw new IOException(err.getMessage(), err.getCause());
            } else {
                log.error("Upload Streaming File to Bucket [{}] Path={} Failed: {}", bucket, destination, err.getMessage());
                throw err;
            }
        }
    }

    @Override
    public String upload(String destination, File localFile) throws IOException {
        return upload(this.defaultBucket, destination, localFile);
    }

    @Override
    public String upload(String bucket, String destination, File localFile, MetaDataPair... metaDataPairs) throws IOException {
        checkParameter(bucket, destination);
        if (localFile == null) {
            throw new IllegalArgumentException("Invalid Local File");
        }
        PutObjectRequest request = genPutObjectRequest(bucket, destination, metaDataPairs);
        try {
            long start = System.currentTimeMillis();
            log.debug("Start Uploading Local File [Path={}, Size={}] to Bucket [{}] Path={}",
                    localFile.getName(), localFile.length(), bucket, destination);
            PutObjectResponse response = s3Client.putObject(request, RequestBody.fromFile(localFile));
            long end = System.currentTimeMillis();
            log.debug("Finish Uploading File [{}] to Bucket [{}] Path={}, Time Utilized: {}ms",
                    localFile.getName(), bucket, destination, (end - start));
            return StringUtils.isEmpty(response.eTag()) ? null : destination;

        } catch (Exception err) {
            if (err instanceof SdkServiceException) {
                throw new IOException(err.getMessage(), err.getCause());
            } else {
                log.error("Upload File [{}] to Bucket [{}] as [{}] Failed: {}",
                        localFile.getName(), bucket, destination, err.getMessage());
                throw err;
            }
        }
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
        } else if (this.s3Client == null) {
            throw new IllegalStateException("Client Has Not Been Initialized");
        }
    }

    private PutObjectRequest genPutObjectRequest(String bucket, String key, MetaDataPair... pairs) {
        if (pairs != null && pairs.length > 0) {
            Map<String, String> metadata = new HashMap<>();
            Arrays.stream(pairs)
                    .filter(m -> !StringUtils.isAnyEmpty(m.getKey(), m.getValue()))
                    .forEach(m -> metadata.put(m.getKey(), m.getValue()));
            return PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .metadata(metadata)
                    .build();
        } else {
            return PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build();
        }
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public void setDefaultBucket(String defaultBucket) {
        if (StringUtils.isBlank(defaultBucket)) {
            throw new IllegalArgumentException("Invalid Default Bucket Name");
        }
        this.defaultBucket = defaultBucket;
    }


    public void setMaxListObjects(int maxListObjects) {
        if (maxListObjects < 1 || maxListObjects > 1000) {
            throw new IllegalArgumentException("OSS Max List Object Keys [1, 1000]");
        }
        this.maxListObjects = maxListObjects;
    }

    public void setS3Client(S3Client s3Client) {
        this.s3Client = s3Client;
    }

}
