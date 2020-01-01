package gew.filesystem.client.aws;

import gew.filesystem.client.common.CloudFileSystemClient;
import gew.filesystem.client.common.FileSystemConfig;
import gew.filesystem.client.model.FileOperation;
import gew.filesystem.client.model.FileSystemType;
import gew.filesystem.client.model.MetaDataPair;
import gew.filesystem.client.model.ObjectMetaInfo;
import gew.filesystem.client.model.ObjectProperty;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
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
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.InputStream;
import java.io.BufferedInputStream;
import java.nio.file.Files;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.LinkedHashMap;
import java.util.stream.Collectors;

/**
 * AWS S3 File System Client Implementation based on AWS SDK v2
 * @author Jason/GeW
 * @since 2019-03-24
 */
public class AwsS3FileSystemClientImpl implements CloudFileSystemClient {

    private String region;
    private String defaultBucket;

    private S3Client s3Client;

    private boolean existenceCheck;
    private boolean useTempFile;


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
        AwsS3ClientConfig awsS3ClientConfig = (AwsS3ClientConfig) config;
        if (StringUtils.isBlank(this.region)) {
            this.region = awsS3ClientConfig.getRegion();
        }
        init(awsS3ClientConfig.getAccessKeyId(), awsS3ClientConfig.getAccessKeySecret());
    }

    public void init(final String accessKey, final String accessSecret) {
        if (StringUtils.isAnyBlank(accessKey, accessSecret)) {
            this.s3Client = S3Client.builder().build();
            log.debug("Default AWS S3 Client Initialized");
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
                metaInfo.setSize(response.contentLength());
                metaInfo.getMetaData().put("lastModifiedTime", response.lastModified());
                metaInfo.getMetaDataValueType().put("lastModifiedTime", Instant.class);
                metaInfo.getMetaData().put("contentType", response.contentType());
                metaInfo.getMetaDataValueType().put("contentType", String.class);
                metaInfo.getMetaData().put("expiration", response.expiration());
                metaInfo.getMetaDataValueType().put("expiration", String.class);
                metaInfo.getMetaData().put("eTag", response.eTag());
                metaInfo.getMetaDataValueType().put("eTag", String.class);
                return Optional.of(metaInfo);

            } else {
                return Optional.empty();
            }
        } catch (Exception err) {
            throw new RuntimeException(err.getMessage(), err.getCause());
        }
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
                            : new ObjectProperty(o.key(), false, o.size()))
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
            log.debug("Download Object [{}] From Bucket [{}]: {}",
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
        checkParameter(this.defaultBucket, source);
        if (localFile == null) {
            throw new IllegalArgumentException("Invalid Local File");
        }
        boolean append = localFileOperation != null && localFileOperation.length > 0
                && FileOperation.APPEND.equals(localFileOperation[0]);
        try (BufferedInputStream bufferedInputStream = new BufferedInputStream(download(source));
             OutputStream outputStream = FileUtils.openOutputStream(localFile, append)) {
            long bytes = IOUtils.copyLarge(bufferedInputStream, outputStream);
            log.debug("Download Object [{}] From Bucket [{}] to File [{}], {} Bytes",
                    source, this.defaultBucket, localFile.getName(), bytes);
            return true;

        } catch (Exception err) {
            if (err instanceof SdkServiceException) {
                throw new IOException(err.getMessage(), err.getCause());
            } else {
                log.error("Download Object [{}] From Bucket [{}] to File [{}] Failed: {}",
                        source, this.defaultBucket, localFile.getName(), err.getMessage());
                throw err;
            }
        }
    }

    @Override
    public Boolean upload(String destination, InputStream in, FileOperation... destFileOperation) throws IOException {
        checkParameter(this.defaultBucket, destination);
        if (in == null) {
            throw new IllegalArgumentException("Invalid InputStream");
        }
        if (destFileOperation != null && destFileOperation.length > 0
                && FileOperation.APPEND.equals(destFileOperation[0])) {
            throw new IllegalArgumentException("Current AWS S3 Does Not Support Append Service");
        }
        if (useTempFile) {
            File tmp = createTempFile("s3-obj-" + System.currentTimeMillis(), in);
            String dest = upload(destination, tmp);
            return StringUtils.isNotBlank(dest);

        } else {
            PutObjectRequest request = PutObjectRequest.builder()
                    .bucket(this.defaultBucket)
                    .key(destination)
                    .build();
            try {
                long start = System.currentTimeMillis();
                log.debug("Start Uploading Local Object as InputStream to Bucket [{}] Path={}",
                        this.defaultBucket, destination);
                PutObjectResponse resp = s3Client.putObject(request, RequestBody.fromBytes(IOUtils.toByteArray(in)));
                long end = System.currentTimeMillis();
                log.debug("Finish Uploading Object to Bucket [{}] Path={}, Time Utilized: {}ms",
                        defaultBucket, destination, (end - start));
                return StringUtils.isNotBlank(resp.eTag());

            } catch (Exception err) {
                if (err instanceof SdkServiceException) {
                    throw new IOException(err.getMessage(), err.getCause());
                } else {
                    log.error("Upload InputStream to Bucket [{}] as [{}] Failed: {}",
                            this.defaultBucket, destination, err.getMessage());
                    throw err;
                }
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
        PutObjectRequest request;
        if (metaDataPairs != null && metaDataPairs.length > 0) {
            Map<String, String> metadata = new LinkedHashMap<>();
            for (MetaDataPair pair : metaDataPairs) {
                if (!StringUtils.isAnyBlank(pair.getKey(), pair.getValue())) {
                    metadata.put(pair.getKey(), pair.getValue());
                }
            }
            request = PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(destination)
                    .metadata(metadata)
                    .build();
        } else {
            request = PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(destination)
                    .build();
        }
        try {
            long start = System.currentTimeMillis();
            log.debug("Start Uploading Local File [Path={}, Size={}] to Bucket [{}] Path={}",
                    localFile.getName(), localFile.length(), bucket, destination);
            PutObjectResponse response = s3Client.putObject(request, RequestBody.fromFile(localFile));
            long end = System.currentTimeMillis();
            log.debug("Finish Uploading File [{}] to Bucket [{}] Path={}, Time Utilized: {}ms",
                    localFile.getName(), bucket, destination, (end - start));
            if (StringUtils.isNotBlank(response.eTag())) {
                return destination;
            } else {
                return null;
            }
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
            if (this.existenceCheck) {
                if (!exist(bucket, path)) {
                    log.info("Delete Failed: Object [{}] Does Not Exist in Bucket [{}}]", path, bucket);
                    return false;
                }
            }
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

    private File createTempFile(final String name, InputStream inputStream) throws IOException {
        File tmp = Files.createTempFile(name, ".tmp").toFile();
        FileUtils.copyToFile(new BufferedInputStream(inputStream), tmp);
        log.debug("Create Temporary File: " + tmp.getAbsolutePath());
        return tmp;
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

    public boolean isUseTempFile() {
        return useTempFile;
    }

    public void setUseTempFile(boolean useTempFile) {
        this.useTempFile = useTempFile;
    }
}
