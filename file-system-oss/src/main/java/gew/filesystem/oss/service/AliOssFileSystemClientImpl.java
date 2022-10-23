package gew.filesystem.oss.service;


import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import com.aliyun.oss.model.ObjectMetadata;
import com.aliyun.oss.model.PutObjectResult;
import com.aliyun.oss.model.VoidResult;
import gew.filesystem.common.config.FileSystemConfig;
import gew.filesystem.common.model.FileOperation;
import gew.filesystem.common.model.FileSystemType;
import gew.filesystem.common.model.MetaDataPair;
import gew.filesystem.common.model.ObjectMetaInfo;
import gew.filesystem.common.model.ObjectProperty;
import gew.filesystem.common.service.CloudFileSystemClient;
import gew.filesystem.oss.config.AliOssConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * AliYun OSS File System Client
 *
 * @author Jason/GeW
 * @since  2019-03-24
 */
@Slf4j
public class AliOssFileSystemClientImpl implements CloudFileSystemClient {

    /**
     * OSS Client
     */
    private OSS ossClient;

    /**
     * Default Bucket for Single Bucket System
     */
    private String defaultBucket;

    /**
     * When list object, the max number
     */
    private int maxListObjects = 1000;


    public AliOssFileSystemClientImpl() {
        // Default Constructor
    }

    public AliOssFileSystemClientImpl(String defaultBucket) {
        this.defaultBucket = defaultBucket;
    }

    @Override
    public void init(FileSystemConfig config) {
        if (!(config instanceof AliOssConfig)) {
            throw new IllegalArgumentException("Invalid Ali OSS Client Config");
        }
        AliOssConfig ossConfig = (AliOssConfig) config;
        if (StringUtils.isBlank(ossConfig.getEndpoint())
                && !ossConfig.getEndpoint().toLowerCase(Locale.ROOT).contains("http")) {
            throw new IllegalArgumentException("Invalid OSS EndPoint Config");
        }
        if (this.ossClient != null) {
            log.debug("OSS Client Has Been Initialized");
            return;
        }
        if (ossConfig.getCredentialsProvider() != null) {
            this.ossClient = new OSSClientBuilder().build(ossConfig.getEndpoint(), ossConfig.getCredentialsProvider());
        } else if (StringUtils.isNotEmpty(ossConfig.getSecurityToken())) {
            this.ossClient = new OSSClientBuilder().build(ossConfig.getEndpoint(), ossConfig.getAccessKeyId(),
                    ossConfig.getAccessKeySecret(), ossConfig.getSecurityToken());
        } else {
            this.ossClient = new OSSClientBuilder().build(ossConfig.getEndpoint(), ossConfig.getAccessKeyId(),
                    ossConfig.getAccessKeySecret());
        }

    }

    @Override
    public FileSystemType getFileSystemType() {
        return FileSystemType.OSS;
    }

    @Override
    public Optional<ObjectMetaInfo> getObjectMetaInfo(String path) {
        try {
            return getObjectMetaInfo(this.defaultBucket, path);
        } catch (IOException ioe) {
            return Optional.empty();
        }
    }

    @Override
    public Optional<ObjectMetaInfo> getObjectMetaInfo(String bucket, String path) throws IOException {
        if (StringUtils.isAnyBlank(bucket, path)) {
            return Optional.empty();
        }
        try {
            ObjectMetadata metadata = this.ossClient.getObjectMetadata(bucket, path);
            if (metadata == null) {
                return Optional.empty();
            }
            ObjectMetaInfo metaInfo = new ObjectMetaInfo();
            metaInfo.setSize(metadata.getContentLength());
            metaInfo.setMetaData(metaInfo.getMetaData());
            metaInfo.setUserData(metadata.getUserMetadata());
            metaInfo.setContentType(metadata.getContentType());
            metaInfo.setModifiedTime(metadata.getLastModified().toInstant());
            return Optional.of(metaInfo);

        } catch (OSSException | ClientException re) {
            log.error("Get Object MetaInfo for Bucket={}, Path={} Failed: {}", bucket, path, re.getMessage());
            throw new IOException("Get MetaInfo Failed", re.getCause());
        }
    }


    @Override
    public List<ObjectProperty> list(String path) throws IOException {
        return list(this.defaultBucket, path);
    }

    @Override
    public List<ObjectProperty> list(String bucket, String path) throws IOException {
        checkParameter(bucket, path);
        ListObjectsRequest request = new ListObjectsRequest(bucket);
        request.setPrefix(path);
        request.setMaxKeys(this.maxListObjects);
        request.setDelimiter("/");
        try {
            ObjectListing listing = this.ossClient.listObjects(request);
            List<OSSObjectSummary> summaries = listing.getObjectSummaries();
            if (summaries == null || summaries.isEmpty()) {
                return new ArrayList<>(0);
            }
            return summaries.stream()
                    .map(s -> new ObjectProperty(s.getKey(), s.getKey().endsWith("/"), s.getSize()))
                    .collect(Collectors.toList());

        } catch (OSSException | ClientException re) {
            log.error("List Objects for Bucket={}, Path={} Failed: {}", bucket, path, re.getMessage());
            throw new IOException(re.getMessage(), re.getCause());
        }
    }


    @Override
    public String mkdir(String path) throws IOException {
        return mkdir(this.defaultBucket, path);
    }

    @Override
    public String mkdir(String bucket, String path) throws IOException {
        try {
            this.ossClient.createDirectory(bucket, path);
            return path;

        } catch (OSSException | ClientException re) {
            throw new IOException("Create Directory Failed", re);
        }
    }


    @Override
    public boolean exist(String path) throws IOException {
        return exist(this.defaultBucket, path);
    }

    @Override
    public boolean exist(String bucket, String path) throws IOException {
        if (StringUtils.isAnyBlank(bucket, path)) {
            return false;
        }
        try {
            return this.ossClient.doesObjectExist(bucket, path);

        } catch (OSSException | ClientException re) {
            throw new IOException(re.getMessage(), re.getCause());
        }
    }

    @Override
    public InputStream download(String source) throws IOException {
        return download(this.defaultBucket, source);
    }

    @Override
    public Boolean download(String source, File localFile, FileOperation... localFileOperation) throws IOException {
        if (StringUtils.isBlank(source) || localFile == null) {
            return false;
        }
        try (InputStream in = download(this.defaultBucket, source);
             FileOutputStream fos = new FileOutputStream(localFile)) {
            IOUtils.copyLarge(in, fos);
            fos.flush();
            return true;

        } catch (Exception err) {
            log.error("Download Object Key={} From Bucket={} Failed: {}", source, this.defaultBucket, err.getMessage());
            throw new IOException(err.getMessage(), err.getCause());
        }
    }

    @Override
    public InputStream download(String bucket, String source) throws IOException {
        checkParameter(bucket, source);
        try {
            OSSObject object = this.ossClient.getObject(bucket, source);
            return object.getObjectContent();
        } catch (OSSException | ClientException re) {
            log.error("Download Object Key={} From Bucket={} Failed: {}", source, bucket, re.getMessage());
            throw new IOException(re.getMessage(), re.getCause());
        }
    }

    @Override
    public Boolean upload(String destination, InputStream in, FileOperation... destFileOperation) throws IOException {
        return null;
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
        try {
            PutObjectResult result;
            if (metaDataPairs.length > 0) {
                ObjectMetadata metadata = new ObjectMetadata();
                for (MetaDataPair pair : metaDataPairs) {
                    if (StringUtils.isAnyBlank(pair.getKey(), pair.getValue())) {
                        continue;
                    }
                    metadata.addUserMetadata(pair.getKey(), pair.getValue());
                }
                result = this.ossClient.putObject(bucket, destination, localFile, metadata);

            } else {
                result = this.ossClient.putObject(bucket, destination, localFile);
            }
            log.info("UploadFile={} to Bucket={}, Key={}, {}", localFile.getPath(), bucket,
                    destination, result.getResponse().isSuccessful() ? "Success" : "Failed");
            return destination;

        } catch (OSSException | ClientException re) {
            log.error("Upload File={} to Bucket={}, Key={} Failed: {}", localFile.getPath(), bucket, destination,
                    re.getMessage());
            throw new IOException(re.getMessage(), re.getCause());
        }
    }


    @Override
    public Boolean delete(String path, FileOperation... deleteFileOperation) throws IOException {
        return delete(this.defaultBucket, path, deleteFileOperation);
    }

    @Override
    public Boolean delete(String bucket, String path, FileOperation... fileOperations) throws IOException {
        if (StringUtils.isAnyBlank(bucket, path)) {
            return false;
        }
        try {
            VoidResult result = this.ossClient.deleteObject(bucket, path);
            return result.getResponse().isSuccessful();

        } catch (OSSException | ClientException re) {
            log.error("Delete Object Key={} From Bucket={} Failed: {}", path, bucket, re.getMessage());
            throw new IOException(re.getMessage(), re.getCause());
        }
    }


    @Override
    public void close() {
        if (this.ossClient != null) {
            this.ossClient.shutdown();
        }
    }

    private void checkParameter(final String bucket, final String path) {
        if (StringUtils.isBlank(bucket)) {
            throw new IllegalArgumentException("Invalid Bucket Name");
        } else if (StringUtils.isBlank(path)) {
            throw new IllegalArgumentException("Invalid Remote Path");
        } else if (this.ossClient == null) {
            throw new IllegalStateException("Client Has Not Been Initialized");
        }
    }

    public void setOssClient(OSSClient ossClient) {
        this.ossClient = ossClient;
    }

    public void setDefaultBucket(String defaultBucket) {
        this.defaultBucket = defaultBucket;
    }

    public void setMaxListObjects(int maxListObjects) {
        if (maxListObjects < 1 || maxListObjects > 1000) {
            throw new IllegalArgumentException("OSS Max List Object Keys [1, 1000]");
        }
        this.maxListObjects = maxListObjects;
    }

}
