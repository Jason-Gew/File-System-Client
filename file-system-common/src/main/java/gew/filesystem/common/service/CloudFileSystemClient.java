package gew.filesystem.common.service;

import gew.filesystem.common.model.FileOperation;
import gew.filesystem.common.model.MetaDataPair;
import gew.filesystem.common.model.ObjectMetaInfo;
import gew.filesystem.common.model.ObjectProperty;
import gew.filesystem.common.config.FileSystemConfig;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;

/**
 * Cloud File System Client for FS Services such as AWS S3, Aliyun OSS etc.
 *
 * @author Jason/GeW
 * @since 2019-03-24
 */
public interface CloudFileSystemClient extends BasicFileSystemClient {

    /**
     * Cloud File System Client Initialization
     *
     * @param config        Corresponding File System Client Config
     * @throws RuntimeException
     */
    void init(final FileSystemConfig config) throws RuntimeException;

    /**
     * Get object meta info from specific bucket
     *
     * @param bucket        Bucket
     * @param path          Object Key
     * @return              ObjectMetaInfo if exists
     * @throws IOException  Exception from client
     */
    Optional<ObjectMetaInfo> getObjectMetaInfo(final String bucket, final String path) throws IOException;

    /**
     * List path on specific bucket
     *
     * @param bucket        Bucket
     * @param path          Object Key
     * @return              List of objects
     * @throws IOException  Exception from client
     */
    List<ObjectProperty> list(final String bucket, final String path) throws IOException;

    /**
     * Make a new directory in specific bucket
     *
     * @param bucket        Bucket
     * @param path          Object Key
     * @return              Complete path
     * @throws IOException  Exception from client
     */
    String mkdir(final String bucket, final String path) throws IOException;

    /**
     * Check object or path existence in specific bucket
     *
     * @param bucket        Bucket
     * @param path          Object Key
     * @return              success in true / false
     * @throws IOException  Exception from client
     */
    boolean exist(final String bucket, final String path) throws IOException;

    /**
     * Download object from specific bucket
     *
     * @param bucket        Bucket
     * @param source        Object Key
     * @return              InputStream
     * @throws IOException  Exception from client
     */
    InputStream download(final String bucket, final String source) throws IOException;

    /**
     * Upload object to specific bucket
     *
     * @param bucket        Bucket
     * @param destination   Destination Path
     * @param localFile     File Object
     * @param metaDataPairs Meta Info
     * @return              Complete Path
     * @throws IOException  Exception from client
     */
    String upload(final String bucket, final String destination, File localFile,
                  MetaDataPair... metaDataPairs) throws IOException;

    /**
     * Delete object from specific bucket
     *
     * @param bucket        Bucket
     * @param path          Destination Path
     * @param fileOperations recursive or not
     * @return              success in true / false
     * @throws IOException  Exception from client
     */
    Boolean delete(final String bucket, final String path, FileOperation... fileOperations) throws IOException;

    /**
     * Close / Destroy Cloud File System Client
     */
    void close();

}
