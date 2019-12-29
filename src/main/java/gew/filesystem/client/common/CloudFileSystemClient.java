package gew.filesystem.client.common;

import gew.filesystem.client.model.FileOperation;
import gew.filesystem.client.model.ObjectMetaInfo;
import gew.filesystem.client.model.ObjectProperty;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

/**
 * Cloud File System Client for FS Services such as AWS S3, AliYun OSS and etc.
 * @author Jason/GeW
 * @since 2019-03-24
 */
public interface CloudFileSystemClient extends BasicFileSystemClient {

    void init(final FileSystemConfig config);

    void init(final String accessKey, final String accessSecret);

    Optional<ObjectMetaInfo> getObjectMetaInfo(final String bucket, final String path) throws IOException;

    List<ObjectProperty> listPath(final String bucket, final String path) throws IOException;

    String mkdir(final String bucket, final String path) throws IOException;

    boolean exist(final String bucket, final String path) throws IOException;


    Boolean delete(final String bucket, final String path, FileOperation... deleteFileOperation) throws IOException;

    void close();
}
