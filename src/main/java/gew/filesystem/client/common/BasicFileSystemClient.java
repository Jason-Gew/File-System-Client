package gew.filesystem.client.common;

import gew.filesystem.client.model.FileOperation;
import gew.filesystem.client.model.FileSystemType;
import gew.filesystem.client.model.ObjectMetaInfo;
import gew.filesystem.client.model.ObjectProperty;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;

/**
 * Basic File System Client, Support Common File Operations.
 * @author Jason/GeW
 * @since 2019-03-24
 */
public interface BasicFileSystemClient {

    FileSystemType getFileSystemType();

    Optional<ObjectMetaInfo> getObjectMetaInfo(final String path);

    List<ObjectProperty> listPath(final String path) throws IOException;

    String mkdir(final String path) throws IOException;

    boolean exist(final String path) throws IOException;

    InputStream download(final String source) throws IOException;

    Boolean download(final String source, File localFile, FileOperation... localFileOperation) throws IOException;

    Boolean upload(final String destination, InputStream in, FileOperation... destFileOperation) throws IOException;

    String upload(final String destination, File localFile) throws IOException;

    Boolean delete(final String path, FileOperation... deleteFileOperation) throws IOException;

}
