package gew.filesystem.client.common;

import gew.filesystem.client.model.FileSystemType;
import gew.filesystem.client.model.ObjectMetaInfo;
import gew.filesystem.client.model.ObjectProperty;

import java.io.IOException;
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




}
