package gew.filesystem.client.common;

import gew.filesystem.client.model.ObjectMetaInfo;
import gew.filesystem.client.model.ObjectProperty;

import java.io.IOException;
import java.util.List;

/**
 * @author Jason/GeW
 * @since 2019-03-24
 */
public interface CloudFileSystemClient extends BasicFileSystemClient {


    ObjectMetaInfo getObjectMetaInfo(final String bucket, final String path);

    List<ObjectProperty> listPath(final String bucket, final String path);

    boolean mkdir(final String bucket, final String path) throws IOException;

    boolean exist(final String bucket, final String path) throws IOException;
}
