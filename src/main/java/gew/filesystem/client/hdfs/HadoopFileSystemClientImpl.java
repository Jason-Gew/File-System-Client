package gew.filesystem.client.hdfs;

import gew.filesystem.client.common.BasicFileSystemClient;
import gew.filesystem.client.common.FileSystemConfig;
import gew.filesystem.client.model.FileOperation;
import gew.filesystem.client.model.FileSystemType;
import gew.filesystem.client.model.ObjectMetaInfo;
import gew.filesystem.client.model.ObjectProperty;
import org.apache.hadoop.fs.FileSystem;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;
import java.util.Optional;

/**
 * Hadoop File System Client Implementation V1
 * @author Jason/GeW
 * @since 2019-03-24
 */
public class HadoopFileSystemClientImpl implements BasicFileSystemClient {

    private URI host;

    private FileSystem fileSystem;


    private static final Logger log = LogManager.getLogger(HadoopFileSystemClientImpl.class);


    public HadoopFileSystemClientImpl() {
        // Default Constructor
    }

    public HadoopFileSystemClientImpl(URI host) {
        this.host = host;
    }

    @Override
    public void init(FileSystemConfig config) {
        if (!(config instanceof HadoopFsClientConfig)) {
            throw new IllegalArgumentException("Invalid Hadoop File System Client Config");
        }
    }

    @Override
    public FileSystemType getFileSystemType() {
        return FileSystemType.HDFS;
    }

    @Override
    public Optional<ObjectMetaInfo> getObjectMetaInfo(String path) {
        return Optional.empty();
    }

    @Override
    public List<ObjectProperty> listPath(String path) throws IOException {
        return null;
    }

    @Override
    public String mkdir(String path) throws IOException {
        return null;
    }

    @Override
    public boolean exist(String path) throws IOException {
        return false;
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
        return null;
    }

    @Override
    public void close() {

    }
}
