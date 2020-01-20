package gew.filesystem.client.hdfs;

import gew.filesystem.client.common.BasicFileSystemClient;
import gew.filesystem.client.common.FileSystemConfig;
import gew.filesystem.client.model.FileOperation;
import gew.filesystem.client.model.FileSystemType;
import gew.filesystem.client.model.ObjectMetaInfo;
import gew.filesystem.client.model.ObjectProperty;
import gew.filesystem.client.model.ObjectType;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.BufferedInputStream;
import java.net.URI;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Hadoop File System Client Implementation V1
 * @author Jason/GeW
 * @since 2019-03-24
 */
public class HadoopFileSystemClientImpl implements BasicFileSystemClient {

    private URI host;

    private FileSystem fileSystem;

    private boolean existenceCheck;


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
        HadoopFsClientConfig hadoopFsConfig = (HadoopFsClientConfig) config;
        Configuration configuration;
        if (hadoopFsConfig.isUseCustomConfig() && hadoopFsConfig.getConfiguration() != null) {
            configuration = hadoopFsConfig.getConfiguration();
        } else {
            configuration = new Configuration();
            if (hadoopFsConfig.getReplication() != null && hadoopFsConfig.getReplication() >= 1) {
                configuration.set("dfs.replication", String.valueOf(hadoopFsConfig.getReplication()));
            }
            if (StringUtils.isNotBlank(hadoopFsConfig.getFsDefaultName())) {
                configuration.set("fs.default.name", hadoopFsConfig.getFsDefaultName());
            }
        }
        if (this.host == null || StringUtils.isBlank(this.host.toString())) {
            if (hadoopFsConfig.getNameNodeUri() == null) {
                throw new IllegalArgumentException("Invalid Hadoop NameNode URI");
            } else {
                this.host = hadoopFsConfig.getNameNodeUri();
            }
        }
        try {
            if (StringUtils.isBlank(hadoopFsConfig.getUsername())) {
                this.fileSystem = FileSystem.get(this.host, configuration);
            } else {
                this.fileSystem = FileSystem.get(this.host, configuration, hadoopFsConfig.getUsername());
            }
        } catch (IOException ioe) {
            throw new IllegalStateException("Init HDFS Client Failed: " + ioe.getMessage(), ioe.getCause());

        } catch (InterruptedException ie) {
            throw new IllegalStateException("HDFS Client Initialization Got Interrupted", ie.getCause());
        }
    }

    @Override
    public FileSystemType getFileSystemType() {
        return FileSystemType.HDFS;
    }

    @Override
    public Optional<ObjectMetaInfo> getObjectMetaInfo(String path) {
        checkParameter(path);
        try {
            FileStatus fileStatus = fileSystem.getFileStatus(new Path(path));
            if (fileStatus == null) {
                return Optional.empty();
            }
            Map<String, byte[]> meta = fileSystem.getXAttrs(new Path(path));
            ObjectMetaInfo metaInfo = new ObjectMetaInfo();
            metaInfo.setSize(fileStatus.getLen());
            if (fileStatus.isDirectory()) {
                metaInfo.setObjectType(ObjectType.DIRECTORY);
            } else if (fileStatus.isFile()) {
                metaInfo.setObjectType(ObjectType.FILE);
            } else if (fileStatus.isSymlink()) {
                metaInfo.setObjectType(ObjectType.SYMBOLIC_LINK);
            }
            if (meta != null && !meta.isEmpty()) {
                metaInfo.getMetaData().putAll(meta);
                meta.forEach(((k, v) -> metaInfo.getMetaDataValueType().put(k, byte[].class)));
            }
            metaInfo.getMetaData().put("lastModifiedTime", fileStatus.getModificationTime());
            metaInfo.getMetaDataValueType().put("lastModifiedTime", Long.class);
            metaInfo.getMetaData().put("owner", fileStatus.getOwner());
            metaInfo.getMetaDataValueType().put("owner", String.class);
            metaInfo.getMetaData().put("group", fileStatus.getGroup());
            metaInfo.getMetaDataValueType().put("group", String.class);
            metaInfo.getMetaData().put("replication", fileStatus.getReplication());
            metaInfo.getMetaDataValueType().put("replication", Short.class);
            metaInfo.getMetaData().put("encrypted", fileStatus.isEncrypted());
            metaInfo.getMetaDataValueType().put("encrypted", Boolean.class);
            metaInfo.getMetaData().put("permission", fileStatus.getPermission().toString());
            metaInfo.getMetaDataValueType().put("permission", String.class);
            return Optional.of(metaInfo);

        } catch (IOException ioe) {
            if (ioe instanceof FileNotFoundException) {
                return Optional.empty();
            } else {
                log.error("Get Object [{}] Meta Data Failed: {}", path, ioe.getMessage());
                throw new RuntimeException(ioe.getMessage(), ioe.getCause());
            }
        }
    }

    @Override
    public List<ObjectProperty> listPath(String path) throws IOException {
        checkParameter(path);
        FileStatus[] fileStatus = fileSystem.listStatus(new Path(path));
        if (fileStatus == null || fileStatus.length == 0) {
            return new ArrayList<>(0);
        }
        try (Stream<FileStatus> fStream = Stream.of(fileStatus)) {
            List<ObjectProperty> objects = fStream
                    .map(f -> new ObjectProperty(f.getPath().getName(), f.isDirectory(), f.getLen()))
                    .collect(Collectors.toList());
            log.debug("List Path [{}] Found {} Items", path, objects.size());
            return objects;
        }
    }

    @Override
    public String mkdir(String path) throws IOException {
        checkParameter(path);
        boolean status = fileSystem.mkdirs(new Path(path));
        return status ? path : null;
    }

    @Override
    public boolean exist(String path) throws IOException {
        checkParameter(path);
        return fileSystem.exists(new Path(path));
    }

    @Override
    public InputStream download(String source) throws IOException {
        checkParameter(source);
        InputStream inputStream = fileSystem.open(new Path(source));
        if (inputStream != null) {
            log.debug("Prepare Object Downloading From HDFS [{}] Success", source);
        }
        return inputStream;
    }

    @Override
    public Boolean download(String source, File localFile, FileOperation... localFileOperation) throws IOException {
        checkParameter(source);
        if (localFile == null) {
            throw new IllegalArgumentException("Invalid Local File");
        }
        if (this.existenceCheck && !exist(source)) {
            log.debug("Download Object from HDFS [{}] Failed: File Does Not Exist", source);
            return false;
        }
        boolean append = localFileOperation != null && localFileOperation.length > 0
                && FileOperation.APPEND.equals(localFileOperation[0]);
        try (InputStream in = fileSystem.open(new Path(source));
             OutputStream out = FileUtils.openOutputStream(localFile, append)) {
            long bytes = IOUtils.copyLarge(in, out);
            log.debug("Download Object From HDFS [{}] to File [{}] Success, {} Bytes",
                    source, localFile.getName(), bytes);
        }
        return true;
    }

    @Override
    public Boolean upload(String destination, InputStream in, FileOperation... destFileOperation) throws IOException {
        checkParameter(destination);
        if (in == null) {
            throw new IllegalArgumentException("Invalid InputStream");
        }
        boolean append = destFileOperation != null && destFileOperation.length > 0
                && FileOperation.APPEND.equals(destFileOperation[0]);
        if (append) {
            if (this.existenceCheck && !exist(destination)) {
                log.debug("Upload Object to HDFS [{}] in Append Mode Failed: File Does Not Exist", destination);
                return false;
            }
            try (BufferedInputStream bin = new BufferedInputStream(in);
                 OutputStream out = fileSystem.append(new Path(destination))) {
                long bytes = IOUtils.copyLarge(bin, out);
                log.debug("Upload Object to HDFS [{}] Success, {} Bytes Saved", destination, bytes);
                return true;
            }
        } else {
            boolean override = destFileOperation != null && destFileOperation.length > 0
                    && FileOperation.OVERWRITE.equals(destFileOperation[0]);
            try (OutputStream out = fileSystem.create(new Path(destination), override)) {
                long bytes = IOUtils.copyLarge(in, out);
                log.debug("Upload Object to HDFS [{}] Success, {} Bytes Saved", destination, bytes);
                return true;
            }
        }
    }

    @Override
    public String upload(String destination, File localFile) throws IOException {
        checkParameter(destination);
        if (localFile == null) {
            throw new IllegalArgumentException("Invalid Local File");
        }
        fileSystem.copyFromLocalFile(false, new Path(localFile.getAbsolutePath()), new Path(destination));
        return destination;
    }

    @Override
    public Boolean delete(String path, FileOperation... deleteFileOperation) throws IOException {
        checkParameter(path);
        boolean recursive = deleteFileOperation != null && deleteFileOperation.length > 0
                && FileOperation.DELETE_RECURSIVE.equals(deleteFileOperation[0]);
        if (this.existenceCheck && !exist(path)) {
            return false;
        }
        return fileSystem.delete(new Path(path), recursive);
    }


    @Override
    public void close() {
        if (this.fileSystem != null) {
            try {
                this.fileSystem.close();
            } catch (IOException ioe) {
                log.warn("Close HDFS Client Failed: {}", ioe.getMessage());
            }
        }
    }


    private void checkParameter(final String path) {
        if (this.fileSystem == null) {
            throw new IllegalStateException("HDFS Client Has Not Been Initialized!");
        } else if (StringUtils.isBlank(path)) {
            throw new IllegalArgumentException("Invalid Path");
        }
    }

    public boolean isExistenceCheck() {
        return existenceCheck;
    }

    public void setExistenceCheck(boolean existenceCheck) {
        this.existenceCheck = existenceCheck;
    }
}
