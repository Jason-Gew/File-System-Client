package gew.filesystem.client.local;

import gew.filesystem.client.common.BasicFileSystemClient;
import gew.filesystem.client.model.FileOperation;
import gew.filesystem.client.model.FileSystemType;
import gew.filesystem.client.model.ObjectMetaInfo;
import gew.filesystem.client.model.ObjectProperty;
import gew.filesystem.client.model.ObjectType;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.FileVisitResult;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.util.List;
import java.util.ArrayList;
import java.util.Optional;
import java.util.stream.Stream;
import java.util.stream.Collectors;

/**
 * Local File System Client Based on Java 8 NIO and Apache Commons-io
 * @author Jason/GeW
 * @since 2019-03-24
 */
public class LocalFileSystemClientImpl implements BasicFileSystemClient {

    private int maxDepth = 10;

    private static final Logger log = LogManager.getLogger(LocalFileSystemClientImpl.class);


    public LocalFileSystemClientImpl() {
        // Default Constructor
    }


    @Override
    public FileSystemType getFileSystemType() {
        return FileSystemType.LOCAL;
    }


    @Override
    public Optional<ObjectMetaInfo> getObjectMetaInfo(String path) {
        if (!exist(path)) {
            return Optional.empty();
        }
        try {
            ObjectMetaInfo objectMetaInfo = new ObjectMetaInfo();
            BasicFileAttributes attributes = Files.readAttributes(Paths.get(path), BasicFileAttributes.class);
            if (attributes != null) {
                objectMetaInfo.setCreationTime(attributes.creationTime().toInstant());
                objectMetaInfo.getMetaData().put("lastModifiedTime", attributes.lastModifiedTime().toInstant());
                objectMetaInfo.getMetaDataValueType().put("lastModifiedTime", Instant.class);
                objectMetaInfo.setSize(attributes.size());
                if (attributes.isDirectory()) {
                    objectMetaInfo.setObjectType(ObjectType.DIRECTORY);
                } else if (attributes.isRegularFile()) {
                    objectMetaInfo.setObjectType(ObjectType.FILE);
                } else if (attributes.isSymbolicLink()) {
                    objectMetaInfo.setObjectType(ObjectType.SYMBOLIC_LINK);
                } else {
                    objectMetaInfo.setObjectType(ObjectType.OTHER);
                }
                return Optional.of(objectMetaInfo);
            }
        } catch (IOException ioe) {
            log.warn("GetObjectMetaInfo for [{}] Failed: {}", path, ioe.getMessage());
        }
        return Optional.empty();
    }


    @Override
    public List<ObjectProperty> listPath(String path) throws IOException {
        if (StringUtils.isBlank(path)) {
            return new ArrayList<>(0);
        }
        Path objectPath = Paths.get(path);
        try (Stream<Path> paths = Files.walk(objectPath, this.maxDepth)) {
            List<ObjectProperty> properties = paths
                    .filter(p -> !p.equals(objectPath))
                    .map(p -> Files.isDirectory(p) ? new ObjectProperty(p.toString(), true)
                            : new ObjectProperty(p.toString(), false, p.toFile().length()))
                    .collect(Collectors.toList());
            log.debug("List Path [{}] Found {} Items with Max Depth [{}]",
                    path, properties.size(), this.maxDepth);
            return properties;
        }
    }


    @Override
    public String mkdir(String path) throws IOException {
        if (StringUtils.isBlank(path)) {
            throw new IllegalArgumentException("Invalid Directory Path");
        }
        String destination = Files.createDirectory(Paths.get(path)).toString();
        log.debug("Create Directory on Path [{}]: {}", path, destination == null ? "Success" : "Failed");
        return destination;
    }


    @Override
    public boolean exist(String path) {
        if (StringUtils.isBlank(path)) {
            return false;
        }
        return Files.exists(Paths.get(path));
    }


    @Override
    public InputStream download(String source) throws IOException {
        if (StringUtils.isBlank(source)) {
            throw new IllegalArgumentException("Invalid Source Path");
        }
        InputStream inputStream = Files.newInputStream(Paths.get(source), StandardOpenOption.READ);
        log.debug("Open InputStream for Downloading File on Path [{}] Success", source);
        return inputStream;
    }

    @Override
    public Boolean download(String source, File localFile, FileOperation... localFileOperation) throws IOException {
        if (StringUtils.isBlank(source)) {
            throw new IllegalArgumentException("Invalid Source Path");
        } else if (localFile == null) {
            throw new IllegalArgumentException("Invalid Local File");
        }
        boolean status;
        boolean append = localFileOperation != null && localFileOperation.length > 0
                && FileOperation.APPEND.equals(localFileOperation[0]);
        try (OutputStream outputStream = FileUtils.openOutputStream(localFile, append)) {
            long bytes = Files.copy(Paths.get(source), outputStream);
            log.debug("Download [{}] to Local File [{}] Success, {} Bytes Saved", source, localFile, bytes);
            status = true;

        } catch (Exception err) {
            if (err instanceof IOException) {
                throw err;
            } else {
                log.warn("Download [{}] to Local File [{}] Failed: {}", source, localFile.getName(),
                        err.getMessage());
                status = false;
            }
        }
        return status;
    }


    @Override
    public Boolean upload(String destination, InputStream in, FileOperation... destFileOperation) throws IOException {
        if (StringUtils.isBlank(destination)) {
            throw new IllegalArgumentException("Invalid Destination Path");
        } else if (in == null) {
            throw new IllegalArgumentException("Invalid InputStream");
        }
        boolean append = destFileOperation != null && destFileOperation.length > 0
                && FileOperation.APPEND.equals(destFileOperation[0]);
        try (OutputStream outputStream = FileUtils.openOutputStream(new File(destination), append)) {
            long bytes = IOUtils.copyLarge(in, outputStream);
            log.debug("Upload InputStream to Local File [{}] Success, {} Bytes Saved", destination, bytes);
            return true;
        } catch (Exception err) {
            if (err instanceof IOException) {
                throw err;
            } else {
                log.warn("Upload InputStream to Local File [{}] Failed: {}", destination, err.getMessage());
                return false;
            }
        }
    }

    @Override
    public String upload(String destination, File localFile) throws IOException {
        if (StringUtils.isBlank(destination)) {
            throw new IllegalArgumentException("Invalid Destination Path");
        } else if (localFile == null) {
            throw new IllegalArgumentException("Invalid Local File");
        }
        Path path = Files.copy(localFile.toPath(), Paths.get(destination));
        log.debug("Upload Local File to [{}]: Success", localFile.getName());
        return path.toString();
    }


    @Override
    public Boolean delete(String path, FileOperation... deleteFileOperation) throws IOException {
        if (StringUtils.isBlank(path)) {
            return false;
        }
        boolean recursive = deleteFileOperation != null && deleteFileOperation.length > 0
                && FileOperation.DELETE_RECURSIVE.equals(deleteFileOperation[0]);
        Path localPath = Paths.get(path);
        if (recursive && Files.isDirectory(localPath)) {
            Files.walkFileTree(localPath, new SimpleFileVisitor<Path>() {

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    Files.delete(dir);
                    return FileVisitResult.CONTINUE;
                }
            });
            return true;
        } else {
            return Files.deleteIfExists(Paths.get(path));
        }
    }


    public int getMaxDepth() {
        return maxDepth;
    }

    public void setMaxDepth(int maxDepth) {
        if (maxDepth < 1) {
            throw new IllegalArgumentException("Invalid Max Depth Number");
        }
        this.maxDepth = maxDepth;
    }
}
