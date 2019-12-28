package gew.filesystem.client.local;

import gew.filesystem.client.common.BasicFileSystemClient;
import gew.filesystem.client.model.FileSystemType;
import gew.filesystem.client.model.ObjectMetaInfo;
import gew.filesystem.client.model.ObjectProperty;
import gew.filesystem.client.model.ObjectType;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * @author Jason/GeW
 * @since 2019-03-24
 */
public class LocalFileSystemClientImpl implements BasicFileSystemClient {


    private static final Logger log = LogManager.getLogger(LocalFileSystemClientImpl.class);

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
        try (Stream<Path> paths = Files.walk(objectPath, 20)) {
            List<ObjectProperty> properties = paths
                    .filter(p -> !p.equals(objectPath))
                    .map(p -> Files.isDirectory(p) ? new ObjectProperty(p.toString(), true)
                            : new ObjectProperty(p.toString(), false))
                    .collect(Collectors.toList());
            log.debug("List Path Found {} Items", properties.size());
            return properties;
        }
    }


    @Override
    public String mkdir(String path) throws IOException {
        if (StringUtils.isBlank(path)) {
            throw new IllegalArgumentException("Invalid Directory Path");
        }
        return Files.createDirectory(Paths.get(path)).toString();
    }


    @Override
    public boolean exist(String path) {
        if (StringUtils.isBlank(path)) {
            return false;
        }
        return Files.exists(Paths.get(path));
    }


}
