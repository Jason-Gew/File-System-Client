package gew.filesystem.common.local;


import gew.filesystem.common.model.FileOperation;
import gew.filesystem.common.model.FileSystemType;
import gew.filesystem.common.model.ObjectMetaInfo;
import gew.filesystem.common.model.ObjectProperty;
import gew.filesystem.common.model.ObjectType;
import gew.filesystem.common.service.BasicFileSystemClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Objects;
import java.util.Optional;


public class LocalFileSystemClientImplTest {

    private BasicFileSystemClient fileSystemClient;

    @BeforeEach
    void setUp() {
        if (fileSystemClient == null) {
            fileSystemClient = new LocalFileSystemClientImpl();
        }
    }

    @Test
    void getFileSystemType() {
        Assertions.assertEquals(FileSystemType.LOCAL, fileSystemClient.getFileSystemType());
        Assertions.assertEquals(10, ((LocalFileSystemClientImpl) fileSystemClient).getMaxDepth());
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ((LocalFileSystemClientImpl) fileSystemClient).setMaxDepth(0));
    }

    @Test
    void existsTest() {
        try {
            Boolean exist = fileSystemClient.exist("Test.txt");
            Assertions.assertNotNull(exist);

        } catch (IOException ioe) {
            Assertions.assertNotNull(ioe);
        }
    }

    @Test
    void mkdirTest() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> fileSystemClient.mkdir("  "));
    }

    @Test
    void getObjectMetaInfoTest() {

        Optional<ObjectMetaInfo> metaInfo = fileSystemClient.getObjectMetaInfo("test.txt");
        Assertions.assertFalse(metaInfo.isPresent());

        String path = getPropertiesFile();
        Optional<ObjectMetaInfo> objectMetaInfo = fileSystemClient.getObjectMetaInfo(path);

        Assertions.assertTrue(objectMetaInfo.isPresent());
        Assertions.assertNotNull(objectMetaInfo.get().getSize());
        Assertions.assertEquals(ObjectType.FILE, objectMetaInfo.get().getObjectType());
    }

    @Test
    void listTest() {
        try {
            List<ObjectProperty> invalid = fileSystemClient.list("  ");
            Assertions.assertTrue(invalid.isEmpty());

            File dummy = new File("");
            List<ObjectProperty> properties = fileSystemClient.list(dummy.getCanonicalPath());
            Assertions.assertFalse(properties.isEmpty());

        } catch (IOException ioe)  {
            Assertions.assertNotNull(ioe);
        }
    }

    @Test
    void downloadTest() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> fileSystemClient.download("  "));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> fileSystemClient.download("test.txt", null));
    }


    @Test
    void deleteTest() {
        String path = "test.txt";
        try {
            Assertions.assertFalse(fileSystemClient.delete("   "));
            Boolean status = fileSystemClient.delete(path, FileOperation.DELETE_RECURSIVE);
            Assertions.assertFalse(status);

        } catch (IOException ioe) {
            Assertions.assertNotNull(ioe);
        }
    }

    @Test
    void tmpTest() {
        String tmpdir = System.getProperty("java.io.tmpdir");
        String testFile = "test-file-system-client.txt";
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> fileSystemClient.upload(testFile, null));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> fileSystemClient.upload(testFile, null, FileOperation.APPEND));
        if (tmpdir != null && !tmpdir.isEmpty()) {
            String completePath = tmpdir + File.separator + testFile;
            try {
                if (fileSystemClient.exist(completePath)) {
                    boolean deleted = fileSystemClient.delete(completePath);
                    Assertions.assertTrue(deleted);
                }

                String dest = fileSystemClient.upload(completePath, new File(getPropertiesFile()));
                Assertions.assertTrue(dest != null && !dest.isEmpty());

                fileSystemClient.delete(completePath);

            } catch (IOException ioe) {
                Assertions.assertNotNull(ioe);
            }
        }
    }

    private String getPropertiesFile() {
        URL url = Thread.currentThread().getContextClassLoader().getResource("pom.properties");
        String path = Objects.requireNonNull(url, "Invalid Url").getPath();
        if (path != null && path.startsWith("/")) {
            path = path.substring(1);
        }
        return path;
    }
}
