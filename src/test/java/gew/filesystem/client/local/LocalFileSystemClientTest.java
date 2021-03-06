package gew.filesystem.client.local;

import gew.filesystem.client.common.BasicFileSystemClient;
import gew.filesystem.client.model.FileOperation;
import gew.filesystem.client.model.FileSystemType;
import gew.filesystem.client.model.ObjectMetaInfo;
import gew.filesystem.client.model.ObjectProperty;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;

/**
 * @author Jason/GeW
 */
public class LocalFileSystemClientTest {

    private BasicFileSystemClient fileSystemClient;


    @Before
    public void setUp() {
        if (fileSystemClient == null) {
            fileSystemClient = new LocalFileSystemClientImpl();
        }
    }

    @Test
    public void getFileSystemType() {
        Assert.assertEquals(FileSystemType.LOCAL, fileSystemClient.getFileSystemType());
    }

    @Test
    public void existsTest() {
        try {
            Boolean exist = fileSystemClient.exist("files/Test.txt");
            System.out.println(exist);
            Assert.assertNotNull(exist);
        } catch (IOException ioe) {
//            ioe.printStackTrace();
            Assert.assertNotNull(ioe);
        }
    }

    @Test
    @Ignore("Ignore tests that will bring real effects")
    public void mkdirTest() {
        try {
            String path = fileSystemClient.mkdir("files/test");
            System.out.println("mkdir: " + path);
        } catch (IOException ioe) {
//            ioe.printStackTrace();
            Assert.assertNotNull(ioe);
        }
    }

    @Test
    public void getObjectMetaInfoTest() {
        String path1 = "files/test";
        Optional<ObjectMetaInfo> objectMetaInfo1 = fileSystemClient.getObjectMetaInfo(path1);
        Assert.assertNotNull(objectMetaInfo1);
        objectMetaInfo1.ifPresent(info -> System.out.printf("MetaInfo Test for Path [%s]: %s%n",
                path1, info));
        String path2 = "files/Test.txt";
        Optional<ObjectMetaInfo> objectMetaInfo2 = fileSystemClient.getObjectMetaInfo(path2);
        Assert.assertNotNull(objectMetaInfo2);
        objectMetaInfo2.ifPresent(info -> System.out.printf("MetaInfo Test for Path [%s]: %s%n",
                path2, info));
    }

    @Test
    public void listTest() {
        String path = "files/";
        try {
            List<ObjectProperty> properties = fileSystemClient.listPath(path);
            properties.forEach(System.out::println);
        } catch (IOException ioe)  {
            Assert.assertNotNull(ioe);
//            ioe.printStackTrace();
        }
    }

    @Test
    @Ignore("Ignore tests that will bring real effects")
    public void downloadTest() {
        String source = "files/test/sub-test.csv";
        String destination = "files/Test.txt";
        try {
            Boolean status = fileSystemClient.download(source, new File(destination), FileOperation.APPEND);
            System.out.println("Download status: " + status);
        } catch (IllegalArgumentException e) {
            Assert.assertFalse(e.getMessage().isEmpty());
        } catch (IOException ioe) {
            Assert.assertNotNull(ioe);
//            ioe.printStackTrace();
        }
    }


    @Test
    @Ignore("Ignore tests that will bring real effects")
    public void deleteTest() {
        String path = "files/random-file.txt";
        try {
            Boolean status = fileSystemClient.delete(path, FileOperation.DELETE_RECURSIVE);
            System.out.printf("Delete Path [%s]: %s%n", path, (status ? "Success" : "Failed"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void timeIntervalTest() throws InterruptedException {
        Instant instant1 = Instant.now();
        Thread.sleep(3000L);
        Instant instant2 = Instant.now();
        System.out.println(Duration.between(instant1, instant2));
        System.out.println(Duration.between(instant1, instant2).compareTo(Duration.of(6, ChronoUnit.SECONDS)));
    }
}