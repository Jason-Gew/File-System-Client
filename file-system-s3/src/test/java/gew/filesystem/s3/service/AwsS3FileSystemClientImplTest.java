package gew.filesystem.s3.service;

import gew.filesystem.common.model.FileOperation;
import gew.filesystem.common.model.MetaDataPair;
import gew.filesystem.common.model.ObjectMetaInfo;
import gew.filesystem.common.model.ObjectProperty;
import gew.filesystem.common.service.CloudFileSystemClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;


@Disabled
class AwsS3FileSystemClientImplTest {

    private AwsS3FileSystemClientImpl cloudFileSystemClient;


    @BeforeEach
    void setUp() {
        this.cloudFileSystemClient = new AwsS3FileSystemClientImpl();
    }

    private String defaultBucket;

    private CloudFileSystemClient client;

    private static final String PROPERTY_PATH = "config/config.properties";


    @Test
    public void listPathTest() {
        String path = "tmp/123/";
        try {
            List<ObjectProperty> objectProperties = client.list(path);
            objectProperties.forEach(System.out::println);
        } catch (IOException e) {
            Assertions.assertNotNull(e);
        }
    }

    @Test
    public void mkdirTest() {
        String directory = "tmp/test";
        try {
            String result = client.mkdir(directory);
            System.out.println("Create Directory: " + result);
        } catch (IOException e) {
            Assertions.assertNotNull(e);
        }
    }

    @Test
    public void checkExistenceTest() {
        String path = "tmp/tmp.a";
        try {
            boolean existence = client.exist(path);
            System.out.println(String.format("Path [%s] Exist: ", path) + existence);
        } catch (IOException e) {
            Assertions.assertNotNull(e);
        }
    }

    @Test
    public void getObjectMetaInfoTest() {
        String path = "tmp/";
        Optional<ObjectMetaInfo> metaInfo = client.getObjectMetaInfo(path);
        if (metaInfo.isPresent()) {
            System.out.println(metaInfo.get());
        } else {
            System.out.println("Unable to get Object MetaInfo for Path: " + path);
        }
    }

    @Test
    public void deleteTest() {
        String destination = "tmp/Test.txt";
        ((AwsS3FileSystemClientImpl) client).setExistenceCheck(true);
        try {
            boolean status = client.delete(destination);
            System.out.printf("Delete Object on Path [%s]: %s%n", destination, status);

        } catch (IOException e) {
            Assertions.assertNotNull(e);
        }
    }

    @Test
    public void uploadTest1() {
        String destination = "tmp/Test.txt";
        String localFile = "files/Test.txt";

        MetaDataPair metaDataPair1 = new MetaDataPair("createTime", ZonedDateTime.now().toString());
        MetaDataPair metaDataPair2 = new MetaDataPair("test-file", "true");

        try {
            String result = client.upload(this.defaultBucket, destination, new File(localFile),
                    metaDataPair1, metaDataPair2);
            System.out.printf("Upload local file [%s] to Remote Path [%s]%n", localFile, result);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void uploadTest2() {
        String destination = "tmp/data-set.csv";
        String localFile = "files/china-unicom-data.csv";
        ((AwsS3FileSystemClientImpl) client).setUseTempFile(true);
        try {
            InputStream inputStream = Files.newInputStream(Paths.get(localFile), StandardOpenOption.READ);
            boolean status = client.upload(destination, inputStream);
            System.out.printf("Upload local file [%s] to Remote Path [%s] : %s%n", localFile, destination, status);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void downloadTest1() {
        String localPath = "files/xyz.q1";
        String src = "tmp/tmp.a";
        try {
            boolean status = client.download(src, new File(localPath), FileOperation.APPEND);
            System.out.printf("Download Object [%s] to Local Path [%s]: %s%n", src, localPath, status);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}