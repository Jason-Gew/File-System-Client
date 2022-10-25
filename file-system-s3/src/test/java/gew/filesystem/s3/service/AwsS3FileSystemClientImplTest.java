package gew.filesystem.s3.service;

import gew.filesystem.common.model.FileOperation;
import gew.filesystem.common.model.MetaDataPair;
import gew.filesystem.common.model.ObjectMetaInfo;
import gew.filesystem.common.model.ObjectProperty;
import gew.filesystem.s3.DefaultMock;
import gew.filesystem.s3.config.AwsS3ClientConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import software.amazon.awssdk.services.s3.S3Client;

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
class AwsS3FileSystemClientImplTest extends DefaultMock {

    private AwsS3FileSystemClientImpl cloudFileSystemClient;


    @Mock
    private S3Client s3;


    private static String PROPERTY_PATH = "pom.properties";


    @BeforeEach
    void setUp() {
        String defaultBucket = "test";
        this.cloudFileSystemClient = new AwsS3FileSystemClientImpl(defaultBucket);
        AwsS3ClientConfig clientConfig = new AwsS3ClientConfig("<accessKeyId>",
                "<accessKeySecret>", "us-east-1");
        this.cloudFileSystemClient.init(clientConfig);
        this.cloudFileSystemClient.setS3Client(this.s3);
    }


    @Test
    public void listPathTest() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> cloudFileSystemClient.setMaxListObjects(9999));
        try {
            List<ObjectProperty> objectProperties = cloudFileSystemClient.list("   ");
            Assertions.assertEquals(0, objectProperties.size());
        } catch (IOException e) {
            Assertions.assertNotNull(e);
        }
    }

    @Test
    public void mkdirTest() {
        String directory = "files/";
        try {
            String result = cloudFileSystemClient.mkdir(directory);
            Assertions.assertEquals(directory, result);

        } catch (Exception e) {
            Assertions.assertNotNull(e);
        }
    }

    @Test
    public void checkExistenceTest() {
        String path = "tmp/tmp.a";
        try {
            boolean existence = cloudFileSystemClient.exist(path);
            System.out.println(String.format("Path [%s] Exist: ", path) + existence);
        } catch (IOException e) {
            Assertions.assertNotNull(e);
        }
    }

    @Test
    public void getObjectMetaInfoTest() {
        String path = "files/Test.txt";
        Optional<ObjectMetaInfo> metaInfo = cloudFileSystemClient.getObjectMetaInfo(path);
        if (metaInfo.isPresent()) {
            System.out.println(metaInfo.get());
        } else {
            System.out.println("Unable to get Object MetaInfo for Path: " + path);
        }
    }

    @Test
    public void deleteTest() {
        String destination = "files/Test.txt";
        try {
            boolean status = cloudFileSystemClient.delete(destination);
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
            String result = cloudFileSystemClient.upload("test", destination, new File(localFile),
                    metaDataPair1, metaDataPair2);
            System.out.printf("Upload local file [%s] to Remote Path [%s]%n", localFile, result);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void uploadTest2() {
        String destination = "tmp/data-set.csv";
        String localFile = "files/test.txt";
        try {
            InputStream inputStream = Files.newInputStream(Paths.get(localFile), StandardOpenOption.READ);
            boolean status = cloudFileSystemClient.upload(destination, inputStream);
            System.out.printf("Upload local file [%s] to Remote Path [%s] : %s%n", localFile, destination, status);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void downloadTest1() {
        String localPath = "files/Test.txt";
        String src = "tmp/tmp.a";
        try {
            boolean status = cloudFileSystemClient.download(src, new File(localPath), FileOperation.APPEND);
            System.out.printf("Download Object [%s] to Local Path [%s]: %s%n", src, localPath, status);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}