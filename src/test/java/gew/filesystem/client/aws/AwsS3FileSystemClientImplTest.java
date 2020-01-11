package gew.filesystem.client.aws;

import gew.filesystem.client.common.CloudFileSystemClient;
import gew.filesystem.client.model.FileOperation;
import gew.filesystem.client.model.MetaDataPair;
import gew.filesystem.client.model.ObjectMetaInfo;
import gew.filesystem.client.model.ObjectProperty;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.time.ZonedDateTime;

/**
 * @author Jason/GeW
 */
@Ignore("Ignore tests that will bring real effects")
public class AwsS3FileSystemClientImplTest {

    private String defaultBucket;

    private CloudFileSystemClient client;

    private static final String PROPERTY_PATH = "config/config.properties";

    @Before
    public void setUp() {
        if (client == null) {
            Properties properties = new Properties();
            try {
                properties.load(Files.newInputStream(Paths.get(PROPERTY_PATH)));
                this.defaultBucket = properties.getProperty("aws.defaultBucket");
                client = new AwsS3FileSystemClientImpl(this.defaultBucket, properties.getProperty("aws.region"));

                client.init(new AwsS3ClientConfig(properties.getProperty("aws.accessKeyId"),
                        properties.getProperty("aws.accessKeySecret")));

            } catch (IOException ioe) {
                ioe.printStackTrace();
                Assert.assertNotNull(ioe);
            }
        }
    }

    @Test
    public void listPathTest() {
        String path = "tmp/";
        try {
            List<ObjectProperty> objectProperties = client.listPath(path);
            objectProperties.forEach(System.out::println);
        } catch (IOException e) {
            Assert.assertNotNull(e);
            e.printStackTrace();
        }
    }

    @Test
    public void mkdirTest() {
        String directory = "tmp/test";
        try {
            String result = client.mkdir(directory);
            System.out.println("Create Directory: " + result);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void checkExistenceTest() {
        String path = "tmp/tmp.a";
        try {
            boolean existence = client.exist(path);
            System.out.println(String.format("Path [%s] Exist: ", path) + existence);
        } catch (IOException e) {
            e.printStackTrace();
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
            System.out.println(String.format("Delete Object on Path [%s]: %s", destination, status));

        } catch (IOException e) {
            e.printStackTrace();
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
            System.out.println(String.format("Upload local file [%s] to Remote Path [%s]", localFile, result));

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
            System.out.println(String.format("Upload local file [%s] to Remote Path [%s] : %s",
                    localFile, destination, status));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void downloadTest1() {
        String localPath = "files/xyz.q";
        String src = "tmp/tmp.a";
        try {
            boolean status = client.download(src, new File(localPath), FileOperation.APPEND);
            System.out.println(String.format("Download Object [%s] to Local Path [%s]: %s",
                    src, localPath, status));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @After
    public void tearDown() {
        if (client != null) {
            client.close();
        }
    }
}