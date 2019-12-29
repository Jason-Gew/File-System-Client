package gew.filesystem.client.aws;

import gew.filesystem.client.common.CloudFileSystemClient;
import gew.filesystem.client.model.ObjectProperty;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;



@Ignore
public class AwsS3FileSystemClientImplTest {

    private CloudFileSystemClient client;

    private static final String PROPERTY_PATH = "config/config.properties";

    @Before
    public void setUp() throws Exception {
        if (client == null) {
            Properties properties = new Properties();
            try {
                properties.load(Files.newInputStream(Paths.get(PROPERTY_PATH)));
                client = new AwsS3FileSystemClientImpl(properties.getProperty("defaultBucket"),
                        properties.getProperty("region"));
                client.init(properties.getProperty("accessKeyId"), properties.getProperty("accessKeySecret"));

            } catch (IOException ioe) {
                ioe.printStackTrace();
                Assert.assertNotNull(ioe);
            }
        }
    }

    private boolean check() {
        return client == null;
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
        String path = "tmp/test";

        try {
            boolean existence = client.exist(path);
            System.out.println(String.format("Path [%s] Exist: ", path) + existence);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void deleteTest() {
        String destination = "tmp/test/20191230.txt";
    }

    @After
    public void tearDown() throws Exception {
        if (client != null) {
            client.close();
        }
    }
}