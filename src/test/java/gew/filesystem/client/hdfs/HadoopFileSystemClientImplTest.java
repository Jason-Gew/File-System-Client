package gew.filesystem.client.hdfs;

import gew.filesystem.client.common.BasicFileSystemClient;
import gew.filesystem.client.model.ObjectProperty;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;


@Ignore("Ignore tests that will bring real effects")
public class HadoopFileSystemClientImplTest {

    private BasicFileSystemClient client;

    private static final String PROPERTY_PATH = "config/config.properties";

    @Before
    public void setUp() {
        if (client == null) {
            Properties properties = new Properties();
            try {
                properties.load(Files.newInputStream(Paths.get(PROPERTY_PATH)));
                client = new HadoopFileSystemClientImpl();
                HadoopFsClientConfig config = new HadoopFsClientConfig(URI.create(properties.getProperty("hdfs.nameNode")),
                        properties.getProperty("hdfs.username"));
                config.setReplication(1);
                client.init(config);
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }


    @Test
    public void getObjectMetaInfo() {

    }

    @Test
    public void listPath() {
        try {
            List<ObjectProperty> objects = client.listPath("/test");
            objects.forEach(System.out::println);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void exist() {
    }

    @Test
    public void download() {
    }

    @Test
    public void upload() {
        String src = "files/china-unicom-data.csv";
        String dest = "/test/telcom-data.csv";
        try {
            String result = client.upload(dest, new File(src));
            System.out.println(result);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void delete() {
        String src = "/test/telcom-data.csv";
        try {
            boolean status = client.delete(src);
            System.out.println(String.format("Delete [%s] Status: %s", src, status));
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