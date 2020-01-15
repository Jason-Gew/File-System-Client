package gew.filesystem.client.hdfs;

import gew.filesystem.client.common.BasicFileSystemClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;


@Ignore("Ignore tests that will bring real effects")
public class HadoopFileSystemClientImplTest {

    private BasicFileSystemClient client;

    private static final String PROPERTY_PATH = "config/config.properties";

    @Before
    public void setUp() {
    }


    @Test
    public void getObjectMetaInfo() {
    }

    @Test
    public void listPath() {
    }

    @Test
    public void exist() {
    }

    @Test
    public void download() {
    }

    @Test
    public void upload() {
    }

    @Test
    public void delete() {
    }

    @After
    public void tearDown() {
        if (client != null) {
            client.close();
        }
    }
}