package gew.filesystem.client.sftp;

import gew.filesystem.client.common.BasicFileSystemClient;
import gew.filesystem.client.model.FileOperation;
import gew.filesystem.client.model.ObjectMetaInfo;
import gew.filesystem.client.model.ObjectProperty;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.Properties;


/**
 * @author Jason/GeW
 */
@Ignore("Ignore tests that will bring real effects")
public class SftpSystemClientImplTest {

    private BasicFileSystemClient client;

    private static final String PROPERTY_PATH = "config/config.properties";

    @Before
    public void setUp() {
        if (client == null) {
            Properties properties = new Properties();
            try {
                properties.load(Files.newInputStream(Paths.get(PROPERTY_PATH)));
                client = new SftpSystemClientImpl();
                SftpClientConfig clientConfig = new SftpClientConfig(properties.getProperty("sftp.host"),
                        properties.getProperty("sftp.username"),  properties.getProperty("sftp.password"));
                client.init(clientConfig);

            } catch (IOException ioe) {
                ioe.printStackTrace();
                Assert.assertNotNull(ioe);
            }
        }
    }

    @Test
    public void listPath() {
        String path = "/home/pi/tmp/";
        try {
            List<ObjectProperty> properties = client.listPath(path);
            properties.forEach(System.out::println);
        } catch (IOException ioe) {
            ioe.printStackTrace();
            Assert.assertNotNull(ioe);
        }
    }

    @Test
    public void getObjectMetaInfoTest() {
        String path = "/home/pi/tmp/Test.txt";
        try {
            Optional<ObjectMetaInfo> metaInfo = client.getObjectMetaInfo(path);
            if (metaInfo.isPresent()) {
                System.out.println(metaInfo.get());
            } else {
                System.out.println("SFTP Get Object Meta Info Not Present");
            }
        } catch (Exception err) {
            err.printStackTrace();
            Assert.assertNotNull(err);
        }
    }

    @Test
    public void existTest() {
        String path = "/home/pi/Application/Test.txt";
        try {
            boolean exist = client.exist(path);
            System.out.println(String.format("Path [%s] Exist: %s", path, exist));

        } catch (IOException ioe) {
            ioe.printStackTrace();
            Assert.assertNotNull(ioe);
        }
    }


    @Test
    public void uploadTest() {
        String dest = "/home/pi/tmp/Test.txt";
        String src = "files/Test.txt";
        FileOperation operation = FileOperation.APPEND;

        try (InputStream inputStream = new FileInputStream(new File(src))) {
            boolean result = client.upload(dest, inputStream, operation);
            System.out.println(String.format("Upload File [%s] to [%s]: %s", src, dest, result ? "Success" : "Fail"));

        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    @Test
    public void downloadTest() {
        String dest = "files/file-1578753613.txt";
        String src = "/home/pi/Temp/Test.txt";

        try {
            boolean status = client.download(src, new File(dest), FileOperation.APPEND);
            System.out.println(String.format("Download File [%s] to -> %s: %s", src, dest, status));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void streamDownloadTest() {
        String src = "/home/pi/Application/apache-artemis-2.8.1-bin.tar.gz";
        String dest = "files/gz-file.tar.gz";
        try (InputStream in = client.download(src);
             OutputStream os = new FileOutputStream(dest)) {
            long bytes = IOUtils.copyLarge(in, os);
            System.out.println(String.format("Download File [%s] to -> %s: %d Bytes", src, dest, bytes));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void mkdirTest() {
        String path = "/home/pi/tmp";

        try {
            String result = client.mkdir(path);
            System.out.println(result);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void deleteTest() {
        String path = "/home/pi/Temp/Test.txt";
        String dir = "/home/pi/Temp/";
        try {
            boolean result = client.delete(dir, FileOperation.DELETE_RECURSIVE);
            System.out.println(String.format("Delete Remote Object [%s]: %s", path, result));

        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    @After
    public void tearDown() {
        if (client != null) {
            client.close();
        }
    }

}