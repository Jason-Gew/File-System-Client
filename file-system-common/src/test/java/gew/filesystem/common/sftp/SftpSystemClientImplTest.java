package gew.filesystem.common.sftp;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;
import gew.filesystem.common.DefaultMock;
import gew.filesystem.common.config.SftpClientConfig;
import gew.filesystem.common.model.FileOperation;
import gew.filesystem.common.model.ObjectMetaInfo;
import gew.filesystem.common.model.ObjectProperty;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Vector;


class SftpSystemClientImplTest extends DefaultMock {

    private SftpSystemClientImpl client;

    @Mock
    private JSch sshClient;

    @Mock
    private Session session;

    @Mock
    private ChannelSftp channel;

    @Mock
    private ChannelSftp.LsEntry dir1;

    @Mock
    private ChannelSftp.LsEntry file1;

    @Mock
    private SftpATTRS dirAttrs;

    @Mock
    private SftpATTRS fileAttrs;


    @BeforeEach
    public void setUp() throws JSchException {

        SftpClientConfig clientConfig = new SftpClientConfig("127.0.0.1", "user", "pass");

        client = new SftpSystemClientImpl();
        client.init(clientConfig);
        client.setSshClient(this.sshClient);
        client.setSftpSession(this.session);

        Mockito.when(sshClient.getSession(Mockito.anyString(), Mockito.anyString())).thenReturn(this.session);
        Mockito.when(this.session.isConnected()).thenReturn(true);
        Mockito.when(this.session.openChannel(Mockito.anyString())).thenReturn(this.channel);
        Mockito.doNothing().when(this.channel).connect();
    }

    @Test
    void listTest() throws SftpException {
        String path = "/home/pi/tmp/";
        Vector<ChannelSftp.LsEntry> entries = new Vector<>();

        Mockito.when(dirAttrs.isDir()).thenReturn(true);
        Mockito.when(fileAttrs.isDir()).thenReturn(false);
        Mockito.when(fileAttrs.getSize()).thenReturn(1024L);

        Mockito.when(dir1.getFilename()).thenReturn("tests");
        Mockito.when(dir1.getAttrs()).thenReturn(dirAttrs);

        Mockito.when(file1.getFilename()).thenReturn("test1.csv");
        Mockito.when(file1.getAttrs()).thenReturn(fileAttrs);

        entries.add(dir1);
        entries.add(file1);
        Mockito.when(this.channel.ls(Mockito.anyString())).thenReturn(entries);
        try {
            List<ObjectProperty> properties = client.list(path);
            Assertions.assertEquals(2, properties.size());

        } catch (IOException ioe) {
            ioe.printStackTrace();
            Assertions.assertNotNull(ioe);
        }
    }

    @Test
    public void getObjectMetaInfoTest() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> client.getObjectMetaInfo("   "));
        String path = "/home/pi/tmp/test1.csv";
        try {
            Mockito.when(fileAttrs.isDir()).thenReturn(false);
            Mockito.when(fileAttrs.getSize()).thenReturn(1024L);

            Mockito.when(this.channel.lstat(Mockito.anyString())).thenReturn(fileAttrs);
            Optional<ObjectMetaInfo> metaInfo = client.getObjectMetaInfo(path);
            Assertions.assertTrue(metaInfo.isPresent());

        } catch (Exception err) {
            err.printStackTrace();
            Assertions.assertNotNull(err);
        }
    }

    @Test
    public void existTest() {
        String path = "/home/pi/tmp/test.txt";
        try {
            boolean exist = client.exist(path);
            Assertions.assertFalse(exist);

        } catch (IOException ioe) {
            ioe.printStackTrace();
            Assertions.assertNotNull(ioe);
        }
    }


    @Test
    public void uploadTest() throws SftpException {
        String dest = "/home/pi/tmp/Test.txt";
        String src = getPropertiesFile();
        FileOperation operation = FileOperation.APPEND;
        Mockito.doNothing().when(this.channel).put(Mockito.any(InputStream.class), Mockito.anyString(),
                Mockito.anyInt());

        try (InputStream inputStream = new FileInputStream(src)) {
            boolean result = client.upload(dest, inputStream, operation);
            Assertions.assertTrue(result);

        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    @Test
    public void downloadTest() {
        String src = "/home/pi/Temp/Test.txt";
        try {
            Assertions.assertThrows(IllegalArgumentException.class, () -> client.download(src, null));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void streamDownloadTest() throws SftpException {
        String src = "/home/pi/tmp/compressed.tar.gz";
        Mockito.when(this.channel.get(Mockito.anyString())).thenThrow(SftpException.class);
        Assertions.assertThrows(IOException.class, () -> client.download(src));
    }

    @Test
    public void mkdirTest() {
        String path = "/home/pi/tmp";
        try {
            Mockito.doNothing().when(this.channel).mkdir(Mockito.anyString());
            String result = client.mkdir(path);
            Assertions.assertEquals(path, result);

        } catch (Exception err) {
            Assertions.assertNotNull(err);
        }
    }

    @Test
    public void deleteTest() {
        String path = "/home/pi/tmp/test.txt";
        try {
            Mockito.doNothing().when(this.channel).rm(Mockito.anyString());
            boolean result = client.delete(path);
            Assertions.assertTrue(result);

        } catch (Exception err) {
            Assertions.assertNotNull(err);
        }
    }

    @AfterEach
    public void tearDown() {
        if (client != null) {
            client.close();
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
