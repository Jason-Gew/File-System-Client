package gew.filesystem.oss.service;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.common.comm.ResponseMessage;
import com.aliyun.oss.common.comm.ServiceClient;
import com.aliyun.oss.internal.OSSHeaders;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import com.aliyun.oss.model.ObjectMetadata;
import com.aliyun.oss.model.VoidResult;
import gew.filesystem.common.model.FileOperation;
import gew.filesystem.common.model.FileSystemType;
import gew.filesystem.common.model.ObjectMetaInfo;
import gew.filesystem.common.model.ObjectProperty;
import gew.filesystem.oss.DefaultMock;
import gew.filesystem.oss.config.AliOssConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Optional;


class AliOssFileSystemClientImplTest extends DefaultMock {


    private AliOssFileSystemClientImpl cloudFileSystemClient;

    @Mock
    private OSSClient ossClient;

    private final String bucket = "test-bucket";


    @BeforeEach
    void setUp() {
        cloudFileSystemClient = new AliOssFileSystemClientImpl(bucket);
        AliOssConfig ossConfig = new AliOssConfig("https://oss-cn-shanghai.aliyuncs.com",
                "<accessKeyId>", "<accessKeySecret>");
        cloudFileSystemClient.init(ossConfig);
        cloudFileSystemClient.setOssClient(this.ossClient);
    }


    @Test
    void listTest() {
        Assertions.assertEquals(FileSystemType.OSS, cloudFileSystemClient.getFileSystemType());
        OSSObjectSummary summary1 = new OSSObjectSummary();
        summary1.setBucketName(bucket);
        summary1.setSize(0L);
        summary1.setKey("folder/");

        OSSObjectSummary summary2 = new OSSObjectSummary();
        summary2.setBucketName(bucket);
        summary2.setSize(123L);
        summary2.setKey("folder/test1.csv");

        ObjectListing objectListing = new ObjectListing();
        objectListing.getObjectSummaries().add(summary1);
        objectListing.getObjectSummaries().add(summary2);

        Mockito.when(ossClient.listObjects(Mockito.any(ListObjectsRequest.class))).thenReturn(objectListing);
        try {
            List<ObjectProperty> properties = this.cloudFileSystemClient.list("folder/");
            Assertions.assertEquals(2, properties.size());

        } catch (IOException ioe) {
            Assertions.assertNotNull(ioe);
        }
    }


    @Test
    void getMetaInfoTest() {
        Assertions.assertFalse(cloudFileSystemClient.getObjectMetaInfo("  ").isPresent());
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.addUserMetadata("test", "123");
        metadata.setLastModified(new Date());
        metadata.setContentType("image/jpg");
        metadata.setContentLength(1024L);
        metadata.setHeader(OSSHeaders.ETAG, "tag");
        metadata.setHeader(OSSHeaders.OSS_VERSION_ID, "123456");

        Mockito.when(this.ossClient.getObjectMetadata(Mockito.anyString(), Mockito.anyString())).thenReturn(metadata);


        Optional<ObjectMetaInfo> meta = this.cloudFileSystemClient.getObjectMetaInfo("files/test.txt");
        Assertions.assertTrue(meta.isPresent());
        Assertions.assertEquals(1024L, meta.get().getSize());
        Assertions.assertEquals("123", meta.get().getUserData().get("test"));
    }

    @Test
    void mkdirTest() throws IOException {
        String dir = "files";
        Mockito.when(this.ossClient.createDirectory(Mockito.anyString(), Mockito.anyString()))
                .thenReturn(new VoidResult());
        Assertions.assertEquals("", cloudFileSystemClient.mkdir(dir));
    }

    @Test
    void deleteTest() throws IOException {
        Assertions.assertFalse(cloudFileSystemClient.delete("  "));
        VoidResult result = new VoidResult();
        ResponseMessage response = new ResponseMessage(new ServiceClient.Request());
        response.setStatusCode(200);
        result.setResponse(response);
        Mockito.when(this.ossClient.deleteObject(Mockito.anyString(), Mockito.anyString())).thenReturn(result);

        Assertions.assertTrue(cloudFileSystemClient.delete("files/test.txt"));
    }


    @Test
    void existTest() throws IOException {
        Assertions.assertFalse(cloudFileSystemClient.exist("  "));
        Mockito.when(this.ossClient.doesObjectExist(Mockito.anyString(), Mockito.anyString())).thenReturn(true);
        Assertions.assertTrue(cloudFileSystemClient.exist("files/test.txt"));
    }

    @Test
    void downloadTest() throws IOException {
        OSSObject object = new OSSObject();
        Mockito.when(this.ossClient.getObject(Mockito.anyString(), Mockito.anyString())).thenReturn(object);
        Assertions.assertNull(cloudFileSystemClient.download("files/test.txt"));

        Assertions.assertThrows(IllegalArgumentException.class, () -> cloudFileSystemClient.download(
                "files/test.txt", null, FileOperation.READ));
    }

    @Test
    void uploadTest() {
        Mockito.when(this.ossClient.putObject(Mockito.anyString(), Mockito.anyString(), Mockito.any(File.class)))
                .thenThrow(OSSException.class);
        try {
            Assertions.assertThrows(IllegalArgumentException.class, () -> cloudFileSystemClient.upload(
                    "files/test.txt", null));
            cloudFileSystemClient.upload("files/test.txt", new File("pom.properties"));

        } catch (IOException ioe) {
            Assertions.assertNotNull(ioe);
        }
    }

    @Test
    void otherTest() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> cloudFileSystemClient.setMaxListObjects(2000));
        cloudFileSystemClient.setDefaultBucket(this.bucket);
    }

    @AfterEach
    void tearDown() {
        cloudFileSystemClient.close();
    }

}
