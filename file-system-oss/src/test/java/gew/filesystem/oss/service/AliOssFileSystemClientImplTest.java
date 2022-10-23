package gew.filesystem.oss.service;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import gew.filesystem.common.model.FileSystemType;
import gew.filesystem.common.model.ObjectProperty;
import gew.filesystem.oss.config.AliOssConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.List;


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

    }


    @AfterEach
    void tearDown() {
        cloudFileSystemClient.close();
    }
}