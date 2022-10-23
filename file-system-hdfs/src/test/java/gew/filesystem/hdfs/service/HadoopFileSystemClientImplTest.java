package gew.filesystem.hdfs.service;


import gew.filesystem.common.model.ObjectProperty;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

@Disabled
class HadoopFileSystemClientImplTest {

    private HadoopFileSystemClientImpl client;

    @org.junit.jupiter.api.BeforeEach
    void setUp() {
        client = new HadoopFileSystemClientImpl();
    }



    @Test
    public void listPath() {
        try {
            List<ObjectProperty> objects = client.list("/test");
            objects.forEach(System.out::println);
        } catch (IOException e) {
            Assertions.assertNotNull(e);
        }
    }

    @Test
    public void upload() {
        String src = "files/china-unicom-data.csv";
        String dest = "/test/telcom-data.csv";
        try {
            String result = client.upload(dest, new File(src));
            System.out.println(result);
        } catch (IOException e) {
            Assertions.assertNotNull(e);
        }
    }

    @Test
    public void delete() {
        String src = "/test/telcom-data.csv";
        try {
            boolean status = client.delete(src);
            System.out.printf("Delete [%s] Status: %s%n", src, status);
        } catch (IOException e) {
            Assertions.assertNotNull(e);
        }
    }

}
