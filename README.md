## < File System Client >
![]("https://github.com/Jason-Gew/File-System-Client/workflows/Java-Build/badge.svg")

                                                                    				 By Jason/GeW

### Introduction                                                             				 
 *  File System Client(s) for multiple file systems such as Local, SFTP, AWS S3, Hadoop File System and AliYun OSS.

 *  Provide common interface `BasicFileSystemClient` and `CloudFileSystemClient` for basic usage.

 *  The `CloudFileSystemClient` is designed for cloud storage like AWS S3, AliYun OSS, Azure Blob and etc.

 *  This project is under CI/CD.


### Usage Example
```java
import gew.filesystem.client.common.BasicFileSystemClient;
import gew.filesystem.client.sftp.SftpClientConfig;
import gew.filesystem.client.sftp.SftpSystemClientImpl;
import gew.filesystem.client.model.FileOperation;
import gew.filesystem.client.model.ObjectMetaInfo;
import gew.filesystem.client.model.ObjectProperty;

import java.io.IOException;

public class Example {

    private BasicFileSystemClient client = new SftpSystemClientImpl();
    private String sftpHost = "192.168.100.160";
    private String sftpUser = "admin";
    private String sftpPass = "YOUR_PASSWORD";

    // SFTP Example
    public static void main(String[] args) throws IOException {
        SftpClientConfig sftpConfig = new SftpClientConfig(sftpHost, sftpUser, sftpPass);
        client.init(sftpConfig);
    
        // Get Object Meta
        String path = "/home/admin/tmp/Test.txt";
        Optional<ObjectMetaInfo> metaInfo = client.getObjectMetaInfo(path);
        if (metaInfo.isPresent()) {
            System.out.println(metaInfo.get());
        } else {
            System.out.println("Object Does Not Exist");
        }
    
        // Upload Object
        String dest = "/home/admin/tmp/Test.txt";
        String src = "files/Test.txt";
        FileOperation operation = FileOperation.APPEND;     // Append Mode
        try (InputStream inputStream = new FileInputStream(new File(src))) {
            boolean result = client.upload(dest, inputStream, operation);
            System.out.println(String.format("Upload File [%s] to [%s]: %s", 
                                             src, dest, result ? "Success" : "Fail"));
        }
    
        // Download Object
        src = "/home/admin/tmp/Test.txt";
        dest = "files/Test.txt";
        boolean status = client.download(src, new File(dest));
        System.out.println(String.format("Download File [%s] to -> %s: %s", src, dest, status));
    }
}

```



                                                                    				
                                                    