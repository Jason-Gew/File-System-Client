## < File System Client >
![]("https://github.com/Jason-Gew/File-System-Client/workflows/Java-Build/badge.svg")
<img src=https://github.com/Jason-Gew/File-System-Client/workflows/Java-Build/badge.svg>

                                                                    			By Jason/GeW

### Introduction                                                             				 
 * File System Client for multiple file systems such as Local, **SFTP**, **AliYun OSS**, **AWS S3**, and HDFS.

 * Provide common **SPI** `BasicFileSystemClient` and `CloudFileSystemClient` for basic usage.

 * The `CloudFileSystemClient` is designed for cloud object store service like AWS S3, AliYun OSS, Azure Blob etc.
 
 * `CompressUtil` supports multiple compression / decompression methods for both file and directory.

 * All modules are tested in production environment.

 * This project is under CI/CD.


### Usage Example

```java
import gew.filesystem.common.service.BasicFileSystemClient;
import gew.filesystem.common.config.SftpClientConfig;
import gew.filesystem.common.sftp.SftpSystemClientImpll;
import gew.filesystem.common.model.FileOperation;
import gew.filesystem.common.model.ObjectMetaInfo;
import gew.filesystem.common.model.ObjectProperty;

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
        String path = "/home/admin/tmp/test.txt";
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
            System.out.printf("Upload File [%s] to [%s]: %s%n",
                    src, dest, result ? "Success" : "Fail");
        }

        // Download Object
        src = "/home/admin/tmp/Test.txt";
        dest = "files/Test.txt";
        boolean status = client.download(src, new File(dest));
        System.out.printf("Download File [%s] to -> %s: %s%n", src, dest, status);
    }
}

```

### Note
You're welcome to submit any question(s) or ticket(s) [here](https://github.com/Jason-Gew/File-System-Client/issues) !

                                                                    				
                                                    