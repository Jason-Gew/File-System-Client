package gew.filesystem.client.sftp;

import gew.filesystem.client.common.FileSystemConfig;
import gew.filesystem.client.model.FileSystemType;

/**
 * @author Jason/GeW
 * @since 2019-03-24
 */
public class SftpClientConfig implements FileSystemConfig {


    @Override
    public FileSystemType fileSystemType() {
        return FileSystemType.SFTP;
    }
}
