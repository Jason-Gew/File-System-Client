package gew.filesystem.common.config;


import lombok.Getter;
import lombok.Setter;


/**
 * SFTP Basic Configuration
 *
 * @author Jason/GeW
 * @since 2019-03-24
 */
@Getter
@Setter
public class SftpClientConfig implements FileSystemConfig {

    private String host;

    private Integer port;

    private AuthMode authMode;

    private String privateKeyPath;

    private String username;

    private String password;

    private String knownHostsFile;

    private Integer timeout;

    private boolean keepAlive;


    public enum AuthMode {

        PUBLIC_KEY,

        CREDENTIALS,
    }


    /**
     * Default Constructor
     */
    public SftpClientConfig() {
        this.port = 22;
    }

    /**
     * Public Key Mode
     * @param host              Host
     * @param privateKeyPath    Key File Path
     */
    public SftpClientConfig(String host, String privateKeyPath) {
        this.host = host;
        this.port = 22;
        this.privateKeyPath = privateKeyPath;
        this.authMode = AuthMode.PUBLIC_KEY;
    }

    /**
     * Credential Mode
     *
     * @param host              Host
     * @param username          Username
     * @param password          Password
     */
    public SftpClientConfig(String host, String username, String password) {
        this.host = host;
        this.port = 22;
        this.username = username;
        this.password = password;
        this.authMode = AuthMode.CREDENTIALS;
    }
}
