package gew.filesystem.client.sftp;

import gew.filesystem.client.common.FileSystemConfig;

import java.nio.file.Path;

/**
 * @author Jason/GeW
 * @since 2019-03-24
 */
public class SftpClientConfig implements FileSystemConfig {

    private String host;

    private Integer port;

    private AuthMode authMode;

    private Path privateKeyPath;

    private String username;

    private String password;

    private Path knownHostsFile;

    private Integer timeout;

    private boolean keepAlive;


    enum AuthMode {

        PUBLIC_KEY,

        CREDENTIALS,
    }


    public SftpClientConfig() {
        this.port = 22;
    }

    public SftpClientConfig(String host, String username, String password) {
        this.host = host;
        this.port = 22;
        this.username = username;
        this.password = password;
        this.authMode = AuthMode.CREDENTIALS;
    }

    public SftpClientConfig(String host, Path privateKeyPath, String username) {
        this.host = host;
        this.port = 22;
        this.privateKeyPath = privateKeyPath;
        this.username = username;
        this.authMode = AuthMode.CREDENTIALS;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public AuthMode getAuthMode() {
        return authMode;
    }

    public void setAuthMode(AuthMode authMode) {
        this.authMode = authMode;
    }

    public Path getPrivateKeyPath() {
        return privateKeyPath;
    }

    public void setPrivateKeyPath(Path privateKeyPath) {
        this.privateKeyPath = privateKeyPath;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public Path getKnownHostsFile() {
        return knownHostsFile;
    }

    public void setKnownHostsFile(Path knownHostsFile) {
        this.knownHostsFile = knownHostsFile;
    }

    public Integer getTimeout() {
        return timeout;
    }

    public void setTimeout(Integer timeout) {
        this.timeout = timeout;
    }

    public boolean isKeepAlive() {
        return keepAlive;
    }

    public void setKeepAlive(boolean keepAlive) {
        this.keepAlive = keepAlive;
    }
}
