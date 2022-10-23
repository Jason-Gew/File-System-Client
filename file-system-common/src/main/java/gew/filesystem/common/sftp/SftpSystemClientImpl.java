package gew.filesystem.common.sftp;

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.SftpException;
import com.jcraft.jsch.JSchException;
import gew.filesystem.common.model.FileOperation;
import gew.filesystem.common.model.FileSystemType;
import gew.filesystem.common.model.ObjectMetaInfo;
import gew.filesystem.common.model.ObjectProperty;
import gew.filesystem.common.model.ObjectType;
import gew.filesystem.common.config.FileSystemConfig;
import gew.filesystem.common.config.SftpClientConfig;
import gew.filesystem.common.service.BasicFileSystemClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.List;
import java.util.ArrayList;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * SFTP Client Implementation V1 Based on Jsch
 *
 * @author Jason/GeW
 * @since 2019-03-24
 */
@Slf4j
public class SftpSystemClientImpl implements BasicFileSystemClient {

    private JSch sshClient;

    private volatile Session sftpSession;

    private String host;

    private Integer timeout;


    public SftpSystemClientImpl() {
        // Default Constructor
    }

    @Override
    public void init(FileSystemConfig config) {
        if (!(config instanceof SftpClientConfig)) {
            throw new IllegalArgumentException("Invalid SFTP Client Config");
        }
        SftpClientConfig sftpClientConfig = (SftpClientConfig) config;
        if (StringUtils.isAllBlank(this.host, sftpClientConfig.getHost())) {
            throw new IllegalArgumentException("Invalid SFTP Host");
        } else if (StringUtils.isBlank(this.host)) {
            this.host = sftpClientConfig.getHost();
        } else if (StringUtils.isBlank(sftpClientConfig.getHost())) {
            sftpClientConfig.setHost(this.host);
        }
        if (this.sshClient != null) {
            log.debug("JSch-SSH Client Has Initialized");
            return;
        }
        this.sshClient = new JSch();
        this.timeout = sftpClientConfig.getTimeout();
        if (sftpClientConfig.getKnownHostsFile() != null
                && Files.exists(Paths.get(sftpClientConfig.getKnownHostsFile()))) {
            try {
                this.sshClient.setKnownHosts(sftpClientConfig.getKnownHostsFile());
            } catch (JSchException e) {
                throw new IllegalArgumentException("Set Known Hosts Failed: " + e.getMessage(), e.getCause());
            }
        }
        if (SftpClientConfig.AuthMode.CREDENTIALS.equals(sftpClientConfig.getAuthMode())) {
            try {
                this.sftpSession = sshClient.getSession(sftpClientConfig.getUsername(),
                        sftpClientConfig.getHost(), sftpClientConfig.getPort());
                this.sftpSession.setPassword(sftpClientConfig.getPassword());
                if (sftpClientConfig.getKnownHostsFile() == null) {
                    this.sftpSession.setConfig("StrictHostKeyChecking", "no");
                }
                log.debug("Initialized SFTP Client to [{}] with CREDENTIAL Mode", sftpClientConfig.getHost());

            } catch (JSchException je) {
                log.error("Initializing SSH Session to [{}] with CREDENTIAL Mode Failed: {}",
                        sftpClientConfig.getHost(), je.getMessage());
                throw new RuntimeException(je);
            }
        } else if (SftpClientConfig.AuthMode.PUBLIC_KEY.equals(sftpClientConfig.getAuthMode())) {
            try {
                this.sshClient.addIdentity(sftpClientConfig.getPrivateKeyPath());
                this.sftpSession = sshClient.getSession(sftpClientConfig.getUsername(),
                        sftpClientConfig.getHost(), sftpClientConfig.getPort());
                if (sftpClientConfig.getKnownHostsFile() == null) {
                    this.sftpSession.setConfig("StrictHostKeyChecking", "no");
                }
                log.debug("Initialized SFTP Client to [{}] with Public-Key Mode", sftpClientConfig.getHost());

            } catch (JSchException je) {
                log.error("Initializing SSH Session to [{}] with Public-Key Mode Failed: {}",
                        sftpClientConfig.getHost(), je.getMessage());
                throw new RuntimeException(je);
            }
        } else {
            throw new IllegalArgumentException("Invalid SFTP Authentication Mode");
        }
    }

    public Boolean connect(Session session) throws IOException {
        if (session != null) {
            try {
                if (this.timeout == null || this.timeout < 1) {
                    session.connect();
                } else {
                    session.connect(this.timeout);
                }
                log.debug("SFTP Session Connecting...");
            } catch (JSchException e) {
                throw new IOException("Connect to SFTP Server Failed: " + e.getMessage(), e.getCause());
            }
            return session.isConnected();
        } else {
            return false;
        }
    }

    @Override
    public FileSystemType getFileSystemType() {
        return FileSystemType.SFTP;
    }

    @Override
    public Optional<ObjectMetaInfo> getObjectMetaInfo(String path) {
        checkParameter(path);
        ChannelSftp sftpChannel = null;
        try {
            sftpChannel = openChannel();
            sftpChannel.connect();
            SftpATTRS attributes = sftpChannel.lstat(path);
            if (attributes == null) {
                return Optional.empty();
            } else {
                ObjectMetaInfo metaInfo = new ObjectMetaInfo();
                metaInfo.setSize(attributes.getSize());
                if (attributes.isDir()) {
                    metaInfo.setObjectType(ObjectType.DIRECTORY);
                } else if (attributes.isReg()) {
                    metaInfo.setObjectType(ObjectType.FILE);
                } else if (attributes.isLink()) {
                    metaInfo.setObjectType(ObjectType.SYMBOLIC_LINK);
                } else {
                    metaInfo.setObjectType(ObjectType.OTHER);
                }
                metaInfo.setCreationTime(Instant.ofEpochSecond(attributes.getATime()));
                metaInfo.getMetaData().put("permissions", attributes.getPermissions());
                metaInfo.getMetaDataValueType().put("permissions", Integer.class);
                metaInfo.getMetaData().put("permissionsString", attributes.getPermissionsString());
                metaInfo.getMetaDataValueType().put("permissionsString", String.class);
                metaInfo.getMetaData().put("lastModifiedTime", attributes.getMTime());
                metaInfo.getMetaDataValueType().put("lastModifiedTime", Integer.class);
                metaInfo.getMetaData().put("flags", attributes.getFlags());
                metaInfo.getMetaDataValueType().put("flags", Integer.class);
                return Optional.of(metaInfo);
            }
        } catch (JSchException je) {
            throw new RuntimeException(je);

        } catch (SftpException se) {
            log.warn("Read Object Meta Info on Path [{}] Failed: {}", path, se.getMessage());
            return Optional.empty();

        } catch (IOException ioe) {
            throw new IllegalStateException("SFTP GetObjectMetaInfo Exception: " + ioe.getMessage(), ioe.getCause());

        } finally {
            disconnectChannel(sftpChannel);
        }

    }

    @Override
    @SuppressWarnings("unchecked")
    public List<ObjectProperty> list(String path) throws IOException {
        checkParameter(path);
        ChannelSftp sftpChannel = null;
        try {
            sftpChannel = openChannel();
            sftpChannel.connect();
            List<ChannelSftp.LsEntry> items = sftpChannel.ls(path);

            if (items == null) {
                return new ArrayList<>(0);
            } else {
                List<ObjectProperty> objects = items.stream()
                        .filter(e -> !e.getFilename().equals(".") && !e.getFilename().equals(".."))
                        .map(e -> e.getAttrs().isDir() ? new ObjectProperty(e.getFilename(), true, null)
                                : new ObjectProperty(e.getFilename(), false, e.getAttrs().getSize()))
                        .collect(Collectors.toList());
                log.debug("List Path [{}] Found {} Items", path, objects.size());
                return objects;
            }
        } catch (SftpException err) {
            log.warn("List Path [{}] Failed: {}", path, err.getMessage());
            return new ArrayList<>(0);

        } catch (JSchException je) {
            throw new RuntimeException(je);

        } finally {
            disconnectChannel(sftpChannel);
        }
    }

    @Override
    public String mkdir(String path) throws IOException {
        checkParameter(path);
        ChannelSftp sftpChannel = null;
        try {
            sftpChannel = openChannel();
            sftpChannel.connect();
            sftpChannel.mkdir(path);
            return path;

        } catch (JSchException je) {
            throw new RuntimeException(je);

        } catch (SftpException err) {
            log.error("Make Directory [{}] on SFTP Failed: {}", path, err.getMessage());
            throw new IOException("Mkdir Failed: " + err.getMessage(), err);

        } finally {
            disconnectChannel(sftpChannel);
        }
    }

    @Override
    public boolean exist(String path) throws IOException {
        checkParameter(path);
        ChannelSftp sftpChannel = null;
        try {
            sftpChannel = openChannel();
            sftpChannel.connect();
            SftpATTRS attributes = sftpChannel.lstat(path);
            return attributes != null;

        } catch (JSchException je) {
            throw new RuntimeException(je);

        } catch (SftpException e) {
            log.warn("Check Object [{}] Existence on SFTP Exception: {}", path, e.getMessage());
            return false;

        } finally {
            disconnectChannel(sftpChannel);
        }
    }

    @Override
    public InputStream download(String source) throws IOException {
        checkParameter(source);
        ChannelSftp sftpChannel;
        try {
            sftpChannel = openChannel();
            sftpChannel.connect();
            InputStream inputStream = sftpChannel.get(source);
            if (inputStream != null) {
                log.debug("Prepare Object Downloading From SFTP [{}] Success", source);
            }
            return inputStream;

        } catch (JSchException je) {
            throw new RuntimeException(je);

        } catch (SftpException err) {
            log.error("Download Object From SFTP [{}] Exception: {}", source, err.getMessage());
            throw new IOException(err);

        }
    }

    @Override
    public Boolean download(String source, File localFile, FileOperation... localFileOperation) throws IOException {
        checkParameter(source);
        if (localFile == null) {
            throw new IllegalArgumentException("Invalid Local File");
        }
        ChannelSftp sftpChannel = null;
        try {
            sftpChannel = openChannel();
            sftpChannel.connect();
            sftpChannel.get(source, localFile.getAbsolutePath(), null, isAppend(localFileOperation));
            log.debug("Download Object From SFTP [{}], Save to [{}] Success",
                    source, localFile.getAbsolutePath());
            return true;

        } catch (JSchException je) {
            throw new RuntimeException(je);

        } catch (SftpException err) {
            log.error("Download Object From SFTP [{}], Save to [{}] Exception: {}",
                    source, localFile.getAbsolutePath(), err.getMessage());
            throw new IOException(err);

        } finally {
            disconnectChannel(sftpChannel);
        }
    }

    @Override
    public Boolean upload(String destination, InputStream in, FileOperation... destFileOperation) throws IOException {
        checkParameter(destination);
        if (in == null) {
            throw new IllegalArgumentException("Invalid InputStream");
        }
        ChannelSftp sftpChannel = null;
        try {
            sftpChannel = openChannel();
            sftpChannel.connect();
            sftpChannel.put(in, destination, isAppend(destFileOperation));
            log.debug("Upload File to SFTP Path [{}] Success", destination);
            return true;

        } catch (JSchException je) {
            throw new RuntimeException(je);

        } catch (SftpException err) {
            log.error("Upload File to SFTP Path [{}] Exception: {}", destination, err.getMessage());
            throw new IOException(err);

        } finally {
            disconnectChannel(sftpChannel);
        }
    }

    @Override
    public String upload(String destination, File localFile) throws IOException {
        checkParameter(destination);
        if (localFile == null) {
            throw new IllegalArgumentException("Invalid Local File");
        }
        ChannelSftp sftpChannel = null;
        try {
            sftpChannel = openChannel();
            sftpChannel.connect();
            sftpChannel.put(localFile.getAbsolutePath(), destination);
            log.debug("Upload File [{}] to SFTP Path [{}] Success", localFile.getName(), destination);
            return destination;

        } catch (JSchException je) {
            throw new RuntimeException(je);

        } catch (SftpException err) {
            log.error("Upload File [{}] to SFTP Path [{}] Exception: {}",
                    localFile.getName(), destination, err.getMessage());
            throw new IOException(err);

        } finally {
            disconnectChannel(sftpChannel);
        }
    }

    @Override
    public Boolean delete(String path, FileOperation... deleteFileOperation) throws IOException {
        checkParameter(path);
        ChannelSftp sftpChannel = null;
        try {
            sftpChannel = openChannel();
            sftpChannel.connect();
            boolean recursive = deleteFileOperation != null && deleteFileOperation.length > 0
                    && FileOperation.DELETE_RECURSIVE.equals(deleteFileOperation[0]);
            if (recursive) {
                sftpChannel.rmdir(path);
                log.debug("Delete Directory [{}] on SFTP Success", path);
            } else {
                sftpChannel.rm(path);
                log.debug("Delete File [{}]  on SFTP Success", path);
            }
            return true;

        } catch (JSchException je) {
            throw new RuntimeException(je);

        } catch (SftpException err) {
            if (err.id == 2 || err.getMessage().equalsIgnoreCase("No such file")) {
                return false;
            } else {
                log.warn("Delete Object [{}] on SFTP Exception: {}", path, err.getMessage());
                throw new IOException(err);
            }

        } finally {
            disconnectChannel(sftpChannel);
        }
    }

    @Override
    public void close() {
        if (sftpSession != null && sftpSession.isConnected()) {
            sftpSession.disconnect();
        }
    }


    private void checkParameter(final String path) {
        if (this.sshClient == null || this.sftpSession == null) {
            throw new IllegalStateException("SFTP Client Has Not Been Initialized!");
        } else if (StringUtils.isBlank(path)) {
            throw new IllegalArgumentException("Invalid Path");
        }
    }

    private ChannelSftp openChannel() throws IOException {
        if (this.sftpSession.isConnected()) {
            try {
                return (ChannelSftp) this.sftpSession.openChannel("sftp");
            } catch (JSchException e) {
                log.error("Open SFTP Channel Failed: {}", e.getMessage());
                throw new IOException(e);
            }
        } else {
            boolean connected = connect(this.sftpSession);
            log.debug("Connect SFTP Session: {}", connected ? "Success" : "Failed");
            return openChannel();
        }
    }

    private void disconnectChannel(ChannelSftp channelSftp) {
        if (channelSftp != null) {
            channelSftp.disconnect();
        }
    }

    private int isAppend(FileOperation... operations) {
        return operations != null && operations.length > 0
                && FileOperation.APPEND.equals(operations[0]) ? 2 : 0;
    }


    public void setSshClient(JSch sshClient) {
        this.sshClient = sshClient;
    }

    public void setSftpSession(Session sftpSession) {
        this.sftpSession = sftpSession;
    }

}
