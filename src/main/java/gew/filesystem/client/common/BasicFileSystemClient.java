package gew.filesystem.client.common;

import gew.filesystem.client.model.FileOperation;
import gew.filesystem.client.model.FileSystemType;
import gew.filesystem.client.model.ObjectMetaInfo;
import gew.filesystem.client.model.ObjectProperty;

import java.io.File;
import java.io.InputStream;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * Basic File System Client. Support common file operations such as ls, mkdir, download, upload and etc.
 * @author Jason/GeW
 * @since 2019-03-24
 */
public interface BasicFileSystemClient {

    /**
     * Default Initialize Method. Not mandatory for all clients.
     * @param config Corresponding File System Client Config
     */
    default void init(final FileSystemConfig config) {/* Default Initial Method */}

    /**
     * Get File System Type/Name.
     * @return FileSystemType
     * @see FileSystemType
     */
    FileSystemType getFileSystemType();

    /**
     * Get Object/File/Directory Meta Info.
     * @param path Remote Path
     * @return ObjectMetaInfo
     * @see ObjectMetaInfo
     */
    Optional<ObjectMetaInfo> getObjectMetaInfo(final String path);

    /**
     * List Remote Object/File/Directory in Directory/Path.
     * @param path Remote Path
     * @return Object Properties
     * @see ObjectProperty
     * @throws IOException Throw IOException When Encounter Corresponding Error
     */
    List<ObjectProperty> listPath(final String path) throws IOException;

    /**
     * Create Directory in Path.
     * @param path Remote Path
     * @return Remote Directory Path
     * @throws IOException Throw IOException When Encounter Corresponding Error
     */
    String mkdir(final String path) throws IOException;

    /**
     * Check Object/File/Directory Existence.
     * @param path Remote Path
     * @return Boolean Exist
     * @throws IOException Throw IOException When Encounter Corresponding Error
     */
    boolean exist(final String path) throws IOException;

    /**
     * Download Object/File as InputStream, not suggest for downloading large size object/file.
     * @param source Remote Path
     * @return InputStream
     * @throws IOException Throw IOException When Encounter Corresponding Error
     */
    InputStream download(final String source) throws IOException;

    /**
     * Download Object/File to Local Path, not suggest for downloading large size file.
     * @param source Remote Path
     * @param localFile Local Destination File
     * @param localFileOperation FileOperation, optional but only the first element will be accepted
     * @see FileOperation
     * @return Boolean Download Success or Not
     * @throws IOException Throw IOException When Encounter Corresponding Error
     */
    Boolean download(final String source, File localFile, FileOperation... localFileOperation) throws IOException;

    /**
     * Upload Object/File as InputStream to Remote Destination.
     * @param destination Remote Path
     * @param in Object/File in InputStream
     * @param destFileOperation FileOperation, optional but only the first element will be accepted
     * @see FileOperation
     * @return Boolean Upload Success or Not
     * @throws IOException Throw IOException When Encounter Corresponding Error
     */
    Boolean upload(final String destination, InputStream in, FileOperation... destFileOperation) throws IOException;

    /**
     * Upload Local Object/File to Remote Destination, not suggested for large size file.
     * @param destination Remote Path
     * @param localFile Local Object/File
     * @return Destination Path
     * @throws IOException Throw IOException When Encounter Corresponding Error
     */
    String upload(final String destination, File localFile) throws IOException;

    /**
     * Delete Object/File/Directory in Path.
     * @param path Remote Path
     * @param deleteFileOperation Delete Operations. If delete a directory, must use DELETE_RECURSIVE
     * @see FileOperation
     * @return Boolean Delete Success or Not
     * @throws IOException Throw IOException When Encounter Corresponding Error
     */
    Boolean delete(final String path, FileOperation... deleteFileOperation) throws IOException;

    /**
     * Default Close Method. Not mandatory for all clients.
     */
    default void close() {/* Default Close Method */}
}
