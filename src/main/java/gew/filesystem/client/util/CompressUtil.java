package gew.filesystem.client.util;

import gew.filesystem.client.model.CompressMethod;

import gew.filesystem.client.model.FileOperation;
import gew.filesystem.client.model.ObjectProperty;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.sevenz.SevenZArchiveEntry;
import org.apache.commons.compress.archivers.sevenz.SevenZFile;
import org.apache.commons.compress.archivers.sevenz.SevenZFileOptions;
import org.apache.commons.compress.archivers.sevenz.SevenZOutputFile;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.Deflater;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;


/**
 * @author Jason/GeW
 * @since 2018-08-18
 */
public class CompressUtil {

    private static boolean AUTO_SUFFIX = true;

    private static boolean DELETE_ON_FAILED = true;

    private static boolean OVERWRITE_PROTECT = true;

    private static long DEFAULT_BUFFER = 8 * FileUtils.ONE_KB;

    private static int DEFAULT_COMPRESS_LVL = Deflater.BEST_COMPRESSION;

    private static CompressMethod DEFAULT_COMPRESSION_TYPE = CompressMethod.ZIP;

    private static Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;

    private static final Logger log = LogManager.getLogger(CompressUtil.class);


    private CompressUtil() {
        // Static Class, Private Constructor
    }

    public static List<ObjectProperty> listPath(Path path, CompressMethod compressionType) throws IOException {
        if (path == null || !Files.exists(path)) {
            throw new IllegalArgumentException("Invalid Path");
        } else if (compressionType == null) {
            throw new IllegalArgumentException("Invalid Compression Type");
        }
        List<ObjectProperty> objects = new ArrayList<>();
        switch (compressionType) {
            case ZIP: {
                ZipFile zipFile = new ZipFile(path.toString());
                Enumeration<? extends ZipEntry> files = zipFile.entries();
                while (files.hasMoreElements()) {
                    ZipEntry entry = files.nextElement();
                    objects.add(new ObjectProperty(entry.getName(), entry.isDirectory(), entry.getSize()));
                }
                break;
            }
        }
        return objects;
    }


    public static boolean zip(final Path src, final Path dest, FileOperation... operations) throws IOException {
        checkParameter(dest);
        String suffix = AUTO_SUFFIX && !dest.toString().toLowerCase().endsWith(".zip") ? ".zip" : "";
        return compress(src, Paths.get(dest.toString() + suffix), CompressMethod.ZIP, operations);
    }

    public static boolean gzip(final Path src, final Path dest, FileOperation... operations) throws IOException {
        checkParameter(dest);
        String suffix = AUTO_SUFFIX && !dest.toString().toLowerCase().endsWith(".gz") ? ".gz" : "";
        return compress(src, Paths.get(dest.toString() + suffix), CompressMethod.GZIP, operations);
    }

    public static boolean tar(final Path src, final Path dest, FileOperation... operations) throws IOException {
        checkParameter(dest);
        String suffix = AUTO_SUFFIX && !dest.toString().toLowerCase().endsWith(".tar") ? ".tar" : "";
        return compress(src, Paths.get(dest.toString() + suffix), CompressMethod.TAR, operations);
    }

    public static boolean tarAndGzip(final Path src, final Path dest, FileOperation... operations) throws IOException {
        checkParameter(dest);
        String suffix = AUTO_SUFFIX && !dest.toString().toLowerCase().endsWith(".tar.gz") ? ".tar.gz" : "";
        Path temp = Paths.get(dest + ".tmp");
        boolean tarStatus = compress(src, temp, CompressMethod.TAR, operations);
        if (tarStatus) {
            boolean gzipStatus = gzip(temp, Paths.get(dest + suffix), operations);
            if (gzipStatus) {
                Files.delete(temp);
                return true;
            } else {
                return false;
            }
        } else {
            return false;
        }
    }

    public static boolean sevenZ(final Path src, final Path dest, FileOperation... operations) throws IOException {
        checkParameter(dest);
        String suffix = AUTO_SUFFIX && !dest.toString().toLowerCase().endsWith(".7z") ? ".7z" : "";
        return compress(src, Paths.get(dest.toString() + suffix), CompressMethod._7Z, operations);
    }


    public static boolean compress(final Path src, final Path dest, final CompressMethod method,
                                   FileOperation... operations) throws IOException {
        checkParameter(src, dest);
        if (method == null) {
            throw new IllegalArgumentException("Invalid Compress Method");
        } else if (!Files.exists(src)) {
            throw new IllegalArgumentException("Invalid Source File Path");
        }
        boolean destExistence = Files.exists(dest);
        OpenOption openOption = StandardOpenOption.CREATE_NEW;
        if (operations != null && ArrayUtils.contains(operations, FileOperation.APPEND)) {
            if (destExistence) {
                throw new IllegalArgumentException("Destination Path Does Not Exist");
            }
            openOption = StandardOpenOption.APPEND;
        } else if (OVERWRITE_PROTECT && operations != null
                && !ArrayUtils.contains(operations, FileOperation.OVERWRITE)) {
            openOption = StandardOpenOption.CREATE;
        }
        boolean status = false;
        switch (method) {
            case ZIP: {
                try (ZipArchiveOutputStream archive = new ZipArchiveOutputStream(
                        Files.newOutputStream(dest, openOption))) {
                    archive.setLevel(DEFAULT_COMPRESS_LVL);
                    if (Files.isDirectory(src)) {
                        status = compressDir(src, Paths.get(src.getFileName().toString()), archive, CompressMethod.ZIP);
                    } else {
                        status = compressFile(src, src.getFileName(), archive, CompressMethod.ZIP, false);
                    }
                    return status;

                } catch (Exception err) {
                    deleteOnFail(dest, !destExistence, DELETE_ON_FAILED, !status);
                    throw err;
                }
            }
            case GZIP: {
                try (GzipCompressorOutputStream archive = new GzipCompressorOutputStream(
                        Files.newOutputStream(dest, openOption))) {
                    status = compressGzip(src, dest, archive);
                    return status;

                } catch (Exception err) {
                    deleteOnFail(dest, !destExistence, DELETE_ON_FAILED, !status);
                    throw err;
                }
            }
            case TAR: {
                try (TarArchiveOutputStream archive = new TarArchiveOutputStream(
                        Files.newOutputStream(dest, openOption))) {
                    if (Files.isDirectory(src)) {
                        status = compressDir(src, Paths.get(src.getFileName().toString()), archive, CompressMethod.TAR);
                    } else {
                        status = compressFile(src, src.getFileName(), archive, CompressMethod.TAR, false);
                    }
                    return status;

                } catch (Exception err) {
                    deleteOnFail(dest, !destExistence, DELETE_ON_FAILED, !status);
                    throw err;
                }
            }
            case _7Z: {
                try (SevenZOutputFile szo = new SevenZOutputFile(dest.toFile())) {
                    if (Files.isDirectory(src)) {
                        status = compress7zDir(src, Paths.get(src.getFileName().toString()), szo);
                    } else {
                        status = compress7zFile(src, src.getFileName(), szo, false);
                    }
                    szo.closeArchiveEntry();
                    return status;

                }  catch (Exception err) {
                    deleteOnFail(dest, !destExistence, DELETE_ON_FAILED, !status);
                    throw err;
                }
            }
            default:
                throw new IllegalArgumentException("Unsupported Compress Method");
        }
    }


    private static boolean compressDir(Path src, Path entryPath, ArchiveOutputStream zos,
                                       CompressMethod method) throws IOException {
        if (entryPath == null) {
            throw new IllegalArgumentException("Invalid Entry File Path");
        }
        List<ObjectProperty> objects = listPath(src);
        if (objects.isEmpty()) {
            compressFile(src, entryPath, zos, method, null);
        } else {
            for (ObjectProperty o : objects) {
                String subPath = o.getName().substring(o.getName().indexOf(entryPath + File.separator)
                        + (entryPath.toString().length()));
                if (o.getDirectory()) {
                    compressDir(Paths.get(o.getName()), Paths.get(entryPath + subPath), zos, method);
                } else {
                    compressFile(Paths.get(o.getName()), Paths.get(entryPath + subPath), zos, method, true);
                }
            }
        }
        return true;
    }

    private static boolean compressFile(Path src, Path entryPath, ArchiveOutputStream zos, CompressMethod method,
                                        Boolean withDir) throws IOException {
        if (entryPath == null) {
            throw new IllegalArgumentException("Invalid Entry File Path");
        }
        ArchiveEntry archiveEntry;
        switch (method) {
            case ZIP: {
                if (withDir == null || withDir) {
                    archiveEntry = new ZipArchiveEntry(src.toFile(), entryPath.toString());
                } else {
                    archiveEntry = new ZipArchiveEntry(src.getFileName().toString());
                }
                break;
            }
            case TAR: {
                if (withDir == null) {
                    archiveEntry = new TarArchiveEntry(src.toFile(), entryPath.toString());
                } else if (withDir) {
                    archiveEntry = new TarArchiveEntry(src.toFile(), entryPath.toString());
                    ((TarArchiveEntry) archiveEntry).setSize(Files.size(src));
                } else {
                    archiveEntry = new TarArchiveEntry(src.getFileName().toString());
                    ((TarArchiveEntry) archiveEntry).setSize(Files.size(src));
                }
                break;
            }
            default:
                throw new IllegalArgumentException("Invalid Compress Method: " + method);
        }
        zos.putArchiveEntry(archiveEntry);
        if (withDir == null) {
            log.debug("{} Dir [{}] Success {} Bytes copied", method, src.getFileName().toString(), 0);
        } else {
            try (BufferedInputStream bis = new BufferedInputStream(Files.newInputStream(src))) {
                long bytes = IOUtils.copyLarge(bis, zos);
                log.debug("{} File [{}] Success {} Bytes copied", method, src.getFileName().toString(), bytes);
            }
        }
        zos.closeArchiveEntry();
        return true;
    }

    private static boolean compressGzip(Path src, Path entryPath, GzipCompressorOutputStream gos) throws IOException {
        if (entryPath == null) {
            throw new IllegalArgumentException("Invalid Entry File Path");
        }
        if (Files.isDirectory(src)) {
            throw new IllegalArgumentException("Source Path is a Directory");
        } else {
            try (BufferedInputStream bis = new BufferedInputStream(Files.newInputStream(src, StandardOpenOption.READ))) {
                long bytes = IOUtils.copyLarge(bis, gos);
                log.debug("GZip File [{}] Success {} Bytes copied", src.getFileName().toString(), bytes);
            }
        }
        return true;
    }

    private static boolean compress7zDir(Path src, Path entryPath, SevenZOutputFile szo) throws IOException {
        if (entryPath == null) {
            throw new IllegalArgumentException("Invalid Entry File Path");
        }
        List<ObjectProperty> objects = listPath(src);
        if (objects.isEmpty()) {
            compress7zFile(src, entryPath, szo, null);
        } else {
            for (ObjectProperty o : objects) {
                String subPath = o.getName().substring(o.getName().indexOf(entryPath + File.separator)
                        + (entryPath.toString().length()));
                if (o.getDirectory()) {
                    compress7zDir(Paths.get(o.getName()), Paths.get(entryPath + subPath), szo);
                } else {
                    compress7zFile(Paths.get(o.getName()), Paths.get(entryPath + subPath), szo, true);
                }
            }
        }
        return true;
    }

    private static boolean compress7zFile(Path src, Path entryPath, SevenZOutputFile szo,
                                          Boolean withDir) throws IOException {
        if (entryPath == null) {
            throw new IllegalArgumentException("Invalid Entry File Path");
        }
        ArchiveEntry archiveEntry;
        archiveEntry = szo.createArchiveEntry(src.toFile(), entryPath.toString());
        szo.putArchiveEntry(archiveEntry);
        if (withDir == null) {
            log.debug("7z Dir [{}] Success {} Bytes copied", src.getFileName().toString(), 0);
        } else {
            try (BufferedInputStream bis = new BufferedInputStream(Files.newInputStream(src))) {
                byte[] buf = new byte[(int) DEFAULT_BUFFER];
                long size = 0;
                int len;
                while (-1 != (len = bis.read(buf))) {
                    szo.write(buf, 0, len);
                    size += len;
                }
                log.debug("7z File [{}] Success {} Bytes copied", src.getFileName().toString(), size);
            }
        }
        return true;
    }


    public static boolean unZip(final Path src, final Path dest, FileOperation... operations) throws IOException {
        return decompress(src, dest, CompressMethod.ZIP, operations);
    }

    public static boolean unGZip(final Path src, final Path dest, FileOperation... operations) throws IOException {
        return decompress(src, dest, CompressMethod.GZIP, operations);
    }

    public static boolean unSevenZ(final Path src, final Path dest, FileOperation... operations) throws IOException {
        return decompress(src, dest, CompressMethod._7Z, operations);
    }

    public static boolean decompress(final Path src, final Path dest, CompressMethod method,
                                     FileOperation... operations) throws IOException {
        checkParameter(src);
        if (method == null) {
            throw new IllegalArgumentException("Invalid Compress Method");
        } else if (!Files.exists(src)) {
            throw new IllegalArgumentException("Invalid Source File Path");
        }
        boolean destExistence = Files.exists(dest);
        OpenOption openOption = StandardOpenOption.CREATE_NEW;
        if (!destExistence && !dest.toString().contains(".")) {
            Path d = Files.createDirectories(dest);
            log.debug("System Create Directory [{}]", d.toAbsolutePath());
        } else if (!destExistence) {
            Path f = Files.createFile(dest);
            log.debug("System Create File [{}]", f.toAbsolutePath());
        }
        boolean status = false;
        switch (method) {
            case ZIP: {
                try (ArchiveInputStream archive = new ZipArchiveInputStream(Files.newInputStream(src,
                        StandardOpenOption.READ))) {
                    status = decompress(src, dest, archive, CompressMethod.ZIP, Files.isDirectory(dest));

                } catch (Exception err) {
                    log.error("Decompress Zip File [{}] to [{}] Failed: {}", src.getFileName(), dest.toString(),
                            err.getMessage());
                    throw err;
                }
                break;
            }
            case GZIP: {
                try (CompressorInputStream archive = new GzipCompressorInputStream(Files.newInputStream(src,
                        StandardOpenOption.READ))) {
                    status = decompressGzip(dest, archive);

                } catch (Exception err) {
                    log.error("Decompress GZip File [{}] to [{}] Failed: {}", src.getFileName(), dest.toString(),
                            err.getMessage());
                    throw err;
                }
                break;
            }
            case _7Z: {
                try (SevenZFile sevenZFile = new SevenZFile(src.toFile(), SevenZFileOptions.DEFAULT)) {
                    status = decompress7z(src, dest, sevenZFile, Files.isDirectory(dest));
                }
            }
        }
        return status;
    }


    private static boolean decompress(Path src, Path dest, ArchiveInputStream ais, CompressMethod method,
                                      boolean isDir) throws IOException {
        if (ais == null) {
            throw new IllegalArgumentException("Invalid Archive InputStream");
        }
        ArchiveEntry archiveEntry;

        while ((archiveEntry = ais.getNextEntry()) != null) {
            if (!ais.canReadEntryData(archiveEntry)) {
                log.warn("Unable to read archive entry [{}] from src [{}]", archiveEntry.getName(), src.toString());
                continue;
            }
            if (archiveEntry.isDirectory()) {
                if (isDir) {
                    Path p = Files.createDirectories(Paths.get(dest + archiveEntry.getName()));
                    return decompress(src, p, ais, method, true);
                }
                return decompress(src, dest, ais, method, false);

            } else {
                Path filePath;
                if (isDir) {
                    filePath = Paths.get(dest + File.separator + archiveEntry.getName());
                    if (!Files.exists(filePath) && filePath.toString().contains(File.separator)) {
                        Path d = Files.createDirectories(filePath.getParent());
                        log.debug("System Create Directories [{}]", d.toAbsolutePath());
                        Path f = Files.createFile(filePath);
                        log.debug("System Create File [{}]", f.toAbsolutePath());
                    }
                } else {
                    filePath = dest;
                }
                try (OutputStream os = Files.newOutputStream(filePath, StandardOpenOption.CREATE)) {
                    long bytes = IOUtils.copyLarge(ais, os);
                    log.debug("Decompressed File [{}] Success, {} Bytes Copied", dest.getFileName(), bytes);
                }
            }
        }
        return true;
    }


    public static boolean decompressGzip(Path dest, CompressorInputStream ais) throws IOException {
        if (ais == null) {
            throw new IllegalArgumentException("Invalid Archive InputStream");
        }
        if (!Files.isRegularFile(dest)) {
            throw new IllegalArgumentException("Destination Path is Not a Regular File");
        }
        try (OutputStream os = Files.newOutputStream(dest, StandardOpenOption.CREATE)) {
            long bytes = IOUtils.copyLarge(ais, os);
            log.debug("Decompressed GZip File [{}] Success, {} Bytes Copied", dest.getFileName(), bytes);
        }
        return true;
    }

    public static boolean decompress7z(Path src, Path dest, SevenZFile _7zFile,
                                       boolean isDir) throws IOException {
        SevenZArchiveEntry entry;
        while ((entry = _7zFile.getNextEntry()) != null) {
            if (entry.isDirectory()) {
                if (isDir) {
                    Path p = Files.createDirectories(Paths.get(dest + entry.getName()));
                    boolean status = decompress7zDir(src, p, entry, true);
                } else {
                    throw new IllegalArgumentException("Cannot Write Directory into File");
                }
            } else {
                boolean status =  decompress7zFile(src, dest, entry);
            }
        }
        return true;
    }

    private static boolean decompress7zDir(Path src, Path dest, SevenZArchiveEntry entry,
                                           boolean isDir) throws IOException {
        return false;
    }

    private static boolean decompress7zFile(Path src, Path dest, SevenZArchiveEntry entry) throws IOException {
//        Path filePath;
//        if (isDir) {
//            filePath = Paths.get(dest + File.separator + entry.getName());
//            if (!Files.exists(filePath) && filePath.toString().contains(File.separator)) {
//                Path d = Files.createDirectories(filePath.getParent());
//                log.debug("System Create Directories [{}]", d.toAbsolutePath());
//                Path f = Files.createFile(filePath);
//                log.debug("System Create File [{}]", f.toAbsolutePath());
//            }
//        } else {
//            filePath = dest;
//        }
//        try (OutputStream os = Files.newOutputStream(filePath, StandardOpenOption.CREATE)) {
//            long bytes = IOUtils.copyLarge(entry., os);
//            log.debug("Decompressed File [{}] Success, {} Bytes Copied", dest.getFileName(), bytes);
//        }
        return false;
    }



    private static void checkParameter(Path... paths) {
        if (paths == null || paths.length == 0) {
            throw new IllegalArgumentException("Invalid Path");
        } else {
            for (Path p : paths) {
                if (p == null || StringUtils.isEmpty(p.toString())) {
                    throw new IllegalArgumentException("Invalid Path");
                }
            }
        }
    }

    private static List<ObjectProperty> listPath(final Path path) throws IOException {
        try (Stream<Path> paths = Files.walk(path, 1)) {
            return paths.filter(p -> !p.equals(path))
                    .map(p -> Files.isDirectory(p) ? new ObjectProperty(p.toString(), true)
                            : new ObjectProperty(p.toString(), false))
                    .collect(Collectors.toList());
        }
    }

    private static void deleteOnFail(final Path path, boolean... conditions) {
        if (path != null && !ArrayUtils.contains(conditions, false)) {
            try {
                if (!Files.isDirectory(path)) {
                    Files.delete(path);
                } else {
                    FileUtils.deleteDirectory(path.toFile());
                }
            } catch (IOException ioe) {
                log.warn("Delete On Failed for File [{}] Failed: {}", path.toString(), ioe.getMessage());
            }
        }
    }


    public static long getDefaultBuffer() {
        return DEFAULT_BUFFER;
    }

    public static void setDefaultBuffer(long defaultBuffer) {
        DEFAULT_BUFFER = defaultBuffer;
    }

    public static CompressMethod getDefaultCompressionType() {
        return DEFAULT_COMPRESSION_TYPE;
    }

    public static void setDefaultCompressionType(CompressMethod defaultCompressionType) {
        DEFAULT_COMPRESSION_TYPE = defaultCompressionType;
    }

    public static Charset getDefaultCharset() {
        return DEFAULT_CHARSET;
    }

    public static void setDefaultCharset(Charset defaultCharset) {
        DEFAULT_CHARSET = defaultCharset;
    }

    public static boolean isAutoSuffix() {
        return AUTO_SUFFIX;
    }

    public static void setAutoSuffix(boolean autoSuffix) {
        AUTO_SUFFIX = autoSuffix;
    }

    public static boolean isDeleteIfFailed() {
        return DELETE_ON_FAILED;
    }

    public static void setDeleteIfFailed(boolean deleteIfFailed) {
        DELETE_ON_FAILED = deleteIfFailed;
    }

    public static int getDefaultCompressLvl() {
        return DEFAULT_COMPRESS_LVL;
    }

    public static void setDefaultCompressLvl(int defaultCompressLvl) {
        DEFAULT_COMPRESS_LVL = defaultCompressLvl;
    }

    public static boolean isOverwriteProtect() {
        return OVERWRITE_PROTECT;
    }

    public static void setOverwriteProtect(boolean overwriteProtect) {
        OVERWRITE_PROTECT = overwriteProtect;
    }
}
