package gew.filesystem.common.util;


import gew.filesystem.common.model.CompressMethod;
import gew.filesystem.common.model.FileOperation;
import gew.filesystem.common.model.ObjectProperty;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;

/**
 * @author Jason/GeW
 */
@Disabled("Ignore tests that will bring real effects")
public class CompressUtilTest {


    @Test
    public void listZipFileTest() {
        String path = "files/zip-1580636725.zip";
        try {
            List<ObjectProperty> files = CompressUtil.listPath(Paths.get(path), CompressMethod.ZIP);
            files.forEach(System.out::println);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void zipFileTest() {
        String src = "files/test/sub-test2.txt";
        String dest = "files/T-2.zip";
        try {
            boolean result = CompressUtil.zip(Paths.get(src), Paths.get(dest), FileOperation.OVERWRITE);
            System.out.println("Zip File Result: " + result);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void zipDirTest() {
        String folder = "files/test";
        String dest = "files/zip-" + System.currentTimeMillis() / 1000;
        try {
            boolean result = CompressUtil.zip(Paths.get(folder), Paths.get(dest), FileOperation.OVERWRITE);
            System.out.println("Zip Directory Result: " + result);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void tarTest() {
        String src = "files/test/sub-test2.txt";
        String dest = "files/T-2";
        try {
            boolean result = CompressUtil.tar(Paths.get(src), Paths.get(dest), FileOperation.OVERWRITE);
            System.out.println("Tar File Result: " + result);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void tarDirTest() {
        String folder = "files/test/pic/";
        String dest = "files/tar-" + System.currentTimeMillis() / 1000;
        try {
            boolean result = CompressUtil.tar(Paths.get(folder), Paths.get(dest), FileOperation.OVERWRITE);
            System.out.println("Tar Directory Result: " + result);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void _7zTest() {
        String src = "files/test/sub-test2.txt";
        String dest = "files/7-2";
        try {
            boolean result = CompressUtil.sevenZ(Paths.get(src), Paths.get(dest), FileOperation.OVERWRITE);
            System.out.println("7Z File Result: " + result);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void _7zDirTest() {
        String folder = "files/test";
        String dest = "files/7z-" + System.currentTimeMillis() / 1000;
        try {
            boolean result = CompressUtil.sevenZ(Paths.get(folder), Paths.get(dest), FileOperation.OVERWRITE);
            System.out.println("7Z Directory Result: " + result);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void gzipTest() {
        String src = "files/Test.txt";
        String dest = "files/gz-test.gz";
        try {
            boolean result = CompressUtil.gzip(Paths.get(src), Paths.get(dest));
            System.out.println("Gzip File Result: " + result);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void tarAndGzipTest() {
        String folder = "files/test";
        String dest = "files/tg-" + System.currentTimeMillis() / 1000;
        try {
            boolean result = CompressUtil.tarAndGzip(Paths.get(folder), Paths.get(dest), FileOperation.OVERWRITE);
            System.out.println("Tar + Gzip Directory Result: " + result);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void unzipDirTest() {
        String src = "files/zip-1580629411.zip";
        String dest = "files/test-" + System.currentTimeMillis() / 10000;
        try {
            boolean result = CompressUtil.unZip(Paths.get(src), Paths.get(dest), FileOperation.OVERWRITE);
            System.out.println("Unzip Directory Result: " + result);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void unzipFileTest() {
        String src = "files/5699.zip";
        String dest = "files/pic-" + System.currentTimeMillis() / 1000 + ".png";
        try {
            boolean result = CompressUtil.unZip(Paths.get(src), Paths.get(dest), FileOperation.OVERWRITE);
            System.out.println("Unzip File Result: " + result);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void unGzipTest() {
        String src = "files/gz-test.gz";
        String dest = "files/ungz-test.txt";
        try {
            boolean result = CompressUtil.unGZip(Paths.get(src), Paths.get(dest));
            System.out.println("UnGZip File Result: " + result);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void unTarTest() {

    }

}
