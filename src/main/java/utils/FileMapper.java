package utils;

import java.io.File;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Utility class to get file absolute path to {@link java.io.File File}
 */

public class FileMapper {

    public static File mapInputFile(String fileName) {
        try {
            Path path = new File(FileMapper.class.getProtectionDomain().getCodeSource().getLocation().toURI()).toPath();
            Path filePath = Paths.get(path.getParent().toString(), fileName);
            return filePath.toFile();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static File mapOutputFile(String fileName) {
        try {
            Path path = new File(FileMapper.class.getProtectionDomain().getCodeSource().getLocation().toURI()).toPath();
            Path filePath = Paths.get(path.getParent().toString(), "output", fileName);
            return filePath.toFile();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        return null;
    }
}
