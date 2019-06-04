package utils;

import java.io.File;
import java.nio.file.Paths;

/**
 * Utility class to get file absolute path to {@link java.io.File File}
 */

public class FileMapper {

    public static File mapFile(String fileName) {
        return Paths.get("", fileName).toAbsolutePath().toFile();
    }
}
