package utils;

import java.io.File;
import java.nio.file.Paths;

public class FileMapper {

    public static File mapFile(String fileName) {
        return Paths.get("", fileName).toAbsolutePath().toFile();
    }
}
