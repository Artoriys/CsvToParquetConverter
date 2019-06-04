package utils;

import org.apache.log4j.Logger;

import java.nio.file.Path;
import java.nio.file.Paths;

public class WinUtilsConfiguration {
    private final static Logger logger = Logger.getLogger(WinUtilsConfiguration.class);

    public static void initWinUtils() {
        Path path = Paths.get("", "src", "main", "resources", "winutils-master", "hadoop-3.0.0").toAbsolutePath();
        System.setProperty("hadoop.home.dir", path.toString());
        logger.info("HADOOP_HOME found at: " + path.toString());
    }
}
