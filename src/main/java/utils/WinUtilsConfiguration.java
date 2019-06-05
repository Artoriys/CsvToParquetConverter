package utils;

import org.apache.log4j.Logger;

import java.io.File;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Class which configure path to WinUtils and add environment hadoop.home.dir to process if machine OS is Windows.
 * WinUtils should be located in directory with jar file.
 */

public class WinUtilsConfiguration {
    private final static Logger logger = Logger.getLogger(WinUtilsConfiguration.class);

    public static void initWinUtils() {
        Path path;
        try {
            path = new File(WinUtilsConfiguration.class.getProtectionDomain().getCodeSource().getLocation().toURI()).toPath();
        } catch (URISyntaxException e) {
            return;
        }

        Path hadoopPath = Paths.get(path.getParent().toString(),"\\winutils-master\\hadoop-3.0.0");
        System.setProperty("hadoop.home.dir", hadoopPath.toString());
        logger.info("HADOOP_HOME found at: " + hadoopPath.toString());
    }
}
