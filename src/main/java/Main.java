import org.apache.log4j.Logger;
import utils.FileMapper;
import utils.WinUtilsConfiguration;

import java.io.File;

/**
 * @author Nikita Alemaskin
 * Main class of the CsvToParquetConverter
 * Usage:
 * First program argument is name of input (.csv) file in project root directory
 * Second prorgam argument is name of output (.parquet) file in project root derectory
 */
public class Main {
    private final static Logger logger = Logger.getLogger(Main.class);

    public static void main(String[] args) {
        WinUtilsConfiguration.initWinUtils();
        File inputFile = FileMapper.mapFile(args[0]);
        File outFile = FileMapper.mapFile(args[1]);
        logger.info("Start converter with properties: input file: " + inputFile.getPath() + ";\noutput file: " + outFile.getPath());
        Converter converter = new Converter();
        converter.write(inputFile, outFile);
    }
}
