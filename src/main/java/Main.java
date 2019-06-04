import org.apache.log4j.Logger;
import utils.FileMapper;
import utils.WinUtilsConfiguration;

import java.io.File;
import java.io.IOException;

public class Main {
    private final static Logger logger = Logger.getLogger(Main.class);

    public static void main(String[] args) throws IOException {
        WinUtilsConfiguration.initWinUtils();
        File inputFile = FileMapper.mapFile(args[0]);
        File outFile = FileMapper.mapFile(args[1]);
        logger.info("Start converter with properties: input file: " + inputFile.getPath() + ";\noutput file: " + outFile.getPath());
        Converter converter = new Converter();
        converter.write(inputFile, outFile);
    }
}
