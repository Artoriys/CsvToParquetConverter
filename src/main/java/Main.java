import utils.FileMapper;

import java.io.File;
import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        File inputFile = FileMapper.mapFile(args[0]);
        File schemaFile = FileMapper.mapFile(args[1]);
        File outFile = FileMapper.mapFile(args[2]);
        Converter converter = new Converter();
        converter.write(schemaFile, inputFile, outFile);
    }
}
