import csv_utils.CsvParquetWriter;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.parquet.schema.MessageType;
import utils.SchemaParser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;

/**
 * Main class which provide writing from CSV file to PARQUET file.
 */

class Converter {
    private final static Logger logger = Logger.getLogger(Converter.class);

    void write(File csvFile, File outFile) {
        CsvParquetWriter csvParquetWriter;
        String line;
        Path path = new Path(outFile.toURI());
        SchemaParser schemaParser = new SchemaParser();
        MessageType messageType = schemaParser.getSchema(csvFile);

        try {
            csvParquetWriter = new CsvParquetWriter(path, messageType, false);
        } catch (NullPointerException e) {
            logger.error("WinUtils directory not found. Set program argument with the path to WinUtils\n" +
                    "Shutting down application.......");
            return;
        } catch (IOException e) {
            logger.error("File already exists: " + outFile.toString() + "\nShutting down application.......");
            return;
        }

        try (BufferedReader bf = new BufferedReader(new FileReader(csvFile))) {
            bf.readLine();
            while ((line = bf.readLine()) != null) {
                csvParquetWriter.write(Arrays.asList(line.split(",")));
            }
            csvParquetWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}