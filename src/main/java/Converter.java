import csv_utils.CsvParquetWriter;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.schema.MessageType;
import utils.SchemaParser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;

class Converter {
    void write(File schemaFile, File csvFile, File outFile) throws IOException {
        String line;
        SchemaParser schemaParser = new SchemaParser();
        Path path = new Path(outFile.toURI());
        System.out.println(Path.isWindowsAbsolutePath(path.toString(), true));
        MessageType messageType = schemaParser.getSchema(schemaFile);
        CsvParquetWriter csvParquetWriter = new CsvParquetWriter(path, messageType, false);

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