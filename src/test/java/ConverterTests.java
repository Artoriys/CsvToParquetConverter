import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

public class ConverterTests {

    @Test
    public void writeExistTest() {
        ClassLoader classLoader = getClass().getClassLoader();
        File inputFile = new File(classLoader.getResource("test_data.csv").getFile());
        File outputFile = new File(inputFile.getAbsoluteFile().getParentFile().getPath() + "\\output\\output.parquet");
        File outputDir = outputFile.getParentFile();

        Converter converter = new Converter(inputFile, outputFile);
        converter.write();

        assertTrue(outputFile.exists());

        try {
            FileUtils.deleteDirectory(outputDir);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void writeTest() {

        String expected = "1,Vasia,20,\n" +
                "2,Vova,16,\n" +
                "3,Mike,19,\n" +
                "4,John,40,\n" +
                "5,Jack,38,";

        ClassLoader classLoader = getClass().getClassLoader();
        File inputFile = new File(classLoader.getResource("test_data.csv").getFile());
        File outputFile = new File(inputFile.getAbsoluteFile().getParentFile().getPath() + "\\output\\output.parquet");
        File outputDir = outputFile.getParentFile();

        Converter converter = new Converter(inputFile, outputFile);
        converter.write();

        Path parquetPath = new Path(outputFile.toURI());
        GenericRecord nextRecord;
        StringBuilder rec = new StringBuilder();
        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(parquetPath).build()) {
            while ((nextRecord = reader.read()) != null) {
                for (int i = 0; i < 3; i++) {
                    if (i == 1) {
                        ByteBuffer byteBuffer = (ByteBuffer) nextRecord.get(i);
                        rec.append(new String(byteBuffer.array())).append(",");
                    } else {
                        rec.append(nextRecord.get(i)).append(",");
                    }
                }
                rec.append("\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        assertEquals(expected, rec.toString().trim());


        try {
            FileUtils.deleteDirectory(outputDir);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
