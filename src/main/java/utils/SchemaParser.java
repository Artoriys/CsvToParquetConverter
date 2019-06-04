package utils;

import org.apache.log4j.Logger;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.*;

/**
 * Utility class with methods parsing Csv schema to Parquet schema {@link org.apache.parquet.schema.MessageType MessageType}
 */

public class SchemaParser {

    private final static Logger logger = Logger.getLogger(SchemaParser.class);

    public MessageType getSchema(File csvFile) {
        String messageSchema = csvToParquetSchemaParser(csvFile);
        return MessageTypeParser.parseMessageType(messageSchema);
    }

    private String csvToParquetSchemaParser(File csvFile) {
        String rawSchema = null;
        String sampleValues = null;

        try (BufferedReader buff = new BufferedReader(new FileReader(csvFile))) {
            rawSchema = buff.readLine();
            sampleValues = buff.readLine();
        } catch (IOException e) {
            logger.error(e);
        }

        String[] names = rawSchema.split(",");
        String[] values = sampleValues.split(",");
        return mapParquetSchema(names, values);
    }

    private String mapParquetSchema(String[] names, String[] values) {
        StringBuilder schemaBuilder = new StringBuilder();

        schemaBuilder.append("message csv {").append("\n");
        int i = 1;
        for (String name : names) {
            schemaBuilder.append("      required ")
                    .append(parseColumnFormat(values[i - 1]))
                    .append(" ")
                    .append(name)
                    .append(" = ")
                    .append(i)
                    .append(";\n");
            i++;
        }
        schemaBuilder.append("}");
        logger.info("Schema building completed");
        return schemaBuilder.toString();
    }

    private String parseColumnFormat(String value) {
        try {
            Integer.parseInt(value);
            return "INT64";
        } catch (NumberFormatException e) {
        }
        try {
            Double.parseDouble(value);
            return "DOUBLE";
        } catch (NumberFormatException e) {
        }
        return "BINARY";
    }
}

