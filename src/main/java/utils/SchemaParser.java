package utils;

import org.apache.log4j.Logger;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.*;

public class SchemaParser {

    private final static Logger logger = Logger.getLogger(SchemaParser.class);

    public MessageType getSchema(File csvFile) {
        String messageSchema = csvToParquetSchemaParser(csvFile);
        if (messageSchema != null) {
            return MessageTypeParser.parseMessageType(messageSchema);
        } else {
            throw new IllegalArgumentException();
        }
    }

    public String csvToParquetSchemaParser(File csvFile) {
        String rawSchema = null;
        String sampleValues = null;
        StringBuilder schemaBuilder = new StringBuilder();

        try (BufferedReader buff = new BufferedReader(new FileReader(csvFile))) {
            rawSchema = buff.readLine();
            sampleValues = buff.readLine();
        } catch (IOException e) {
            logger.error(e);
        }
        String[] names = rawSchema.split(",");
        String[] values = sampleValues.split(",");
        int i = 1;
        schemaBuilder.append("message csv {").append("\n");

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

