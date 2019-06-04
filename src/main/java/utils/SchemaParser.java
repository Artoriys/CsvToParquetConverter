package utils;

import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;

public class SchemaParser {


    public MessageType getSchema(File schemaFile) {
        String messageSchema = null;
        try {
            messageSchema = String.join("\n", Files.readAllLines(schemaFile.toPath(), Charset.forName("UTF-8")));
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (messageSchema != null) {
            return MessageTypeParser.parseMessageType(messageSchema);
        }
        else {
            throw new IllegalArgumentException();
        }
    }
}

