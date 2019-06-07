import org.apache.parquet.schema.*;
import org.junit.jupiter.api.Test;
import utils.SchemaParser;
import static org.junit.jupiter.api.Assertions.*;

import java.io.File;

public class SchemaParserTests {

    @Test
    public void getSchemaTest() {
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource("test_data.csv").getFile());
        SchemaParser schemaParser = new SchemaParser();

        MessageType expected = Types.buildMessage()
                .required(PrimitiveType.PrimitiveTypeName.INT64)
                .id(1)
                .named("id")
                .required(PrimitiveType.PrimitiveTypeName.BINARY)
                .id(2)
                .named("name")
                .required(PrimitiveType.PrimitiveTypeName.INT64)
                .id(3)
                .named("age")
                .named("csv");

        MessageType actual = MessageTypeParser.parseMessageType(schemaParser.getSchema(file));
        assertEquals(expected, actual);
    }
}
