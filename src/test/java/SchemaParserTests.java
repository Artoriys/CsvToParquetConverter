import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.junit.jupiter.api.Test;
import utils.SchemaParser;

import java.io.File;

public class SchemaParserTests {

    @Test
    public void getSchemaTest() {
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource("test_data.csv").getFile());
        SchemaParser schemaParser = new SchemaParser();
        //MessageType messageType = new MessageType("csv", );
        schemaParser.getSchema(file);
    }
}
