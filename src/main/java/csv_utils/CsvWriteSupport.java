package csv_utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.ParquetEncodingException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

import java.util.HashMap;
import java.util.List;

/**
 * Class which override WriteSupport and add realization to write method
 */

public class CsvWriteSupport extends WriteSupport<List<String>> {
    private final static Logger logger = Logger.getLogger(CsvWriteSupport.class);
    private final MessageType messageType;
    private List<ColumnDescriptor> cols;
    private RecordConsumer recordConsumer;

    public CsvWriteSupport(MessageType messageType) {
        this.messageType = messageType;
        this.cols = messageType.getColumns();
    }

    @Override
    public WriteSupport.WriteContext init(Configuration configuration) {
        logger.info("Init write support");
        return new WriteSupport.WriteContext(messageType, new HashMap<>());
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
        logger.info("Prepare for write");
        this.recordConsumer = recordConsumer;
    }

    @Override
    public void write(List<String> values) {
        if (values.size() != cols.size()) {
            throw new ParquetEncodingException("Invalid input data. Expecting " +
                    cols.size() + " columns. Input had " + values.size() + " columns (" + cols + ") : " + values);
        }

        recordConsumer.startMessage();
        for (int i = 0; i < cols.size(); ++i) {
            String val = values.get(i);
            if (val.length() > 0) {
                recordConsumer.startField(cols.get(i).getPath()[0], i);
                switch (cols.get(i).getType()) {
                    case BOOLEAN:
                        recordConsumer.addBoolean(Boolean.parseBoolean(val));
                        break;
                    case FLOAT:
                        recordConsumer.addFloat(Float.parseFloat(val));
                        break;
                    case DOUBLE:
                        recordConsumer.addDouble(Double.parseDouble(val));
                        break;
                    case INT32:
                        recordConsumer.addInteger(Integer.parseInt(val));
                        break;
                    case INT64:
                        recordConsumer.addLong(Long.parseLong(val));
                        break;
                    case BINARY:
                        recordConsumer.addBinary(stringToBinary(val));
                        break;
                    default:
                        throw new ParquetEncodingException(
                                "Unsupported column type: " + cols.get(i).getType());
                }
                recordConsumer.endField(cols.get(i).getPath()[0], i);
            }
        }
        recordConsumer.endMessage();
    }

    private Binary stringToBinary(Object value) {
        return Binary.fromString(value.toString());
    }
}
