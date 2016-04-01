package xiaomei.gobblin.core.converter;

import com.google.gson.JsonObject;
import gobblin.configuration.WorkUnitState;
import gobblin.converter.Converter;
import gobblin.converter.DataConversionException;
import gobblin.converter.SchemaConversionException;
import gobblin.converter.SingleRecordIterable;

/**
 * Created by Lambor Ryan on 16/4/1.
 * Email: ruanchengfeng@xiaomei.com
 */
public class JsonToBytesConverter extends Converter<Object, Object, JsonObject, byte[]> {


    @Override
    public Object convertSchema(Object inputSchema, WorkUnitState workUnit) throws SchemaConversionException {
        return inputSchema;
    }

    @Override
    public Iterable<byte[]> convertRecord(Object outputSchema, JsonObject inputRecord, WorkUnitState workUnit) throws DataConversionException {
        return new SingleRecordIterable<byte[]>(inputRecord.toString().getBytes());
    }
}
