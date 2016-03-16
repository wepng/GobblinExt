package xiaomei.gobblin.core.writer.partitioner;

import com.google.common.base.Optional;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.util.ForkOperatorUtils;
import gobblin.writer.partitioner.TimeBasedWriterPartitioner;
import org.json.JSONObject;
import java.util.List;

/**
 * Created by Lambor Ryan on 16/3/11.
 * Email: ruanchengfeng@xiaomei.com
 */
public class TimeBasedJsonWriterPartitioner extends TimeBasedWriterPartitioner<byte[]> {

    public static final String WRITER_PARTITION_COLUMNS = ConfigurationKeys.WRITER_PREFIX + ".partition.columns";

    private final Optional<List<String>> partitionColumns;

    public TimeBasedJsonWriterPartitioner(State state, int numBranches, int branchId) {
        super(state, numBranches, branchId);

        this.partitionColumns = getWriterPartitionColumns(state, numBranches, branchId);

    }

    private Optional<List<String>> getWriterPartitionColumns(State state, int numBranches, int branchId) {
        String propName = ForkOperatorUtils.getPropertyNameForBranch(WRITER_PARTITION_COLUMNS, numBranches, branchId);
        return state.contains(propName) ? Optional.of(state.getPropAsList(propName)) : Optional.<List<String>> absent();
    }

    /**
     *  Check if the partition column value is present and is a Long object. Otherwise, use current system time.
     */
    private long getRecordTimestamp(Optional<Object> writerPartitionColumnValue) {

        return writerPartitionColumnValue.orNull()  instanceof Long ? (Long) writerPartitionColumnValue.get()
                : System.currentTimeMillis();
    }

    @Override
    public long getRecordTimestamp(byte[] record) {

        return getRecordTimestamp(getWriterPartitionColumnValue(record));
    }


    /**
     * Retrieve the value of the partition column field specified by this.partitionColumns
     */
    private Optional<Object> getWriterPartitionColumnValue(byte[] record){
        if (!this.partitionColumns.isPresent()) {
            return Optional.absent();
        }

        Optional<Object> fieldValue = Optional.absent();

        for (String partitionColumn : this.partitionColumns.get()) {
            JSONObject jsonObject = new JSONObject(new String(record));
            fieldValue = Optional.of(jsonObject.get(partitionColumn));
            if(fieldValue.isPresent()){
                return fieldValue;
            }
        }

        return fieldValue;
    }
}