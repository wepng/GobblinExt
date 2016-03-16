package xiaomei.gobblin.core.source.extractor.kafka;

import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.extract.kafka.KafkaExtractor;
import kafka.message.MessageAndOffset;
import xiaomei.gobblin.core.util.Decompress;

import java.io.IOException;

/**
 * Created by Lambor Ryan on 16/3/11.
 * Email: ruanchengfeng@xiaomei.com
 */
public class KafkaGzipExtractor extends KafkaExtractor<String, byte[]> {

    public KafkaGzipExtractor(WorkUnitState state) {
        super(state);
    }

    @Override
    protected byte[] decodeRecord(MessageAndOffset messageAndOffset) throws IOException {
        return Decompress.decompressByGzip(getBytes(messageAndOffset.message().payload()));
    }

    @Override
    public String getSchema() throws IOException {
        return this.topicName;
    }
}
