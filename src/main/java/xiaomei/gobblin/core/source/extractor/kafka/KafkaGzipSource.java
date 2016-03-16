package xiaomei.gobblin.core.source.extractor.kafka;

import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.extract.kafka.KafkaSource;

import java.io.IOException;

/**
 * Created by Lambor Ryan on 16/3/11.
 * Email: ruanchengfeng@xiaomei.com
 */
public class KafkaGzipSource extends KafkaSource<String, byte[]> {
    public Extractor<String, byte[]> getExtractor(WorkUnitState workUnitState) throws IOException {
        return new KafkaGzipExtractor(workUnitState);
    }
}
