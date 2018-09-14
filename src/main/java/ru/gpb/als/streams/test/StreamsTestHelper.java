package ru.gpb.als.streams.test;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.streams.StreamsBuilder;
import ru.gpb.als.streams.test.helpers.StreamsTestHelperContext;

import java.util.Properties;

/**
 *
 * Major factory class for start working with this lib.
 *
 * This lib is constructed for pipe using:
 * Examples:
 * @<code>
 *
 *   StreamsTestHelper
 *             .run(builder, prop)
 *             .sender("stream", () -> newKey(), () -> newValue())
 *             .send(20)
 *             .pipe()
 *             .receiver().read();
 *
 *  StreamsTestHelper
 *             .run(builder, streamProp)
 *             .sender("stream_1", () -> null, () -> newCustomer())
 *             .send(20)
 *             .pipe()
 *             .keeper("ыекуфь_2", Tuple1.class, Customer.class)
 *             .find(H.wrap("1"));
 *
 * </code>
 * Created by Boris Zhguchev on 12/09/2018
 */
public interface StreamsTestHelper {

  /**
   * init method
   * @param streamsBuilder topology builder for starting KafkaStreams @see {@link StreamsBuilder}
   * @param streamsProperties property map for streams
   * @return @see {@link StreamsTestHelperContext}
   *
   * */
  static StreamsTestHelperContext run(StreamsBuilder streamsBuilder, Properties streamsProperties) {
    return new StreamsTestHelperContext(streamsBuilder, streamsProperties);
  }

}