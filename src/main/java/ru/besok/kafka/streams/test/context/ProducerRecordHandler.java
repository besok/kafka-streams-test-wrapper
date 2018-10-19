package ru.besok.kafka.streams.test.context;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Input records handler. @see {@link ProducerRecord}
 * @param <K> key type for record @see {@link ProducerRecord}
 * @param <V> value type for record @see {@link ProducerRecord}
 *
 * This handler do all action for only one topic.
 *
 * Created by Boris Zhguchev on 12/09/2018
 */
public class ProducerRecordHandler<K extends SpecificRecordBase, V extends SpecificRecordBase> {
  private String topic;
  private Class<K> clzzKey;
  private Class<V> clzzVal;
  private StreamsTestHelperContext ctx;
  public ProducerRecordHandler(String topic, Class<K> clzzKey, Class<V> clzzVal,StreamsTestHelperContext ctx) {
    this.topic = topic;
    this.clzzKey = clzzKey;
    this.clzzVal = clzzVal;
    this.ctx=ctx;
  }


  /**
   *
   * read a new record from topic
   *
   * */
  public ProducerRecord<K, V> read() {
    return ctx.driver.readOutput(topic,
        ctx.serde(clzzKey, true).deserializer(),
        ctx.serde(clzzVal, false).deserializer()
    );
  }

  /**
   *
   * read batch of records from topic.
   *
   * @param batch batch size
   *
   * */
  public List<ProducerRecord<K, V>> read(int batch) {
    return IntStream.range(0, batch).mapToObj(v -> read()).collect(Collectors.toList());
  }

  /**
   *
   * method for returning to context and call other entities.
   *
   * */
  public StreamsTestHelperContext pipe(){
    return this.ctx;
  }
}
