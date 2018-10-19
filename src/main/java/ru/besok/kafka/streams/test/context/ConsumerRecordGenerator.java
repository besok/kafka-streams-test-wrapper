package ru.besok.kafka.streams.test.context;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.test.ConsumerRecordFactory;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 *
 * Records AvroGenerator. It is based on @see {@link ConsumerRecordGenerator}
 * @param <K> key type for record @see {@link ConsumerRecord}
 * @param <V> value type for record @see {@link ConsumerRecord}
 *
 * This generator do all action for only one topic.
 *
 * @see StreamsTestHelperContext
 *
 * Created by Boris Zhguchev on 12/09/2018
 */
public class ConsumerRecordGenerator<K extends SpecificRecordBase, V extends SpecificRecordBase> {
  private ValueProducer<K> keyProducer;
  private ValueProducer<V> valProducer;
  private StreamsTestHelperContext ctx;
  protected String topic;
  protected ConsumerRecordFactory<K, V> consumerRecordFactory;

  protected String getTopic() {
    return topic;
  }

  protected ValueProducer<K> getKeyProducer() {
    return keyProducer;
  }

  protected ValueProducer<V> getValProducer() {
    return valProducer;
  }

  protected ConsumerRecordGenerator(String topic,
                                    ValueProducer<K> keyProducer,
                                    ValueProducer<V> valProducer,
                                    StreamsTestHelperContext ctx
                                 ) {
    this.keyProducer = keyProducer;
    this.valProducer = valProducer;
    this.topic = topic;
    this.consumerRecordFactory = new ConsumerRecordFactory<>(
        topic,
        ctx.serde(keyProducer.getValClass(), true).serializer(),
        ctx.serde(valProducer.getValClass(), false).serializer()
    );
    this.ctx = ctx;
  }


  /**
   * generate new record. It based on key/val producer @see {@link ValueProducer}
   *
   * */
  public ConsumerRecord<byte[], byte[]> generate() {
    return consumerRecordFactory.create(keyProducer.produce(), valProducer.produce());
  }

  /**
   * generate a batch of new records. It based on key/val producer @see {@link ValueProducer}
   * @param batch batch size
   * */
  public List<ConsumerRecord<byte[], byte[]>> generate(int batch) {
    return IntStream.range(0, batch).mapToObj(i -> this.generate()).collect(Collectors.toList());
  }


  /**
   * generate a new record and sent it  to topic. @see {@link org.apache.kafka.streams.TopologyTestDriver}
   *
   * **/
  public ConsumerRecordGenerator<K, V> send() {
    ctx.driver.pipeInput(generate());
    return this;
  }


  /**
   * generate a batch of new records and sent them to topic. @see {@link org.apache.kafka.streams.TopologyTestDriver}
   *
   * **/
  public ConsumerRecordGenerator<K, V> send(int batch) {
    ctx.driver.pipeInput(generate(batch));
    return this;
  }

/**
 * sent new record to topic. @see {@link org.apache.kafka.streams.TopologyTestDriver}
 * */
  public ConsumerRecordGenerator<K, V> send(ConsumerRecord<byte[], byte[]> record) {
    ctx.driver.pipeInput(record);
    return this;
  }
  /**
 * sent new records to topic. @see {@link org.apache.kafka.streams.TopologyTestDriver}
 * */
  public ConsumerRecordGenerator<K, V> send(List<ConsumerRecord<byte[], byte[]>> records) {
    ctx.driver.pipeInput(records);
    return this;
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
