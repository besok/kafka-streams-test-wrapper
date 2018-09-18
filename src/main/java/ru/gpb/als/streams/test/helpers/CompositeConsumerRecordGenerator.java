package ru.gpb.als.streams.test.helpers;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import ru.gpb.als.streams.test.helpers.generators.AvroGenerator;

import java.util.List;

/**
 * Created by Boris Zhguchev on 18/09/2018
 */
public class CompositeConsumerRecordGenerator<K extends SpecificRecordBase, V extends SpecificRecordBase> extends ConsumerRecordGenerator<K, V> {

  private AvroValueProducer<K> keyProducer;
  private AvroValueProducer<V> valProducer;


  @Override
  public ConsumerRecord<byte[], byte[]> generate() {
    return super.consumerRecordFactory.create(keyProducer.produce(), valProducer.produce());
  }

  public ConsumerRecord<K,V> last() {
    return new ConsumerRecord<>("",0,0,keyProducer.producedValue(), valProducer.producedValue());
  }

  @Override
  public CompositeConsumerRecordGenerator<K, V> send() {
    super.send();
    return this;
  }

  @Override
  public CompositeConsumerRecordGenerator<K, V> send(int batch) {
    super.send(batch);
    return this;
  }

  @Override
  public CompositeConsumerRecordGenerator<K, V> send(ConsumerRecord<byte[], byte[]> record) {
    super.send(record);
    return this;
  }

  @Override
  public CompositeConsumerRecordGenerator<K, V> send(List<ConsumerRecord<byte[], byte[]>> records) {
    super.send(records);
    return this;
  }

  protected CompositeConsumerRecordGenerator(
      String topic, ValueProducer<K> keyProducer, ValueProducer<V> valProducer, StreamsTestHelperContext ctx) {
    super(topic, keyProducer, valProducer, ctx);
    this.keyProducer = (AvroValueProducer<K>) keyProducer;
    this.valProducer = (AvroValueProducer<V>) valProducer;
  }
}
