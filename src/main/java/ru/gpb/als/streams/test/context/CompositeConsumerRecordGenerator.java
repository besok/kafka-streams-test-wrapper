package ru.gpb.als.streams.test.context;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import ru.gpb.als.streams.test.context.generators.FieldUpdater;
import ru.gpb.als.streams.test.context.generators.FieldUpdaterPredicate;

import java.util.List;

/**
 *
 * Specified @see {@link ConsumerRecordGenerator} for generating values from @see {@link AvroValueProducer}
 *
 * @param <K> key type
 * @param <V> value type
 *
 * Created by Boris Zhguchev on 18/09/2018
 */
public class CompositeConsumerRecordGenerator<K extends SpecificRecordBase, V extends SpecificRecordBase> extends ConsumerRecordGenerator<K, V> {

  private AvroValueProducer<K> keyProducer;
  private AvroValueProducer<V> valProducer;


  protected CompositeConsumerRecordGenerator(
	String topic, ValueProducer<K> keyProducer, ValueProducer<V> valProducer, StreamsTestHelperContext ctx) {
	super(topic, keyProducer, valProducer, ctx);
	this.keyProducer = (AvroValueProducer<K>) keyProducer;
	this.valProducer = (AvroValueProducer<V>) valProducer;
  }

  @Override
  public ConsumerRecord<byte[], byte[]> generate() {
	return super.consumerRecordFactory.create(keyProducer.produce(), valProducer.produce());
  }

  public ConsumerRecord<K, V> last() {
	return new ConsumerRecord<>("", 0, 0, keyProducer.producedValue(), valProducer.producedValue());
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

  /**
   * add rule for needed avro generator
   *
   * @param <F> type for generated value(field type)
   * @param predicate @see {@link FieldUpdaterPredicate}
   * @param updater @see {@link FieldUpdater}
   * @param isKey define needed generator
   * @return this
   * */
  public<F> CompositeConsumerRecordGenerator<K, V> rule(FieldUpdaterPredicate predicate, FieldUpdater<F> updater, boolean isKey) {
	if (isKey) {
	  keyProducer.rule(predicate, updater);
	} else {
	  valProducer.rule(predicate, updater);
	}
	return this;
  }
}
