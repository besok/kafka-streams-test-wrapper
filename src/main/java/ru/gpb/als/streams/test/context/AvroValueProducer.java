package ru.gpb.als.streams.test.context;

import org.apache.avro.specific.SpecificRecordBase;
import ru.gpb.als.streams.test.context.generators.AvroGeneratorImpl;
import ru.gpb.als.streams.test.context.generators.FieldUpdater;
import ru.gpb.als.streams.test.context.generators.FieldUpdaterPredicate;


/**
 * Default avro producer for avro values. @see {@link ValueProducer}
 *
 * @param <V> value type
 *
 * Created by Boris Zhguchev on 18/09/2018
 */
public class AvroValueProducer<V extends SpecificRecordBase> implements ValueProducer<V> {

  private AvroGeneratorImpl<V> generator;

  public AvroValueProducer(Class<V> genClass) {
	generator = new AvroGeneratorImpl<>(genClass);
  }

  @Override
  public V produce() {
	return generator.generate();
  }

  /**
   * latest sending value
   * */
  public V producedValue() {
	return generator.generatedValue();
  }


  /**
   * add rule for needed avro generator
   *
   * @param <F> type for generated value(field type)
   * @param predicate @see {@link FieldUpdaterPredicate}
   * @param updater @see {@link FieldUpdater}
   *
   * @return this
   * */
  public<F> AvroValueProducer<V> rule(FieldUpdaterPredicate predicate, FieldUpdater<F> updater) {
	generator.rule(predicate, updater);
	return this;
  }
}
