package ru.gpb.als.streams.test.helpers;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecordBase;
import ru.gpb.als.streams.test.helpers.generators.AvroGeneratorImpl;
import ru.gpb.als.streams.test.helpers.generators.FieldUpdater;
import ru.gpb.als.streams.test.helpers.generators.FieldUpdaterPredicate;

import java.util.function.Predicate;

/**
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

  public V producedValue() {
	return generator.generatedValue();
  }

  public AvroGeneratorImpl<V> getGenerator() {
	return generator;
  }

  public void setGenerator(AvroGeneratorImpl<V> generator) {
	this.generator = generator;
  }

  public<F> AvroValueProducer<V> rule(FieldUpdaterPredicate predicate, FieldUpdater<F> updater) {
	generator.rule(predicate, updater);
	return this;
  }
}
