package ru.gpb.als.streams.test.helpers;

import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificRecordBase;
import ru.gpb.als.streams.test.helpers.generators.AvroGenerator;

/**
 * Created by Boris Zhguchev on 18/09/2018
 */
public class AvroValueProducer<V extends SpecificRecordBase> implements ValueProducer<V> {

  private AvroGenerator<V> generator;

  public AvroValueProducer(Class<V> genClass) {
    generator = new AvroGenerator<>(genClass);
  }

  @Override
  public V produce() {
    return generator.generate();
  }

  public V producedValue(){
    return generator.generatedValue();
  }

  public AvroGenerator<V> getGenerator() {
    return generator;
  }

  public void setGenerator(AvroGenerator<V> generator) {
    this.generator = generator;
  }
}
