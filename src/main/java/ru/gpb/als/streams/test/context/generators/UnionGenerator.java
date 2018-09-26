package ru.gpb.als.streams.test.context.generators;

import org.apache.avro.Schema;

/**
 *
 * Field generator for Union @see {@link AvroGeneratorByType}
 * Created by Boris Zhguchev on 24/09/2018
 */
public class UnionGenerator implements AvroGeneratorByType<Object> {

  /**
   * It's option wrapper for other types @see Schema.Type#UNION
   *
   * @return can return null value
   * */
  @Override
  public Object generate(Schema schema) {
	Schema elSchema = schema.getTypes().get(1);
	return new ComplexGenerator<>().generate(elSchema);
  }

}
