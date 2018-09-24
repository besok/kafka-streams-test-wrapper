package ru.gpb.als.streams.test.helpers.generators;

import org.apache.avro.Schema;

/**
 * Created by Boris Zhguchev on 24/09/2018
 */
public class CommonGenerator<V> implements AvroGeneratorByType<V>{

  @Override
  @SuppressWarnings("unchecked")
  public V generate(Schema schema) {
	switch (schema.getType()) {
	  case STRING: return (V) new StringGenerator().generate(schema);
	  case UNION: return (V) new UnionGenerator().generate(schema);
	  case INT: return (V) new IntGenerator().generate(schema);
	  case ARRAY: return (V) new ArrayGenerator<>().generate(schema);
	  case MAP: return (V) new MapGenerator<>().generate(schema);
	  case ENUM: return (V) new EnumGenerator<>().generate(schema);
	  case LONG: return (V) new LongGenerator().generate(schema);
	  case NULL: return (V) new NullGenerator().generate(schema);
	  case BOOLEAN: return (V) new BooleanGenerator().generate(schema);
	  case BYTES: return (V) new BytesGenerator().generate(schema);
	  case FLOAT: return (V) new FloatGenerator().generate(schema);
	  case DOUBLE: return (V) new DoubleGenerator().generate(schema);
	  case RECORD: return (V) new RecordGenerator<>().generate(schema);
	  default:
		return null;
	}
  }
}
