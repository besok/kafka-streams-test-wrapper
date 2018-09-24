package ru.gpb.als.streams.test.helpers.generators;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecordBase;

import java.util.List;

/**
 * Created by Boris Zhguchev on 24/09/2018
 */
public class RecordGenerator<R extends SpecificRecordBase> implements AvroGeneratorByType<R> {
  @Override
  public R generate(Schema schema) {
	try {
	  @SuppressWarnings("unchecked")
	  R instance = (R) Class.forName(schema.getFullName()).newInstance();
	  List<Schema.Field> fields = instance.getSchema().getFields();
	  for (Schema.Field f : fields) {
		Object resField = new CommonGenerator<>().generate(f.schema());
		instance.put(f.name(), resField);
	  }

	  return instance;

	} catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
	  e.printStackTrace();
	}
	return null;
  }
}
