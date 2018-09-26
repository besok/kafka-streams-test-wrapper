package ru.gpb.als.streams.test.context.generators;

import org.apache.avro.Schema;

import java.util.ArrayList;

/**
 * Field generator for Array @see {@link AvroGeneratorByType}
 *
 * @param <V> internal element type for ArrayList
 *
 * Created by Boris Zhguchev on 24/09/2018
 */
public class ArrayGenerator<V> implements AvroGeneratorByType<ArrayList<V>> {

  @Override
  public ArrayList<V> generate(Schema schema) {
	ArrayList<V> arrs = new ArrayList<>();
	Schema elSchema = schema.getElementType();
	@SuppressWarnings("unchecked")
	V objVal = (V) new ComplexGenerator<>().generate(elSchema);
	arrs.add(objVal);

	return arrs;
  }
}
