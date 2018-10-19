package ru.besok.kafka.streams.test.context.generators;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecordBase;

import java.util.*;


/**
 *
 * AvroGenerator implementation @see {@link AvroGenerator}
 *
 * @param <T> record or field type. It should be inherited @see {@link SpecificRecordBase}
 *
 * Created by Boris Zhguchev on 18/09/2018
 */
public class AvroGeneratorImpl<T extends SpecificRecordBase> implements AvroGenerator<T> {

  protected Schema schema;
  protected T instance;

  private Set<FieldUpdateHandler<?>> updaters;


  public AvroGeneratorImpl(Class<T> clzz) {

	try {
	  this.instance = clzz.newInstance();
	  this.schema = this.instance.getSchema();
	  this.updaters = new LinkedHashSet<>();
	} catch (InstantiationException | IllegalAccessException e) {
	  e.printStackTrace();
	}

  }


  @Override
  public T generate() {
	List<Schema.Field> fields = schema.getFields();
	for (Schema.Field field : fields) {
	  Object fieldVal = new ComplexGenerator<>().generate(field.schema());
	  try {
	  instance.put(field.name(), combine(field, fieldVal));
	  }catch (ClassCastException ex){
	    throw new GeneratorException(
	      " ClassCastException for field[ " + field.name()
			+ " ] => expected type["+field.schema().getType().getName()+"]",ex);
	  }
	}

	return instance;
  }

  @SuppressWarnings("unchecked")
  private Object combine(Schema.Field field, Object fieldValue) {
	Object tempValue = fieldValue;
	for (FieldUpdateHandler handler : updaters) {
	  tempValue = handler.updateIf(field, tempValue);
	}
	return tempValue;
  }


  /**
   * latest generated value
   * @return previous {@link #combine(Schema.Field, Object)} invocation.
   * */
  public T generatedValue() {
	return instance;
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
  @SuppressWarnings("unchecked")
  public<F> void rule(FieldUpdaterPredicate predicate, FieldUpdater<F> updater) {
    updaters.add(new FieldUpdateHandler(predicate,updater));
  }

  private class FieldUpdateHandler<V> {

    private FieldUpdaterPredicate fieldPredicate;
	private FieldUpdater<V> fieldUpdater;

	public FieldUpdateHandler(FieldUpdaterPredicate fieldPredicate, FieldUpdater<V> fieldUpdater) {
	  this.fieldPredicate = fieldPredicate;
	  this.fieldUpdater = fieldUpdater;
	}

	public V updateIf(Schema.Field field,V oldVal) {
	  if(fieldPredicate.test(field))
	    return fieldUpdater.update(oldVal);
	  return oldVal;
	}
  }
}
