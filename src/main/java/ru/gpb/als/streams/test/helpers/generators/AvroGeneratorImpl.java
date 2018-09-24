package ru.gpb.als.streams.test.helpers.generators;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecordBase;

import java.util.*;
import java.util.function.Predicate;


/**
 * Created by Boris Zhguchev on 18/09/2018
 */
public class AvroGeneratorImpl<T extends SpecificRecordBase> implements AvroGenerator<T> {

  protected Schema schema;
  protected T instance;

  private Set<FieldUpdateHandler> updaters;


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
	  Object fieldVal = new CommonGenerator<>().generate(field.schema());
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


  public T generatedValue() {
	return instance;
  }

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
