package ru.gpb.als.streams.test.context.generators;

import org.apache.avro.Schema;

/**
 * Predicate for checking needed fields. If the check is right it will be process @see {@link FieldUpdater#update(Object)}
 *
 * Created by Boris Zhguchev on 24/09/2018
 */
public interface FieldUpdaterPredicate {

  /**
   * method checks field condition
   * @param field schema field
   *
   * */
  boolean test(Schema.Field field);

/**
 * simplify some patterns
 * */
  static FieldUpdaterPredicate name(String name){
    return field -> field.name().equals(name);
  }



}
