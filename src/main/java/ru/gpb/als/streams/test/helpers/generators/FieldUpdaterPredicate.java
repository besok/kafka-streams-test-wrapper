package ru.gpb.als.streams.test.helpers.generators;

import org.apache.avro.Schema;

/**
 * Created by Boris Zhguchev on 24/09/2018
 */
public interface FieldUpdaterPredicate {

  boolean test(Schema.Field field);


  static FieldUpdaterPredicate name(String name){
    return field -> field.name().equals(name);
  }



}
