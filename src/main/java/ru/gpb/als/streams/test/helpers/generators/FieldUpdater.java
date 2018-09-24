package ru.gpb.als.streams.test.helpers.generators;

import org.apache.avro.Schema;

import java.util.function.Predicate;

/**
 * Interface for updating returned field
 * @param <V> field type
 *
 * Created by Boris Zhguchev on 24/09/2018
 */
public interface FieldUpdater<V> {

  /**
   * @param oldVal value getting from generators
   * @return newVal generated this method. It can be a new value or modified the old value.
   * */
  V update(V oldVal);

  /**
   * Simplify development style for plain cases.
   *
   * */
  static  <V> FieldUpdater<V> through(V val){
    return oldVal -> val;
  }

}
