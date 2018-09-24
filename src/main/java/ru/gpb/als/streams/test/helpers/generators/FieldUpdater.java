package ru.gpb.als.streams.test.helpers.generators;

import org.apache.avro.Schema;

import java.util.function.Predicate;

/**
 * Created by Boris Zhguchev on 24/09/2018
 */
public interface FieldUpdater<V> {

  V update(V oldVal);


}
