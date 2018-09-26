package ru.gpb.als.streams.test.context;

import org.apache.avro.specific.SpecificRecordBase;

import static java.util.Objects.*;

/**
 * Functional interface for new objects generating.
 * @param <V> type for generating object.
 *
 * Created by Boris Zhguchev on 12/09/2018
 */
public interface ValueProducer<V> {
  V produce();

  @SuppressWarnings("unchecked")
  default Class<V> getValClass() {
    V v = produce();
    return (Class<V>)(isNull(v)? SpecificRecordBase.class:v.getClass());
  }

  ValueProducer<SpecificRecordBase> NULL_PRODUCER = ()->null;
}
