package ru.gpb.als.streams.test.helpers;

import org.apache.avro.specific.SpecificRecord;

import java.util.Objects;

import static java.util.Objects.*;

/**
 * Functional interface for new objects generating.
 * @param <V> type for generating object.
 * Created by Boris Zhguchev on 12/09/2018
 */
public interface ValueProducer<V> {
  V produce();

  @SuppressWarnings("unchecked")
  default Class<V> getValClass() {
    V v = produce();
    return (Class<V>)(isNull(v)? SpecificRecord.class:v.getClass());
  }

  ValueProducer<SpecificRecord> NULL_PRODUCER = ()->null;
}
