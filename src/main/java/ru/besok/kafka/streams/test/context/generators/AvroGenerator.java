package ru.besok.kafka.streams.test.context.generators;

/**
 * Generator for avro classes
 * Created by Boris Zhguchev on 18/09/2018
 */
public interface AvroGenerator<T> {

  T generate();

}
