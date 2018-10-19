package ru.besok.kafka.streams.test.context.generators;

/**
 *
 * Exception for generating events
 *
 * Created by Boris Zhguchev on 24/09/2018
 */
public class GeneratorException extends RuntimeException {

  public GeneratorException(String message, RuntimeException ex) {
	super(message,ex);
  }
}
