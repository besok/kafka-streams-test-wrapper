package ru.gpb.als.streams.test.helpers.generators;

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
