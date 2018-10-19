package ru.besok.kafka.streams.test.integration;

import org.junit.Test;
import ru.gpb.als.model.Country;
import ru.besok.kafka.streams.test.context.generators.AvroGeneratorImpl;

import static java.lang.String.join;
import static java.lang.String.valueOf;
import static org.junit.Assert.*;
import static ru.besok.kafka.streams.test.context.generators.FieldUpdater.*;
import static ru.besok.kafka.streams.test.context.generators.FieldUpdaterPredicate.*;

/**
 * Created by Boris Zhguchev on 18/09/2018
 */
public class GenerateAvroTest {

  @Test
  public void rulesTest() {

	AvroGeneratorImpl<Country> g = new AvroGeneratorImpl<>(Country.class);

	g.rule(name("ask_id"), constant(1));
	g.rule(name("name"), constant("name"));
	g.rule(name("sadko_id"), constant(10));
	g.rule(name("id"), constant(0));

	Country generatedCountry = g.generate();
	Country expectedCountry = Country.newBuilder().setId(0).setAskId(1).setSadkoId(10).setName("name").build();

	assertEquals(expectedCountry,generatedCountry);
  }


}
