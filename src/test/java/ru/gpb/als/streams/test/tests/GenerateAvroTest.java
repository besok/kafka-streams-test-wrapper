package ru.gpb.als.streams.test.tests;

import org.junit.Assert;
import org.junit.Test;
import ru.gpb.als.model.Country;
import ru.gpb.als.streams.test.helpers.generators.AvroGeneratorImpl;
import ru.gpb.als.streams.test.helpers.generators.FieldUpdater;
import ru.gpb.als.streams.test.helpers.generators.FieldUpdaterPredicate;

import static java.lang.String.join;
import static java.lang.String.valueOf;
import static org.junit.Assert.*;
import static ru.gpb.als.streams.test.helpers.generators.FieldUpdaterPredicate.*;

/**
 * Created by Boris Zhguchev on 18/09/2018
 */
public class GenerateAvroTest {

  @Test
  public void test() {

	AvroGeneratorImpl<Country> g = new AvroGeneratorImpl<>(Country.class);

	g.rule(name("ask_id"), oldVal -> 1);
	g.rule(name("name"), oldVal -> "name");
	g.rule(name("sadko_id"), oldVal -> 10);
	g.rule(name("id"), oldVal -> 0);

	Country generatedCountry = g.generate();
	Country expectedCountry = Country.newBuilder().setId(0).setAskId(1).setSadkoId(10).setName("name").build();

	assertEquals(expectedCountry,generatedCountry);
  }


}
