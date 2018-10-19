package ru.besok.kafka.streams.test.data;// 2018.08.01

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.gpb.als.model.Country;
import ru.besok.kafka.streams.test.data.StreamsUtils.H;

import javax.annotation.PostConstruct;

import static org.apache.kafka.streams.kstream.Materialized.*;
import static ru.besok.kafka.streams.test.data.StreamsUtils.H.*;

/**
 * @author Boris Zhguchev
 */
@Slf4j
@Component
public class CountryWorker {

  @Autowired
  private StreamsBuilder builder;

  @PostConstruct
  public void process() {

    KStream<?, Country> countries = builder.stream("internal.country");

    countries
        .groupBy((k,v)->wrap(v.getAskId()))
        .aggregate(Country::new, H::lastWin,as("country_group_ask"))
        .toStream()
        .peek(StreamsUtils.mark("country-ask")::peek)
        .to("internal.country_group_by_ask");


  }


}

