package ru.besok.kafka.streams.test.data;// 2018.08.01

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ru.gpb.als.model.Customer;
import ru.gpb.als.model.Tuple1;
import ru.gpb.als.model.collectors.CountryCustomerCollector;

import javax.annotation.PostConstruct;
import java.util.function.Function;


/**
 * @author Boris Zhguchev
 */
@Slf4j
@Service
public class CustomerWorker {

  private String input = "internal.customer";

  @Autowired
  private StreamsBuilder builder;

  @PostConstruct
  public void process() {

    KStream<?, Customer> customers = builder.stream("internal.customer");

    customers
        .groupBy(StreamsUtils.field("inn")::wrap)
        .aggregate(Customer::new, StreamsUtils.H::lastWin)
        .toStream()
        .peek(StreamsUtils.mark("new-customer-inn")::peek)
        .to("internal.customer_group_by_inn");

    customers
        .map(StreamsUtils.field(askId())::newKVFromV)
        .groupByKey()
        .aggregate(Customer::new, StreamsUtils.H::lastWin)
        .toStream()
        .peek(StreamsUtils.mark("new-customer-ask")::peek)
        .to("internal.customer_group_by_ask");

    customers
        .map(StreamsUtils.field(countryId())::newKVFromV)
        .groupByKey()
        .aggregate(this::createCollector, this::aggregateByCountry)
        .toStream()
        .peek(StreamsUtils.mark("new-customer-country")::peek)
        .to("internal.customers_group_by_country");

    customers
        .map(StreamsUtils.field(id())::newKVFromV)
        .groupByKey()
        .aggregate(Customer::new, StreamsUtils.H::lastWin)
        .toStream()
        .peek(StreamsUtils.mark("new-customer-id")::peek)
        .to("internal.customers_group_by_id");

  }

  private CountryCustomerCollector aggregateByCountry(Tuple1 key, Customer value, CountryCustomerCollector aggregate) {
    aggregate.setKey(key);
    aggregate.getValues().add(value);
    aggregate.setProperty("ru.gpb.als.streams.test.data.CustomerWorker ");
    return aggregate;
  }

  private Function<Customer, String> askId() {
    return (Customer c) -> c.getAskId().toString();
  }

  private Function<Customer, String> countryId() {
    return (Customer c) -> c.getCountryId().toString();
  }

  private Function<Customer, String> id() {
    return (Customer c) -> c.getId().toString();
  }

  private CountryCustomerCollector createCollector() {
    return CountryCustomerCollector.newBuilder().build();
  }

}

