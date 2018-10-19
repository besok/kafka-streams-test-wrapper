package ru.besok.kafka.streams.test.integration;

import org.apache.kafka.streams.StreamsBuilder;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import ru.besok.kafka.streams.test.BaseStreamsTest;

import java.util.Properties;

/**
 * Created by Boris Zhguchev on 26/09/2018
 */
public class TopologyTest extends BaseStreamsTest {
  @Autowired
  private StreamsBuilder b;
  @Autowired
  @Qualifier("streams")
  private Properties p;


  @Test
  public void test() {


  }
}
