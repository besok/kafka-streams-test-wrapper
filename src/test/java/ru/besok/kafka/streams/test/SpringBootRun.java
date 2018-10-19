package ru.besok.kafka.streams.test;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Configuration;

/**
 * Created by Boris Zhguchev on 18/09/2018
 */

@Configuration
@SpringBootApplication
public class SpringBootRun {
  public static void main(String[] args) {
    SpringApplication.run(SpringBootRun.class);
  }
}
