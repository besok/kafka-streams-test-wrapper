package ru.gpb.als.streams.test;

import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import ru.gpb.als.streams.test.mock.MockConfig;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 *
 * Parent class for all tests using this lib.
 *
 *
 *
 * Created by Boris Zhguchev on 11/09/2018
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = MockConfig.class)
public class BaseStreamsTest {
  // FIXME: 9/28/2018 Написать тесты без спринга(онли юнит)
  // FIXME: 9/28/2018 Перенести все avro файлы внутрь приложения в тестовый блок

  /**
   * by default kafka can't do normal collaboration with Windows file system,
   * especially in cleaning case(before closing tests).
   * This method do it before tests started.
   * */
  @BeforeClass
  public static void cleanUpDir() throws IOException {
    Path p = Paths.get("/tmp/kafka-streams");
    if (Files.exists(p))
      Files
          .walk(p)
          .map(Path::toFile)
          .forEach(File::delete);
  }



}
