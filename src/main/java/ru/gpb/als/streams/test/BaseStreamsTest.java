package ru.gpb.als.streams.test;

import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import ru.gpb.als.streams.test.configuration.TestConfig;

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
@SpringBootTest(classes = TestConfig.class)
public class BaseStreamsTest {


  /**
   * by default kafka can't do normal collaboration with Windows file system,
   * especially in cleaning case(before closing tests).
   * This method do it before tests started.
   * */
  @BeforeClass
  public static void cleanUp() throws IOException {
    Path p = Paths.get("/tmp/kafka-streams");
    if (Files.exists(p))
      Files
          .walk(p)
          .map(Path::toFile)
          .forEach(File::delete);
  }



}
