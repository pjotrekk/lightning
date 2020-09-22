package lightning;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@EnableAsync
public class LightningStoreApplication {
  public static void main(String[] args) {
    SpringApplication.run(LightningStoreApplication.class, args);
  }
}
