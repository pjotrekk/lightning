package lightning.emulator;

import lightning.model.Coordinates;
import lightning.model.Lightning;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.time.Instant;
import java.util.Random;

@Component
public class LightningGenerator implements Serializable {
  protected static final int MAX_LONGITUDE = 180;
  protected static final int MAX_LATITUDE = 90;

  private final Random random = new Random();

  public Lightning generateLightning() {
    return Lightning.builder()
        .coordinates(generateCoordinates())
        .struckTheGround(random.nextBoolean())
        .power(random.nextLong())
        .timestamp(Instant.now().toEpochMilli())
        .build();
  }

  private Coordinates generateCoordinates() {
    double longitude = random.nextDouble() * 2 * MAX_LONGITUDE - MAX_LONGITUDE;
    double latitude = random.nextDouble() * 2 * MAX_LATITUDE - MAX_LATITUDE;
    return Coordinates.of(longitude, latitude);
  }
}
