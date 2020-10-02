package lightning.emulator;

import lightning.model.Coordinates;
import lightning.model.Lightning;

import java.util.concurrent.atomic.AtomicInteger;

public class FakeGenerator extends LightningGenerator {
  private final AtomicInteger counter = new AtomicInteger();

  public Lightning generateLightning(int i) {
    return Lightning.builder()
        .coordinates(Coordinates.of(i % (MAX_LONGITUDE * 2) - MAX_LONGITUDE, i % (MAX_LATITUDE * 2) - MAX_LATITUDE))
        .strokeTheGround(i % 2 == 0)
        .power((i + 1) * 1_000_000_000L)
        .timestamp(i + 20)
        .build();
  }

  @Override
  public Lightning generateLightning() {
    return generateLightning(counter.getAndIncrement());
  }
}
