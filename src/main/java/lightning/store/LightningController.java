package lightning.store;

import lightning.model.Lightning;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Sort;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequiredArgsConstructor
public class LightningController {
  private final LightningRepository repository;

  @GetMapping("/lightning")
  public List<Lightning> getAllLightning(@RequestParam(required = false) Long timestamp) {
    if (timestamp != null) {
      return repository.findByTimestampGreaterThan(timestamp);
    }
    return repository.findAll(Sort.by("timestamp").descending());
  }

}
