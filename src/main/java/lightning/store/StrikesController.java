package lightning.store;

import lightning.model.Strikes;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequiredArgsConstructor
public class StrikesController {
  private final StrikesRepository repository;

  @GetMapping("/strikes")
  public List<Strikes> getAllStrikes() {
    return repository.findAll();
  }
}
