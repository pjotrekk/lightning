package lightning.store;

import lightning.model.Lightning;
import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.List;

public interface LightningRepository extends MongoRepository<Lightning, String> {
  List<Lightning> findByTimestampGreaterThan(long timestamp);
}
