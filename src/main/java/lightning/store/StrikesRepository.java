package lightning.store;

import lightning.model.Strikes;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface StrikesRepository extends MongoRepository<Strikes, String> { }
