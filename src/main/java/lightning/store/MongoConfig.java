package lightning.store;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.data.mongodb.config.AbstractMongoClientConfiguration;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;

@Configuration
@EnableMongoRepositories
@RequiredArgsConstructor
public class MongoConfig extends AbstractMongoClientConfiguration {
  private final Environment env;

  @Override
  protected String getDatabaseName() {
    return env.getProperty("mongo.database");
  }

  @Override
  public MongoClient mongoClient() {
    ConnectionString connectionString = new ConnectionString(
        String.format("%s/%s", env.getProperty("mongo.host"), env.getProperty("mongo.database")));
    MongoClientSettings mongoClientSettings = MongoClientSettings.builder()
        .applyConnectionString(connectionString)
        .build();

    return MongoClients.create(mongoClientSettings);
  }
}