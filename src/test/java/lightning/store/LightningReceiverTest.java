package lightning.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import lightning.model.Coordinates;
import lightning.model.Lightning;
import lightning.model.Strikes;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.Environment;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;

@RunWith(SpringRunner.class)
@ContextConfiguration(
    classes = {LightningRepository.class, MongoConfig.class, StrikesRepository.class},
    initializers = {LightningReceiverTest.Initializer.class})
public class LightningReceiverTest {
  private static final String KAFKA_TOPIC = "lightning_topic";
  private static final int KAFKA_LIMIT = 100;
  private static final long NOW = Instant.now().toEpochMilli();

  @Autowired
  LightningRepository lightningRepository;

  @Autowired
  StrikesRepository strikesRepository;

  @Autowired
  Environment environment;

  @ClassRule
  public static KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.5.2"));

  @ClassRule
  public static MongoDBContainer mongoDbContainer = new MongoDBContainer(DockerImageName.parse("mongo:4.0.10"));

  private final ObjectMapper objectMapper = new ObjectMapper();

  static class Initializer
      implements ApplicationContextInitializer<ConfigurableApplicationContext> {
    public void initialize(ConfigurableApplicationContext configurableApplicationContext) {
      TestPropertyValues.of(
          "mongo.host=" + mongoDbContainer.getReplicaSetUrl().replace("/test", ""),
          "mongo.database=test",
          "mongo.collection.lightnings=lightnings",
          "mongo.collection.strikes=strikes",
          "kafka.bootstrap.servers=" + kafkaContainer.getBootstrapServers(),
          "kafka.topic=" + KAFKA_TOPIC,
          "kafka.auto.offset.reset=earliest",
          "kafka.limit=" + KAFKA_LIMIT,
          "beam.window.size=60"
      ).applyTo(configurableApplicationContext.getEnvironment());
    }
  }

  @Test
  public void testWritesFromKafkaToMongo() {
    LightningReceiver lightningReceiver = new LightningReceiver(environment, pipelineOptions(), null);

    sendRecordsToKafka(kafkaContainer.getBootstrapServers());

    lightningReceiver.lightningStreaming().waitUntilFinish();

    List<Lightning> lightnings = lightningRepository.findAll();

    Lightning[] expectedLightnings = Stream.iterate(0, i -> ++i)
        .limit(KAFKA_LIMIT)
        .map(i -> Lightning.builder()
            .coordinates(Coordinates.of(i, i))
            .power(i)
            .timestamp(NOW)
            .strokeTheGround(i % 2 == 0)
            .build())
        .toArray(Lightning[]::new);

    assertThat(lightnings, containsInAnyOrder(expectedLightnings));

    List<Strikes> strikes = strikesRepository.findAll();
    assertThat(strikes, hasSize(greaterThan(0)));
  }

  private void sendRecordsToKafka(String bootstrapServers) {
    KafkaProducer<Long, String> producer = createProducer(bootstrapServers);
    Stream.iterate(0, i -> ++i)
        .limit(KAFKA_LIMIT)
        .forEach(i -> {
          try {
            producer.send(createRecord(i)).get();
          } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
          }
        });
  }

  private ProducerRecord<Long, String> createRecord(long i) {
    Lightning lightning = Lightning.builder()
        .coordinates(Coordinates.of(i, i))
        .power(i)
        .timestamp(NOW)
        .strokeTheGround(i % 2 == 0)
        .build();
    String jsonLightning;
    try {
      jsonLightning = objectMapper.writeValueAsString(lightning);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return new ProducerRecord<>(KAFKA_TOPIC, null, lightning.getTimestamp(), i, jsonLightning);
  }

  private KafkaProducer<Long, String> createProducer(String bootstrapServers) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "lightning_test_client");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    return new KafkaProducer<>(props);
  }

  private PipelineOptions pipelineOptions() {
    PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(FlinkRunner.class);
    return options;
  }
}
