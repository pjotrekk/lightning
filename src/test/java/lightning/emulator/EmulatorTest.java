package lightning.emulator;

import com.fasterxml.jackson.databind.ObjectMapper;
import lightning.model.Lightning;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.core.env.Environment;
import org.springframework.mock.env.MockEnvironment;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;

public class EmulatorTest {
  @Rule
  public KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.5.2"));

  private final ObjectMapper objectMapper = new ObjectMapper();
  private final FakeGenerator generator = new FakeGenerator();

  private static final String KAFKA_TOPIC = "test_lightning_topic";

  @Test
  public void testWritesToKafkaFixedNumberOfRecords() {
    Environment env = mockEnvironment(kafkaContainer.getBootstrapServers());
    LightningEmulator emulator = new LightningEmulator(env, generator, pipelineOptions(), null);
    emulator.sendLightningData().waitUntilFinish();
    Consumer<Long, String> consumer = createConsumer();
    ConsumerRecords<Long, String> records = consumer.poll(Duration.ofMinutes(2));

    List<ConsumerRecord<Long, String>> recordList = ImmutableList.copyOf(records.iterator());
    assertThat(recordList, hasSize(2));

    List<Long> keyList = recordList.stream()
        .sorted(Comparator.comparingLong(ConsumerRecord::key))
        .map(ConsumerRecord::key)
        .collect(toList());

    assertThat(keyList, contains(0L, 1L));

    List<Lightning> lightningList = parseRecordToLightningList(recordList);
    assertThat(lightningList, contains(generator.generateLightning(0), generator.generateLightning(1)));
  }

  private List<Lightning> parseRecordToLightningList(List<ConsumerRecord<Long, String>> records) {
    return records.stream().sorted(Comparator.comparingLong(ConsumerRecord::key)).map(record -> {
      try {
        return objectMapper.readValue(record.value(), Lightning.class);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }).collect(Collectors.toList());
  }

  private Consumer<Long, String> createConsumer() {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "lightning_group");
    KafkaConsumer<Long, String> consumer = new KafkaConsumer<>(props);
    consumer.subscribe(Collections.singletonList(KAFKA_TOPIC));
    return consumer;
  }

  public static Environment mockEnvironment(String bootstrapServers) {
    MockEnvironment environment = new MockEnvironment();
    environment.setProperty("beam.window_size", "5");
    environment.setProperty("kafka.bootstrap.servers", bootstrapServers);
    environment.setProperty("kafka.topic", KAFKA_TOPIC);
    environment.setProperty("kafka.auto.offset.reset", "earliest");
    environment.setProperty("kafka.limit", "2");
    return environment;
  }

  private PipelineOptions pipelineOptions() {
    PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(FlinkRunner.class);
    return options;
  }
}
