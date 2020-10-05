package lightning.emulator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lightning.model.Lightning;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.ProducerRecordCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;

@Component
@Profile("withEmulator")
@Log4j2
@RequiredArgsConstructor
public class LightningEmulator {
  private final Environment env;
  private final LightningGenerator lightningGenerator;
  private final PipelineOptions pipelineOptions;
  private final Executor executor;

  @PostConstruct
  public void runPipeline() {
    executor.execute(this::sendLightningData);
  }

  public PipelineResult sendLightningData() {
    Pipeline pipeline = Pipeline.create(pipelineOptions);

    pipeline
        .apply("Generate one number/2sec", createGenerator(env.getProperty("kafka.limit", Integer.class)))
        .apply("Map to KV", MapElements.via(new MapToProducerRecord(lightningGenerator, env.getProperty("kafka.topic"))))
        .setCoder(ProducerRecordCoder.of(VarLongCoder.of(), StringUtf8Coder.of()))
        .apply("Write to Kafka", KafkaIO.<Long, String>writeRecords()
            .withBootstrapServers(env.getProperty("kafka.bootstrap.servers"))
            .withTopic(env.getProperty("kafka.topic"))
            .withKeySerializer(LongSerializer.class)
            .withValueSerializer(StringSerializer.class));

    return pipeline.run();
  }

  private GenerateSequence createGenerator(@Nullable Integer limit) {
    GenerateSequence generator = GenerateSequence.from(0L)
        .withRate(1, Duration.standardSeconds(2L));

    if (limit != null) {
      generator = generator.to(limit);
    }
    return generator;
  }

  @RequiredArgsConstructor
  private static class MapToProducerRecord extends SimpleFunction<Long, ProducerRecord<Long, String>> {
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final AtomicLong idCounter = new AtomicLong();

    private final LightningGenerator lightningGenerator;
    private final String topic;

    @Override
    public ProducerRecord<Long, String> apply(Long number) {
      Lightning lightning = lightningGenerator.generateLightning();
      String jsonLightning;
      try {
        jsonLightning = objectMapper.writeValueAsString(lightning);
      } catch (JsonProcessingException e) {
        throw new RuntimeException("Could not map lightning object to json string", e);
      }
      log.info("Writing lightning {} to kafka", jsonLightning);
      return new ProducerRecord<>(topic, null, lightning.getTimestamp(), idCounter.getAndIncrement(), jsonLightning);
    }
  }
}
