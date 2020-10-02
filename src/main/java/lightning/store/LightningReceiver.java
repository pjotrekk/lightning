package lightning.store;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.io.mongodb.MongoDbIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.bson.Document;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.concurrent.Executor;

@Component
@Log4j2
@RequiredArgsConstructor
public class LightningReceiver {
  private final Environment env;
  private final PipelineOptions pipelineOptions;
  private final Executor executor;

  @PostConstruct
  public void runPipeline() {
    executor.execute(this::lightningStreaming);
  }

  public PipelineResult lightningStreaming() {
    Pipeline pipeline = Pipeline.create(pipelineOptions);

    KafkaIO.Read<Long, String> kafkaRead = KafkaIO.<Long, String>read()
        .withBootstrapServers(env.getProperty("kafka.bootstrap.servers"))
        .withTopic(env.getProperty("kafka.topic"))
        .withConsumerConfigUpdates(ImmutableMap.of("auto.offset.reset", env.getProperty("kafka.auto.offset.reset")))
        .withKeyDeserializer(LongDeserializer.class)
        .withValueDeserializer(StringDeserializer.class);

    Integer limit = env.getProperty("kafka.limit", Integer.class);
    if (limit != null) {
      kafkaRead = kafkaRead.withMaxNumRecords(limit);
    }

    PCollection<KafkaRecord<Long, String>> kafkaRecords = pipeline.apply("Read data from Kafka", kafkaRead);

    PCollection<Document> jsonLightningData = kafkaRecords.apply(
        "Parse Kafka record to Mongo Document",
        MapElements.into(TypeDescriptor.of(Document.class))
            .via(kafkaRecord -> {
              log.info("Read lightning {} from kafka", kafkaRecord.getKV().getValue());
              return Document.parse(kafkaRecord.getKV().getValue());
            }));

    // Add separate branch to the pipeline tree
    applyCountStrokeTheGroundLightnings(jsonLightningData);

    jsonLightningData.apply("Write lightning data to MongoDB",
        MongoDbIO.write()
        .withDatabase(env.getProperty("mongo.database"))
        .withCollection(env.getProperty("mongo.collection.lightnings"))
        .withUri(env.getProperty("mongo.host")));

    return pipeline.run();
  }

  private void applyCountStrokeTheGroundLightnings(PCollection<Document> pc) {
    Integer windowSize = env.getProperty("beam.window.size", Integer.class);
    pc
      .apply("Filter hit the ground ones", Filter.by(document -> document.getBoolean("strokeTheGround")))
      .apply("Add timestamp", WithTimestamps.of(document -> Instant.now()))
      .apply("Apply fixed windows", Window.into(FixedWindows.of(Duration.standardSeconds(windowSize))))
      .apply(
          "Count lightnings that stroke the ground this minute",
          Combine.globally(Count.<Document>combineFn()).withoutDefaults())
      .apply("Map to Mongo Document", ParDo.of(new CreateStrikesJson(windowSize)))
      .apply("Write strikes count to database", MongoDbIO.write()
          .withDatabase(env.getProperty("mongo.database"))
          .withCollection(env.getProperty("mongo.collection.strikes"))
          .withUri(env.getProperty("mongo.host")));
  }

  private static class CreateStrikesJson extends DoFn<Long, Document> {
    private final long seconds;

    CreateStrikesJson(long seconds) {
      this.seconds = seconds;
    }

    @ProcessElement
    public void processElement(ProcessContext ctx) {
      Document document = new Document()
          .append("strikes", ctx.element())
          .append("from", ctx.timestamp().minus(Duration.standardSeconds(seconds)).getMillis())
          .append("to", ctx.timestamp().getMillis());
      log.info("Writing strikes {} to database.", document);
      ctx.output(document);
    }
  }
}
