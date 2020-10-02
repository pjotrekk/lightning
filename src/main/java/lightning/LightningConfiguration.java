package lightning;

import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;

@Configuration
@EnableAsync
public class LightningConfiguration {
  @Bean
  public Executor taskExecutor() {
    ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
    executor.setCorePoolSize(4);
    executor.setMaxPoolSize(4);
    executor.setQueueCapacity(2);
    executor.setThreadNamePrefix("Lightning-");
    executor.initialize();
    return executor;
  }

  @Bean
  public PipelineOptions pipelineOptions() {
    PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(FlinkRunner.class);
    return options;
  }
}
