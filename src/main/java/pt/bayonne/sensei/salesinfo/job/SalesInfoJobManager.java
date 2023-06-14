package pt.bayonne.sensei.salesinfo.job;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.integration.chunk.RemoteChunkingManagerStepBuilderFactory;
import org.springframework.batch.integration.config.annotation.EnableBatchIntegration;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.handler.LoggingHandler;
import org.springframework.integration.kafka.dsl.Kafka;
import org.springframework.integration.kafka.outbound.KafkaProducerMessageHandler;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import pt.bayonne.sensei.salesinfo.dto.SalesInfoDTO;

@Profile("manager")
@Configuration
@EnableBatchIntegration
@RequiredArgsConstructor
public class SalesInfoJobManager {

  private final RemoteChunkingManagerStepBuilderFactory remoteChunkingManagerStepBuilderFactory;

  private final KafkaTemplate<String, SalesInfoDTO> saleInfoKafkaTemplate;

  @Bean
  public Job salesManagerJob1(JobRepository jobRepository, Step salesInfoStepManager1) {
    return new JobBuilder("Sales-Info-Manager-job1", jobRepository)
        .incrementer(new RunIdIncrementer())
        .start(salesInfoStepManager1)
        .build();
  }

  @Bean
  public Job salesManagerJob2(JobRepository jobRepository, Step salesInfoStepManager2) {
    return new JobBuilder("Sales-Info-Manager-job2", jobRepository)
        .incrementer(new RunIdIncrementer())
        .start(salesInfoStepManager2)

        .build();
  }


  @Bean
  public TaskletStep salesInfoStepManager1() {
    return this.remoteChunkingManagerStepBuilderFactory
        .get("Manager1-Step")
        .<SalesInfoDTO, SalesInfoDTO>chunk(2)
        .reader(salesInfoReader(new ClassPathResource("/data/sales-info1.csv")))
        .outputChannel(outboundChannel()) //produces the chunkRequest<SalesInfoDTO> to kafka
        .inputChannel(inboundChannel()) //consumes the chunkResponse<SalesInfoDTO> from kafka
        .allowStartIfComplete(Boolean.TRUE)
        .build();
  }

  @Bean
  public TaskletStep salesInfoStepManager2() {
    return this.remoteChunkingManagerStepBuilderFactory
        .get("Manager2-Step")
        .<SalesInfoDTO, SalesInfoDTO>chunk(5)
        .reader(salesInfoReader(new ClassPathResource("/data/sales-info2.csv")))
        .outputChannel(outboundChannel()) //produces the chunkRequest<SalesInfoDTO> to kafka
        .inputChannel(inboundChannel()) //consumes the chunkResponse<SalesInfoDTO> from kafka
        .allowStartIfComplete(Boolean.TRUE)
        .build();
  }

  public FlatFileItemReader<SalesInfoDTO> salesInfoReader(Resource resource) {
    return new FlatFileItemReaderBuilder<SalesInfoDTO>()
        .resource(resource)
        .name("salesInfoReader")
        .delimited()
        .delimiter(",")
        .names("product", "seller", "sellerId", "price", "city", "category")
        .linesToSkip(1)//skipping the header of the file
        .targetType(SalesInfoDTO.class)
        .build();
  }

  @Bean
  public DirectChannel outboundChannel() {
    return new DirectChannel();
  }

  @Bean
  public IntegrationFlow outboundFlow() {
    var producerMessageHandler = new KafkaProducerMessageHandler<String, SalesInfoDTO>(
        saleInfoKafkaTemplate);
    producerMessageHandler.setTopicExpression(new LiteralExpression("sales-chunkRequests"));
    return IntegrationFlow.from(outboundChannel()).enrich(enricherSpec -> {
      enricherSpec.header("batchJobIdentifier", "1");
    }).enrich(enricherSpec -> {
      enricherSpec.headerExpression("sbJobId", "payload.jobId.toString()");
    }).handle(producerMessageHandler).get();
  }

  @Bean
  public QueueChannel inboundChannel() {
    return new QueueChannel();
  }

  @Bean
  public IntegrationFlow inboundFlow1(ConsumerFactory<String, SalesInfoDTO> consumerFactory) {

    Map<String, Object> map = new HashMap<>();
    map.put(ConsumerConfig.GROUP_ID_CONFIG, "cg1");
    map.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    map.put(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");
    map.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "pt.bayonne.sensei.salesinfo.serde.ChunkRequestSerializer");
    map.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");
    map.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        "pt.bayonne.sensei.salesinfo.serde.ChunkResponseDeserializer");
    IntegrationFlow flow = IntegrationFlow
        .from(Kafka.messageDrivenChannelAdapter(new DefaultKafkaConsumerFactory<String, String>(map),
            "sales-chunkReplies"))
        .filter("headers['sbJobId'] == '1'")
        .log(LoggingHandler.Level.ERROR)
        .channel(inboundChannel())
        .get();
    return flow;
  }

  @Bean
  public IntegrationFlow inboundFlow2(ConsumerFactory<String, SalesInfoDTO> consumerFactory) {
    Map<String, Object> map = new HashMap<>();
    map.put(ConsumerConfig.GROUP_ID_CONFIG, "cg2");
    map.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    map.put(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");
    map.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "pt.bayonne.sensei.salesinfo.serde.ChunkRequestSerializer");
    map.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");
    map.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        "pt.bayonne.sensei.salesinfo.serde.ChunkResponseDeserializer");
    IntegrationFlow flow = IntegrationFlow
        .from(Kafka.messageDrivenChannelAdapter(new DefaultKafkaConsumerFactory<String, String>(map),
            "sales-chunkReplies"))
        .filter("headers['sbJobId'] == '2'")
        .log(LoggingHandler.Level.WARN)
        .channel(inboundChannel())
        .get();
    return flow;
  }

  @Bean
  public ExecutorService simpleExecutorService() {
    return Executors.newCachedThreadPool();
  }

  @Bean
  @Profile("manager")
  public MultipleBatchJobsParallelService multipleBatchJobsParallelService(
      ExecutorService simpleExecutorService, JobLauncher jobLauncher, List<Job> salesManagerJobs) {
    return new MultipleBatchJobsParallelService(simpleExecutorService,
        jobLauncher,
        salesManagerJobs);
  }
}
