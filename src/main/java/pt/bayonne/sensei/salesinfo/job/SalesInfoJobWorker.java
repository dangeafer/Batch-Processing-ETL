package pt.bayonne.sensei.salesinfo.job;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.integration.chunk.RemoteChunkingWorkerBuilder;
import org.springframework.batch.integration.config.annotation.EnableBatchIntegration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.handler.LoggingHandler;
import org.springframework.integration.kafka.dsl.Kafka;
import org.springframework.integration.kafka.outbound.KafkaProducerMessageHandler;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import pt.bayonne.sensei.salesinfo.dto.SalesInfoDTO;

@Profile("worker")
@EnableBatchProcessing
@EnableBatchIntegration
@Configuration
@RequiredArgsConstructor
@Slf4j
public class SalesInfoJobWorker {

  private final RemoteChunkingWorkerBuilder<SalesInfoDTO, SalesInfoDTO> remoteChunkingWorkerBuilder;

  private final KafkaTemplate<String, SalesInfoDTO> saleInfoKafkaTemplate;

  @Bean
  public IntegrationFlow salesWorkerStep() {
    return this.remoteChunkingWorkerBuilder.inputChannel(inboundChannel()) //consumes the
        // chunkRequest<SalesInfoDTO> from kafka
        .outputChannel(outboundChannel()) //produces the chunkResponse<SalesInfoDTO> to kafka
        .itemProcessor(salesInfoDTO -> {
          log.info(
              "item processing: {}",
              salesInfoDTO); //simples ItemProcessor<SalesInfoDTO,SalesInfoDTO>
          return salesInfoDTO;
        })
        .itemWriter(items -> log.info(
            "item writing: {}",
            items)) //simple writer, here you can write into database, file or whatever
        .build();

  }

  @Bean
  public QueueChannel inboundChannel() {
    return new QueueChannel();
  }

  @Bean
  public IntegrationFlow inboundFlow(ConsumerFactory<String, SalesInfoDTO> consumerFactory) {
    return IntegrationFlow
        .from(Kafka.messageDrivenChannelAdapter(consumerFactory, "sales-chunkRequests"))
        .log(LoggingHandler.Level.WARN)
        .channel(inboundChannel())
        .get();
  }

  @Bean
  public DirectChannel outboundChannel() {
    return new DirectChannel();
  }

  @Bean
  public IntegrationFlow outboundFlow() {
    var producerMessageHandler = new KafkaProducerMessageHandler<String, SalesInfoDTO>(
        saleInfoKafkaTemplate);
    producerMessageHandler.setTopicExpression(new LiteralExpression("sales-chunkReplies"));
    return IntegrationFlow
        .from(outboundChannel())
        .log(LoggingHandler.Level.WARN)
        .enrich(enricherSpec -> enricherSpec.headerExpression(
            "batchJobIdentifier",
            "headers['batchJobIdentifier']"))
        .enrich(enricherSpec -> enricherSpec.headerExpression("sbJobId", "headers['sbJobId']"))
        .handle(producerMessageHandler)
        .get();
  }
}
