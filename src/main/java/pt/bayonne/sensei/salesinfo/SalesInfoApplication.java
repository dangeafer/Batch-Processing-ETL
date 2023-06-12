package pt.bayonne.sensei.salesinfo;

import java.util.Arrays;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.annotation.EnableScheduling;
import pt.bayonne.sensei.salesinfo.job.MultipleBatchJobsParallelService;

@SpringBootApplication
@EnableScheduling
public class SalesInfoApplication {

  public static void main(String[] args) {
    ApplicationContext ctx = SpringApplication.run(SalesInfoApplication.class, args);
    runManagerBehaviour(ctx);
  }

  private static void runManagerBehaviour(ApplicationContext ctx) {
    if (Arrays.asList(ctx.getEnvironment().getActiveProfiles()).contains("manager")) {
      MultipleBatchJobsParallelService multipleBatchJobsParallelService = ctx.getBean(
          MultipleBatchJobsParallelService.class);
      try {
        multipleBatchJobsParallelService.run();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
