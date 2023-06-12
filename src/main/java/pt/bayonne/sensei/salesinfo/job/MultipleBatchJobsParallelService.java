package pt.bayonne.sensei.salesinfo.job;

import java.util.List;
import java.util.concurrent.ExecutorService;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;

@RequiredArgsConstructor
public class MultipleBatchJobsParallelService {

  private final ExecutorService executorService;
  private final JobLauncher jobLauncher;
  private final List<Job> salesManagerJobs;


  public void run() {
    salesManagerJobs.forEach(jobManager -> {
      JobParameters jobParameters = new JobParametersBuilder()
          .addString("batchJobIdentifier", String.valueOf(System.currentTimeMillis()))
          .toJobParameters();
      executorService.submit(() -> jobLauncher.run(jobManager, jobParameters));
    });
  }
}
