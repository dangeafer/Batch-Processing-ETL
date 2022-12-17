package pt.bayonne.sensei.salesinfo.job;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Profile("manager")
@Component
@RequiredArgsConstructor
public class JobScheduler {
    private final Job salesManagerJob;

    private final JobLauncher jobLauncher;

    @Scheduled(cron = "* * * * * ?")
    public void perform()throws Exception{
        JobParameters jobParameters = new JobParametersBuilder()
                .addString("jobId",String.valueOf(System.currentTimeMillis()))
                .toJobParameters();

        jobLauncher.run(salesManagerJob,jobParameters);
    }
}
