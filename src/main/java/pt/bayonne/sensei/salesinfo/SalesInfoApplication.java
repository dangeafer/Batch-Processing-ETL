package pt.bayonne.sensei.salesinfo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class SalesInfoApplication {

	public static void main(String[] args) {
		SpringApplication.run(SalesInfoApplication.class, args);
	}

}
