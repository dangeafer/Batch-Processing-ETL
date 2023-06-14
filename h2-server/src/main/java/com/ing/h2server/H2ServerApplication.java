package com.ing.h2server;

import java.sql.SQLException;
import org.h2.tools.Server;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class H2ServerApplication {

  public static void main(String[] args) {
    SpringApplication.run(H2ServerApplication.class, args);
  }

  /**
   * Start internal H2 server so we can query the DB from IDE
   *
   * @return H2 Server instance
   *
   * @throws SQLException
   */
  @Bean(initMethod = "start",
        destroyMethod = "stop")
  public Server h2Server() throws SQLException {
    return Server.createTcpServer("-tcp", "-tcpAllowOthers", "-tcpPort", "8443");
  }

}
