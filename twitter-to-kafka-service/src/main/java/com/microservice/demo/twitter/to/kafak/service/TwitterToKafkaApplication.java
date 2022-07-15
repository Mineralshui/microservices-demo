package com.microservice.demo.twitter.to.kafak.service;

import com.microservice.demo.twitter.to.kafak.service.config.TwitterToKafkaServiceConfig;
import com.microservice.demo.twitter.to.kafak.service.runner.StreamRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Arrays;

@SpringBootApplication
public class TwitterToKafkaApplication implements CommandLineRunner {
    private static final Logger LOG = LoggerFactory.getLogger(TwitterToKafkaApplication.class);

    private final TwitterToKafkaServiceConfig twitterToKafkaServiceConfig;

    private final StreamRunner streamRunner;

    public TwitterToKafkaApplication(TwitterToKafkaServiceConfig twitter,
                                     StreamRunner runner){
        this.twitterToKafkaServiceConfig = twitter;
        this.streamRunner = runner;
    }

    public static void main(String[] args) {
        SpringApplication.run(TwitterToKafkaApplication.class,args);
    }

    @Override
    public void run(String... args) throws Exception {
        LOG.info("Application Starts...");
        LOG.info(Arrays.toString(twitterToKafkaServiceConfig.getTwitterKeyword().toArray(new String[] {})));
        streamRunner.start();
    }
}
