package com.microservice.demo.twitter.to.kafak.service.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Data
@Configuration
@ConfigurationProperties("twitter-to-kafka-service")
public class TwitterToKafkaServiceConfig {
    private List<String> twitterKeyword;
    private Boolean enableMockTweets;
    private Long mockSleepMs;
    private Integer mockMinTweetsLength;
    private Integer mockMaxTweetsLength;
}
