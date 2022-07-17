package com.microservice.demo.twitter.to.kafak.service.runner.impl;

import com.microservice.demo.twitter.to.kafak.service.config.TwitterToKafkaServiceConfig;
import com.microservice.demo.twitter.to.kafak.service.listener.TwitterKafkaStatusListener;
import com.microservice.demo.twitter.to.kafak.service.runner.StreamRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import twitter4j.FilterQuery;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

import javax.annotation.PreDestroy;
import java.util.Arrays;

@Component
@ConditionalOnProperty(name = "twitter-to-kafka-service.enable-mock-tweets", havingValue = "false", matchIfMissing = true)
public class TwitterKafkaStreamRunner implements StreamRunner {

    private static final Logger LOG = LoggerFactory.getLogger(TwitterKafkaStreamRunner.class);
    private final TwitterToKafkaServiceConfig twitterToKafkaServiceConfig;
    private final TwitterKafkaStatusListener twitterKafkaStatusListener;
    private TwitterStream twitterStream;
    public TwitterKafkaStreamRunner(TwitterToKafkaServiceConfig config,
                                    TwitterKafkaStatusListener statusListener){
        this.twitterKafkaStatusListener = statusListener;
        this.twitterToKafkaServiceConfig = config;

    }

    @Override
    public void start() throws TwitterException {
        twitterStream = new TwitterStreamFactory().getInstance();
        twitterStream.addListener(twitterKafkaStatusListener);
        addFilter();
    }

    @PreDestroy
    public void shutdown(){
        if (twitterStream != null){
            LOG.info("closing twitter stream");
            twitterStream.shutdown();
        }
    }

    private void addFilter() {
        String[] keys = twitterToKafkaServiceConfig.getTwitterKeyword().toArray(new String[0]);
        FilterQuery filterQuery = new FilterQuery(keys);
        LOG.info("Started filtering twitter stream for keywords {}", Arrays.toString(keys));
    }
}
