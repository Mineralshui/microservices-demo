package com.microservice.demo.twitter.to.kafak.service.runner.impl;

import com.microservice.demo.twitter.to.kafak.service.config.TwitterToKafkaServiceConfig;
import com.microservice.demo.twitter.to.kafak.service.exception.TwitterToKafkaServiceException;
import com.microservice.demo.twitter.to.kafak.service.listener.TwitterKafkaStatusListener;
import com.microservice.demo.twitter.to.kafak.service.runner.StreamRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

@Component
@ConditionalOnProperty(name = "twitter-to-kafka-service.enable-mock-tweets", havingValue = "true")
public class MockKafkaStreamRunner implements StreamRunner {
    private static final Logger LOG = LoggerFactory.getLogger(MockKafkaStreamRunner.class);
    private final TwitterToKafkaServiceConfig twitterToKafkaServiceConfig;
    private final TwitterKafkaStatusListener twitterKafkaStatusListener;

    private static final Random RANDOM = new Random();

    private static final String[] WORDS = new String[]{
            "Lorem",
            "ipsum",
            "dolor",
            "sit",
            "amet",
            "consectetuer",
            "adipiscing",
            "elit",
            "Maecenas",
            "porttitor",
            "congue",
            "massa",
            "Fusce",
            "posuere",
            "magna",
            "sed",
            "pulvinar",
            "ultricies",
            "purus",
            "lectus",
            "malesuada",
            "libero"
    };

    private static final String tweetAsRawJson = "{"+
            "\"created_at\":\"{0}\","+
            "\"id\":\"{1}\","+
            "\"text\":\"{2}\","+
            "\"user\":{\"id\":\"{3}\"}"+
            "}";

    private static final String TWITTER_STATUS_DATE_FORMAT = "EEE MMM dd HH:mm:ss zzz yyyy";
    public MockKafkaStreamRunner(TwitterToKafkaServiceConfig config,
                                 TwitterKafkaStatusListener listener) {
        this.twitterToKafkaServiceConfig = config;
        this.twitterKafkaStatusListener = listener;
    }
    @Override
    public void start() throws TwitterException {
        String[] keywords = twitterToKafkaServiceConfig.getTwitterKeyword().toArray(new String[0]);
        int minTweetsLength = twitterToKafkaServiceConfig.getMockMinTweetsLength();
        int maxTweetsLength = twitterToKafkaServiceConfig.getMockMaxTweetsLength();
        long sleepTimeMs = twitterToKafkaServiceConfig.getMockSleepMs();
        LOG.info("Starting mock filtering twitter streams for keywords {}", Arrays.toString(keywords));
        //循环模拟流
        simulateTwitterStream(keywords, minTweetsLength, maxTweetsLength, sleepTimeMs);
    }

    private void simulateTwitterStream(String[] keywords, int minTweetsLength, int maxTweetsLength, long sleepTimeMs){
        Executors.newSingleThreadExecutor().submit(()->{
            try {
                while (true) {
                    String formattedTweetsAsRawJson = getFormattedTweet(keywords, minTweetsLength, maxTweetsLength);
                    Status status = TwitterObjectFactory.createStatus(formattedTweetsAsRawJson);
                    twitterKafkaStatusListener.onStatus(status);
                    sleep(sleepTimeMs);
                }
            } catch (TwitterException e){
                LOG.error("Error creating twitter stream status!",e);
            }
        });
    }

    private void sleep(long sleepTimeMs) {
        try {
            Thread.sleep(sleepTimeMs);
        } catch (InterruptedException e) {
            throw new TwitterToKafkaServiceException("Error While Sleep for waiting");
        }
    }

    private String getFormattedTweet(String[] keywords, int minTweetsLength, int maxTweetsLength) {
        String[] params = new String[]{
                ZonedDateTime.now().format(DateTimeFormatter.ofPattern(TWITTER_STATUS_DATE_FORMAT, Locale.ENGLISH)),
                String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE)),
                getRandomTweetContent(keywords,minTweetsLength,maxTweetsLength),
                String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE))
        };
        return formattedTweetAsJsonWithParam(params);
    }

    private String formattedTweetAsJsonWithParam(String[] params) {
        String tweet = tweetAsRawJson;
        for (int i = 0; i< params.length; i++){
            tweet = tweet.replace("{" + i + "}", params[i]);
        }
        return tweet;
    }

    private String getRandomTweetContent(String[] keywords, int minTweetsLength, int maxTweetsLength) {
        StringBuilder tweet = new StringBuilder();
        int tweetLength = RANDOM.nextInt(maxTweetsLength - minTweetsLength + 1) + minTweetsLength;
        return constructRandomTweet(keywords, tweet, tweetLength);
    }

    private String constructRandomTweet(String[] keywords, StringBuilder tweet, int tweetLength) {
        for (int i = 0; i< tweetLength; i++){
            tweet.append(WORDS[RANDOM.nextInt(WORDS.length)]).append(" ");
            if ( i == tweetLength / 2){
                tweet.append(keywords[RANDOM.nextInt(keywords.length)]).append(" ");
            }
        }
        return tweet.toString().trim();
    }
}
