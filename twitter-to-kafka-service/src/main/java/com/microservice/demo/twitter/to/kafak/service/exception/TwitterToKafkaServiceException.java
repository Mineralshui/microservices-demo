package com.microservice.demo.twitter.to.kafak.service.exception;

public class TwitterToKafkaServiceException extends RuntimeException{
    public TwitterToKafkaServiceException() {
        super();
    }

    public TwitterToKafkaServiceException(String message) {
        super(message);
    }

    public TwitterToKafkaServiceException(String message, Throwable cause) {
        super(message, cause);
    }
}
