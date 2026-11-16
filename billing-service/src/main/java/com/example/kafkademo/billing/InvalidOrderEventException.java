package com.example.kafkademo.billing;

public class InvalidOrderEventException extends RuntimeException {

    public InvalidOrderEventException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidOrderEventException(String message) {
        super(message);
    }
}
