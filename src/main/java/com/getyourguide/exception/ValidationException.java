package com.getyourguide.exception;

/**
 * Author: Aleksander
 * Since: 21.07.2018.
 */
public class ValidationException extends RuntimeException {

    public ValidationException(String message) {
        super(message);
    }

    public ValidationException(String message, Throwable cause) {
        super(message, cause);
    }
}
