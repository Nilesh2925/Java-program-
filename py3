package com.tcs.userservice.exception;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.tcs.userservice.dto.ApiResponse;
import jakarta.validation.ConstraintViolationException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.serializer.SerializationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.web.ErrorResponse;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.context.request.WebRequest;

import java.time.Instant;
import java.util.stream.Collectors;

/**
 * Global exception handler for the CommonRequestService.
 * This class uses the {@link ControllerAdvice @ControllerAdvice} annotation to
 * provide centralized exception handling across all controllers. It logs errors
 * and returns consistent {@link ApiResponse} objects with appropriate HTTP
 * status codes for various exception types.
 */
@ControllerAdvice
@Slf4j
public class GlobalExceptionHandler {

    /**
     * Handles {@link ResourceNotFoundException}.
     * Returns an HTTP 404 Not Found response.
     *
     * @param ex      The ResourceNotFoundException instance.
     * @param request The current web request.
     * @return A ResponseEntity containing an ApiResponse with an error message and HTTP status 404.
     */
    @ExceptionHandler(ResourceNotFoundException.class)
    public ResponseEntity<ApiResponse<Object>> handleResourceNotFoundException(ResourceNotFoundException ex, WebRequest request) {
        log.error("Resource not found: {}", ex.getMessage());
        return new ResponseEntity<>(ApiResponse.error(ex.getMessage()), HttpStatus.NOT_FOUND);
    }

    /**
     * Handles exceptions related to processing JSON payloads, such as malformed JSON or invalid data types.
     * Catches {@link JsonProcessingException} and {@link HttpMessageNotReadableException}.
     * Returns an HTTP 400 Bad Request response.
     *
     * @param ex      The exception instance.
     * @param request The current web request.
     * @return A ResponseEntity containing an ApiResponse with an error message and HTTP status 400.
     */
    @ExceptionHandler({JsonProcessingException.class, HttpMessageNotReadableException.class})
    public ResponseEntity<ApiResponse<Object>> handleJsonProcessingException(Exception ex, WebRequest request) {
        log.error("Error processing JSON payload: {}", ex.getMessage());
        return new ResponseEntity<>(ApiResponse.error("Invalid request payload format. Please check the JSON structure and data types."), HttpStatus.BAD_REQUEST);
    }

    /**
     * Handles {@link DataIntegrityViolationException}, typically thrown when database constraints are violated.
     * Checks for specific messages related to unique constraints or pending requests and returns a 409 Conflict status.
     *
     * @param ex      The DataIntegrityViolationException instance.
     * @param request The current web request.
     * @return A ResponseEntity containing an ApiResponse with a specific or generic conflict error message and HTTP status 409.
     */
    @ExceptionHandler(DataIntegrityViolationException.class)
    public ResponseEntity<ApiResponse<Object>> handleDataIntegrityViolationException(DataIntegrityViolationException ex, WebRequest request) {
        log.error("Data integrity violation: {}", ex.getMessage());
        // Check for common unique constraint violation message
        log.error("error: {}", ex.getMostSpecificCause().getMessage());
        if (ex.getMostSpecificCause().getMessage().contains("unique constraint")) {
            return new ResponseEntity<>(ApiResponse.error("A resource with the provided identifier already exists."), HttpStatus.CONFLICT);
        } else if (ex.getMostSpecificCause().getMessage().contains("pending request")) {
            return new ResponseEntity<>(ApiResponse.error("A pending request is already exists."), HttpStatus.CONFLICT);
        }
        return new ResponseEntity<>(ApiResponse.error("Database constraint violation. A required field may be missing or a value is invalid."), HttpStatus.CONFLICT);
    }

    /**
     * Handles general argument and state-related exceptions.
     * Catches {@link IllegalArgumentException} and {@link IllegalStateException}.
     * Returns an HTTP 400 Bad Request response.
     *
     * @param ex      The runtime exception instance.
     * @param request The current web request.
     * @return A ResponseEntity containing an ApiResponse with the exception's message and HTTP status 400.
     */
    @ExceptionHandler({IllegalArgumentException.class, IllegalStateException.class})
    public ResponseEntity<ApiResponse<Object>> handleArgumentAndStateExceptions(RuntimeException ex, WebRequest request) {
        log.error("Illegal argument or state: {}", ex.getMessage());
        return new ResponseEntity<>(ApiResponse.error(ex.getMessage()), HttpStatus.BAD_REQUEST);
    }

    /**
     * A catch-all handler for any other unhandled exceptions.
     * Ensures that unexpected errors are logged and a generic HTTP 500 Internal Server Error response is returned.
     *
     * @param ex      The exception instance.
     * @param request The current web request.
     * @return A ResponseEntity containing a generic internal server error ApiResponse and HTTP status 500.
     */
    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiResponse<Object>> handleGlobalException(Exception ex, WebRequest request) {
        log.error("An unexpected error occurred: {}", ex.getMessage(), ex);
        return new ResponseEntity<>(ApiResponse.error("An internal server error occurred."), HttpStatus.INTERNAL_SERVER_ERROR);
    }


    /**
     * Handles exceptions thrown when {@link jakarta.validation.Valid @Valid} validation fails on a request body.
     * Collects all field errors and returns a 400 Bad Request with a list of validation errors.
     *
     * @param ex The MethodArgumentNotValidException instance.
     * @return A ResponseEntity containing an ApiResponse with detailed validation errors and HTTP status 400.
     */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ApiResponse<Object>> handleValidationExceptions(MethodArgumentNotValidException ex) {
        // 1. Detailed logging for INTERNAL use (keep this)
        String detailedErrors = ex.getBindingResult().getFieldErrors().stream()
                .map(error -> error.getField() + ": " + error.getDefaultMessage())
                .collect(Collectors.joining(", "));
        log.warn("Validation failed for incoming request: {}", detailedErrors);

        // 2. Generic message for EXTERNAL users (Prevents Information Disclosure)
        return new ResponseEntity<>(
                ApiResponse.error("Invalid request parameters. Please ensure all fields follow the required format."),
                HttpStatus.BAD_REQUEST
        );
    }


    /**
     * Handles exceptions thrown by the Jakarta Validator, for example,
     * when validation is manually triggered or constraints on service methods are violated.
     * Returns a 400 Bad Request with a list of constraint violations.
     *
     * @param ex The ConstraintViolationException instance.
     * @return A ResponseEntity containing an ApiResponse with detailed constraint violations and HTTP status 400.
     */
    @ExceptionHandler(ConstraintViolationException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ResponseEntity<ApiResponse<Object>> handleConstraintViolationException(ConstraintViolationException ex) {
        String errors = ex.getConstraintViolations().stream()
                .map(cv -> cv.getPropertyPath() + ": " + cv.getMessage())
                .collect(Collectors.joining(", "));

        log.warn("Constraint violation during processing: {}", errors);
        return new ResponseEntity<>(ApiResponse.error("Validation Failed: " + errors), HttpStatus.BAD_REQUEST);
    }

    /**
     * Handles security-related access denial.
     * Returns an HTTP 403 Forbidden response.
     */
    @ExceptionHandler(AccessDeniedException.class)
    public ResponseEntity<ApiResponse<Object>> handleAccessDeniedException(AccessDeniedException ex, WebRequest request) {
        log.warn("Access denied: {}", ex.getMessage());
        return new ResponseEntity<>(ApiResponse.error("You do not have permission to perform this action."), HttpStatus.FORBIDDEN);
    }

    @ExceptionHandler(ApplicationException.class)
    public ResponseEntity<ApiResponse<Object>> handleApplicationException(ApplicationException ex, WebRequest request) {
        log.error("Application Error [{}]: {}", ex.getErrorCode(), ex.getMessage(), ex);
        return new ResponseEntity<>(ApiResponse.error(ex.getMessage()), ex.getStatus());
    }

    // Handle Raw Redis Connection Failures (if not wrapped)
    @ExceptionHandler(RedisConnectionFailureException.class)
    public ResponseEntity<ApiResponse<Object>> handleRedisConnectionFailure(RedisConnectionFailureException ex, WebRequest request) {
        log.error("Redis Connection Failed", ex);
        return new ResponseEntity<>(ApiResponse.error("Cache Service Temporarily Unavailable : REDIS_DOWN"), HttpStatus.SERVICE_UNAVAILABLE);
    }

    // Handle Serialization Issues
    @ExceptionHandler(SerializationException.class)
    public ResponseEntity<ApiResponse<Object>> handleSerializationException(SerializationException ex, WebRequest request) {
        log.error("Data Serialization Failed", ex);
        return new ResponseEntity<>(ApiResponse.error("Internal Data Processing Error : SERIALIZATION_FAILURE"), HttpStatus.INTERNAL_SERVER_ERROR);
    }
}
