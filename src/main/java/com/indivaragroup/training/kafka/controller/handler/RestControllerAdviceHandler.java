package com.indivaragroup.training.kafka.controller.handler;

import com.indivaragroup.training.kafka.dto.exception.CoreThrowHandlerException;
import com.indivaragroup.training.kafka.dto.response.RestApiResponse;
import com.indivaragroup.training.kafka.dto.response.RestApiResponseError;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import static org.springframework.http.HttpStatus.*;

/**
 * Global exception handler for REST API endpoints.
 * <p>
 * This class handles various types of exceptions thrown by REST controllers
 * and transforms them into standardized REST API error responses.
 * It uses Spring's {@link RestControllerAdvice} to intercept exceptions
 * across all controllers and provide consistent error handling.
 * </p>
 *
 * <p>
 * The handler supports automatic detection of {@link com.fasterxml.jackson.annotation.JsonProperty}
 * annotations for validation errors, ensuring that error messages use the correct field names
 * as defined in the API contract rather than internal Java field names.
 * </p>
 *
 * @author Supriyandi La Awe
 * @version 1.0.0
 * @since 2025-11-22
 * @see RestControllerAdvice
 * @see ExceptionHandler
 */
@RestControllerAdvice
@Slf4j
@RequiredArgsConstructor
public class RestControllerAdviceHandler {

    /**
     * Handles validation exceptions thrown when request body validation fails.
     * <p>
     * This method intercepts {@link MethodArgumentNotValidException} which is thrown
     * when {@code @Valid} or {@code @Validated} annotation validation fails on request body.
     * It automatically detects {@code @JsonProperty} annotations using reflection
     * to ensure error messages use the correct JSON field names.
     * </p>
     *
     * <p><b>Example scenario:</b></p>
     * <pre>
     * // Request body with validation annotations
     * public class UserRequest {
     *     {@literal @}NotBlank(message = "Name is required")
     *     {@literal @}JsonProperty("full_name")
     *     private String fullName;
     * }
     *
     * // If validation fails, the error response will use "full_name" instead of "fullName"
     * </pre>
     *
     * @param ex the exception thrown when validation fails
     * @return ResponseEntity containing a standardized error response with HTTP 400 status
     *         and a map of field names to error messages
     * @see MethodArgumentNotValidException
     * @see com.fasterxml.jackson.annotation.JsonProperty
     */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<RestApiResponse<Void>> handleValidationException(
            MethodArgumentNotValidException ex) {

        log.warn("Validation error occurred");

        Map<String, Serializable> errors = new HashMap<>();

        for (FieldError fieldError : ex.getBindingResult().getFieldErrors()) {
            String fieldName = fieldError.getField(); // default = Java field name
            String errorMessage = fieldError.getDefaultMessage();

            try {
                // Auto-detect @JsonProperty annotation using reflection
                Class<?> targetClass = ex.getBindingResult().getTarget().getClass();
                Field field = targetClass.getDeclaredField(fieldName);
                com.fasterxml.jackson.annotation.JsonProperty jsonProp =
                        field.getAnnotation(com.fasterxml.jackson.annotation.JsonProperty.class);

                if (jsonProp != null && !jsonProp.value().isEmpty()) {
                    fieldName = jsonProp.value(); // Use name from @JsonProperty
                }
            } catch (Exception ignored) {
                // If reflection fails, use the default field name
            }

            errors.put(fieldName, errorMessage);
        }

        RestApiResponseError error = RestApiResponseError.builder()
                .restApiResponseRequestError(errors)
                .build();

        RestApiResponse<Void> apiResponse = RestApiResponse.<Void>builder()
                .restApiResponseCode(BAD_REQUEST.value())
                .restApiResponseMessage("Validation failed")
                .restApiResponseResults(null)
                .restApiResponseError(error)
                .build();

        return ResponseEntity.status(BAD_REQUEST).body(apiResponse);
    }

    /**
     * Handles illegal state exceptions indicating business rule violations.
     * <p>
     * This method intercepts {@link IllegalStateException} which typically indicates
     * that an operation cannot be performed due to the current state of the system.
     * Common use cases include attempting to create duplicate resources or
     * performing operations on resources in an invalid state.
     * </p>
     *
     * <p><b>Example scenarios:</b></p>
     * <ul>
     *   <li>User already has a referral code</li>
     *   <li>Account is already activated</li>
     *   <li>Transaction is already processed</li>
     * </ul>
     *
     * @param ex the exception thrown when an illegal state is detected
     * @return ResponseEntity containing a standardized error response with HTTP 409 status
     * @see IllegalStateException
     */
    @ExceptionHandler(IllegalStateException.class)
    public ResponseEntity<RestApiResponse<Void>> handleIllegalStateException(
            IllegalStateException ex) {

        log.warn("IllegalStateException: {}", ex.getMessage());

        Map<String, Serializable> errorDetails = new HashMap<>();
        errorDetails.put("type", "CONFLICT");

        RestApiResponseError error = RestApiResponseError.builder()
                .restApiResponseRequestError(errorDetails)
                .build();

        RestApiResponse<Void> response = RestApiResponse.<Void>builder()
                .restApiResponseCode(CONFLICT.value())
                .restApiResponseMessage(ex.getMessage())
                .restApiResponseResults(null)
                .restApiResponseError(error)
                .build();

        return ResponseEntity.status(CONFLICT).body(response);
    }

    /**
     * Handles resource not found exceptions.
     * <p>
     * This method intercepts {@link CoreThrowHandlerException} which is thrown
     * when a requested resource cannot be found in the system. This is typically
     * used for database lookups that return no results.
     * </p>
     *
     * <p><b>Example scenarios:</b></p>
     * <ul>
     *   <li>User ID does not exist in database</li>
     *   <li>Transaction not found</li>
     *   <li>Wallet does not exist</li>
     * </ul>
     *
     * @param ex the exception thrown when a resource is not found
     * @return ResponseEntity containing a standardized error response with HTTP 404 status
     * @see CoreThrowHandlerException
     */
    @ExceptionHandler(CoreThrowHandlerException.class)
    public ResponseEntity<RestApiResponse<Void>> handleResourceNotFoundException(
            CoreThrowHandlerException ex) {

        log.error("ResourceNotFoundException: {}", ex.getMessage());

        Map<String, Serializable> errorDetails = new HashMap<>();
        errorDetails.put("type", "NOT_FOUND");

        RestApiResponseError error = RestApiResponseError.builder()
                .restApiResponseRequestError(errorDetails)
                .build();

        RestApiResponse<Void> response = RestApiResponse.<Void>builder()
                .restApiResponseCode(NOT_FOUND.value())
                .restApiResponseMessage(ex.getMessage())
                .restApiResponseResults(null)
                .restApiResponseError(error)
                .build();

        return ResponseEntity.status(NOT_FOUND).body(response);
    }

    /**
     * Handles illegal argument exceptions indicating invalid input parameters.
     * <p>
     * This method intercepts {@link IllegalArgumentException} which is thrown
     * when a method receives an argument that is inappropriate or invalid.
     * This is commonly used for business logic validation that goes beyond
     * simple field validation.
     * </p>
     *
     * <p><b>Example scenarios:</b></p>
     * <ul>
     *   <li>Invalid transaction amount (negative or zero)</li>
     *   <li>Invalid date range (end date before start date)</li>
     *   <li>Unsupported transaction type</li>
     * </ul>
     *
     * @param ex the exception thrown when invalid arguments are provided
     * @return ResponseEntity containing a standardized error response with HTTP 400 status
     * @see IllegalArgumentException
     */
    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<RestApiResponse<Void>> handleIllegalArgumentException(
            IllegalArgumentException ex) {

        log.warn("IllegalArgumentException: {}", ex.getMessage());

        Map<String, Serializable> errorDetails = new HashMap<>();
        errorDetails.put("type", "INVALID_REQUEST");

        RestApiResponseError error = RestApiResponseError.builder()
                .restApiResponseRequestError(errorDetails)
                .build();

        RestApiResponse<Void> response = RestApiResponse.<Void>builder()
                .restApiResponseCode(BAD_REQUEST.value())
                .restApiResponseMessage(ex.getMessage())
                .restApiResponseResults(null)
                .restApiResponseError(error)
                .build();

        return ResponseEntity.status(BAD_REQUEST).body(response);
    }

    /**
     * Handles null pointer exceptions indicating unexpected null values.
     * <p>
     * This method intercepts {@link NullPointerException} which typically indicates
     * a programming error where a null value was not properly handled. This should
     * ideally be prevented through proper null-checking and validation, but serves
     * as a safety net for unexpected scenarios.
     * </p>
     *
     * <p><b>Note:</b> Frequent occurrence of this exception may indicate code quality issues
     * that should be addressed through proper null safety practices.</p>
     *
     * @param ex the null pointer exception that occurred
     * @return ResponseEntity containing a standardized error response with HTTP 500 status
     * @see NullPointerException
     */
    @ExceptionHandler(NullPointerException.class)
    public ResponseEntity<RestApiResponse<Void>> handleNullPointerException(
            NullPointerException ex) {

        log.error("NullPointerException occurred", ex);

        Map<String, Serializable> errorDetails = new HashMap<>();
        errorDetails.put("type", "SERVER_ERROR");

        RestApiResponseError error = RestApiResponseError.builder()
                .restApiResponseRequestError(errorDetails)
                .build();

        RestApiResponse<Void> response = RestApiResponse.<Void>builder()
                .restApiResponseCode(INTERNAL_SERVER_ERROR.value())
                .restApiResponseMessage("An internal server error occurred")
                .restApiResponseResults(null)
                .restApiResponseError(error)
                .build();

        return ResponseEntity.status(INTERNAL_SERVER_ERROR).body(response);
    }

    /**
     * Handles general runtime exceptions.
     * <p>
     * This method intercepts {@link RuntimeException} and delegates to the
     * general throwable handler. It serves as a catch-all for unchecked exceptions
     * that are not handled by more specific exception handlers.
     * </p>
     *
     * @param ex the runtime exception that occurred
     * @return ResponseEntity containing a standardized error response with HTTP 500 status
     * @see RuntimeException
     * @see #handleAnyThrowable(Throwable)
     */
    @ExceptionHandler(RuntimeException.class)
    public ResponseEntity<RestApiResponse<Void>> handleRuntimeException(RuntimeException ex) {
        return handleAnyThrowable(ex);
    }

    /**
     * Fallback handler for all unhandled exceptions.
     * <p>
     * This method serves as the last resort exception handler, catching any
     * throwable that is not handled by more specific handlers. It ensures that
     * all exceptions result in a properly formatted error response rather than
     * exposing internal stack traces to clients.
     * </p>
     *
     * <p><b>Security Note:</b> This handler intentionally provides generic error messages
     * to avoid leaking sensitive information about the application's internal structure.</p>
     *
     * <p><b>Best Practice:</b> While this handler provides a safety net, it's recommended
     * to handle specific exceptions explicitly for better error reporting and debugging.</p>
     *
     * @param ex the throwable that was not handled by any specific handler
     * @return ResponseEntity containing a standardized error response with HTTP 500 status
     * @see Throwable
     */
    @ExceptionHandler(Throwable.class)
    public ResponseEntity<RestApiResponse<Void>> handleAnyThrowable(Throwable ex) {
        Map<String, Serializable> errorDetails = new HashMap<>();
        log.error("Unexpected error occurred: {}", ex.getMessage(), ex);
        errorDetails.put("type", "UNEXPECTED_ERROR");

        RestApiResponseError error = RestApiResponseError.builder()
                .restApiResponseRequestError(errorDetails)
                .build();

        RestApiResponse<Void> response = RestApiResponse.<Void>builder()
                .restApiResponseCode(INTERNAL_SERVER_ERROR.value())
                .restApiResponseMessage("An unexpected error occurred")
                .restApiResponseResults(null)
                .restApiResponseError(error)
                .build();

        return ResponseEntity.status(INTERNAL_SERVER_ERROR).body(response);
    }
}
