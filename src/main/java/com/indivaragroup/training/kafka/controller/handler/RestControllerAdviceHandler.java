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

@RestControllerAdvice
@Slf4j
@RequiredArgsConstructor
public class RestControllerAdviceHandler {

    /**
     * ✅ Handle Validation Exception dengan auto-detect @JsonProperty
     */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<RestApiResponse<Void>> handleValidationException(
            MethodArgumentNotValidException ex) {

        log.warn("Validation error occurred");

        Map<String, Serializable> errors = new HashMap<>();

        for (FieldError fieldError : ex.getBindingResult().getFieldErrors()) {
            String fieldName = fieldError.getField(); // default = nama field Java
            String errorMessage = fieldError.getDefaultMessage();

            try {
                // ✅ Auto-detect @JsonProperty annotation using reflection
                Class<?> targetClass = ex.getBindingResult().getTarget().getClass();
                Field field = targetClass.getDeclaredField(fieldName);
                com.fasterxml.jackson.annotation.JsonProperty jsonProp =
                        field.getAnnotation(com.fasterxml.jackson.annotation.JsonProperty.class);

                if (jsonProp != null && !jsonProp.value().isEmpty()) {
                    fieldName = jsonProp.value(); // Gunakan nama dari @JsonProperty
                }
            } catch (Exception ignored) {
            }

            errors.put(fieldName, errorMessage);
        }

        RestApiResponseError error = RestApiResponseError.builder()
                .restApiResponseRequestError(errors)
                .build();

        RestApiResponse<Void> apiResponse = RestApiResponse.<Void>builder()
                .restApiResponseCode(BAD_REQUEST.value())
                .restApiResponseMessage("Validasi gagal")
                .restApiResponseResults(null)
                .restApiResponseError(error)
                .build();

        return ResponseEntity.status(BAD_REQUEST).body(apiResponse);
    }

    /**
     * Handle IllegalStateException - User already has referral code
     */
    @ExceptionHandler(IllegalStateException.class)
    public ResponseEntity<RestApiResponse<Void>> handleIllegalStateException(
            IllegalStateException ex) {

        log.warn("IllegalStateException: {}", ex.getMessage());

        Map<String, Serializable> errorDetails = new HashMap<>();
        errorDetails.put("tipe", "KONFLIK");

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
     * Handle ResourceNotFoundException - User not found
     */
    @ExceptionHandler(CoreThrowHandlerException.class)
    public ResponseEntity<RestApiResponse<Void>> handleResourceNotFoundException(
            CoreThrowHandlerException ex) {

        log.error("ResourceNotFoundException: {}", ex.getMessage());

        Map<String, Serializable> errorDetails = new HashMap<>();
        errorDetails.put("tipe", "TIDAK_DITEMUKAN");

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
     * Handle IllegalArgumentException - Invalid input
     */
    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<RestApiResponse<Void>> handleIllegalArgumentException(
            IllegalArgumentException ex) {

        log.warn("IllegalArgumentException: {}", ex.getMessage());

        Map<String, Serializable> errorDetails = new HashMap<>();
        errorDetails.put("tipe", "PERMINTAAN_TIDAK_VALID");

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
     * Handle NullPointerException
     */
    @ExceptionHandler(NullPointerException.class)
    public ResponseEntity<RestApiResponse<Void>> handleNullPointerException(
            NullPointerException ex) {

        log.error("NullPointerException occurred", ex);

        Map<String, Serializable> errorDetails = new HashMap<>();
        errorDetails.put("tipe", "KESALAHAN_SERVER");

        RestApiResponseError error = RestApiResponseError.builder()
                .restApiResponseRequestError(errorDetails)
                .build();

        RestApiResponse<Void> response = RestApiResponse.<Void>builder()
                .restApiResponseCode(INTERNAL_SERVER_ERROR.value())
                .restApiResponseMessage("Terjadi kesalahan pada server")
                .restApiResponseResults(null)
                .restApiResponseError(error)
                .build();

        return ResponseEntity.status(INTERNAL_SERVER_ERROR).body(response);
    }

    /**
     * Handle RuntimeException
     */
    @ExceptionHandler(RuntimeException.class)
    public ResponseEntity<RestApiResponse<Void>> handleRuntimeException(RuntimeException ex) {
        return handleAnyThrowable(ex);
    }

    /**
     * Fallback: Handle all other exceptions
     */
    @ExceptionHandler(Throwable.class)
    public ResponseEntity<RestApiResponse<Void>> handleAnyThrowable(Throwable ex) {
        Map<String, Serializable> errorDetails = new HashMap<>();
        log.info("DEBUG Transaction: {}" + ex.getMessage());
        errorDetails.put("tipe", "KESALAHAN_TIDAK_TERDUGA");

        RestApiResponseError error = RestApiResponseError.builder()
                .restApiResponseRequestError(errorDetails)
                .build();

        RestApiResponse<Void> response = RestApiResponse.<Void>builder()
                .restApiResponseCode(INTERNAL_SERVER_ERROR.value())
                .restApiResponseMessage("Terjadi kesalahan yang tidak terduga")
                .restApiResponseResults(null)
                .restApiResponseError(error)
                .build();

        return ResponseEntity.status(INTERNAL_SERVER_ERROR).body(response);
    }
}
