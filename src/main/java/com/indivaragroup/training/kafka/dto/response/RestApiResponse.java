package com.indivaragroup.training.kafka.dto.response;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class RestApiResponse<T> {
    @JsonProperty("code")
    private Integer restApiResponseCode;

    @JsonProperty("results")
    private T restApiResponseResults;

    @JsonProperty("message")
    private String restApiResponseMessage;

    @JsonProperty("error")
    private RestApiResponseError restApiResponseError;
}
