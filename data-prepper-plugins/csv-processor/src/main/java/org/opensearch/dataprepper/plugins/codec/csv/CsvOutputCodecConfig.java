/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.codec.csv;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Configuration class for {@link CsvOutputCodec}.
 */
public class CsvOutputCodecConfig {
    static final String DEFAULT_DELIMITER = ",";

    @JsonProperty("delimiter")
    private String delimiter = DEFAULT_DELIMITER;

    @JsonProperty("schema")
    private List<String> header;
    public CsvOutputCodecConfig(List<String> schema){
        this.header = schema;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public List<String> getHeader() {
        return header;
    }
}