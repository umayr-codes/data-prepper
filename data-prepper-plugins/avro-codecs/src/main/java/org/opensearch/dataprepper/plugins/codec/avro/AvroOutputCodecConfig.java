/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.codec.avro;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.avro.Schema;

/**
 * Configuration class for {@link AvroOutputCodec}.
 */
public class AvroOutputCodecConfig {

    @JsonProperty("schema_string")
    private String schemaString;

    public AvroOutputCodecConfig(String schemaString){
        this.schemaString = schemaString;
    }

    public String getSchemaString() {
        return schemaString;
    }
}
