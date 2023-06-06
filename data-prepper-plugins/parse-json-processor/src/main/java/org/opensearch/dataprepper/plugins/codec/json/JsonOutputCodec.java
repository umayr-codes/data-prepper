/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.codec.json;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.event.Event;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;

/**
 * An implementation of {@link OutputCodec} which deserializes Data-Prepper events and writes them to Output Stream as JSON Data
 */
@DataPrepperPlugin(name="json", pluginType = OutputCodec.class)
public class JsonOutputCodec implements OutputCodec {

    private static JsonGenerator generator;
    private static final String JSON="json";
    private static final JsonFactory factory = new JsonFactory();

    @Override
    public void start (final OutputStream outputStream) throws IOException {
        Objects.requireNonNull(outputStream);
        generator = factory.createGenerator(outputStream, JsonEncoding.UTF8);
        generator.writeStartArray();
    }

    @Override
    public void complete(final OutputStream outputStream) throws IOException {
        generator.writeEndArray();
        generator.close();
        outputStream.flush();
        outputStream.close();
    }

    @Override
    public void writeEvent(final Event event,final OutputStream outputStream) throws IOException {
        Objects.requireNonNull(event);
        generator.writeStartObject();
        for (final String key: event.toMap().keySet()){
            generator.writeStringField(key,event.toMap().get(key).toString());
        }
        generator.writeEndObject();
        generator.flush();
    }

    @Override
    public String getExtension() {
        return JSON;
    }
}
