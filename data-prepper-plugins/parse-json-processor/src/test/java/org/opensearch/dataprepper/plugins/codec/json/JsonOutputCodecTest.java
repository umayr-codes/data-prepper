/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.codec.json;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.hamcrest.Matchers;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.record.Record;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventType;
import org.opensearch.dataprepper.model.record.Record;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.mock;



class JsonOutputCodecTest{

    private ByteArrayOutputStream outputStream;

    private static int numberOfRecords;


    private JsonOutputCodec createObjectUnderTest() {
        return new JsonOutputCodec();
    }













    @ParameterizedTest
    @ValueSource(ints = {1, 2, 10, 100})
    void test_happy_case(final int numberOfRecords) throws IOException {
        this.numberOfRecords = numberOfRecords;
        JsonOutputCodec jsonOutputCodec=createObjectUnderTest();
        outputStream = new ByteArrayOutputStream();
        jsonOutputCodec.start(outputStream);
        for (int index=0; index < numberOfRecords; index++) {
            final Event event = (Event) getRecord(index).getData();
            // Write Event
            jsonOutputCodec.writeEvent(event, outputStream);
        }
        jsonOutputCodec.complete(outputStream);
        List<HashMap> expectedRecords = generateRecords(numberOfRecords);
        //List<HashMap<String, Object>> actualRecords = new ArrayList<>();
        int index=0;
        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonNode = mapper.readTree(outputStream.toByteArray());
        for (JsonNode element : jsonNode) {
            Set<String> keys = expectedRecords.get(index).keySet();
            Map<String , Object> actualMap = new HashMap<>();
            for(String key: keys){
                actualMap.put(key, element.get(key).asText());
            }
            Object a=actualMap;
            Object b=expectedRecords.get(index);
            //actualRecords.add((HashMap) actualMap);
            assertThat(expectedRecords.get(index), Matchers.equalTo(actualMap));
            index++;

        }

        /*for (int i = 0; i < actualRecords.size(); i++) {

            final Record<Event> actualRecord = actualRecords.get(i);
            assertThat(actualRecord, notNullValue());
            assertThat(actualRecord.getData(), notNullValue());
            assertThat(actualRecord.getData().getMetadata(), notNullValue());
            assertThat(actualRecord.getData().getMetadata().getEventType(), equalTo(EventType.LOG.toString()));

            final Map<String, Object> expectedMap = jsonObjects.get(i);
            assertThat(actualRecord.getData().toMap(), equalTo(expectedMap));
        }*/
    }




    private static Record getRecord(int index){
        List<HashMap> recordList=generateRecords(numberOfRecords);
        final Event event = JacksonLog.builder().withData(recordList.get(index)).build();
        return new Record<>(event);
    }

    private static List<HashMap> generateRecords(int numberOfRecords) {

        List<HashMap> recordList = new ArrayList<>();

        for(int rows = 0; rows < numberOfRecords; rows++){

            HashMap<String, String> eventData = new HashMap<>();

            eventData.put("name", "Person"+rows);
            eventData.put("age", Integer.toString(rows));
            recordList.add((eventData));

        }

        return recordList;

    }
}