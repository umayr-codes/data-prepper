/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.codec.csv;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;
import org.hamcrest.Matchers;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.record.Record;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;

public class CsvOutputCodecTest {
    private ByteArrayOutputStream outputStream;

    private static int numberOfRecords;
    private CsvOutputCodecConfig config;



    private CsvOutputCodec createObjectUnderTest() {

        config = new CsvOutputCodecConfig((List<String>)header());
        return new CsvOutputCodec(config);
    }


    @ParameterizedTest
    @ValueSource(ints = {1, 2, 10, 100})
    void test_happy_case(final int numberOfRecords) throws IOException {
        this.numberOfRecords = numberOfRecords;
        CsvOutputCodec csvOutputCodec = createObjectUnderTest();
        outputStream = new ByteArrayOutputStream();
        csvOutputCodec.start(outputStream);
        for (int index = 0; index < numberOfRecords; index++) {
            final Event event = (Event) getRecord(index).getData();
            csvOutputCodec.writeEvent(event, outputStream);
        }
        csvOutputCodec.complete(outputStream);
        // Convert output stream to string
        String csvData = new String(outputStream.toByteArray(), StandardCharsets.UTF_8);

        // Create a StringReader from the CSV data
        StringReader stringReader = new StringReader(csvData);

        // Create a CSVReader using the StringReader
        CSVReader csvReader = new CSVReaderBuilder(stringReader).build();

        try {
            // Read the CSV data line by line
            String[] line;
            int index=0;
            int headerIndex;
            List<String> headerList = header();
            List<HashMap> expectedRecords = generateRecords(numberOfRecords);
            while ((line = csvReader.readNext()) != null) {
                // Process each line of the CSV
                if(index==0){
                    headerIndex=0;
                    for(String value: line){
                        assertThat(headerList.get(headerIndex), Matchers.equalTo(value));
                        headerIndex++;
                    }
                }
                else{
                    headerIndex=0;
                    for (String value : line) {
                        assertThat(expectedRecords.get(index-1).get(headerList.get(headerIndex)), Matchers.equalTo(value));
                        headerIndex++;
                    }
                }
                index++;
            }
        } catch (IOException | CsvValidationException e) {
            e.printStackTrace();
        } finally {
            try {
                // Close the CSVReader and StringReader
                csvReader.close();
                stringReader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    private static Record getRecord(int index) {
        List<HashMap> recordList = generateRecords(numberOfRecords);
        final Event event = JacksonLog.builder().withData(recordList.get(index)).build();
        return new Record<>(event);
    }

    private static List<HashMap> generateRecords(int numberOfRecords) {

        List<HashMap> recordList = new ArrayList<>();

        for (int rows = 0; rows < numberOfRecords; rows++) {

            HashMap<String, Object> eventData = new HashMap<>();

            eventData.put("name", "Person" + rows);
            eventData.put("age", Integer.toString(rows));
            recordList.add((eventData));

        }
        return recordList;
    }

    private static List<String> header(){
        List<String> header = new ArrayList<>();
        header.add("name");
        header.add("age");
        return header;
    }
}
