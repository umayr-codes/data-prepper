package org.opensearch.dataprepper.plugins.codec.avro;

import org.apache.avro.Schema;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AvroSchemaParserFromTabularFormat {

    private static final String END_SCHEMA_STRING = "]}";

    static Schema generateSchemaFromTabular(String inputString){
        String recordSchemaOutputString = getString();
        recordSchemaOutputString = recordSchemaOutputString.trim();

        String[] words = recordSchemaOutputString.split("\\s+");

        String tableName = null;
        if (words.length >= 2) {
            tableName = words[1];
        } else {
            // TODO: Throw Exception
        }

        Pattern pattern = Pattern.compile("\\(([^()]*|)*\\)");
        Matcher matcher = pattern.matcher(recordSchemaOutputString);

        if (matcher.find()) {
            recordSchemaOutputString = matcher.group(0);
        }

        recordSchemaOutputString = recordSchemaOutputString.replaceAll("\\(", "");
        recordSchemaOutputString = recordSchemaOutputString.replaceAll("\\)", "");

        recordSchemaOutputString = recordSchemaOutputString.trim();

        recordSchemaOutputString = recordSchemaOutputString.replaceAll("\\n", "");
        recordSchemaOutputString = recordSchemaOutputString.replaceAll(",\\s+", ",");
        recordSchemaOutputString = recordSchemaOutputString.replaceAll(">\\s+", ">");

        final StringBuilder mainSchemaBuilder = new StringBuilder();
        String baseSchemaStr = "{\"type\":\"record\",\"name\":\"" + tableName + "\",\"fields\":[";
        mainSchemaBuilder.append(baseSchemaStr);

        iterateRecursively(mainSchemaBuilder, recordSchemaOutputString, false, tableName, 0);

        mainSchemaBuilder.append(END_SCHEMA_STRING);

        return new Schema.Parser().parse(mainSchemaBuilder.toString());
    }

    private static String buildBaseSchemaString(String part) {
        return "{\"type\":\"record\",\"name\":\"" + part + "\",\"fields\":[";
    }

    public static void iterateRecursively(StringBuilder mainSchemaBuilder, String recordSchema,
                                          boolean isStructString, String tableName, int innerRecCounter) {
        boolean isNameStringFormed = false;
        StringBuilder fieldNameBuilder = new StringBuilder();
        StringBuilder fieldTypeBuilder = new StringBuilder();
        boolean isFirstRecordForName = true;

        char[] schemaStrCharArr = recordSchema.toCharArray();
        int curPosInSchemaStrCharArr = 0;

        while (curPosInSchemaStrCharArr < schemaStrCharArr.length) {
            char currentCharFromArr = schemaStrCharArr[curPosInSchemaStrCharArr];
            curPosInSchemaStrCharArr++;

            if (!isNameStringFormed) {
                if (isStructString && currentCharFromArr == ':') {
                    if (isFirstRecordForName) {
                        mainSchemaBuilder.append("{\"name\":\"" + fieldNameBuilder.toString() + "\",\"type\":\"");
                    } else {
                        mainSchemaBuilder.append(",{\"name\":\"" + fieldNameBuilder.toString() + "\",\"type\":\"");
                    }
                    isNameStringFormed = true;
                    fieldNameBuilder = new StringBuilder();
                    isFirstRecordForName = false;
                    continue;
                } else if (currentCharFromArr == ' ') {
                    if (isFirstRecordForName) {
                        mainSchemaBuilder.append("{\"name\":\"" + fieldNameBuilder.toString() + "\",\"type\":\"");
                    } else {
                        mainSchemaBuilder.append(",{\"name\":\"" + fieldNameBuilder.toString() + "\",\"type\":\"");
                    }
                    isNameStringFormed = true;
                    fieldNameBuilder = new StringBuilder();
                    isFirstRecordForName = false;
                    continue;
                }
                fieldNameBuilder.append(currentCharFromArr);
            }

            if (isNameStringFormed) {

                if (currentCharFromArr == ',' || curPosInSchemaStrCharArr == schemaStrCharArr.length) {
                    if (curPosInSchemaStrCharArr == schemaStrCharArr.length) {
                        fieldTypeBuilder.append(currentCharFromArr);
                    }
                    String type = fieldTypeBuilder.toString().trim() + "\"}";

                    mainSchemaBuilder.append(type);
                    isNameStringFormed = false;
                    fieldTypeBuilder = new StringBuilder();
                    continue;
                }

                fieldTypeBuilder.append(currentCharFromArr);
                if ("struct".equals(fieldTypeBuilder.toString())) {
                    mainSchemaBuilder.deleteCharAt(mainSchemaBuilder.length() - 1);
                    mainSchemaBuilder.append(buildBaseSchemaString(tableName + "_" + innerRecCounter));
                    String structSchemaStr = recordSchema.substring(curPosInSchemaStrCharArr);
                    StringBuilder structString = new StringBuilder();
                    int openClosedCounter = 0;
                    int structSchemaStrEndBracketPos = 0;
                    for (char innerChar : structSchemaStr.toCharArray()) {
                        structSchemaStrEndBracketPos++;
                        if (innerChar == '<') {
                            openClosedCounter++;
                        } else if (innerChar == '>') {
                            openClosedCounter--;
                        }
                        structString.append(innerChar);
                        if (openClosedCounter == 0) {
                            break;
                        }
                    }

                    String innerRecord = structString.toString().substring(1, structSchemaStrEndBracketPos - 1);
                    iterateRecursively(mainSchemaBuilder, innerRecord, true,
                            tableName, innerRecCounter + 1);
                    mainSchemaBuilder.append("}");
                    curPosInSchemaStrCharArr = curPosInSchemaStrCharArr + structSchemaStrEndBracketPos;
                    if (curPosInSchemaStrCharArr < schemaStrCharArr.length) {
                        // Skip one comma after the close struct close
                        curPosInSchemaStrCharArr++;
                    }
                    isNameStringFormed = false;
                    fieldTypeBuilder = new StringBuilder();
                }
            }
        }

        if (isStructString) {
            mainSchemaBuilder.append(END_SCHEMA_STRING);
        }
    }

    private static String getString() {
        String inputString = "TABLE sesblog (\n" +
                "  eventType string,\n" +
                "  ews string,\n" +
                "  mail struct<col1:string,\n" +
                "              innercolName struct<colInner:string> \n" +
                "              >,\n" +
                "  collumn2 string) ";
        return inputString;
    }

}