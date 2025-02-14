/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.translate;


import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;


public class TranslateProcessorConfig {

    @JsonProperty("file_path")
    private String filePath;

    @JsonProperty("mappings")
    @Valid
    private List<MappingsParameterConfig> mappingsParameterConfigs = new ArrayList<>();

    @JsonIgnore
    private List<MappingsParameterConfig> combinedParameterConfigs;

    public String getFilePath() {
        return filePath;
    }

    public List<MappingsParameterConfig> getMappingsParameterConfigs() {
        return mappingsParameterConfigs;
    }

    public List<MappingsParameterConfig> getCombinedParameterConfigs() {
        if(Objects.isNull(combinedParameterConfigs)){
            combinedParameterConfigs = mappingsParameterConfigs;
        }
        return combinedParameterConfigs;
    }

    @AssertTrue(message = "Please ensure that at least one of the options, either \"mappings\" or \"file_path\", is properly configured.")
    public boolean hasMappings() {
        return (Objects.nonNull(mappingsParameterConfigs) && !mappingsParameterConfigs.isEmpty()) || Objects.nonNull(filePath);
    }

    @AssertTrue(message = "\"mappings\" option should not be empty.")
    public boolean isMappingsValid() {
        return Objects.nonNull(mappingsParameterConfigs);
    }

    @AssertTrue(message = "The file specified in the \"file_path\" option is not properly configured.")
    public boolean isFileValid() {
        return Objects.isNull(filePath) || readFileMappings(filePath);
    }

    private boolean readFileMappings(String filePath) {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        try {
            FilePathParser fileParser = mapper.readValue(new File(filePath), FilePathParser.class);
            Optional<List<MappingsParameterConfig>> optionalCombinedConfigs = fileParser.getCombinedMappings(mappingsParameterConfigs);
            optionalCombinedConfigs.ifPresent(combinedConfigs -> combinedParameterConfigs = combinedConfigs);
            return Optional.ofNullable(combinedParameterConfigs).map(configs -> true).orElse(false);
        } catch (IOException ex) {
            Logger LOG = LoggerFactory.getLogger(TranslateProcessor.class);
            LOG.error("Unable to parse the mappings from file", ex);
            return false;
        }
    }

}
