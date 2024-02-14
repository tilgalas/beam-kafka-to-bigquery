/*
 * Copyright 2024 Bhupinder Sindhwani
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.bhupi.beam.examples;

import java.util.List;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation.Required;

public interface KafkaAvroExampleOptions extends PipelineOptions {

  @Description("Kafka server host")
  String getKafkaHost();

  void setKafkaHost(String value);

  @Description("Kafka schema registry url")
  String getKafkaSchemaRegistryUrl();

  void setKafkaSchemaRegistryUrl(String value);

  @Description("List of Kafka topics to read data from")
  @Required
  List<String> getTopicNames();

  void setTopicNames(List<String> value);

  @Description("BigQuery project name")
  String getBigQueryProjectName();

  void setBigQueryProjectName(String value);

  @Description("BigQuery dataset name")
  String getBigQueryDatasetName();

  void setBigQueryDatasetName(String value);

  @Description("Output directory for the Avro files")
  String getAvroOutput();

  void setAvroOutput(String value);
}
