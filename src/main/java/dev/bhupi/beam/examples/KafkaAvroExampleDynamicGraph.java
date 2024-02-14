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

import com.google.common.base.Preconditions;
import dev.bhupi.beam.examples.common.BigQueryDynamicWriteTransform;
import dev.bhupi.beam.examples.common.VersionedGenericRecord;
import dev.bhupi.beam.examples.common.VersionedGenericRecordKafkaAvroDeserializer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaAvroExampleDynamicGraph {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaAvroExampleDynamicGraph.class);

  public static void main(String[] args) {
    KafkaAvroExampleOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(KafkaAvroExampleOptions.class);
    final Pipeline p = Pipeline.create(options);

    final Map<String, Object> consumerConfig = new HashMap<>();
    consumerConfig.put("auto.offset.reset", "earliest");

    List<String> topicNames = options.getTopicNames();

    topicNames.forEach(
        topicName -> {
          PCollection<KafkaRecord<String, VersionedGenericRecord>> messages =
              p.apply(
                  "Read KafkaTopic:" + topicName,
                  KafkaIO.<String, VersionedGenericRecord>read()
                      .withBootstrapServers(options.getKafkaHost())
                      .withTopic(topicName)
                      .withConsumerConfigUpdates(consumerConfig)
                      .withKeyDeserializer(StringDeserializer.class)
                      .withValueDeserializer(VersionedGenericRecordKafkaAvroDeserializer.class));

          messages.apply(
              "Extract Records:" + topicName,
              ParDo.of(
                  new DoFn<KafkaRecord<String, VersionedGenericRecord>, Void>() {
                    @ProcessElement
                    public void processElement(
                        @Element KafkaRecord<String, GenericRecord> element) {
                      LOG.info(
                          String.valueOf(
                              Objects.requireNonNull(element.getKV().getValue()).getSchema()));
                      LOG.info(element.getKV().getKey());
                      LOG.info(Objects.requireNonNull(element.getKV().getValue()).toString());
                    }
                  }));

          PCollection<VersionedGenericRecord> records =
              messages.apply(
                  MapElements.into(new TypeDescriptor<VersionedGenericRecord>() {})
                      .via(
                          kafkaRecord ->
                              Preconditions.checkNotNull(kafkaRecord).getKV().getValue()));

          records.apply(
              "BQ Write:" + topicName,
              new BigQueryDynamicWriteTransform(
                  options.getBigQueryProjectName(),
                  options.getBigQueryDatasetName(),
                  options.getKafkaSchemaRegistryUrl()));
        });

    p.run();
  }
}
