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

import com.google.common.collect.ImmutableMap;
import dev.bhupi.beam.examples.common.BigQueryDynamicWriteTransform;
import dev.bhupi.beam.examples.common.Util.GenericRecordCoder;
import dev.bhupi.beam.examples.common.Util.AvroGenericRecordDeserializer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.io.kafka.KafkaRecordCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaAvroExample {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaAvroExample.class);

  public static void main(String[] args) {
    KafkaAvroExampleOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(KafkaAvroExampleOptions.class);
    final Pipeline p = Pipeline.create(options);

    p.getCoderRegistry().registerCoderForClass(GenericRecord.class, GenericRecordCoder.of());

    final Map<String, Object> consumerConfig = new HashMap<>();
    consumerConfig.put("auto.offset.reset", "earliest");

    List<String> topicNames = options.getTopicNames();

    PCollection<KafkaRecord<String, GenericRecord>> messages =
        p.apply("Read KafkaTopics",
                KafkaIO.<String, GenericRecord>read()
                    .withBootstrapServers(
                        options.getKafkaHost())
                    .withTopics(topicNames)
                    .withConsumerConfigUpdates(consumerConfig)
                    .withConsumerConfigUpdates(
                        ImmutableMap.of("schema.registry.url", options.getKafkaSchemaRegistryUrl()))
                    .withKeyDeserializer(StringDeserializer.class)
                    .withValueDeserializer(AvroGenericRecordDeserializer.class)
            )
            .setCoder(KafkaRecordCoder.of(StringUtf8Coder.of(), GenericRecordCoder.of()));

    messages
        .apply(
            "Extract Records",
            ParDo.of(
                new DoFn<KafkaRecord<String, GenericRecord>, KafkaRecord<String, GenericRecord>>() {
                  @ProcessElement
                  public void processElement(
                      @Element KafkaRecord<String, GenericRecord> element,
                      OutputReceiver<KafkaRecord<String, GenericRecord>> out) {
                    LOG.info(String.valueOf(
                        Objects.requireNonNull(element.getKV().getValue()).getSchema()));
                    LOG.info(element.getKV().getKey());
                    LOG.info(Objects.requireNonNull(element.getKV().getValue()).toString());
                  }
                }));

    messages
        .apply(
            "BQ Write", new BigQueryDynamicWriteTransform(
                options.getBigQueryProjectName(),
                options.getBigQueryDatasetName()));

    p.run();
  }
}
