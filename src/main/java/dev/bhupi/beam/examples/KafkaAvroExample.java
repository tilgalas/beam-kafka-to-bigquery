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
import dev.bhupi.beam.examples.common.GenericRecordSchemaRegistryCoder;
import dev.bhupi.beam.examples.common.GenericRecordWithTopic;
import dev.bhupi.beam.examples.common.MaciekWriteToBQTransform;
import dev.bhupi.beam.examples.common.Util.AvroGenericRecordDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.kafka.DeserializerProvider;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.io.kafka.KafkaRecordCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.KeyForBottom;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class KafkaAvroExample {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaAvroExample.class);

  public static void main(String[] args) {
    KafkaAvroExampleOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(KafkaAvroExampleOptions.class);
    final Pipeline p = Pipeline.create(options);
    Coder<GenericRecordWithTopic> genericRecordCoder = new GenericRecordSchemaRegistryCoder(options.getKafkaSchemaRegistryUrl(), false);
    p.getCoderRegistry().registerCoderForClass(GenericRecordWithTopic.class, genericRecordCoder);

    final Map<String, Object> consumerConfig = new HashMap<>();
    consumerConfig.put("auto.offset.reset", "earliest");

    List<String> topicNames = options.getTopicNames();

    PCollection<KafkaRecord<String, GenericRecordWithTopic>> messages =
        p.apply("Read KafkaTopics",
                KafkaIO.<String, GenericRecordWithTopic>read()
                    .withBootstrapServers(
                        options.getKafkaHost())
                    .withTopics(topicNames)
                    .withConsumerConfigUpdates(consumerConfig)
                    .withConsumerConfigUpdates(
                        ImmutableMap.of("schema.registry.url", options.getKafkaSchemaRegistryUrl()))
                    .withKeyDeserializer(StringDeserializer.class)
                    .withValueDeserializer(AvroGenericRecordDeserializer.class)
            )
            .setCoder(KafkaRecordCoder.of(StringUtf8Coder.of(), genericRecordCoder));

    messages
        .apply(
            "Extract Records",
            ParDo.of(
                new DoFn<KafkaRecord<String, GenericRecordWithTopic>, Void>() {
                  @ProcessElement
                  public void processElement(
                      @Element KafkaRecord<String, GenericRecordWithTopic> element) {
                    LOG.info(String.valueOf(
                        Objects.requireNonNull(element.getKV().getValue()).getRecord().getSchema()));
                    LOG.info(element.getKV().getKey());
                    LOG.info(Objects.requireNonNull(element.getKV().getValue()).toString());
                  }
                }));

    messages.apply(new MaciekWriteToBQTransform(options.getBigQueryProjectName(), options.getBigQueryDatasetName(), options.getKafkaSchemaRegistryUrl()));

    p.run().waitUntilFinish();
  }
}
