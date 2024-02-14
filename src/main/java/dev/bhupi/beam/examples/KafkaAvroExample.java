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
import com.google.common.collect.ImmutableMap;
import dev.bhupi.beam.examples.common.BigQueryDynamicWriteTransform;
import dev.bhupi.beam.examples.common.ConfluentVersion;
import dev.bhupi.beam.examples.common.VersionedAvroSchema;
import dev.bhupi.beam.examples.common.VersionedAvroSchemaCoder;
import dev.bhupi.beam.examples.common.VersionedGenericRecord;
import dev.bhupi.beam.examples.common.VersionedGenericRecordCoder;
import dev.bhupi.beam.examples.common.VersionedGenericRecordKafkaAvroDeserializer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.avro.io.AvroIO;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.io.kafka.KafkaRecordCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaAvroExample {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaAvroExample.class);

  public static void main(String[] args) {
    KafkaAvroExampleOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(KafkaAvroExampleOptions.class);
    final Pipeline p = Pipeline.create(options);
    Coder<VersionedGenericRecord> genericRecordCoder =
        new VersionedGenericRecordCoder(options.getKafkaSchemaRegistryUrl(), false);
    p.getCoderRegistry().registerCoderForClass(VersionedGenericRecord.class, genericRecordCoder);

    final Map<String, Object> consumerConfig = new HashMap<>();
    consumerConfig.put("auto.offset.reset", "earliest");

    List<String> topicNames = options.getTopicNames();

    PCollection<KafkaRecord<String, VersionedGenericRecord>> messages =
        p.apply(
                "Read KafkaTopics",
                KafkaIO.<String, VersionedGenericRecord>read()
                    .withBootstrapServers(options.getKafkaHost())
                    .withTopics(topicNames)
                    .withConsumerConfigUpdates(consumerConfig)
                    .withConsumerConfigUpdates(
                        ImmutableMap.of("schema.registry.url", options.getKafkaSchemaRegistryUrl()))
                    .withKeyDeserializer(StringDeserializer.class)
                    .withValueDeserializer(VersionedGenericRecordKafkaAvroDeserializer.class))
            .setCoder(KafkaRecordCoder.of(StringUtf8Coder.of(), genericRecordCoder));

    messages.apply(
        "Extract Records",
        ParDo.of(
            new DoFn<KafkaRecord<String, VersionedGenericRecord>, Void>() {
              @ProcessElement
              public void processElement(
                  @Element KafkaRecord<String, VersionedGenericRecord> element) {
                LOG.info(
                    String.valueOf(
                        Preconditions.checkNotNull(element.getKV().getValue())
                            .getRecord()
                            .getSchema()));
                LOG.info(element.getKV().getKey());
                LOG.info(Preconditions.checkNotNull(element.getKV().getValue()).toString());
              }
            }));
    PCollection<VersionedGenericRecord> records =
        messages.apply(
            MapElements.into(new TypeDescriptor<VersionedGenericRecord>() {})
                .via(kafkaRecord -> Preconditions.checkNotNull(kafkaRecord).getKV().getValue()));

    records.apply(
        "Write to BQ with Confluent Schema Registry",
        new BigQueryDynamicWriteTransform(
            options.getBigQueryProjectName(),
            options.getBigQueryDatasetName(),
            options.getKafkaSchemaRegistryUrl()));
    records
        // to write the records into a file we need some windowing
        .apply(Window.into(FixedWindows.of(Duration.standardMinutes(1))))
        .apply(
            "Write to AvroIO sink",
            FileIO.<VersionedAvroSchema, VersionedGenericRecord>writeDynamic()
                // to every record we can assign its destination represented by a
                // VersionedAvroSchema object
                .by(
                    genericRecord ->
                        VersionedAvroSchema.of(
                            Preconditions.checkNotNull(genericRecord, "genericRecord is null")
                                .getVersion(),
                            genericRecord.getRecord().getSchema()))
                // its coder
                .withDestinationCoder(
                    new VersionedAvroSchemaCoder(options.getKafkaSchemaRegistryUrl(), null))
                // first fn transforms the VersionedGenericRecord into a GenericRecord which is what
                // the Avro sink expects
                // second fn maps the destination computed in the by function above into an
                // AvroIO.sink
                .via(
                    Contextful.fn(
                        versionedGenericRecord ->
                            Preconditions.checkNotNull(
                                    versionedGenericRecord, "versionedGenericRecord is null")
                                .getRecord()), // we write generic record
                    Contextful.fn(
                        versionedAvroSchema ->
                            AvroIO.sink(
                                Preconditions.checkNotNull(
                                        versionedAvroSchema, "versionedAvroSchema is null")
                                    .getSchema())) // with AvroIO sink based on the destination
                    )
                // the naming strategy of the file is also determined from the destination
                .withNaming(
                    Contextful.fn(
                        versionedAvroSchema -> {
                          ConfluentVersion confluentVersion =
                              Preconditions.checkNotNull(
                                      versionedAvroSchema, "versionedAvroSchema is null")
                                  .getVersion();
                          String namingPrefix =
                              String.format(
                                  "%s-v%d",
                                  confluentVersion.getSubject(), confluentVersion.getVersion());
                          return FileIO.Write.defaultNaming(namingPrefix, "");
                        }))
                // the directory to put it all into
                .to(options.getAvroOutput()));

    p.run().waitUntilFinish();
  }
}
