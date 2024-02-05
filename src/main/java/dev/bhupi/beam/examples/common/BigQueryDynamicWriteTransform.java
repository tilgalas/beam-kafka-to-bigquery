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
package dev.bhupi.beam.examples.common;

import com.google.api.services.bigquery.model.TableSchema;
import java.util.Objects;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.io.kafka.KafkaRecordCoder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigQueryDynamicWriteTransform extends
    PTransform<PCollection<KafkaRecord<String, GenericRecord>>, WriteResult> {

  public static final Logger LOG = LoggerFactory.getLogger(BigQueryDynamicWriteTransform.class);

  private final String bigQueryProjectName;
  private final String bigQueryDatasetName;

  public BigQueryDynamicWriteTransform(String bigQueryProjectName,
      String bigQueryDatasetName) {
    this.bigQueryProjectName = bigQueryProjectName;
    this.bigQueryDatasetName = bigQueryDatasetName;
  }

  @Override
  public WriteResult expand(PCollection<KafkaRecord<String, GenericRecord>> input) {

    return input
        .apply(BigQueryIO.<KafkaRecord<String, GenericRecord>>write()
            .to(new DynamicDestinations<KafkaRecord<String, GenericRecord>, KafkaRecord<String, GenericRecord>>() {
              @Override
              public KafkaRecord<String, GenericRecord> getDestination(
                  @Nullable @UnknownKeyFor @Initialized ValueInSingleWindow<KafkaRecord<String, GenericRecord>> element) {
                return Objects.requireNonNull(element).getValue();
              }

              @Override
              public @UnknownKeyFor @NonNull @Initialized TableDestination getTable(
                  KafkaRecord<String, GenericRecord> destination) {
                return new TableDestination(
                    String.format("%s:%s.%s", bigQueryProjectName, bigQueryDatasetName,
                        destination.getTopic()),
                    String.format("Table for KafKa %s", destination.getTopic())
                );
              }

              @Override
              public @Nullable @UnknownKeyFor @Initialized TableSchema getSchema(
                  KafkaRecord<String, GenericRecord> destination) {
                return BigQueryUtils.toTableSchema(
                    AvroUtils.toBeamSchema(
                        Objects.requireNonNull(destination.getKV().getValue()).getSchema()));
              }

              @Override
              public @Nullable @UnknownKeyFor @Initialized Coder<KafkaRecord<String, GenericRecord>> getDestinationCoder() {
                return KafkaRecordCoder.of(StringUtf8Coder.of(), Util.GenericRecordCoder.of());
              }
            })
            .withAvroFormatFunction(avroWriteRequest -> Objects.requireNonNull(
                avroWriteRequest.getElement()).getKV().getValue())
            .withMethod(BigQueryIO.Write.Method.STORAGE_WRITE_API)
            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
            .withTriggeringFrequency(Duration.standardSeconds(1L))
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

  }
}
