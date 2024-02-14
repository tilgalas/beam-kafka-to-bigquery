package dev.bhupi.beam.examples.common;

import com.google.common.base.Preconditions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.joda.time.Duration;

public class BigQueryDynamicWriteTransform
    extends PTransform<@NonNull PCollection<VersionedGenericRecord>, @NonNull WriteResult> {
  private final String bqProject;
  private final String bqDataset;
  private final String schemaRegistryUrl;

  public BigQueryDynamicWriteTransform(
      String bqProject, String bqDataset, String schemaRegistryUrl) {
    this.bqProject = bqProject;
    this.bqDataset = bqDataset;
    this.schemaRegistryUrl = schemaRegistryUrl;
  }

  @Override
  @NonNull
  public WriteResult expand(PCollection<VersionedGenericRecord> input) {
    return input.apply(
        BigQueryIO.<VersionedGenericRecord>write()
            .to(new ConfluentVersionedDynamicDestinations(bqProject, bqDataset, schemaRegistryUrl))
            .withAvroFormatFunction(
                avroWriteRequest -> {
                  VersionedGenericRecord versionedGenericRecord =
                      Preconditions.checkNotNull(
                          avroWriteRequest.getElement(), "AvroWriteRequest contains null element");
                  return versionedGenericRecord.getRecord();
                })
            .withMethod(BigQueryIO.Write.Method.STORAGE_WRITE_API)
            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
            .withTriggeringFrequency(Duration.standardSeconds(1L))
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
  }
}
