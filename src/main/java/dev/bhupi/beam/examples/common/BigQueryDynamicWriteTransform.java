package dev.bhupi.beam.examples.common;

import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.io.gcp.bigquery.*;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.joda.time.Duration;

public class BigQueryDynamicWriteTransform
    extends PTransform<PCollection<KafkaRecord<String, VersionedGenericRecord>>, WriteResult> {
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
  public WriteResult expand(PCollection<KafkaRecord<String, VersionedGenericRecord>> input) {
    return input.apply(
        BigQueryIO.<KafkaRecord<String, VersionedGenericRecord>>write()
            .to(
                new DynamicDestinations<
                    KafkaRecord<String, VersionedGenericRecord>, VersionedGenericRecord>() {
                  @Override
                  public VersionedGenericRecord getDestination(ValueInSingleWindow<KafkaRecord<String, VersionedGenericRecord>>
                              element) {
                    return element.getValue().getKV().getValue();
                  }

                  @Override
                  @NonNull
                  public TableDestination getTable(
                      VersionedGenericRecord destination) {
                    // maybe some massaging is needed to make sure that the subject forms a valid
                    // table name in BQ
                    String subject = destination.getSubject().replace('-', '_');
                    Integer version = destination.getVersion();
                    return new TableDestination(
                        String.format("%s:%s.%s_v%d", bqProject, bqDataset, subject, version),
                        String.format("A table for subject '%s' ver. %d", destination.getSubject(), version));
                  }

                  @Override
                  public TableSchema getSchema(
                      VersionedGenericRecord destination) {
                    return BigQueryUtils.toTableSchema(
                        AvroUtils.toBeamSchema(destination.getRecord().getSchema()));
                  }

                  @Override
                  public Coder<VersionedGenericRecord>
                      getDestinationCoder() {
                    return new GenericRecordSchemaRegistryCoder(schemaRegistryUrl, false);
                  }
                })
            .withAvroFormatFunction(
                avroWriteRequest -> avroWriteRequest.getElement().getKV().getValue().getRecord())
            .withMethod(BigQueryIO.Write.Method.STORAGE_WRITE_API)
            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
            .withTriggeringFrequency(Duration.standardSeconds(1L))
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
  }
}
