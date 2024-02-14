package dev.bhupi.beam.examples.common;

import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.base.Preconditions;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.checkerframework.checker.nullness.qual.NonNull;

class ConfluentVersionedDynamicDestinations
    extends DynamicDestinations<VersionedGenericRecord, VersionedAvroSchema> {

  private final String bqProject;
  private final String bqDataset;
  private final String schemaRegistryUrl;

  public ConfluentVersionedDynamicDestinations(
      String bqProject, String bqDataset, String schemaRegistryUrl) {

    this.bqProject = bqProject;
    this.bqDataset = bqDataset;
    this.schemaRegistryUrl = schemaRegistryUrl;
  }

  @Override
  public VersionedAvroSchema getDestination(ValueInSingleWindow<VersionedGenericRecord> element) {
    Preconditions.checkNotNull(element, "element is null");
    VersionedGenericRecord record =
        Preconditions.checkNotNull(element.getValue(), "element contains null value");

    return VersionedAvroSchema.of(record.getVersion(), record.getRecord().getSchema());
  }

  @Override
  @NonNull
  public TableDestination getTable(VersionedAvroSchema destination) {
    // maybe some massaging is needed to make sure that the subject forms a valid
    // table name in BQ
    Preconditions.checkNotNull(destination, "destination is null");
    String subject = destination.getVersion().getSubject();
    String bqTable = subject.replace('-', '_');
    Integer version = destination.getVersion().getVersion();
    return new TableDestination(
        String.format("%s:%s.%s_v%d", bqProject, bqDataset, bqTable, version),
        String.format("A table for subject '%s' ver. %d", subject, version));
  }

  @Override
  public TableSchema getSchema(VersionedAvroSchema destination) {
    Preconditions.checkNotNull(destination, "destination is null");
    return BigQueryUtils.toTableSchema(AvroUtils.toBeamSchema(destination.getSchema()));
  }

  @Override
  public Coder<VersionedAvroSchema> getDestinationCoder() {
    return new VersionedAvroSchemaCoder(schemaRegistryUrl, null);
  }
}
