package dev.bhupi.beam.examples.common;

import com.google.auto.value.AutoValue;
import org.apache.avro.Schema;

/**
 * This class is similar to the {@link VersionedGenericRecord} except that it doesn't contain any
 * payload, only its governing {@link Schema}. It is needed in the final step of the pipeline, as
 * part of the {@link ConfluentVersionedDynamicDestinations} class to map the record to a table
 * spec. The objects of this class will serve as grouping keys, that's why they only consist of a
 * {@code Schema} and the set of metadata, and are lacking the record as the table spec doesn't
 * depend on its contents.
 */
@AutoValue
public abstract class VersionedAvroSchema {

  public abstract ConfluentVersion getVersion();

  public abstract Schema getSchema();

  public abstract Builder toBuilder();

  public static VersionedAvroSchema of(ConfluentVersion version, Schema schema) {
    return new AutoValue_VersionedAvroSchema.Builder().version(version).schema(schema).build();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder version(ConfluentVersion version);

    public abstract Builder schema(Schema schema);

    public abstract VersionedAvroSchema build();
  }
}
