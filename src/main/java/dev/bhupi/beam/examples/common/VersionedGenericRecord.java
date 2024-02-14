package dev.bhupi.beam.examples.common;

import com.google.auto.value.AutoValue;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.io.kafka.KafkaRecordCoder;

/**
 * This class contains a {@link GenericRecord} along with a {@link ConfluentVersion} object to be
 * able to serialize/encode it with the help of Confluent Schema Registry.
 *
 * <p>Note that putting a topic within an object that is later being used as part of a {@link
 * KafkaRecord} is somewhat redundant as the latter already has this information (among others).
 * Unfortunately the standard {@link KafkaRecordCoder} is unwilling to pass that metadata to its
 * component value coder, and so for the purpose of this PoC we must explicitly encode the topic
 * name in the serialized value.
 */
@AutoValue
public abstract class VersionedGenericRecord {

  /**
   * Version metadata for the payload
   *
   * @return the {@link ConfluentVersion} for the {@link GenericRecord} contained in this object
   */
  public abstract ConfluentVersion getVersion();

  /**
   * The payload
   *
   * @return avro {@link GenericRecord} read from kafka
   */
  public abstract GenericRecord getRecord();

  public abstract Builder toBuilder();

  protected VersionedGenericRecord() {}

  public static VersionedGenericRecord of(ConfluentVersion version, GenericRecord record) {
    return new AutoValue_VersionedGenericRecord.Builder().version(version).record(record).build();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder version(ConfluentVersion version);

    public abstract Builder record(GenericRecord record);

    public abstract VersionedGenericRecord build();
  }
}
