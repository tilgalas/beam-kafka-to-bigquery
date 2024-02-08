package dev.bhupi.beam.examples.common;

import com.google.auto.value.AutoValue;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.io.kafka.KafkaRecordCoder;

/**
 * This is class is meant to carry a schema registry metadata (topic, subject, version) along with
 * the corresponding {@link GenericRecord}, so that it can be serialized/deserialized by the
 * Confluent's kafka serializer classes, which need that information to decide what schema registry
 * subject to consult for the relevant schemas.
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
   * Needed by kafka avro serializers to determine the schema registry subject this records belongs
   * to.
   *
   * @return the topic this record was read from
   */
  public abstract String getTopic();

  /**
   * Needed to determine the BQ table to write the record to. Computed by kafka avro deserializer.
   *
   * @return the schema registry subject of this record
   */
  public abstract String getSubject();

  /**
   * Needed to determine the BQ table to write the record to. Computed by kafka avro deserializer.
   *
   * @return the subject's version of this record
   */
  public abstract Integer getVersion();

  /**
   * The payload
   *
   * @return avro generic record read from kafka
   */
  public abstract GenericRecord getRecord();

  public abstract Builder toBuilder();

  protected VersionedGenericRecord() {}

  public static VersionedGenericRecord of(
      String topic, String subject, Integer version, GenericRecord record) {
    return new AutoValue_VersionedGenericRecord.Builder()
        .topic(topic)
        .subject(subject)
        .version(version)
        .record(record)
        .build();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder topic(String topic);

    public abstract Builder subject(String subject);

    public abstract Builder version(Integer version);

    public abstract Builder record(GenericRecord record);

    public abstract VersionedGenericRecord build();
  }
}
