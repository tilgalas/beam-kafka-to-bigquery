package dev.bhupi.beam.examples.common;

import com.google.auto.value.AutoValue;

/**
 * This class is meant to carry the schema registry metadata (topic, subject, version) associated
 * with some serializable object (such as {@link org.apache.avro.Schema} or {@link
 * org.apache.avro.generic.GenericRecord}), needed during the serialization/encoding of that object
 * with the help of Confluent's kafka serializer classes or the {@link
 * io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient} itself.
 *
 * <p>The information contained here may be used to query the schema registry to obtain the {@code
 * Schema} objects needed later during the serialization/encoding.
 */
@AutoValue
public abstract class ConfluentVersion {

  /**
   * The kafka topic that the object was read from. Kafka avro serializers can use it to determine
   * the schema registry subject name once they're given a record read from this topic.
   *
   * @return the topic the versioned object was read from
   */
  public abstract String getTopic();

  /**
   * Confluent Schema Registry subject name. Can be computed by kafka avro deserializer from the
   * topic and record if the latter is available.
   *
   * @return the schema registry subject of this record
   */
  public abstract String getSubject();

  /**
   * Subject's schema version. Can be computed by kafka avro deserializer from the topic and record
   * if the latter is available.
   *
   * @return the subject's version
   */
  public abstract Integer getVersion();

  public abstract Builder toBuilder();

  public static ConfluentVersion of(String topic, String subject, Integer version) {
    return new AutoValue_ConfluentVersion.Builder()
        .topic(topic)
        .subject(subject)
        .version(version)
        .build();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder topic(String topic);

    public abstract Builder subject(String subject);

    public abstract Builder version(Integer version);

    public abstract ConfluentVersion build();
  }
}
