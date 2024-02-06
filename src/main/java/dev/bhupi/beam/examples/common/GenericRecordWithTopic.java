package dev.bhupi.beam.examples.common;

import com.google.auto.value.AutoValue;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.io.kafka.KafkaRecordCoder;

/**
 * This is class is meant to carry a topic name along with the {@link GenericRecord} that it was
 * read from, so that it can be serialized/deserialized by the Confluent's kafka serializer classes,
 * which need that information to decide what schema registry subject to consult for the relevant
 * schemas.
 *
 * <p>Note that putting a topic within an object that is later being used as part of a {@link
 * KafkaRecord} is somewhat redundant as the latter already has this information (among others).
 * Unfortunately the standard {@link KafkaRecordCoder} is unwilling to pass that metadata to its
 * component value coder, and so for the purpose of this PoC we must explicitly encode the topic
 * name in the serialized value.
 */
@AutoValue
public abstract class GenericRecordWithTopic {

  public abstract String getTopic();

  public abstract GenericRecord getRecord();

  public abstract Builder toBuilder();

  protected GenericRecordWithTopic() {}

  public static GenericRecordWithTopic of(String topic, GenericRecord record) {
    return new AutoValue_GenericRecordWithTopic.Builder().topic(topic).record(record).build();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder topic(String topic);

    public abstract Builder record(GenericRecord record);

    public abstract GenericRecordWithTopic build();
  }
}
