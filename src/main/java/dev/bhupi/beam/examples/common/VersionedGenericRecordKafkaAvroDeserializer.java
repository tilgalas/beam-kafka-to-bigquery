package dev.bhupi.beam.examples.common;

import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer;
import io.confluent.kafka.serializers.GenericContainerWithVersion;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * A subclass of the {@link AbstractKafkaAvroDeserializer} made in order to use the protected
 * methods. Specifically, we want to call the {@link
 * AbstractKafkaAvroDeserializer#deserialize(boolean, String, Boolean, byte[], Schema)} method with
 * {@code includeSchemaAndVersion} set to true, so that we could obtain the version of the subject
 * used during the deserialization, as well as the {@link
 * AbstractKafkaAvroDeserializer#getSubjectName(String, boolean, Object, Schema)} to get the subject
 * itself, both of which we later use to populate the {@code VersionedGenericRecord}'s fields
 */
public class VersionedGenericRecordKafkaAvroDeserializer extends AbstractKafkaAvroDeserializer
    implements Deserializer<VersionedGenericRecord> {

  private boolean isKey = false;

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    configure(deserializerConfig(configs));
    this.isKey = isKey;
  }

  @Override
  public VersionedGenericRecord deserialize(String topic, byte[] data) {

    GenericContainerWithVersion genericContainerWithVersion =
        (GenericContainerWithVersion) deserialize(true, topic, isKey, data, null);
    GenericRecord genericRecord = (GenericRecord) genericContainerWithVersion.container();
    String subject = getSubjectName(topic, isKey, genericRecord, genericRecord.getSchema());
    return VersionedGenericRecord.of(
        ConfluentVersion.of(topic, subject, genericContainerWithVersion.version()), genericRecord);
  }
}
