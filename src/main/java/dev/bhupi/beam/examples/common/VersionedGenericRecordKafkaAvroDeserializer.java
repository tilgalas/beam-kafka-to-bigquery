package dev.bhupi.beam.examples.common;

import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer;
import io.confluent.kafka.serializers.GenericContainerWithVersion;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;

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
        topic, subject, genericContainerWithVersion.version(), genericRecord);
  }
}
