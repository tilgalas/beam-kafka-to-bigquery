package dev.bhupi.beam.examples.common;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link org.apache.beam.sdk.coders.Coder} for the {@link VersionedGenericRecord}.
 *
 * <p>It works by delegating the translation of the records into bytes to {@link
 * KafkaAvroSerializer} and {@link VersionedGenericRecordKafkaAvroDeserializer} classes for
 * respectively serialization and deserialization. Those classes in turn use caching {@link
 * io.confluent.kafka.schemaregistry.client.SchemaRegistryClient} to perform the mapping between
 * avro {@link org.apache.avro.Schema}s and their assigned ids which is what is ultimately read from
 * and written to the byte streams.
 */
public class VersionedGenericRecordCoder extends CustomCoder<VersionedGenericRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(VersionedGenericRecordCoder.class);
  private static final StringUtf8Coder STRING_UTF_8_CODER = StringUtf8Coder.of();
  private static final ByteArrayCoder BYTE_ARRAY_CODER = ByteArrayCoder.of();

  private final String schemaRegistryUrl;
  private final boolean isKey;

  // it seems that the serializer and deserializer classes are thread-safe
  private static final LazySupplier<VersionedGenericRecordKafkaAvroDeserializer>
      deserializerDelegate = new LazySupplier<>();
  private static final LazySupplier<KafkaAvroSerializer> serializerDelegate = new LazySupplier<>();

  public VersionedGenericRecordCoder(String schemaRegistryUrl, boolean isKey) {
    this.schemaRegistryUrl = schemaRegistryUrl;
    this.isKey = isKey;
  }

  private Map<String, Object> configProps() {

    return ImmutableMap.of(
        AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
        schemaRegistryUrl,
        AbstractKafkaAvroSerDeConfig.AUTO_REGISTER_SCHEMAS,
        false);
  }

  private static <T> T getOrInitGeneric(
      String name,
      Supplier<T> tFactory,
      LazySupplier<T> reference,
      Function<T, BiConsumer<Map<String, Object>, Boolean>> configureMethod,
      Supplier<Map<String, Object>> configPropsSupplier,
      boolean isKey) {

    return reference.get(
        () -> {
          LOG.info("initializing {}", name);
          T t = tFactory.get();
          configureMethod.apply(t).accept(configPropsSupplier.get(), isKey);
          return t;
        });
  }

  private static VersionedGenericRecordKafkaAvroDeserializer getOrInitDeserializer(
      Supplier<Map<String, Object>> configs, boolean isKey) {
    return getOrInitGeneric(
        "deserializer",
        VersionedGenericRecordKafkaAvroDeserializer::new,
        deserializerDelegate,
        deserializer -> deserializer::configure,
        configs,
        isKey);
  }

  private static KafkaAvroSerializer getOrInitSerializer(
      Supplier<Map<String, Object>> configs, boolean isKey) {
    return getOrInitGeneric(
        "serializer",
        KafkaAvroSerializer::new,
        serializerDelegate,
        serializer -> serializer::configure,
        configs,
        isKey);
  }

  private VersionedGenericRecordKafkaAvroDeserializer getDeserializer() {
    return getOrInitDeserializer(this::configProps, isKey);
  }

  private KafkaAvroSerializer getSerializer() {
    return getOrInitSerializer(this::configProps, isKey);
  }

  @Override
  public void encode(VersionedGenericRecord value, @NonNull OutputStream outStream)
      throws IOException {
    Preconditions.checkNotNull(value, "VersionedGenericRecord is null");
    KafkaAvroSerializer serializer = getSerializer();
    byte[] bytes = serializer.serialize(value.getVersion().getTopic(), value.getRecord());

    Counter c = Metrics.counter(VersionedGenericRecordCoder.class, "genericRecordBytesEncoded");
    c.inc(bytes.length);
    STRING_UTF_8_CODER.encode(value.getVersion().getTopic(), outStream);
    BYTE_ARRAY_CODER.encode(bytes, outStream);
  }

  @Override
  public VersionedGenericRecord decode(@NonNull InputStream inStream) throws IOException {
    String topic = STRING_UTF_8_CODER.decode(inStream);
    byte[] bytes = BYTE_ARRAY_CODER.decode(inStream);
    Counter c = Metrics.counter(VersionedGenericRecordCoder.class, "genericRecordBytesDecoded");
    c.inc(bytes.length);
    VersionedGenericRecordKafkaAvroDeserializer deserializer = getDeserializer();
    return deserializer.deserialize(topic, bytes);
  }

  @Override
  public void verifyDeterministic() {}
}
