package dev.bhupi.beam.examples.common;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GenericRecordSchemaRegistryCoder extends CustomCoder<VersionedGenericRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(GenericRecordSchemaRegistryCoder.class);
  private static final StringUtf8Coder STRING_UTF_8_CODER = StringUtf8Coder.of();
  private static final ByteArrayCoder BYTE_ARRAY_CODER = ByteArrayCoder.of();

  private final String schemaRegistryUrl;
  private final boolean isKey;

  // it seems that the serializer and deserializer classes are thread-safe
  private static final AtomicReference<VersionedGenericRecordKafkaAvroDeserializer>
      deserializerDelegate = new AtomicReference<>();
  private static final AtomicReference<KafkaAvroSerializer> serializerDelegate =
      new AtomicReference<>();

  public GenericRecordSchemaRegistryCoder(String schemaRegistryUrl, boolean isKey) {
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
      AtomicReference<T> reference,
      Function<T, BiConsumer<Map<String, Object>, Boolean>> configureMethod,
      Supplier<Map<String, Object>> configPropsSupplier,
      boolean isKey) {

    T t = reference.get();
    if (t == null) {
      synchronized (reference) {
        if ((t = reference.get()) == null) {
          LOG.info("initializing {}", name);
          t = tFactory.get();
          configureMethod.apply(t).accept(configPropsSupplier.get(), isKey);
          reference.set(t);
        }
      }
    }

    return t;
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
    KafkaAvroSerializer serializer = getSerializer();
    byte[] bytes = serializer.serialize(value.getTopic(), value.getRecord());

    Counter c =
        Metrics.counter(GenericRecordSchemaRegistryCoder.class, "genericRecordBytesEncoded");
    c.inc(bytes.length);
    STRING_UTF_8_CODER.encode(value.getTopic(), outStream);
    BYTE_ARRAY_CODER.encode(bytes, outStream);
  }

  @Override
  public VersionedGenericRecord decode(@NonNull InputStream inStream) throws IOException {
    String topic = STRING_UTF_8_CODER.decode(inStream);
    byte[] bytes = BYTE_ARRAY_CODER.decode(inStream);
    Counter c =
        Metrics.counter(GenericRecordSchemaRegistryCoder.class, "genericRecordBytesDecoded");
    c.inc(bytes.length);
    VersionedGenericRecordKafkaAvroDeserializer deserializer = getDeserializer();
    return deserializer.deserialize(topic, bytes);
  }

  @Override
  public void verifyDeterministic() {}
}
