package dev.bhupi.beam.examples.common;

import com.google.common.base.Preconditions;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.avro.Schema;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VersionedAvroSchemaCoder extends CustomCoder<VersionedAvroSchema> {
  private static final Logger LOG = LoggerFactory.getLogger(VersionedAvroSchemaCoder.class);
  private static final LazySupplier<SchemaRegistryClient> schemaRegistryClient =
      new LazySupplier<>();

  private final ConfluentVersionCoder confluentVersionCoder = ConfluentVersionCoder.of();
  private final VarIntCoder varIntCoder = VarIntCoder.of();

  private final String schemaRegistryUrl;
  private final Integer identityMapCapacity;

  public VersionedAvroSchemaCoder(String schemaRegistryUrl, @Nullable Integer identityMapCapacity) {
    this.schemaRegistryUrl = schemaRegistryUrl;
    this.identityMapCapacity = identityMapCapacity == null ? 10 : identityMapCapacity;
  }

  private static SchemaRegistryClient getOrInitSchemaRegistryClient(
      String schemaRegistryUrl, int identityMapCapacity) {
    return schemaRegistryClient.get(
        () -> {
          LOG.info(
              "creating new CachedSchemaRegistryClient with url {} and schema capacity {}",
              schemaRegistryUrl,
              identityMapCapacity);
          return new CachedSchemaRegistryClient(schemaRegistryUrl, identityMapCapacity);
        });
  }

  private Integer getSchemaId(String subject, Schema schema) throws IOException {
    SchemaRegistryClient client =
        getOrInitSchemaRegistryClient(schemaRegistryUrl, identityMapCapacity);
    try {
      return client.getId(subject, schema);
    } catch (RestClientException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void encode(VersionedAvroSchema value, @NonNull OutputStream outStream)
      throws IOException {
    Preconditions.checkNotNull(value, "VersionedAvroSchema is null");
    confluentVersionCoder.encode(value.getVersion(), outStream);
    Integer schemaId = getSchemaId(value.getVersion().getSubject(), value.getSchema());
    varIntCoder.encode(schemaId, outStream);
  }

  private Schema getSchemaBySubjectAndId(String subject, Integer schemaId) throws IOException {
    SchemaRegistryClient client =
        getOrInitSchemaRegistryClient(schemaRegistryUrl, identityMapCapacity);
    try {
      return client.getBySubjectAndId(subject, schemaId);
    } catch (RestClientException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public VersionedAvroSchema decode(@NonNull InputStream inStream) throws IOException {
    ConfluentVersion confluentVersion = confluentVersionCoder.decode(inStream);
    Integer schemaId = varIntCoder.decode(inStream);
    Schema schema = getSchemaBySubjectAndId(confluentVersion.getSubject(), schemaId);
    return VersionedAvroSchema.of(confluentVersion, schema);
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {}
}
