/*
 * Copyright 2024 Bhupinder Sindhwani
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.bhupi.beam.examples.common;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.gcp.bigquery.AvroWriteRequest;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Util {

  public static final Logger LOG = LoggerFactory.getLogger(Util.class);

  public static class AvroGenericRecordDeserializer extends
      AbstractKafkaAvroDeserializer implements Deserializer<GenericRecordWithTopic> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
      configure(new KafkaAvroDeserializerConfig(configs));
    }

    @Override
    public GenericRecordWithTopic deserialize(String topic, byte[] bytes) {
      return GenericRecordWithTopic.of(topic, (GenericRecord) this.deserialize(bytes));
    }

    @Override
    public void close() {
    }
  }

  public static TableFieldSchema processUnionType(Schema.Field field, Schema.Type unionType,
      List<Schema> unionSchemas) {
    if (unionSchemas.size() == 2 && unionSchemas.contains(Schema.create(Schema.Type.NULL))) {
      // Handle nullable union types (e.g., ["null", "string"])
      Schema nonNullType = unionSchemas.get(
          unionSchemas.indexOf(Schema.create(Schema.Type.NULL)) == 0 ? 1 : 0);
      return new TableFieldSchema()
          .setName(field.name())
          .setType(mapAvroTypeToBigQueryType(nonNullType.getType()))
          .setMode("NULLABLE");
    } else {
      // TODO: Handle complex union types
      return new TableFieldSchema()
          .setName(field.name())
          .setType("STRING")
          .setMode("NULLABLE");
    }
  }

  public static String mapAvroTypeToBigQueryType(Schema.Type avroType) {
    switch (avroType) {
      case STRING:
        return "STRING";
      case LONG:
        return "INT64";
      case FLOAT:
        return "FLOAT";
      case DOUBLE:
        return "FLOAT64";
      case BOOLEAN:
        return "BOOL";
      // TODO: Handle other AVRO types as required for the use-case
      default:
        throw new IllegalArgumentException("Unsupported Avro type: " + avroType);
    }
  }

  public static String mapAvroModeToBigQueryMode(Schema schema) {
    if (schema.getType() == Schema.Type.NULL) {
      return "NULLABLE";
    } else if (isRepeatedType(schema)) {
      return "REPEATED";
    } else {
      return "REQUIRED";
    }
  }

  public static boolean isRepeatedType(Schema schema) {
    return schema.getType() == Schema.Type.ARRAY || schema.getType() == Schema.Type.MAP;
  }

  public static class GenericRecordCoder extends AtomicCoder<GenericRecord> {

    // Cache that holds AvroCoders for the encountered Avro records
    private static final ConcurrentHashMap<String, AvroCoder<GenericRecord>> codersCache =
        new ConcurrentHashMap<>();

    public static GenericRecordCoder of() {
      return new GenericRecordCoder();
    }

    @Override
    public void encode(GenericRecord record, OutputStream outputStream) throws IOException {
      // Add base64-encoded schema to the output stream
      String schema = record.getSchema().toString();
      String schema64 = Base64.getEncoder().encodeToString(schema.getBytes());
      StringUtf8Coder.of().encode(schema64, outputStream);

      // Get AvroCoder associated with the schema
      AvroCoder<GenericRecord> coder =
          codersCache.computeIfAbsent(schema64, key -> AvroCoder.of(record.getSchema()));

      // Add encoded record to the output stream
      coder.encode(record, outputStream);
    }

    @Override
    public GenericRecord decode(InputStream inputStream) throws IOException {
      // Fetch the base64-encoded schema
      String schema64 = StringUtf8Coder.of().decode(inputStream);

      // Get AvroCoder associated with the schema
      AvroCoder<GenericRecord> coder =
          codersCache.computeIfAbsent(
              schema64,
              key ->
                  AvroCoder.of(
                      new Schema.Parser()
                          .parse(Arrays.toString(Base64.getDecoder().decode(schema64)))));

      // Decode the Avro record
      return coder.decode(inputStream);
    }
  }
}
