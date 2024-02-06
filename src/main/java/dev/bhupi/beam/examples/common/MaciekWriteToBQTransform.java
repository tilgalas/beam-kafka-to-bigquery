package dev.bhupi.beam.examples.common;

import com.google.api.services.bigquery.model.TableSchema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.io.gcp.bigquery.*;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.io.kafka.KafkaRecordCoder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.joda.time.Duration;

public class MaciekWriteToBQTransform extends PTransform<PCollection<KafkaRecord<String, GenericRecordWithTopic>>, WriteResult> {
    private final String bqProject;
    private final String bqDataset;
    private final String schemaRegistryUrl;

    public MaciekWriteToBQTransform(String bqProject, String bqDataset, String schemaRegistryUrl) {
        this.bqProject = bqProject;
        this.bqDataset = bqDataset;
        this.schemaRegistryUrl = schemaRegistryUrl;
    }

    @Override
    public WriteResult expand(PCollection<KafkaRecord<String, GenericRecordWithTopic>> input) {
        return input
                .apply(BigQueryIO.<KafkaRecord<String, GenericRecordWithTopic>>write()
                .to(new DynamicDestinations<KafkaRecord<String, GenericRecordWithTopic>, GenericRecordWithTopic>() {
                    @Override
                    public GenericRecordWithTopic getDestination(@Nullable @UnknownKeyFor @Initialized ValueInSingleWindow<KafkaRecord<String, GenericRecordWithTopic>> element) {
                        return element.getValue().getKV().getValue();
                    }

                    @Override
                    public @UnknownKeyFor @NonNull @Initialized TableDestination getTable(GenericRecordWithTopic destination) {
                        // maybe some massaging is needed to make sure that the topic name forms a valid table name in BQ
                        String topic = destination.getTopic();
                        return new TableDestination(
                                String.format("%s:%s.%s", bqProject, bqDataset, topic),
                                String.format("table for topic %s", topic)
                        );
                    }

                    @Override
                    public @Nullable @UnknownKeyFor @Initialized TableSchema getSchema(GenericRecordWithTopic destination) {
                        return BigQueryUtils.toTableSchema(AvroUtils.toBeamSchema(destination.getRecord().getSchema()));
                    }

                    @Override
                    public @Nullable @UnknownKeyFor @Initialized Coder<GenericRecordWithTopic> getDestinationCoder() {
                        return new GenericRecordSchemaRegistryCoder(schemaRegistryUrl, false);
                    }
                })
                .withAvroFormatFunction(avroWriteRequest -> avroWriteRequest.getElement().getKV().getValue().getRecord())
                .withMethod(BigQueryIO.Write.Method.STORAGE_WRITE_API)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withTriggeringFrequency(Duration.standardSeconds(1L))
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

    }
}
