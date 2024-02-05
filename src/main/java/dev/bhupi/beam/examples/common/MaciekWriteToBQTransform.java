package dev.bhupi.beam.examples.common;

import com.google.api.services.bigquery.model.TableSchema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.io.gcp.bigquery.*;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.io.kafka.KafkaRecordCoder;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.joda.time.Duration;

import java.util.Objects;

public class MaciekWriteToBQTransform extends PTransform<PCollection<KafkaRecord<String, GenericRecord>>, WriteResult> {
    private final String bqProject;
    private final String bqDataset;

    public MaciekWriteToBQTransform(String bqProject, String bqDataset) {
        this.bqProject = bqProject;
        this.bqDataset = bqDataset;
    }

    @Override
    public WriteResult expand(PCollection<KafkaRecord<String, GenericRecord>> input) {

        return input.apply(BigQueryIO.<KafkaRecord<String, GenericRecord>>write()
                .to(new DynamicDestinations<KafkaRecord<String, GenericRecord>, KafkaRecord<String, GenericRecord>>() {
                    @Override
                    public KafkaRecord<String, GenericRecord> getDestination(@Nullable @UnknownKeyFor @Initialized ValueInSingleWindow<KafkaRecord<String, GenericRecord>> element) {
                        return element.getValue();
                    }

                    @Override
                    public @UnknownKeyFor @NonNull @Initialized TableDestination getTable(KafkaRecord<String, GenericRecord> destination) {
                        // maybe some massaging is needed to make sure that the topic name forms a valid table name in BQ
                        String topic = destination.getTopic();
                        return new TableDestination(
                                String.format("%s:%s.%s", bqProject, bqDataset, topic),
                                String.format("table for topic %s", topic)
                        );
                    }

                    @Override
                    public @Nullable @UnknownKeyFor @Initialized TableSchema getSchema(KafkaRecord<String, GenericRecord> destination) {
                        return BigQueryUtils.toTableSchema(AvroUtils.toBeamSchema(destination.getKV().getValue().getSchema()));
                    }

                    @Override
                    public @Nullable @UnknownKeyFor @Initialized Coder<KafkaRecord<String, GenericRecord>> getDestinationCoder() {
                        return KafkaRecordCoder.of(StringUtf8Coder.of(), Util.GenericRecordCoder.of());
                    }
                })
                .withAvroFormatFunction(avroWriteRequest -> avroWriteRequest.getElement().getKV().getValue())
                .withMethod(BigQueryIO.Write.Method.STORAGE_WRITE_API)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withTriggeringFrequency(Duration.standardSeconds(1L))
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

    }
}
