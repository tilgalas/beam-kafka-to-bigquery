package dev.bhupi.beam.examples.common;

import com.google.api.services.bigquery.model.TableSchema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.io.gcp.bigquery.*;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
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
        PCollection<GenericRecord> genericRecords = input.apply("extract GenericRecord",
                MapElements.into(new TypeDescriptor<GenericRecord>() {})
                        .via(kr -> Objects.requireNonNull(kr).getKV().getValue()));


        return genericRecords.apply(BigQueryIO.writeGenericRecords().
                to(new DynamicDestinations<GenericRecord, GenericRecord>() {
                    @Override
                    public GenericRecord getDestination(@Nullable @UnknownKeyFor @Initialized ValueInSingleWindow<GenericRecord> element) {
                        return element.getValue();
                    }

                    @Override
                    public @UnknownKeyFor @NonNull @Initialized TableDestination getTable(GenericRecord destination) {
                        return new TableDestination(
                                String.format("%s:%s.%s", bqProject, bqDataset, "maciektable"),
                                "maciek table"
                        );
                    }

                    @Override
                    public @Nullable @UnknownKeyFor @Initialized TableSchema getSchema(GenericRecord destination) {
                        return BigQueryUtils.toTableSchema(AvroUtils.toBeamSchema(destination.getSchema()));
                    }
                })
                .withMethod(BigQueryIO.Write.Method.STORAGE_WRITE_API)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withTriggeringFrequency(Duration.standardSeconds(1L))
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

    }
}
