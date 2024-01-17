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

import com.google.api.services.bigquery.model.TableRow;
import java.util.Objects;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigQueryDynamicWriteTransform extends
    PTransform<PCollection<KV<String, TableRow>>, WriteResult> {

  public static final Logger LOG = LoggerFactory.getLogger(BigQueryDynamicWriteTransform.class);

  private final String bigQueryProjectName;
  private final String bigQueryDatasetName;

  public BigQueryDynamicWriteTransform(String bigQueryProjectName,
      String bigQueryDatasetName) {
    this.bigQueryProjectName = bigQueryProjectName;
    this.bigQueryDatasetName = bigQueryDatasetName;
  }

  @Override
  public WriteResult expand(PCollection<KV<String, TableRow>> input) {

    return input
        .apply(
            "Write to BQ Tables", BigQueryIO.<KV<String, TableRow>>write()
                .to(new SerializableFunction<ValueInSingleWindow<KV<String, TableRow>>, TableDestination>() {
                  @Override
                  public TableDestination apply(
                      ValueInSingleWindow<KV<String, TableRow>> value) {
                    String tableSpec = String.format("%s:%s.%s",
                        bigQueryProjectName,
                        bigQueryDatasetName,
                        Objects.requireNonNull(value.getValue()).getKey());
                    String tableDescription = value.getValue().getKey() + "table";
                    return new TableDestination(tableSpec, tableDescription);
                  }
                })
                .withFormatFunction(KV::getValue)
                .ignoreUnknownValues()
                .useAvroLogicalTypes()
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .optimizedWrites());

  }

}
