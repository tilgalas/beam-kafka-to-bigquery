# beam-kafka-to-bigquery
Example beam pipeline(s) to read from Kafka and write to BigQuery

### Read multiple Kafka topics using .withTopics KafkaIO method with BigQuery dynamic destinations

Consolidated read of multiple Kafka topics using the `.withTopics` method of `KafkaIO` source and 
`BigQueryIO` dynamic destination to write to different tables based on the input topic

Example Dataflow Pipeline

![Conslidated Pipeline](diagrams/ReadMultipleKafkaTopics_using_withTopics.png)

Example of how the records are being read in the same branch interleaved b/w topics; there are just two topics in this example, however it  can be expanded to multiple 
topics as 
required 

![Interleaved Records Reading](diagrams/InterleavedRecordsReading.png)


# Run the samples

### Run on Dataflow Runner

```
mvn compile exec:java \
    -Dexec.mainClass=dev.bhupi.beam.examples.KafkaAvroExample \
    -Pdataflow-runner \
    -Dexec.args=" \
    --runner=DataflowRunner \
    --streaming \
    --project=${PROJECT_ID} \
    --region=${REGION} \
    --gcpTempLocation=gs://${STORAGE_BUCKET}/temp \
    --jobName=KafkaAvroExample \
    --kafkaHost=${KAFKA_HOST} \
    --kafkaSchemaRegistryUrl=${KAFKA_SCHEMA_REGISTRY} \
    --topicNames=<comma_serparated_list_of_topic_names> \
    --bigQueryProjectName=${BQ_PROJECT_ID} \
    --bigQueryDatasetName=${BQ_DATASET_NAME}"
```

### Run on Direct Runner

```
mvn compile exec:java -Dexec.mainClass=dev.bhupi.beam.examples.KafkaAvroExampleWithEnum \
-Dexec.args=" \
    --kafkaHost=${KAFKA_HOST} \
    --kafkaSchemaRegistryUrl=${KAFKA_SCHEMA_REGISTRY} \
    --topicNames=<comma_serparated_list_of_topic_names> \
    --bigQueryProjectName=${BQ_PROJECT_ID} \
    --bigQueryDatasetName=${BQ_DATASET_NAME}" -Pdirect-runner
```