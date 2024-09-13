package com.javatechie.service;

import com.javatechie.dto.Customer;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

@Service
public class KafkaMessagePublisher {

    @Autowired
    private KafkaTemplate<String, GenericRecord> kafkaTemplate;

    @Value("${kafka.topic.name}")
    private String topicName;

    @Value("${schema.registry.url}")
    private String schemaRegistryUrl;

    private SchemaRegistryClient schemaRegistryClient;

    public KafkaMessagePublisher() {
        this.schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 100);
    }

    @Autowired
    public KafkaMessagePublisher(Environment environment) {
        String schemaRegistryUrl = environment.getProperty("spring.kafka.properties.schema.registry.url");
        if (schemaRegistryUrl == null) {
            throw new IllegalArgumentException("Schema Registry URL is not set");
        }
        this.schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 100);
    }

    public void sendEventsToTopic(Customer customer) {
        try {
            // Retrieve schema by ID (assuming you have schema ID)
            // For example, schema ID = 1 (just an example, replace with actual schema ID)
            Schema schema = null;
            try {
                schema = schemaRegistryClient.getById(1);
            } catch (RestClientException e) {
                throw new RuntimeException(e);
            }

            // Create Avro record
            GenericRecord record = new GenericData.Record(schema);
            record.put("id", customer.getId());
            record.put("name", customer.getName());
            record.put("email", customer.getEmail());
            record.put("contactNo", customer.getContactNo());

            // Send to Kafka
            kafkaTemplate.send(topicName, record);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
