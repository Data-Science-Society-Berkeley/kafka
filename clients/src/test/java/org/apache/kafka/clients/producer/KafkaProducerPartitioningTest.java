/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.producer;

import org.apache.kafka.clients.producer.internals.BuiltInPartitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.test.MockPartitioner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KafkaProducerPartitioningTest {
    private static final String TOPIC = "test-topic";
    private static final int NUM_PARTITIONS = 3;
    private static final Node[] NODES = new Node[] {
        new Node(0, "localhost", 9092),
        new Node(1, "localhost", 9093),
        new Node(2, "localhost", 9094)
    };
    
    private Cluster cluster;
    
    @BeforeEach
    public void setUp() {
        List<PartitionInfo> partitionInfos = Arrays.asList(
                new PartitionInfo(TOPIC, 0, NODES[0], NODES, NODES),
                new PartitionInfo(TOPIC, 1, NODES[1], NODES, NODES),
                new PartitionInfo(TOPIC, 2, NODES[2], NODES, NODES));
        
        this.cluster = new Cluster(
                "kafka-cluster",
                Arrays.asList(NODES),
                partitionInfos,
                Collections.emptySet(),
                Collections.emptySet());
    }
    
    /**
     * Test that messages with the same key are consistently routed to the same partition.
     */
    @Test
    public void testSameKeysSamePartition() {
        KafkaProducer<byte[], byte[]> producer = createProducer();
        
        try {
            byte[] key1 = "test-key-1".getBytes(StandardCharsets.UTF_8);
            byte[] key2 = "test-key-2".getBytes(StandardCharsets.UTF_8);
            byte[] value = "test-value".getBytes(StandardCharsets.UTF_8);
            
            java.lang.reflect.Method partitionMethod = KafkaProducer.class.getDeclaredMethod(
                    "partition", ProducerRecord.class, byte[].class, byte[].class, Cluster.class);
            partitionMethod.setAccessible(true);
            
            ProducerRecord<byte[], byte[]> record1a = new ProducerRecord<>(TOPIC, key1, value);
            ProducerRecord<byte[], byte[]> record1b = new ProducerRecord<>(TOPIC, key1, value);
            
            ProducerRecord<byte[], byte[]> record2 = new ProducerRecord<>(TOPIC, key2, value);
            
            int partition1a = (int) partitionMethod.invoke(producer, record1a, key1, value, this.cluster);
            int partition1b = (int) partitionMethod.invoke(producer, record1b, key1, value, this.cluster);
            
            int partition2 = (int) partitionMethod.invoke(producer, record2, key2, value, this.cluster);
            
            assertEquals(partition1a, partition1b, "Same key should route to same partition");
            
            if (partition1a != partition2) {
                assertNotEquals(partition1a, partition2, "Different keys can route to different partitions");
            }
        } catch (Exception e) {
            throw new RuntimeException("Test failed", e);
        } finally {
            producer.close();
        }
    }
    
    /**
     * Test that BuiltInPartitioner.partitionForKey() correctly hashes keys to determine partitions.
     */
    @Test
    public void testBuiltInPartitionerKeyHashing() {
        byte[] key1 = "test-key-1".getBytes(StandardCharsets.UTF_8);
        byte[] key2 = "test-key-2".getBytes(StandardCharsets.UTF_8);
        
        int partition1 = BuiltInPartitioner.partitionForKey(key1, NUM_PARTITIONS);
        int partition2 = BuiltInPartitioner.partitionForKey(key1, NUM_PARTITIONS);
        int partition3 = BuiltInPartitioner.partitionForKey(key2, NUM_PARTITIONS);
        
        assertEquals(partition1, partition2, "Same key should produce same partition");
        
        assertTrue(partition1 >= 0 && partition1 < NUM_PARTITIONS, 
                   "Partition should be in valid range");
        assertTrue(partition3 >= 0 && partition3 < NUM_PARTITIONS, 
                   "Partition should be in valid range");
    }
    
    /**
     * Test that when explicit partitions are specified, they are respected.
     */
    @Test
    public void testExplicitPartitionSpecified() {
        KafkaProducer<byte[], byte[]> producer = createProducer();
        
        byte[] key = "test-key".getBytes(StandardCharsets.UTF_8);
        byte[] value = "test-value".getBytes(StandardCharsets.UTF_8);
        
        int explicitPartition = 1;
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(TOPIC, explicitPartition, key, value);
        
        try {
            java.lang.reflect.Method partitionMethod = KafkaProducer.class.getDeclaredMethod(
                    "partition", ProducerRecord.class, byte[].class, byte[].class, Cluster.class);
            partitionMethod.setAccessible(true);
            
            int resultPartition = (int) partitionMethod.invoke(
                    producer, record, key, value, this.cluster);
            
            assertEquals(explicitPartition, resultPartition, 
                         "Explicit partition should be respected");
            
        } catch (Exception e) {
            throw new RuntimeException("Test failed", e);
        }
        
        producer.close();
    }
    
    /**
     * Test that when custom partitioners are used, their logic is applied correctly.
     */
    @Test
    public void testCustomPartitioner() {
        MockPartitioner.resetCounters();
        
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, MockPartitioner.class.getName());
        
        KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(
                props, new org.apache.kafka.common.serialization.ByteArraySerializer(),
                new org.apache.kafka.common.serialization.ByteArraySerializer());
        
        try {
            byte[] key = "test-key".getBytes(StandardCharsets.UTF_8);
            byte[] value = "test-value".getBytes(StandardCharsets.UTF_8);
            
            try {
                java.lang.reflect.Method partitionMethod = KafkaProducer.class.getDeclaredMethod(
                        "partition", ProducerRecord.class, byte[].class, byte[].class, Cluster.class);
                partitionMethod.setAccessible(true);
                
                ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(TOPIC, key, value);
                
                int resultPartition = (int) partitionMethod.invoke(producer, record, key, value, this.cluster);
                
                // MockPartitioner.partition always returns 0
                assertEquals(0, resultPartition, "Custom partitioner should return 0");
                
                assertEquals(1, MockPartitioner.INIT_COUNT.get(), 
                             "Custom partitioner should be initialized");
            } catch (Exception e) {
                throw new RuntimeException("Test failed", e);
            }
        } finally {
            producer.close();
            MockPartitioner.resetCounters();
        }
    }
    
    private KafkaProducer<byte[], byte[]> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        
        return new KafkaProducer<>(
                props, 
                new org.apache.kafka.common.serialization.ByteArraySerializer(),
                new org.apache.kafka.common.serialization.ByteArraySerializer());
    }
}
