/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.mqtt;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;

import javax.annotation.Nullable;

/**
 * An unbounded source and a sink for <a href="http://kafka.apache.org/">Kafka</a> topics.
 * Kafka version 0.9 and above are supported.
 *
 * <h3>Reading from Kafka topics</h3>
 *
 * <p>MQTTIO source returns unbounded collection of Kafka records as
 * {@code PCollection<MQTTRecord<K, V>>}. A {@link } includes basic
 * metadata like topic-partition and offset, along with key and value associated with a Kafka
 * record.
 *
 * <p>Although most applications consumer single topic, the source can be configured to consume
 * multiple topics or even a specific set of {@link }s.
 *
 * <p>To configure a Kafka source, you must specify at the minimum Kafka <tt>bootstrapServers</tt>
 * and one or more topics to consume. The following example illustrates various options for
 * configuring the source :
 *
 * <pre>{@code
 *
 *  pipeline
 *    .apply(MQTTIO.read()
 *       .withBootstrapServers("broker_1:9092,broker_2:9092")
 *       .withTopics(ImmutableList.of("topic_a", "topic_b"))
 *       // above two are required configuration. returns PCollection<MQTTRecord<byte[], byte[]>
 *
 *       // rest of the settings are optional :
 *
 *       // set a Coder for Key and Value (note the change to return type)
 *       .withKeyCoder(BigEndianLongCoder.of()) // PCollection<MQTTRecord<Long, byte[]>
 *       .withValueCoder(StringUtf8Coder.of())  // PCollection<MQTTRecord<Long, String>
 *
 *       // you can further customize KafkaConsumer used to read the records by adding more
 *       // settings for ConsumerConfig. e.g :
 *       .updateConsumerProperties(ImmutableMap.of("receive.buffer.bytes", 1024 * 1024))
 *
 *       // custom function for calculating record timestamp (default is processing time)
 *       .withTimestampFn(new MyTypestampFunction())
 *
 *       // custom function for watermark (default is record timestamp)
 *       .withWatermarkFn(new MyWatermarkFunction())
 *
 *       // finally, if you don't need Kafka metadata, you can drop it
 *       .withoutMetadata() // PCollection<KV<Long, String>>
 *    )
 *    .apply(Values.<String>create()) // PCollection<String>
 *     ...
 * }</pre>
 *
 * <h3>Partition Assignment and Checkpointing</h3>
 * The Kafka partitions are evenly distributed among splits (workers).
 * Dataflow checkpointing is fully supported and
 * each split can resume from previous checkpoint. See
 * {@link UnboundedKafkaSource#generateInitialSplits(int, PipelineOptions)} for more details on
 * splits and checkpoint support.
 *
 * <p>When the pipeline starts for the first time without any checkpoint, the source starts
 * consuming from the <em>latest</em> offsets. You can override this behavior to consume from the
 * beginning by setting appropriate appropriate properties in {@link }, through
 * {@link Read#updateConsumerProperties(Map)}.
 *
 * <h3>Writing to Kafka</h3>
 *
 * <p>MQTTIO sink supports writing key-value pairs to a Kafka topic. Users can also write
 * just the values. To configure a Kafka sink, you must specify at the minimum Kafka
 * <tt>bootstrapServers</tt> and the topic to write to. The following example illustrates various
 * options for configuring the sink:
 *
 * <pre>{@code
 *
 *  PCollection<KV<Long, String>> kvColl = ...;
 *  kvColl.apply(MQTTIO.write()
 *       .withBootstrapServers("broker_1:9092,broker_2:9092")
 *       .withTopic("results")
 *
 *       // set Coder for Key and Value
 *       .withKeyCoder(BigEndianLongCoder.of())
 *       .withValueCoder(StringUtf8Coder.of())

 *       // you can further customize KafkaProducer used to write the records by adding more
 *       // settings for ProducerConfig. e.g, to enable compression :
 *       .updateProducerProperties(ImmutableMap.of("compression.type", "gzip"))
 *    );
 * }</pre>
 *
 * <p>Often you might want to write just values without any keys to Kafka. Use {@code values()} to
 * write records with default empty(null) key:
 *
 * <pre>{@code
 *  PCollection<String> strings = ...;
 *  strings.apply(MQTTIO.write()
 *      .withBootstrapServers("broker_1:9092,broker_2:9092")
 *      .withTopic("results")
 *      .withValueCoder(StringUtf8Coder.of()) // just need coder for value
 *      .values() // writes values to Kafka with default key
 *    );
 * }</pre>
 *
 * <h3>Advanced Kafka Configuration</h3>
 * KafakIO allows setting most of the properties in {@link } for source or in
 * {@link } for sink. E.g. if you would like to enable offset
 * <em>auto commit</em> (for external monitoring or other purposes), you can set
 * <tt>"group.id"</tt>, <tt>"enable.auto.commit"</tt>, etc.
 */
public class MQTTIO {

    public static Read<Byte[], Byte[]> read() {
        return new Read<>();
    }

    public static Write<byte[], byte[]> write() {
        return new Write<>();
    }

    private static class UnboundedMQTTReader<K, V> extends UnboundedSource.UnboundedReader<MQTTRecord<K, V>> {

        @Override
        public MQTTRecord<K, V> getCurrent() throws NoSuchElementException {
            return null;
        }

        @Override
        public Instant getCurrentTimestamp() throws NoSuchElementException {
            return null;
        }

        @Override
        public void close() throws IOException {

        }

        @Override
        public boolean start() throws IOException {
            return false;
        }

        @Override
        public boolean advance() throws IOException {
            return false;
        }

        @Override
        public Instant getWatermark() {
            return null;
        }

        @Override
        public UnboundedSource.CheckpointMark getCheckpointMark() {
            return null;
        }

        @Override
        public UnboundedSource<MQTTRecord<K, V>, ?> getCurrentSource() {
            return null;
        }
    }

    private static class TypedRead<K, V>
            extends PTransform<PBegin, PCollection<MQTTRecord<K, V>>> {

        /**
         * A function to assign a timestamp to a record. Default is processing timestamp.
         */
        public TypedRead<K, V> withTimestampFn2(
                SerializableFunction<MQTTRecord<K, V>, Instant> timestampFn) {
            checkNotNull(timestampFn);
            return new TypedRead<K, V>(topics, keyCoder, valueCoder,
                    timestampFn, watermarkFn, consumerFactoryFn, consumerConfig,
                    maxNumRecords, maxReadTime);
        }

        /**
         * A function to calculate watermark after a record. Default is last record timestamp
         * @see #withTimestampFn(SerializableFunction)
         */
        public TypedRead<K, V> withWatermarkFn2(
                SerializableFunction<MQTTRecord<K, V>, Instant> watermarkFn) {
            checkNotNull(watermarkFn);
            return new TypedRead<K, V>(topics, keyCoder, valueCoder,
                    timestampFn, watermarkFn, consumerFactoryFn, consumerConfig,
                    maxNumRecords, maxReadTime);
        }

        /**
         * A function to assign a timestamp to a record. Default is processing timestamp.
         */
        public TypedRead<K, V> withTimestampFn(SerializableFunction<KV<K, V>, Instant> timestampFn) {
            checkNotNull(timestampFn);
            return withTimestampFn2(unwrapKafkaAndThen(timestampFn));
        }

        /**
         * A function to calculate watermark after a record. Default is last record timestamp
         * @see #withTimestampFn(SerializableFunction)
         */
        public TypedRead<K, V> withWatermarkFn(SerializableFunction<KV<K, V>, Instant> watermarkFn) {
            checkNotNull(watermarkFn);
            return withWatermarkFn2(unwrapKafkaAndThen(watermarkFn));
        }


        @Override
        public PCollection<MQTTRecord<K, V>> apply(PBegin input) {
            // Handles unbounded source to bounded conversion if maxNumRecords or maxReadTime is set.
            org.apache.beam.sdk.io.Read.Unbounded<MQTTRecord<K, V>> unbounded =
                    org.apache.beam.sdk.io.Read.from(makeSource());

            PTransform<PBegin, PCollection<MQTTRecord<K, V>>> transform = unbounded;

            if (maxNumRecords < Long.MAX_VALUE) {
                transform = unbounded.withMaxNumRecords(maxNumRecords);
            } else if (maxReadTime != null) {
                transform = unbounded.withMaxReadTime(maxReadTime);
            }

            return input.getPipeline().apply(transform);
        }

        ////////////////////////////////////////////////////////////////////////////////////////

        protected final List<String> topics;
        protected final Coder<K> keyCoder;
        protected final Coder<V> valueCoder;
        @Nullable
        protected final SerializableFunction<MQTTRecord<K, V>, Instant> timestampFn;
        @Nullable protected final SerializableFunction<MQTTRecord<K, V>, Instant> watermarkFn;
        protected final
        SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>> consumerFactoryFn;
        protected final Map<String, Object> consumerConfig;
        protected final long maxNumRecords; // bounded read, mainly for testing
        protected final Duration maxReadTime; // bounded read, mainly for testing

        private TypedRead(List<String> topics,
                          Coder<K> keyCoder,
                          Coder<V> valueCoder,
                          @Nullable SerializableFunction<MQTTRecord<K, V>, Instant> timestampFn,
                          @Nullable SerializableFunction<MQTTRecord<K, V>, Instant> watermarkFn,
                          SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>> consumerFactoryFn,
                          Map<String, Object> consumerConfig,
                          long maxNumRecords,
                          @Nullable Duration maxReadTime) {
            super("KafkaIO.Read");

            this.topics = topics;
            this.keyCoder = keyCoder;
            this.valueCoder = valueCoder;
            this.timestampFn = timestampFn;
            this.watermarkFn = watermarkFn;
            this.consumerFactoryFn = consumerFactoryFn;
            this.consumerConfig = consumerConfig;
            this.maxNumRecords = maxNumRecords;
            this.maxReadTime = maxReadTime;
        }

        /**
         * Creates an {@link UnboundedSource UnboundedSource&lt;MQTTRecord&lt;K, V&gt;, ?&gt;} with the
         * configuration in {@link TypedRead}. Primary use case is unit tests, should not be used in an
         * application.
         */
        @VisibleForTesting
        UnboundedSource<MQTTRecord<K, V>, MQTTCheckpointMark> makeSource() {
            return new UnboundedKafkaSource<K, V>(
                    -1,
                    topics,
                    keyCoder,
                    valueCoder,
                    timestampFn,
                    Optional.fromNullable(watermarkFn),
                    consumerFactoryFn,
                    consumerConfig);
        }

        // utility method to convert KafkRecord<K, V> to user KV<K, V> before applying user functions
        private static <KeyT, ValueT, OutT> SerializableFunction<MQTTRecord<KeyT, ValueT>, OutT>
        unwrapKafkaAndThen(final SerializableFunction<KV<KeyT, ValueT>, OutT> fn) {
            return new SerializableFunction<MQTTRecord<KeyT, ValueT>, OutT>() {
                public OutT apply(MQTTRecord<KeyT, ValueT> record) {
                    return fn.apply(record.getKV());
                }
            };
        }
    }


    private static class UnboundedKafkaSource<K, V>
            extends UnboundedSource<MQTTRecord<K, V>, MQTTCheckpointMark> {

        @Override
        public void validate() {

        }

        @Override
        public Coder<MQTTRecord<K, V>> getDefaultOutputCoder() {
            return null;
        }

        @Override
        public List<? extends UnboundedSource<MQTTRecord<K, V>, MQTTCheckpointMark>> generateInitialSplits(int desiredNumSplits, PipelineOptions options) throws Exception {
            return null;
        }

        @Override
        public UnboundedReader<MQTTRecord<K, V>> createReader(PipelineOptions options, @Nullable MQTTCheckpointMark checkpointMark) throws IOException {
            return null;
        }

        @Nullable
        @Override
        public Coder<MQTTCheckpointMark> getCheckpointMarkCoder() {
            return null;
        }
    }
    public static class Read<K, V> extends TypedRead<K, V>{
        private Read() {
            //
        }

    }

    private static class Write<K, V> {
        private Write() {
            //
        }
    }
}
