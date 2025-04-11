package org.improving.workshop.phase3;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.WindowStore;
import org.msse.demo.mockdata.customer.address.Address;
import org.msse.demo.mockdata.customer.profile.Customer;
import org.msse.demo.mockdata.music.artist.Artist;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import static java.util.stream.Collectors.toMap;
import static java.util.Collections.reverseOrder;

import static org.apache.kafka.streams.state.Stores.persistentKeyValueStore;
import static org.improving.workshop.Streams.*;

@Slf4j
public class TrendingArtistByGeneration {
    // MUST BE PREFIXED WITH "kafka-workshop-"
    public static final String OUTPUT_TOPIC = "kafka-workshop-trending-artist-by-generation";
    
    // Serializer for our result object
    public static final JsonSerde<TrendingArtistResult> TRENDING_ARTIST_RESULT_SERDE = 
            new JsonSerde<>(TrendingArtistResult.class);

    public static final JsonSerde<CustomerAddress> CUSTOMER_ADDRESS_JSON_SERDE =
            new JsonSerde<>(CustomerAddress.class);

    public static final JsonSerde<TrendingArtistCount> TRENDING_ARTIST_COUNT_JSON_SERDE =
            new JsonSerde<>(TrendingArtistCount.class);

    public static final JsonSerde<StreamWithContext> STREAM_WITH_CONTEXT_JSON_SERDE =
        new JsonSerde<>(StreamWithContext.class);

    // create this as public static variables so we can all outside of class => ensure consistency
    public static final String PRE_BOOMER = "Pre-Boomer";
    public static final String BOOMER = "Boomer";
    public static final String GENERATION_X = "Gen X";
    public static final String MILLENNIAL = "Millennial";
    public static final String GENERATION_Z = "Gen Z";
    public static final String GENERATION_ALPHA = "Gen Alpha";

    /**
     * The Streams application as a whole can be launched like any normal Java application that has a `main()` method.
     */
    public static void main(final String[] args) {
        final StreamsBuilder builder = new StreamsBuilder();

        // configure the processing topology
        configureTopology(builder);

        // fire up the engines
        startStreams(builder);
    }

    static void configureTopology(final StreamsBuilder builder) {
        // Create KTables for lookup data with proper materialization
        KTable<String, Customer> customerTable = builder
            .table(
                TOPIC_DATA_DEMO_CUSTOMERS,
                Consumed.with(Serdes.String(), SERDE_CUSTOMER_JSON),
                Materialized
                    .<String, Customer>as(persistentKeyValueStore("customer-store"))
                    .withKeySerde(Serdes.String())
                    .withValueSerde(SERDE_CUSTOMER_JSON)
            );

        KTable<String, Address> addressTable = builder
            .table(
                TOPIC_DATA_DEMO_ADDRESSES,
                Consumed.with(Serdes.String(), SERDE_ADDRESS_JSON),
                Materialized
                    .<String, Address>as(persistentKeyValueStore("address-store"))
                    .withKeySerde(Serdes.String())
                    .withValueSerde(SERDE_ADDRESS_JSON)
            );

        KTable<String, Artist> artistTable = builder
            .table(
                TOPIC_DATA_DEMO_ARTISTS,
                Consumed.with(Serdes.String(), SERDE_ARTIST_JSON),
                Materialized
                    .<String, Artist>as(persistentKeyValueStore("artist-store"))
                    .withKeySerde(Serdes.String())
                    .withValueSerde(SERDE_ARTIST_JSON)
            );

        // Log table entries for debugging
        customerTable.toStream().peek((key, customer) -> log.info("Customer added with ID '{}'", customer.id()));
        addressTable.toStream().peek((key, address) -> log.info("Address added with ID '{}'", address.id()));
        artistTable.toStream().peek((key, artist) -> log.info("Artist added with ID '{}'", artist.id()));

        // First, create a rekeyed version of the address table with customerid as the key
        KTable<String, Address> addressByCustomerTable = addressTable
            .toStream()
            .selectKey((addressId, address) -> address.customerid(), Named.as("rekey-address-by-customerid"))
            .peek((customerId, address) -> log.info("Address rekeyed with customerId: '{}'", customerId))
            .toTable(
                Materialized
                    .<String, Address>as(persistentKeyValueStore("address-by-customer-store"))
                    .withKeySerde(Serdes.String())
                    .withValueSerde(SERDE_ADDRESS_JSON)
            );

        // Now join the customer table with the rekeyed address table
        KTable<String, CustomerAddress> customerAddressTable = customerTable
            .join(
                addressByCustomerTable,
                (customer, address) -> new CustomerAddress(
                    customer.id(),
                    customer.birthdt(),
                    address.state()
                ),
                Materialized
                    .<String, CustomerAddress>as(persistentKeyValueStore("customer-address-store"))
                    .withKeySerde(Serdes.String())
                    .withValueSerde(CUSTOMER_ADDRESS_JSON_SERDE)
            );

        customerAddressTable.toStream().peek((key, value) -> log.info("CustomerAddressTable key: {}, customerId: {}", key, value.getCustomerId()));

        // Set up a tumbling window for 5-minute aggregation periods
        TimeWindows tumblingWindow = TimeWindows.of(java.time.Duration.ofMinutes(5));

        // Process stream data
        builder
            .stream(TOPIC_DATA_DEMO_STREAMS, Consumed.with(Serdes.String(), SERDE_STREAM_JSON))
            .peek((streamId, streamValue) -> log.info("Stream received: {}", streamValue))

            // First rekey the stream by customerid for joining
            .selectKey((key, streamValue) -> streamValue.customerid(), Named.as("rekey-stream-by-customerid"))
            .peek((key, streamValue) -> log.info("Before join - Stream with key: {}, customerId: {}", key, streamValue))

            // Join with customerAddress using proper Serdes
            .join(
                customerAddressTable,
                (stream, customerAddress) -> {
                    // Determine generation based on birthdate
                    String generation = determineGeneration(customerAddress.getBirthdt());
                    return new StreamWithContext(
                        stream.id(),
                        stream.customerid(),
                        stream.artistid(),
                        generation,
                        customerAddress.getState()
                    );
                },
                Joined.with(Serdes.String(), SERDE_STREAM_JSON, CUSTOMER_ADDRESS_JSON_SERDE)
            )
            .peek((customerId, streamWithContext) -> log.info("After join - Customer ID: {}, StreamWithContext: {}", customerId, streamWithContext))

            // Key by generation for grouping
            .selectKey((k, v) -> v.getGeneration(), Named.as("rekey-by-generation"))
            .peek((generation, streamWithContext) -> log.info("Stream keyed by generation: '{}', stream: {}", generation, streamWithContext))

            // Group by generation key
            .groupByKey(Grouped.with(Serdes.String(), STREAM_WITH_CONTEXT_JSON_SERDE))

            // Apply tumbling window of 5 minutes
            .windowedBy(tumblingWindow)

            // Aggregate streams by artist within each generation
            .aggregate(
                TrendingArtistCount::new,

                // Aggregator - increment artist count
                (generationKey, streamContext, artistCounts) -> {
                    if (!artistCounts.isInitialized()) {
                        artistCounts.initialize(streamContext.getGeneration());
                    }
                    log.info("Incrementing Stream count for artist {} in generation {}", streamContext.getArtistId(), generationKey);
                    artistCounts.incrementStreamCount(streamContext.getArtistId(), streamContext.getState());
                    return artistCounts;
                },

                Materialized
                    .<String, TrendingArtistCount, WindowStore<Bytes, byte[]>>as("artist-stream-counts-by-generation")
                    .withKeySerde(Serdes.String())
                    .withValueSerde(TRENDING_ARTIST_COUNT_JSON_SERDE)
            )

            // Convert to stream with window info as part of the key
            .toStream()
            .peek((windowedKey, artistCounts) -> log.info("Window: {}, Generation: {}, Top Artist: {}", windowedKey.window(), windowedKey.key(), artistCounts.getTopStreamedArtistId()))

            // Extract the artist ID to join with artist table
            .selectKey((windowedKey, artistCount) -> artistCount.getTopStreamedArtistId(), Named.as("rekey-by-top-artist"))

            // Join with artist table to get artist details
            .join(
                artistTable,
                (artistCounts, artist) -> new TrendingArtistResult(
                    artistCounts.getGeneration(),
                    artistCounts.getTopStreamedArtistId(),
                    artist.name(),
                    artistCounts.getTopArtistCount(),
                    artistCounts.getTopStreamedState()
                ),
                Joined.with(Serdes.String(), TRENDING_ARTIST_COUNT_JSON_SERDE, SERDE_ARTIST_JSON)
            )

            // rekey by the generation
            .selectKey((artistId, result) -> result.getGeneration())

            // Log the results
            .peek((artistId, result) -> log.info("Top trending artist for generation {}: {} ({}) with {} streams, most streams from: {}", result.getGeneration(), result.getArtistName(), artistId, result.getStreamCount(), result.getState()))

            // Output to topic
            .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), TRENDING_ARTIST_RESULT_SERDE));
    }

    /**
     * Determine generation based on birthdate
     */
    private static String determineGeneration(String birthDate) {
        if (birthDate == null || birthDate.isEmpty()) {
            return "Unknown";
        }

        try {
            LocalDate dob = LocalDate.parse(birthDate, DateTimeFormatter.ISO_DATE);
            int year = dob.getYear();

            if (year < 1946) return PRE_BOOMER;
            if (year < 1965) return BOOMER;
            if (year < 1981) return GENERATION_X;
            if (year < 1997) return MILLENNIAL;
            if (year < 2013) return GENERATION_Z;
            return GENERATION_ALPHA;

        } catch (Exception e) {
            log.error("Error parsing date: {}", birthDate, e);
            return "Unknown";
        }
    }

    // Data classes for holding intermediate state

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CustomerAddress {
        private String customerId;
        private String birthdt;
        private String state;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class StreamWithContext {
        private String streamId;
        private String customerId;
        private String artistId;
        private String generation;
        private String state;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TrendingArtistResult {
        private String generation;
        private String artistId;
        private String artistName;
        private Integer streamCount;
        private String state;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TrendingArtistCount {
        private boolean initialized;
        private String generation;
        // ArtistId to Stream count map
        private Map<String, Integer> streamCount;
        private Map<String, Integer> stateStreamCount;
        @Getter
        private String topStreamedArtistId;
        private Integer topArtistCount;
        private String topStreamedState;
        private Integer topStreamedStateCount;

        public void initialize(String generation) {
            this.initialized = true;
            this.generation = generation;
            this.streamCount = new HashMap<>();
            this.stateStreamCount = new HashMap<>();
            this.topStreamedArtistId = "";
            this.topArtistCount = 0;
            this.topStreamedState = "";
            this.topStreamedStateCount = 0;
        }

        public void incrementStreamCount(String artistId, String state) {
            // Update artist stream count
            this.streamCount.put(artistId, streamCount.getOrDefault(artistId, 0) + 1);

            // Update state stream count
            this.stateStreamCount.put(state, stateStreamCount.getOrDefault(state, 0) + 1);

            // Update top artist if current artist has more streams
            if (this.streamCount.get(artistId) > topArtistCount) {
                log.info("New TopArtist {} for generation {} with {} streams",
                    artistId, generation, this.streamCount.get(artistId));
                topArtistCount = this.streamCount.get(artistId);
                topStreamedArtistId = artistId;
            }

            // Update top state if current state has more streams
            if (this.stateStreamCount.get(state) > (topStreamedStateCount != null ? topStreamedStateCount : 0)) {
                log.info("New Top Streamed State {} for generation {} with {} streams",
                    state, generation, this.stateStreamCount.get(state));
                topStreamedStateCount = this.stateStreamCount.get(state);
                topStreamedState = state;
            }
        }
    }
}