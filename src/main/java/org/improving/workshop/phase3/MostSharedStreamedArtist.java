package org.improving.workshop.phase3;

import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;
import org.msse.demo.mockdata.customer.profile.Customer;
import org.msse.demo.mockdata.music.artist.Artist;
import org.msse.demo.mockdata.music.stream.Stream;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.time.Duration;
import java.util.*;

import static org.apache.kafka.streams.state.Stores.persistentKeyValueStore;
import static org.improving.workshop.Streams.*;

@Slf4j
public class MostSharedStreamedArtist {
    // MUST BE PREFIXED WITH "kafka-workshop-"
    public static final String OUTPUT_TOPIC = "kafka-workshop-most-shared-streamed-artist";

    public static final JsonSerde<MostSharedStreamedArtistResult> MOST_SHARED_STREAMED_ARTIST_RESULT_JSON_SERDE = new JsonSerde<>(MostSharedStreamedArtistResult.class);
    public static final JsonSerde<CustomerArtistCount> TOP_ARTIST_PER_CUSTOMER_JSON_SERDE = new JsonSerde<>(CustomerArtistCount.class);
    public static final JsonSerde<TopArtistCount> TOP_ARTIST_COUNT_JSON_SERDE = new JsonSerde<>(TopArtistCount.class);

    /**
     * Streams app launched via main => this should implement the Topology for 'finding customers with same #1 artist'
     */
    public static void main(final String[] args) {
        final StreamsBuilder builder = new StreamsBuilder();

        // configure the processing topology
        configureTopology(builder);

        // fire up engine
        startStreams(builder);
    }

    static void configureTopology(final StreamsBuilder builder) {
        // Create KTable of customers keyed by customerId
        KTable<String, Customer> customerKTable = builder
            .table(
                TOPIC_DATA_DEMO_CUSTOMERS,
                Materialized
                    .<String, Customer>as(persistentKeyValueStore("customers"))
                    .withKeySerde(Serdes.String())
                    .withValueSerde(SERDE_CUSTOMER_JSON)
            );

        // Create KTable of artists keyed by artistId
        KTable<String, Artist> artistKTable = builder
            .table(
                TOPIC_DATA_DEMO_ARTISTS,
                Materialized
                    .<String, Artist>as(persistentKeyValueStore("artists"))
                    .withKeySerde(Serdes.String())
                    .withValueSerde(SERDE_ARTIST_JSON)
            );

        TimeWindows tumblingWindow = TimeWindows.of(java.time.Duration.ofMinutes(5));

        customerKTable.toStream().peek((key, customer) -> log.info("Customer added with ID '{}'", customer.id()));
        artistKTable.toStream().peek((key, artist) -> log.info("Artist added with ID '{}'", artist.id()));

        // Process the stream events
        builder
            .stream(TOPIC_DATA_DEMO_STREAMS, Consumed.with(Serdes.String(), SERDE_STREAM_JSON))
            .peek((streamId, streamRequest) -> log.info("Stream Request: {}", streamRequest))

            // rekey by the customerId
            .selectKey((streamId, streamRequest) -> streamRequest.customerid(), Named.as("rekey-by-customerid"))

            // Join with customers table to get customer information
            .join(
                customerKTable,
                (streamRequest, customer) -> new CustomerStream(customer, streamRequest),
                Joined.with(Serdes.String(), SERDE_STREAM_JSON, SERDE_CUSTOMER_JSON)
            )
            .peek((customerId, customerStream) -> log.info("Customer ID: {}, Customer-Stream: {}", customerId, customerStream))

            // Group by customer ID
            .groupByKey()

            // now we need to aggregate over the tumbling window
            .windowedBy(tumblingWindow)

            /**
             * This Aggregate is a SUM on streamtime
             * to find the most streamed artist
             */
            .aggregate(
                CustomerArtistCount::new,

                (customerId, customerStream, customerArtistCount) -> {
                    if (!customerArtistCount.initialized) {
                        customerArtistCount.initialize(customerStream.getCustomer());
                    }
                    customerArtistCount.incrementStreamCount(customerStream.getStream().artistid());
                    return customerArtistCount;
                },
                Materialized
                    .<String, CustomerArtistCount, WindowStore<Bytes, byte[]>>as("customer-artist-table")
                    .withKeySerde(Serdes.String())
                    .withValueSerde(TOP_ARTIST_PER_CUSTOMER_JSON_SERDE)
            )
            .toStream((Windowed windowedKey, CustomerArtistCount customerArtistCount) -> windowedKey.toString())
            .peek((windowedCustomerId, topArtistCount) -> log.info("windowedCustomerId {}", windowedCustomerId))

            // using stored CustomerArtistCount aggregate, we need to aggregate over them and find the global top shared artist for a tumbling window
            .selectKey((windowedCustomerId, customerArtistCount) -> "global")
            .peek((globalKey, topArtistCount) -> log.info("global {}", globalKey))

            // group by artistId and collect list of customerIds
            .groupByKey()

            // now we need to aggregate over the tumbling window again here since they don't carry the previous global top artists
            .windowedBy(tumblingWindow)

            // now aggregate the top artists for this window. This will produce a top artist, and it's list of customers
            .aggregate(
                TopArtistCount::new,

                (globalKey, customerArtistCount, topArtistCount) -> {
                    String artistId = customerArtistCount.getTopStreamedArtistId();
                    Customer customer = customerArtistCount.getCustomer();
                    topArtistCount.addStream(artistId, customer);
                    log.info("Adding Customer {} to artistID {} in GLOBAL", customer.id(), artistId);
                    return topArtistCount;
                },
                Materialized
                    .<String, TopArtistCount, WindowStore<Bytes, byte[]>>as("artist-count-table")
                    .withKeySerde(Serdes.String())
                    .withValueSerde(TOP_ARTIST_COUNT_JSON_SERDE)
            )
            .toStream((Windowed windowedKey, TopArtistCount topArtistCount) -> windowedKey.toString())

            .selectKey((artistId, topArtistCount) -> topArtistCount.currentTopArtistId)
            .peek((topArtistId, topArtistCount) -> log.info("TopArtistID {}", topArtistId))

            // Join with customers table to get customer information
            .join(
                artistKTable,
                (topArtistId, topArtistCount, artist) -> new MostSharedStreamedArtistResult(topArtistId, artist, topArtistCount.getTopArtistCustomerList()),
                Joined.with(Serdes.String(), TOP_ARTIST_COUNT_JSON_SERDE, SERDE_ARTIST_JSON)
            )

            .peek((artistId, mostSharedStreamedArtistResult) -> log.info("TopArtistID {} has {}", artistId, mostSharedStreamedArtistResult))

            .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), MOST_SHARED_STREAMED_ARTIST_RESULT_JSON_SERDE));
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class MostSharedStreamedArtistResult {
        private String artistId;
        private Artist artist;
        private List<String> customerList;
    }

    /**
     * Class used for our Customer Stream Join
     */
    @Data
    @AllArgsConstructor
    public static class CustomerStream {
        private Customer customer;
        private Stream stream;
    }

    @Data
    @Builder
    @AllArgsConstructor
    public static class TopArtistCount {
        private String currentTopArtistId;
        private Integer currentTopArtistCount;
        // store a map from artistId to their streamCount
        private Map<String, Integer> artistStreamCountMap;
        private Map<String, Set<String>> artistToCustomerMap;

        // constructor not init
        public TopArtistCount() {
            this.currentTopArtistId = null;
            this.currentTopArtistCount = 0;
            this.artistStreamCountMap = new HashMap<>();
            this.artistToCustomerMap = new HashMap<>();
        }

        public void addStream(String artistId, Customer customer) {
            // check if this venueId exists, else put it into map
            if (!artistStreamCountMap.containsKey(artistId)) {
                artistStreamCountMap.put(artistId, 0);
            }
            // index into map, update venue revenue map
            artistStreamCountMap.replace(artistId, artistStreamCountMap.get(artistId) + 1);
            log.info("Artist {} Count {}", artistId, artistStreamCountMap.get(artistId));

            // check if it's greater than current max then update accordingly
            if (this.currentTopArtistCount < artistStreamCountMap.get(artistId)) {
                log.info("Artist {} Has Greater", artistId);
                this.currentTopArtistCount = artistStreamCountMap.get(artistId);
                this.currentTopArtistId = artistId;
            }

            // add this customer
            artistToCustomerMap.computeIfAbsent(artistId, k -> new HashSet<>()).add(customer.id());
            log.info("Artist {} Customer List {}", artistId, artistToCustomerMap.get(artistId));
        }

        public List<String> getTopArtistCustomerList() {
            Set<String> set = artistToCustomerMap.get(this.currentTopArtistId);
            return (set != null) ? new ArrayList<>(set) : Collections.emptyList();
        }
    }

    @Data
    @AllArgsConstructor
    public static class CustomerArtistCount {
        private boolean initialized;
        private String customerId;
        private Customer customer;
        private Map<String, Integer> streamCount;
        @Getter
        private String topStreamedArtistId;
        private Integer topArtistCount;

        public CustomerArtistCount() { initialized = false; }

        public void initialize(Customer customer) {
            this.initialized = true;
            this.customerId = customer.id();
            this.customer = customer;
            this.streamCount = new HashMap<>();
            this.topStreamedArtistId = "";
            this.topArtistCount = 0;
        }

        public void incrementStreamCount(String artistId) {
            streamCount.put(artistId, streamCount.getOrDefault(artistId, 0) + 1);

            // check if count is greater now
            if (streamCount.get(artistId) > topArtistCount) {
                log.info("New TopArtist {} for Customer {}", artistId, customerId);
                topArtistCount = streamCount.get(artistId);
                topStreamedArtistId = artistId;
            }
        }
    }
}