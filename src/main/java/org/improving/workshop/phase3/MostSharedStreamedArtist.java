package org.improving.workshop.phase3;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
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
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.apache.kafka.streams.state.Stores.persistentKeyValueStore;
import static org.improving.workshop.Streams.*;

@Slf4j
public class MostSharedStreamedArtist {
    // MUST BE PREFIXED WITH "kafka-workshop-"
    public static final String OUTPUT_TOPIC = "kafka-workshop-most-shared-streamed-artist";

    public static final JsonSerde<MostSharedStreamedArtistResult> MOST_SHARED_STREAMED_ARTIST_RESULT_JSON_SERDE =
        new JsonSerde<>(MostSharedStreamedArtistResult.class);
    public static final JsonSerde<TopArtistPerCustomer> TOP_ARTIST_PER_CUSTOMER_JSON_SERDE =
        new JsonSerde<>(TopArtistPerCustomer.class);
    public static final JsonSerde<CustomerList> CUSTOMER_LIST_JSON_SERDE =
        new JsonSerde<>(CustomerList.class);

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

        TimeWindows tumblingWindow = TimeWindows.of(Duration.ofMinutes(5));

        // Process the stream events
        builder
            .stream(TOPIC_DATA_DEMO_STREAMS, Consumed.with(Serdes.String(), SERDE_STREAM_JSON))
            .peek((streamId, streamRequest) -> log.info("Stream ID: {}, Request: {}", streamId, streamRequest))

            // Join with customers table to get customer information
            .join(
                customerKTable,
                (customerId, stream, customer) -> new CustomerStream(customer, stream),
                Joined.with(Serdes.String(), SERDE_STREAM_JSON, SERDE_CUSTOMER_JSON)
            )
            .peek((streamId, customerStream) -> log.info("Stream ID: {}, Customer-Stream: {}", streamId, customerStream))

            // Rekey based on customerId for aggregation per customer
            .selectKey((streamId, customerStream) -> customerStream.getCustomer().id())
            .peek((customerId, customerStream) -> log.info("Customer ID: {}, Customer-Stream: {}", customerId, customerStream))

            // Group by customer ID
            .groupByKey()

            /**
             * TODO README
             * This aggregate is creating an object which stores a key-value pair of a customer to another key-value pair.
             * This inner key-value pair maps a artistID to total stream time for this artist, for a given customer
             * this can be used later to determine which artist has the highest total stream time
             * Using this we should be able to find all the customers who have this artist as their #1 streamed artist
             */
            .aggregate(
                TopArtistPerCustomer::new,

                (customerId, customerStream, topArtistData) -> {
                    topArtistData.addStreamTime(customerStream.stream.artistid(), customerStream.customer, customerStream.stream.streamtime());
                    return topArtistData;
                },
                Materialized
                    .<String, TopArtistPerCustomer>as(persistentKeyValueStore("top-artist-per-customer"))
                    .withKeySerde(Serdes.String())
                    .withValueSerde(TOP_ARTIST_PER_CUSTOMER_JSON_SERDE)
            )

            // Convert back to stream
            .toStream()
            .peek((customerId, topArtistData) -> log.info("Customer ID: {}, Top-Artist-Data: {}", customerId, topArtistData))

            // Re-key by top artist ID
            .selectKey((artistId, topArtistData) -> topArtistData.topArtistId())
            .peek((artistId, topArtistData) -> log.info("Top-Artist-ID: {}, Top-Artist-Data: {}", artistId, topArtistData))

            // Group streams by artist ID
            .groupByKey()

            // Use tumbling window of 5 minutes
            .windowedBy(tumblingWindow)

            /**
             * TODO README
             * The goal of this aggregate is to use the previous aggregate to determine the
             * #1 streamed artist, find all the customers who have this as their #1 too and add them
             */
            .aggregate(
                CustomerList::new,

                (artistId, topArtistData, customerList) -> {

                }

                Materialized
                    .<String, CustomerList, WindowStore<Bytes, byte[]>>as(persistentKeyValueStore("customers-per-top-artist"))
                    .withKeySerde(Serdes.String())
                    .withValueSerde(CUSTOMER_LIST_JSON_SERDE)
            )

            // Convert back to stream
            .toStream()

            // Join with artist table to include artist details
            .join(
                artistKTable,
                (key, customerList, artist) -> {
                    return MostSharedStreamedArtistResult.builder()
                        .artistId(key.key())  // Extract key from windowed key
                        .artist(artist)
                        .customerList(customerList.get())
                        .build();
                },
                Joined.with(
                    WindowedSerdes.timeWindowedSerdeFrom(String.class, 5 * 60 * 1000),
                    CUSTOMER_LIST_JSON_SERDE,
                    SERDE_ARTIST_JSON
                )
            )

            // Output to result topic
            .to(
                OUTPUT_TOPIC,
                Produced.with(
                    WindowedSerdes.timeWindowedSerdeFrom(String.class, 5 * 60 * 1000),
                    MOST_SHARED_STREAMED_ARTIST_RESULT_JSON_SERDE
                )
            );
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class MostSharedStreamedArtistResult {
        // key
        private String artistId;
        // values
        private Artist artist;
        private List<Customer> customerList;
    }

    @Data
    @NoArgsConstructor
    public static class CustomerList {
        private List<Customer> customers = new ArrayList<>();

        public void addCustomer(Customer customer) {
            if (customers == null) {
                customers = new ArrayList<>();
            }
            customers.add(customer);
        }
    }

    @Data
    @AllArgsConstructor
    public static class CustomerStream {
        private Customer customer;
        private Stream stream;
    }

    @Data
    @NoArgsConstructor
    public static class TopArtistPerCustomer {
        // key is artist id and value is total stream time
        private HashMap<Customer, HashMap<String, Long>> customerStreamTimes = new HashMap<>();
        private Customer customer;

        public void addStreamTime(String artistId, Customer customer, String streamTime) {
            try {
                // convert incoming streamtime string into seconds
                Long streamTimeSeconds = Instant.parse(streamTime).getEpochSecond();

                // check if this artistId exists, else put it into map
                if (!customerStreamTimes.containsKey(customer)) {
                    customerStreamTimes.put(customer, new HashMap<>());
                }

                // index into map, update artist stream time
                customerStreamTimes.get(customer).put(artistId, streamTimeSeconds + customerStreamTimes.get(customer).get(artistId));
            } catch (Exception e) {
                log.error("Error parsing stream time: {}", streamTime, e);
            }
        }

        public String topArtistId() {
            if (customerStreamTimes == null || customerStreamTimes.isEmpty()) {
                return null;
            }

            Long currMax = 0L;
            String topArtistId = null;

            // loop over customer stream times and find the top artist
            for (Customer customer : customerStreamTimes.keySet()) {
                var artistMap = customerStreamTimes.get(customer);
                for (String artistId : artistMap.keySet()) {
                    Long curr = artistMap.get(artistId);
                    if (curr > currMax) {
                        currMax = curr;
                        topArtistId = artistId;
                    }
                }
            }
            return topArtistId;
        }
    }
}