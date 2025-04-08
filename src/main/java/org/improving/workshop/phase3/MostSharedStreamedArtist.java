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
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;

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

        customerKTable.toStream().peek((key, customer) -> log.info("Customer added with ID '{}'", key, customer.id()));
        artistKTable.toStream().peek((key, artist) -> log.info("Artist added with ID '{}'", key, artist.id()));

        // Process the stream events
        builder
            .stream(TOPIC_DATA_DEMO_STREAMS, Consumed.with(Serdes.String(), SERDE_STREAM_JSON))
            .peek((streamId, streamRequest) -> log.info("Stream Request: {}", streamRequest))

            // rekey by the streamId
            .selectKey((streamId, streamRequest) -> streamRequest.customerid(), Named.as("rekey-by-customerid"))

            // Join with customers table to get customer information
            .join(
                customerKTable,
                (customerId, stream, customer) -> new CustomerStream(customer, stream)
            )
            .peek((customerId, customerStream) -> log.info("Customer ID: {}, Customer-Stream: {}", customerId, customerStream))

            // Group by customer ID
            .groupByKey()

            .windowedBy(tumblingWindow)

            /**
             * This aggregate is creating an object which stores a key-value pair of a customer to another key-value pair.
             * This inner key-value pair maps a artistID to total stream time for this artist, for a given customer
             * this can be used later to determine which artist has the highest total stream time
             * Using this we should be able to find all the customers who have this artist as their #1 streamed artist
             */
            .aggregate(
                // Initialize the aggregate as an empty TopArtistPerCustomer
                TopArtistPerCustomer::new,
                // Aggregator: update the TopArtistPerCustomer instance with each stream event
                (globalKey, customerStream, aggregate) -> {
                    // Assume Stream has methods getArtistId() and getStreamTime() that return the required values
                    aggregate.addStreamTime(
                        customerStream.getStream().artistid(),
                        customerStream.getCustomer(),
                        customerStream.getStream().streamtime()
                    );
                    return aggregate;
                },
                Materialized.<String, TopArtistPerCustomer, WindowStore<Bytes, byte[]>>as("global-top-artist-store")
                    .withKeySerde(Serdes.String())
                    .withValueSerde(TOP_ARTIST_PER_CUSTOMER_JSON_SERDE)
            )

            .toStream()

            // rekey and map our windowedkey to be a key value pair of the top artist id and the list of customers
            .map((windowedKey, topArtistPerCustomer) -> {
                MostSharedStreamedArtistResult streamedArtistResult = new MostSharedStreamedArtistResult(topArtistPerCustomer.getTopArtistId(), null ,topArtistPerCustomer.getTopArtistCustomers());
                return KeyValue.pair(topArtistPerCustomer.getTopArtistId(), streamedArtistResult);
            })

            // join the artist into our streamedArtistResult
            .join(artistKTable, (result, artist) -> {
                result.setArtist(artist);
                return result;
            })

            .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), MOST_SHARED_STREAMED_ARTIST_RESULT_JSON_SERDE));
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class MostSharedStreamedArtistResult {
        private String artistId;
        private Artist artist;
        private List<Customer> customerList;
    }

    @Data
    @AllArgsConstructor
    /**
     * Class used for our Custerm Stream Join
     */
    public static class CustomerStream {
        private Customer customer;
        private Stream stream;
    }

    @Data
    @AllArgsConstructor
    /**
     * Used as internal aggregate helper obejct
     */
    public static class CustomerStreamTime {
        private Customer customer;
        private HashMap<String, Long> streamTimesMap;
        @Getter
        private Long topArtistTime;
        private String topArtistId;

        public void addStreamTime(String artistId, String streamTime) {
            try {
                // convert incoming streamtime string into seconds
                Long streamTimeSeconds = Instant.parse(streamTime).getEpochSecond();

                // check if this artistId exists, else put it into map
                if (!streamTimesMap.containsKey(artistId)) {
                    streamTimesMap.put(artistId, 0L);
                }

                // index into map, update artist stream time
                streamTimesMap.put(artistId, streamTimeSeconds + streamTimesMap.get(artistId));

                // check if this current artist is not the top
                if (streamTimesMap.get(artistId) > topArtistTime) {
                    topArtistTime = streamTimesMap.get(artistId);
                    topArtistId = artistId;
                }
            } catch (Exception e) {
                log.error("Error parsing stream time: {}", streamTime, e);
            }
        }
    }

    @Data
    @NoArgsConstructor
    /**
     * Used by aggregator to keep track of the top artist and customer's top artist
     */
    public static class TopArtistPerCustomer {
        // key is artist id and value is total stream time
        private HashMap<Customer, CustomerStreamTime> customerStreamTimes = new HashMap<>();
        private HashMap<String, Long> totalStreamTimesMap = new HashMap<>();
        private String topArtistId = "";
        private Long topArtistTime = 0L;

        public void addStreamTime(String artistId, Customer customer, String streamTime) {
            // check if customerStreamTimes has this customer
            if (!customerStreamTimes.containsKey(customer)) {
                customerStreamTimes.put(customer, new CustomerStreamTime(customer, new HashMap<>(), 0L, artistId));
            }

            // add this stream
            customerStreamTimes.get(customer).addStreamTime(artistId, streamTime);

            // add this stream time to this artist
            try {
                Long streamTimeSeconds = Instant.parse(streamTime).getEpochSecond();
                if (!totalStreamTimesMap.containsKey(artistId)) {
                    totalStreamTimesMap.put(artistId, streamTimeSeconds);
                } else {
                    totalStreamTimesMap.put(artistId, totalStreamTimesMap.get(artistId) + streamTimeSeconds);
                }
            } catch (Exception e) {
                log.error("Error parsing stream time: {}", streamTime, e);
            }

            // check if this artist now has the highest stream time
            if (totalStreamTimesMap.get(artistId) > topArtistTime) {
                topArtistTime = totalStreamTimesMap.get(artistId);
                topArtistId = artistId;
            }
        }

        public List<Customer> getTopArtistCustomers() {
            if (customerStreamTimes == null || customerStreamTimes.isEmpty()) {
                return null;
            }

            List<Customer> result = new ArrayList<>();
            // loop over all customers and see if they have the current topArtistID as their top
            for (Customer customer : customerStreamTimes.keySet()) {
                if (Objects.equals(customerStreamTimes.get(customer).getTopArtistId(), topArtistId)) {
                    result.add(customer);
                }
            }
            return result;
        }
    }
}