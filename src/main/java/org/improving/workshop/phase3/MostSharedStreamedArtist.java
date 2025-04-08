package org.improving.workshop.phase3;

import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
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

    public static final JsonSerde<TopArtistPerCustomer> TOP_ARTIST_PER_CUSTOMER_JSON_SERDE = new JsonSerde<>(TopArtistPerCustomer.class);

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

            .windowedBy(tumblingWindow)

            /**
             * This aggregate is creating an object which tracks artist stream time per customer
             * and determines which artist has the highest total stream time
             */
            .aggregate(
                // Initialize the aggregate as an empty TopArtistPerCustomer
                TopArtistPerCustomer::new,
                // Aggregator: update the TopArtistPerCustomer instance with each stream event
                (customerId, customerStream, aggregate) -> {
                    // Update artist stream time for this customer
                    String artistId = customerStream.getStream().artistid();
                    aggregate.addStreamTime(artistId, customerStream.getCustomer());
                    return aggregate;
                },
                Materialized.<String, TopArtistPerCustomer, WindowStore<Bytes, byte[]>>as("top-artist-per-customer-store")
                    .withKeySerde(Serdes.String())
                    .withValueSerde(TOP_ARTIST_PER_CUSTOMER_JSON_SERDE)
            )

            .toStream()

//            .peek((windowKey, topArtistPerCustomer) -> log.info("WindowKey: `{}` with topArtistPerCustomer: {}", windowKey, topArtistPerCustomer))

            // Extract just the key without the window information
            .selectKey((windowKey, topArtistPerCustomer) -> topArtistPerCustomer.getTopArtistId())

            .peek((artistId, topArtistPerCustomer) -> log.info("Artist ID: {}, Top Artist: {}", artistId, topArtistPerCustomer.getTopArtistId()))

            // join the artist into our streamedArtistResult
            .join(
                artistKTable,
                (topArtistPerCustomer, artist) -> {
                    List<Customer> customers = topArtistPerCustomer.getTopArtistCustomers();
                    return new MostSharedStreamedArtistResult(artist.id(), artist, customers);
                }
            )

            .peek((artistId, result) -> log.info("Final result - ArtistId: {} with {} customers", artistId, result.customerList.size()))

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
     * Class used for our Customer Stream Join
     */
    public static class CustomerStream {
        private Customer customer;
        private Stream stream;
    }

    @Data
    @NoArgsConstructor
    /**
     * Used by aggregator to keep track of the top artist for each customer
     */
    public static class TopArtistPerCustomer {
        // store mapping of customerId, to a map of ArtistId to count
        private Map<String, Map<String, Integer>> customerArtistStreams = new HashMap<>();
        // customer Id to artistId
        private Map<String, String> customerTopArtist = new HashMap<>();
        // artistId to StreamCount
        private Map<String, Integer> artistTotalStreams = new HashMap<>();
        @Getter
        private String topArtistId = "";
        private int topArtistCount = 0;

        // store the customers
        private Map<String, Customer> customerMap = new HashMap<>();

        public void addStreamTime(String artistId, Customer customer) {
            String customerId = customer.id();
            customerMap.put(customerId, customer);

            // init the map if case it's empty
            if (!customerArtistStreams.containsKey(customer.id())) {
                customerArtistStreams.put(customer.id(), new HashMap<>());
            }

            // increment this customer's stream count
            Map<String, Integer> customerStreams = customerArtistStreams.get(customerId);
            customerStreams.put(artistId, customerStreams.getOrDefault(artistId, 0) + 1);

            // Update the customer's top artist if needed
            String currentTopArtist = customerTopArtist.getOrDefault(customerId, null);
            if (currentTopArtist != null) {
                int currentTopCount = customerStreams.getOrDefault(currentTopArtist, 0);
                if (customerStreams.get(artistId) > currentTopCount) {
                    customerTopArtist.put(customerId, artistId);
                }
            } else {
                customerTopArtist.put(customerId, artistId);
            }

            // Update global artist stream count
            int artistTotal = artistTotalStreams.getOrDefault(artistId, 0) + 1;
            artistTotalStreams.put(artistId, artistTotal);

            // Update global top artist if needed
            if (artistTotal > topArtistCount) {
                topArtistId = artistId;
                topArtistCount = artistTotal;
            }
        }

        public List<Customer> getTopArtistCustomers() {
            if (topArtistId == null || topArtistId.isEmpty() || customerTopArtist.isEmpty()) {
                return Collections.emptyList();
            }

            List<Customer> result = new ArrayList<>();
            // Find all customers who have the global top artist as their top artist
            for (Map.Entry<String, String> entry : customerTopArtist.entrySet()) {
                if (entry.getValue().equals(topArtistId)) {
                    Customer customer = customerMap.get(entry.getKey());
                    if (customer != null) {
                        result.add(customer);
                    }
                }
            }
            return result;
        }
    }
}