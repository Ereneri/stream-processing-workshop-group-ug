package org.improving.workshop.samples;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.common.utils.Bytes;
import org.improving.workshop.Streams;
import org.msse.demo.mockdata.customer.address.Address;
import org.msse.demo.mockdata.customer.profile.Customer;
import org.msse.demo.mockdata.music.artist.Artist;
import org.msse.demo.mockdata.music.stream.Stream;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
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
        // Create KTables for lookup data
        KTable<String, Customer> customerTable = builder.table(
                TOPIC_DATA_DEMO_CUSTOMERS,
                Consumed.with(Serdes.String(), SERDE_CUSTOMER_JSON)
        );
        
        KTable<String, Address> addressTable = builder.table(
                TOPIC_DATA_DEMO_ADDRESSES,
                Consumed.with(Serdes.String(), SERDE_ADDRESS_JSON)
        );
        
        KTable<String, Artist> artistTable = builder.table(
                TOPIC_DATA_DEMO_ARTISTS,
                Consumed.with(Serdes.String(), SERDE_ARTIST_JSON)
        );
        
        // Join address with customer (flipping the join direction based on diagram)
        KTable<String, CustomerAddress> customerAddressTable = addressTable
            .join(customerTable,
                  address -> address.customerid(),  
                  (address, customer) -> new CustomerAddress(
                      customer.id(),
                      customer.birthdt(),
                      address.state()
                  )
            );
        
        // Process streaming events
        KStream<String, StreamWithContext> enrichedStreams = builder
            .stream(TOPIC_DATA_DEMO_STREAMS, Consumed.with(Serdes.String(), SERDE_STREAM_JSON))
            .peek((key, stream) -> log.info("Stream Received: {}", stream))
            
            // First rekey the stream by customerid
            .selectKey((key, streamValue) -> streamValue.customerid())
            // Then join with customerAddress (now using the stream key)
            .join(
                customerAddressTable,
                (stream, customerAddress) -> {
                    // Determine generation based on birth date
                    String generation = determineGeneration(customerAddress.getBirthdt());
                    
                    return new StreamWithContext(
                        stream.id(),
                        stream.customerid(),
                        stream.artistid(),
                        generation,
                        customerAddress.getState()
                    );
                }
            );
            
        // Group by generation and count by artist
        enrichedStreams
            // Key by generation (for grouping)
            .selectKey((k, v) -> v.generation)
            
            // Group by generation
            .groupByKey()
            .aggregate(
                // Initialize - empty map of artist counts
                () -> new LinkedHashMap<String, Long>(),
                
                // Aggregator - increment artist count
                (generationKey, streamContext, artistCounts) -> {
                    String artistId = streamContext.artistId;
                    artistCounts.compute(artistId, (k, v) -> v == null ? 1L : v + 1L);
                    
                    // Sort by count (descending)
                    return artistCounts.entrySet().stream()
                        .sorted(reverseOrder(Map.Entry.comparingByValue()))
                        .collect(toMap(
                            Map.Entry::getKey, 
                            Map.Entry::getValue, 
                            (e1, e2) -> e1, 
                            LinkedHashMap::new
                        ));
                },
                
                // Materialized config
                Materialized.<String, LinkedHashMap<String, Long>, KeyValueStore<Bytes, byte[]>>as(
                    "artist-stream-counts-by-generation")
                    .withKeySerde(Serdes.String())
                    .withValueSerde(new JsonSerde<>(LinkedHashMap.class))
            )
            
            // Convert to stream to output results
            .toStream()
            
            // Extract the top artist (first entry in sorted map)
            .mapValues((generation, artistCounts) -> {
                if (artistCounts.isEmpty()) {
                    return new TrendingArtistResult(generation, null, 0);
                }
                
                Map.Entry<String, Long> topArtist = artistCounts.entrySet().iterator().next();
                return new TrendingArtistResult(
                    generation, 
                    topArtist.getKey(), 
                    topArtist.getValue()
                );
            })
            
            // Log the results
            .peek((generation, result) -> 
                log.info("Top trending artist for generation {}: {} with {} streams", 
                    generation, result.artistId, result.streamCount))
                    
            // Output to topic
            .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), TRENDING_ARTIST_RESULT_SERDE));
    }

    /**
     * Determine generation based on birth date
     */
    private static String determineGeneration(String birthDate) {
        if (birthDate == null || birthDate.isEmpty()) {
            return "Unknown";
        }
        
        try {
            LocalDate dob = LocalDate.parse(birthDate, DateTimeFormatter.ISO_DATE);
            int year = dob.getYear();
            
            if (year < 1946) return "Pre-Boomer";
            if (year < 1965) return "Boomer"; 
            if (year < 1981) return "Gen X";
            if (year < 1997) return "Millennial";
            if (year < 2013) return "Gen Z";
            return "Gen Alpha";
            
        } catch (Exception e) {
            log.error("Error parsing date: {}", birthDate, e);
            return "Unknown";
        }
    }
    
    // Data classes for holding intermediate state
    
    @Data
    @AllArgsConstructor
    static class CustomerAddress {
        private String customerId;
        private String birthdt;
        private String state;
    }
    
    @Data
    @AllArgsConstructor
    static class StreamWithContext {
        private String streamId;
        private String customerId;
        private String artistId;
        private String generation;
        private String state;
    }
    
    @Data
    @AllArgsConstructor
    static class TrendingArtistResult {
        private String generation;
        private String artistId;
        private long streamCount;
    }
}