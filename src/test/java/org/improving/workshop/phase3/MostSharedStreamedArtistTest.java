package org.improving.workshop.phase3;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.improving.workshop.Streams;
import org.improving.workshop.utils.DataFaker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.msse.demo.mockdata.customer.profile.Customer;
import org.msse.demo.mockdata.music.artist.Artist;
import org.msse.demo.mockdata.music.stream.Stream;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MostSharedStreamedArtistTest {
    private final static Serializer<String> stringSerializer = Serdes.String().serializer();
    private final static Deserializer<String> stringDeserializer = Serdes.String().deserializer();

    // Define the missing serde for List<Customer>
    public static final JsonSerde<List<Customer>> SERDE_LIST_CUSTOMER_JSON = new JsonSerde<>(List.class);

    private TopologyTestDriver driver;

    // inputs
    private TestInputTopic<String, Customer> customerInputTopic;
    private TestInputTopic<String, Stream> streamInputTopic;
    private TestInputTopic<String, Artist> artistInputTopic;

    // outputs
    private TestOutputTopic<String, MostSharedStreamedArtist.MostSharedStreamedArtistResult> outputTopic;

    @BeforeEach
    void setUp() {
        // instantiate new builder
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // build the topology
        MostSharedStreamedArtist.configureTopology(streamsBuilder);

        // build the TopologyTestDriver
        driver = new TopologyTestDriver(streamsBuilder.build(), Streams.buildProperties());

        customerInputTopic = driver.createInputTopic(
            Streams.TOPIC_DATA_DEMO_CUSTOMERS,
            stringSerializer,
            Streams.SERDE_CUSTOMER_JSON.serializer()
        );

        streamInputTopic = driver.createInputTopic(
            Streams.TOPIC_DATA_DEMO_STREAMS,
            stringSerializer,
            Streams.SERDE_STREAM_JSON.serializer()
        );

        artistInputTopic = driver.createInputTopic(
            Streams.TOPIC_DATA_DEMO_ARTISTS,
            stringSerializer,
            Streams.SERDE_ARTIST_JSON.serializer()
        );

        outputTopic = driver.createOutputTopic(
            MostSharedStreamedArtist.OUTPUT_TOPIC,
            stringDeserializer,
            MostSharedStreamedArtist.MOST_SHARED_STREAMED_ARTIST_RESULT_JSON_SERDE.deserializer()
        );
    }

    @AfterEach
    void cleanup() {
        driver.close();
    }

    @Test
    @DisplayName("most shared streamed artist test")
    void testMostSharedStreamedArtist() {
        /**
         * Have 3 artists, artist-1, artist-2, artist-3
         * - will attempt to have them shuffle through to show tumbling window
         * - will be artist-1, then artist-2, last artist-3
         *
         * Customers will not matter, fully random
         * Will need to track number of streams per artist
         * For all rounds new customers will be created
         *
         * Round #1
         * - will have artist-1 get 3 streams
         * - will have artist-2 get 1 stream
         * - will have artist-3 get 1 stream
         * = Result of tumbling window => artist-1, customers from those 3 streams
         *
         * Round #2
         * - will have artist-1 get 0 streams
         * - will have artist-2 get 3 stream
         * - will have artist-3 get 1 stream
         * = Result of tumbling window => artist-2, customers from those 3 streams
         *
         * Round #3
         * - will have artist-1 get 3 streams
         * - will have artist-2 get 1 stream
         * - will have artist-3 get 5 stream
         * = Result of tumbling window => artist-3, customers from those 5 streams
         */
        String artist1 = "artist-1";
        String artist2 = "artist-2";
        String artist3 = "artist-3";

        // Create the 3 artists
        Artist artist1Obj = DataFaker.ARTISTS.generate(artist1);
        Artist artist2Obj = DataFaker.ARTISTS.generate(artist2);
        Artist artist3Obj = DataFaker.ARTISTS.generate(artist3);

        artistInputTopic.pipeInput(artist1, artist1Obj);
        artistInputTopic.pipeInput(artist2, artist2Obj);
        artistInputTopic.pipeInput(artist3, artist3Obj);

        // Round #1
        List<String> round1CustomerIds = new ArrayList<>();
        List<Customer> round1Customers = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            Customer c = DataFaker.CUSTOMERS.generate();
            round1CustomerIds.add(c.id());
            round1Customers.add(c);
            customerInputTopic.pipeInput(c.id(), c);
        }

        // Create streams for Round #1
        // artist-1: 3 streams
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(round1CustomerIds.get(0), artist1));
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(round1CustomerIds.get(1), artist1));
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(round1CustomerIds.get(2), artist1));

        // artist-2: 1 stream
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(round1CustomerIds.get(3), artist2));

        // artist-3: 1 stream
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(round1CustomerIds.get(4), artist3));

        // Advance the wall clock time to end the first window
        driver.advanceWallClockTime(java.time.Duration.ofMinutes(5));

        // Verify Round #1 results
        var outputRecords = outputTopic.readRecordsToList();
        TestRecord<String, MostSharedStreamedArtist.MostSharedStreamedArtistResult> round1Result = outputRecords.get(outputRecords.size() - 1);

        assertEquals(artist1, round1Result.key());
        assertEquals(artist1, round1Result.value().getArtistId());
        assertEquals(3, round1Result.value().getCustomerList().size());
        // Verify the right customers are in the result
        List<String> resultCustomerIds = round1Result.value().getCustomerList().stream()
            .map(Customer::id)
            .toList();
        assertTrue(resultCustomerIds.contains(round1CustomerIds.get(0)));
        assertTrue(resultCustomerIds.contains(round1CustomerIds.get(1)));
        assertTrue(resultCustomerIds.contains(round1CustomerIds.get(2)));

        // Round #2
        List<String> round2CustomerIds = new ArrayList<>();
        List<Customer> round2Customers = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            Customer c = DataFaker.CUSTOMERS.generate();
            round2CustomerIds.add(c.id());
            round2Customers.add(c);
            customerInputTopic.pipeInput(c.id(), c);
        }

        // Create streams for Round #2
        // artist-1: 0 streams

        // artist-2: 3 streams
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(round2CustomerIds.get(0), artist2));
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(round2CustomerIds.get(1), artist2));
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(round2CustomerIds.get(2), artist2));

        // artist-3: 1 stream
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(round2CustomerIds.get(3), artist3));

        // Advance the wall clock time to end the second window
        driver.advanceWallClockTime(java.time.Duration.ofMinutes(5));

        // Verify Round #2 results
        outputRecords = outputTopic.readRecordsToList();
        TestRecord<String, MostSharedStreamedArtist.MostSharedStreamedArtistResult> round2Result =
            outputRecords.get(outputRecords.size() - 1);

        assertEquals(artist2, round2Result.key());
        assertEquals(artist2, round2Result.value().getArtistId());
        assertEquals(3, round2Result.value().getCustomerList().size());
        // Verify the right customers are in the result
        resultCustomerIds = round2Result.value().getCustomerList().stream()
            .map(Customer::id)
            .toList();
        assertTrue(resultCustomerIds.contains(round2CustomerIds.get(0)));
        assertTrue(resultCustomerIds.contains(round2CustomerIds.get(1)));
        assertTrue(resultCustomerIds.contains(round2CustomerIds.get(2)));

        // Round #3
        List<String> round3CustomerIds = new ArrayList<>();
        List<Customer> round3Customers = new ArrayList<>();
        for (int i = 0; i < 9; i++) {
            Customer c = DataFaker.CUSTOMERS.generate();
            round3CustomerIds.add(c.id());
            round3Customers.add(c);
            customerInputTopic.pipeInput(c.id(), c);
        }

        // Create streams for Round #3
        // artist-1: 3 streams
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(round3CustomerIds.get(0), artist1));
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(round3CustomerIds.get(1), artist1));
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(round3CustomerIds.get(2), artist1));

        // artist-2: 1 stream
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(round3CustomerIds.get(3), artist2));

        // artist-3: 5 streams
        for (int i = 4; i < 9; i++) {
            streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(round3CustomerIds.get(i), artist3));
        }

        // Advance the wall clock time to end the third window
        driver.advanceWallClockTime(java.time.Duration.ofMinutes(5));

        // Verify Round #3 results
        outputRecords = outputTopic.readRecordsToList();
        TestRecord<String, MostSharedStreamedArtist.MostSharedStreamedArtistResult> round3Result =
            outputRecords.get(outputRecords.size() - 1);

        assertEquals(artist3, round3Result.key());
        assertEquals(artist3, round3Result.value().getArtistId());
        assertEquals(5, round3Result.value().getCustomerList().size());
        // Verify the right customers are in the result
        resultCustomerIds = round3Result.value().getCustomerList().stream()
            .map(Customer::id)
            .toList();
        for (int i = 4; i < 9; i++) {
            assertTrue(resultCustomerIds.contains(round3CustomerIds.get(i)));
        }
    }
}