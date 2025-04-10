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

import java.time.Instant;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

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
        for (int i = 0; i < 5; i++) {
            Customer c = DataFaker.CUSTOMERS.generate();
            round1CustomerIds.add(c.id());
            customerInputTopic.pipeInput(c.id(), c);
        }

        // Create streams for Round #1
        // artist-1: 3 streams
        Instant ts = Instant.now();
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(round1CustomerIds.get(0), artist1), ts);
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(round1CustomerIds.get(1), artist1), ts);
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(round1CustomerIds.get(2), artist1), ts);

        // artist-2: 1 stream
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(round1CustomerIds.get(3), artist2));

        // artist-3: 1 stream
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(round1CustomerIds.get(4), artist3));

        // Verify Round #1 results
        var outputRecords = outputTopic.readRecordsToList();
        TestRecord<String, MostSharedStreamedArtist.MostSharedStreamedArtistResult> round1Result = outputRecords.getLast();

        assertEquals(artist1, round1Result.key());
        assertEquals(artist1Obj.name(), round1Result.value().getArtist().name());
        assertEquals(artist1, round1Result.value().getArtistId());
        assertEquals(3, round1Result.value().getCustomerList().size());
        // Verify the right customers are in the result
        List<String> resultCustomerIds = round1Result.value().getCustomerList();
        assertTrue(resultCustomerIds.contains(round1CustomerIds.get(0)));
        assertTrue(resultCustomerIds.contains(round1CustomerIds.get(1)));
        assertTrue(resultCustomerIds.contains(round1CustomerIds.get(2)));

        // Round #2
        List<String> round2CustomerIds = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            Customer c = DataFaker.CUSTOMERS.generate();
            round2CustomerIds.add(c.id());
            customerInputTopic.pipeInput(c.id(), c);
        }

        // Create streams for Round #2
        // artist-1: 0 streams

        // artist-2: 3 streams
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(round2CustomerIds.get(0), artist2), ts.plusSeconds(15 * 60));
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(round2CustomerIds.get(1), artist2), ts.plusSeconds(15 * 60));
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(round2CustomerIds.get(2), artist2), ts.plusSeconds(15 * 60));

        // artist-3: 1 stream
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(round2CustomerIds.get(3), artist3), ts.plusSeconds(15 * 60));

        // Verify Round #2 results
        outputRecords = outputTopic.readRecordsToList();
        TestRecord<String, MostSharedStreamedArtist.MostSharedStreamedArtistResult> round2Result = outputRecords.getLast();

        assertEquals(artist2, round2Result.key());
        assertEquals(artist2Obj.name(), round2Result.value().getArtist().name());
        assertEquals(artist2, round2Result.value().getArtistId());
        assertEquals(3, round2Result.value().getCustomerList().size());
        // Verify the right customers are in the result
        resultCustomerIds = round2Result.value().getCustomerList();
        assertTrue(resultCustomerIds.contains(round2CustomerIds.get(0)));
        assertTrue(resultCustomerIds.contains(round2CustomerIds.get(1)));
        assertTrue(resultCustomerIds.contains(round2CustomerIds.get(2)));

        // Round #3
        // Advance the wall clock time to end the third window
        driver.advanceWallClockTime(java.time.Duration.ofMinutes(15));
        ts.plus(java.time.Duration.ofMinutes(15));

        List<String> round3CustomerIds = new ArrayList<>();
        for (int i = 0; i < 9; i++) {
            Customer c = DataFaker.CUSTOMERS.generate();
            round3CustomerIds.add(c.id());
            customerInputTopic.pipeInput(c.id(), c);
        }

        // Create streams for Round #3
        // artist-1: 3 streams
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(round3CustomerIds.get(0), artist1), ts.plusSeconds(30 * 60));
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(round3CustomerIds.get(1), artist1), ts.plusSeconds(30 * 60));
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(round3CustomerIds.get(2), artist1), ts.plusSeconds(30 * 60));

        // artist-2: 1 stream
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(round3CustomerIds.get(3), artist2), ts.plusSeconds(30 * 60));

        // artist-3: 5 streams
        for (int i = 4; i < 9; i++) {
            streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(round3CustomerIds.get(i), artist3), ts.plusSeconds(30 * 60));
        }

        // Verify Round #3 results
        outputRecords = outputTopic.readRecordsToList();
        TestRecord<String, MostSharedStreamedArtist.MostSharedStreamedArtistResult> round3Result = outputRecords.getLast();

        assertEquals(artist3, round3Result.key());
        assertEquals(artist3Obj.name(), round3Result.value().getArtist().name());
        assertEquals(artist3, round3Result.value().getArtistId());
        assertEquals(5, round3Result.value().getCustomerList().size());
        // Verify the right customers are in the result
        resultCustomerIds = round3Result.value().getCustomerList();
        for (int i = 4; i < 9; i++) {
            assertTrue(resultCustomerIds.contains(round3CustomerIds.get(i)));
        }
    }

    @Test
    @DisplayName("duelling most shared streamed artist test")
    void testDuelMostSharedStreamedArtist() {
        /**
         * Have 3 artists, artist-1, artist-2, artist-3 that are dueling for being the most streamed
         * - will attempt to have them shuffle through to show tumbling window
         * - will be artist-1, then artist-2, last artist-3
         *
         * Customers will not matter, fully random
         * Will need to track number of streams per artist
         * For all rounds new customers will be created
         *
         * Round #1
         * - will have artist-1 get 5 streams
         * - will have artist-2 get 4 stream
         * - will have artist-3 get 3 stream
         * - will have artist-1 gets 3 more streams
         * - will have artist-3 gets 6 more streams
         * = Result of tumbling window => artist-3, customers from those 9 streams
         *
         * Round #2 => test ties, will keep previous max
         * - will have artist-1 get 1 streams
         * - will have artist-2 get 2 stream
         * - will have artist-3 get 1 stream
         * - will have artist-2 gets 1 more stream
         * - will have artist-3 get 2 stream
         * = Result of tumbling window => artist-2, customers from those 3 streams (tied with artist-3 but we keep previous)
         *
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

        // –----- Round #1
        // get current time
        Instant ts = Instant.now();

        // add customers
        HashMap<String, List<String>> artistMap = new HashMap();
        List<String> customerList = new ArrayList();
        for (int i = 0; i < 21; i++) {
            Customer c = DataFaker.CUSTOMERS.generate();
            customerList.add(c.id());
            customerInputTopic.pipeInput(c.id(), c);
        }

        // put customers into their own lists => probably more complex than needed but oh well
        artistMap.put(artist1, customerList.subList(0, 8));
        artistMap.put(artist2, customerList.subList(8, 12));
        artistMap.put(artist3, customerList.subList(12, 21));

        // artist-1 5 streams => winner
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(artistMap.get(artist1).get(0), artist1), ts);
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(artistMap.get(artist1).get(1), artist1), ts);
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(artistMap.get(artist1).get(2), artist1), ts);
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(artistMap.get(artist1).get(3), artist1), ts);
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(artistMap.get(artist1).get(4), artist1), ts);

        // artist-2 4 streams
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(artistMap.get(artist2).get(0), artist2), ts);
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(artistMap.get(artist2).get(1), artist2), ts);
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(artistMap.get(artist2).get(2), artist2), ts);
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(artistMap.get(artist2).get(3), artist2), ts);

        // artist-3 3 streams
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(artistMap.get(artist3).get(0), artist3), ts);
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(artistMap.get(artist3).get(1), artist3), ts);
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(artistMap.get(artist3).get(2), artist3), ts);

        // artist-1 3 streams
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(artistMap.get(artist1).get(5), artist1), ts);
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(artistMap.get(artist1).get(6), artist1), ts);
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(artistMap.get(artist1).get(7), artist1), ts);

        // artist-3 6 streams => winner*
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(artistMap.get(artist3).get(3), artist3), ts);
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(artistMap.get(artist3).get(4), artist3), ts);
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(artistMap.get(artist3).get(5), artist3), ts);
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(artistMap.get(artist3).get(6), artist3), ts);
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(artistMap.get(artist3).get(7), artist3), ts);
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(artistMap.get(artist3).get(8), artist3), ts);

        // –----- Round #1 CHECKS
        var outputRecords = outputTopic.readRecordsToList();
        TestRecord<String, MostSharedStreamedArtist.MostSharedStreamedArtistResult> round1Result = outputRecords.getLast();

        assertEquals(artist3, round1Result.key());
        assertEquals(artist3Obj.name(), round1Result.value().getArtist().name());
        assertEquals(artist3, round1Result.value().getArtistId());
        assertEquals(9, round1Result.value().getCustomerList().size());
        for (int i = 0; i < round1Result.value().getCustomerList().size(); i++) {
            assertTrue(artistMap.get(artist3).contains(round1Result.value().getCustomerList().get(i)));
        }

        // –----- Round #2

        // add customers, clear map and queue
        artistMap.clear();
        customerList.clear();
        for (int i = 0; i < 7; i++) {
            Customer c = DataFaker.CUSTOMERS.generate();
            customerList.add(c.id());
            customerInputTopic.pipeInput(c.id(), c);
        }

        // put customers into their own lists => probably more complex than needed but oh well
        artistMap.put(artist1, customerList.subList(0, 1));
        artistMap.put(artist2, customerList.subList(1, 4));
        artistMap.put(artist3, customerList.subList(4, 7));

        // artist-1 1 stream
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(artistMap.get(artist1).get(0), artist1), ts.plusSeconds(15 * 60));

        // artist-2 2 streams => winner
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(artistMap.get(artist2).get(0), artist1), ts.plusSeconds(15 * 60));
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(artistMap.get(artist2).get(1), artist2), ts.plusSeconds(15 * 60));

        // artist-3 1 stream
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(artistMap.get(artist3).get(0), artist3), ts.plusSeconds(15 * 60));

        // artist-2 1 stream => window
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(artistMap.get(artist2).get(2), artist2), ts.plusSeconds(15 * 60));

        // artist-3 2 streams
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(artistMap.get(artist3).get(1), artist3), ts.plusSeconds(15 * 60));
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(artistMap.get(artist3).get(2), artist3), ts.plusSeconds(15 * 60));

        // –----- Round #2 CHECKS
        outputRecords = outputTopic.readRecordsToList();
        TestRecord<String, MostSharedStreamedArtist.MostSharedStreamedArtistResult> round2Result = outputRecords.getLast();

        assertEquals(artist3, round2Result.key());
        assertEquals(artist3Obj.name(), round2Result.value().getArtist().name());
        assertEquals(artist3, round2Result.value().getArtistId());
        assertEquals(3, round2Result.value().getCustomerList().size());
        for (int i = 0; i < round2Result.value().getCustomerList().size(); i++) {
            assertTrue(artistMap.get(artist3).contains(round2Result.value().getCustomerList().get(i)));
        }
    }

    @Test
    @DisplayName("test shared customers between artists")
    void testSharedCustomersBetweenArtists() {
        /**
         * Test scenario where customers stream multiple artists within the same window
         * - Customer 0 => streams a1, a2, a3 => a1 will be most streamed
         * - Customer 1 => streams a1, a2 => a1 will be most streamed
         * - Customer 2 => streams a2, a3 => a2 will be most streamed
         * - Customer 3 => streams a2, a2, a2  => a2 will be most streamed
         * - Customer 4 => streams a3, a3 => a3 will be most streamed
         *
         * Artists
         * 1: 2
         * 2: 6
         * 3: 4
         */
        String artist1 = "artist-1";
        String artist2 = "artist-2";
        String artist3 = "artist-3";

        // Create the artists
        Artist artist1Obj = DataFaker.ARTISTS.generate(artist1);
        Artist artist2Obj = DataFaker.ARTISTS.generate(artist2);
        Artist artist3Obj = DataFaker.ARTISTS.generate(artist3);

        artistInputTopic.pipeInput(artist1, artist1Obj);
        artistInputTopic.pipeInput(artist2, artist2Obj);
        artistInputTopic.pipeInput(artist3, artist3Obj);

        // Create customers
        Instant ts = Instant.now();
        List<String> customerIds = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            Customer c = DataFaker.CUSTOMERS.generate();
            customerIds.add(c.id());
            customerInputTopic.pipeInput(c.id(), c);
        }

        // Customer 0 streams all three artists => equal number
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(customerIds.get(0), artist1), ts);
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(customerIds.get(0), artist2), ts);
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(customerIds.get(0), artist3), ts);

        // Customer 1 streams artist1 and artist2 => equal number
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(customerIds.get(1), artist1), ts);
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(customerIds.get(1), artist2), ts);

        // Customer 2 streams artist2 and artist3 => equal number
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(customerIds.get(2), artist2), ts);
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(customerIds.get(2), artist3), ts);

        // Customer 3 streams artist2 three times (making artist2 the winner)
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(customerIds.get(3), artist2), ts);
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(customerIds.get(3), artist2), ts);
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(customerIds.get(3), artist2), ts);

        // Customer 4 streams artist3 twice
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(customerIds.get(4), artist3), ts);
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(customerIds.get(4), artist3), ts);

        // Verify results - artist2 should be most streamed with 6 streams
        var outputRecords = outputTopic.readRecordsToList();
        TestRecord<String, MostSharedStreamedArtist.MostSharedStreamedArtistResult> result = outputRecords.getLast();

        assertEquals(artist2, result.key());
        assertEquals(artist2Obj.name(), result.value().getArtist().name());

        // Verify customer list contains customers who streamed artist2
        List<String> resultCustomerIds = result.value().getCustomerList();
        assertEquals(4, resultCustomerIds.size(), "There should be 4 unique customers who streamed artist2");
        assertTrue(resultCustomerIds.contains(customerIds.get(0)));
        assertTrue(resultCustomerIds.contains(customerIds.get(1)));
        assertTrue(resultCustomerIds.contains(customerIds.get(2)));
        assertTrue(resultCustomerIds.contains(customerIds.get(3)));
        assertFalse(resultCustomerIds.contains(customerIds.get(4)));
    }


}