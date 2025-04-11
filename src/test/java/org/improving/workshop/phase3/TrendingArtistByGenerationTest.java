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
import org.msse.demo.mockdata.customer.address.Address;
import org.msse.demo.mockdata.customer.profile.Customer;
import org.msse.demo.mockdata.music.artist.Artist;
import org.msse.demo.mockdata.music.artist.ArtistFaker;
import org.msse.demo.mockdata.music.stream.Stream;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public class TrendingArtistByGenerationTest {
    private final static Serializer<String> stringSerializer = Serdes.String().serializer();
    private final static Deserializer<String> stringDeserializer = Serdes.String().deserializer();

    private TopologyTestDriver driver;

    // inputs
    private TestInputTopic<String, Customer> customerInputTopic;
    private TestInputTopic<String, Address> addressInputTopic;
    private TestInputTopic<String, Artist> artistInputTopic;
    private TestInputTopic<String, Stream> streamInputTopic;

    // outputs
    private TestOutputTopic<String, TrendingArtistByGeneration.TrendingArtistResult> outputTopic;

    @BeforeEach
    void setUp() {
        // instantiate new builder
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // build the topology
        TrendingArtistByGeneration.configureTopology(streamsBuilder);

        // build the TopologyTestDriver
        driver = new TopologyTestDriver(streamsBuilder.build(), Streams.buildProperties());

        customerInputTopic = driver.createInputTopic(
            Streams.TOPIC_DATA_DEMO_CUSTOMERS,
            stringSerializer,
            Streams.SERDE_CUSTOMER_JSON.serializer()
        );

        addressInputTopic = driver.createInputTopic(
            Streams.TOPIC_DATA_DEMO_ADDRESSES,
            stringSerializer,
            Streams.SERDE_ADDRESS_JSON.serializer()
        );

        artistInputTopic = driver.createInputTopic(
            Streams.TOPIC_DATA_DEMO_ARTISTS,
            stringSerializer,
            Streams.SERDE_ARTIST_JSON.serializer()
        );

        streamInputTopic = driver.createInputTopic(
            Streams.TOPIC_DATA_DEMO_STREAMS,
            stringSerializer,
            Streams.SERDE_STREAM_JSON.serializer()
        );

        outputTopic = driver.createOutputTopic(
            TrendingArtistByGeneration.OUTPUT_TOPIC,
            stringDeserializer,
            TrendingArtistByGeneration.TRENDING_ARTIST_RESULT_SERDE.deserializer()
        );
    }

    @AfterEach
    void cleanup() {
        driver.close();
    }

    // helper arrays
    static String[] birthdays = {"1944", "1964", "1969", "1995", "2004", "2016"};
    static String[] generations = {
        TrendingArtistByGeneration.PRE_BOOMER,
        TrendingArtistByGeneration.BOOMER,
        TrendingArtistByGeneration.GENERATION_X,
        TrendingArtistByGeneration.MILLENNIAL,
        TrendingArtistByGeneration.GENERATION_Z,
        TrendingArtistByGeneration.GENERATION_ALPHA
    };

    @Test
    @DisplayName("most trending artist by generation")
    void testTrendingArtistByGeneration() {
        /**
         * We have 6 generation groups, we will have 18 customers, 3 per generation
         * For each generation group we will have 3 streams with 3 artists, 2 unique per generation, 1 repeat
         * - thus if we repeat there will always be a majority
         * 1. Pre-Boomers
         *  - Frank Sinatra
         *  - Elvis Presley
         * 2. Boomers
         *  - The Beatles
         *  - Rolling Stones
         * 3. Generation X
         * - Queen
         * - Michael Jackson
         * 4. Millennials
         * - Linkin Park
         * - Bowling for Soup
         * 5. Generation Z
         * - Playboi Carti
         * - Post Malone
         * 6. Generation Alpha
         * - Rich Amiri
         * - Kendrick Lamar
         */

        // 1. Pre-Boomers
        Artist frankSinatra = new Artist("fs", "Frank Sinatra", "Jazz");
        Artist elvisPresley = new Artist("ep", "Elvis Presley", "Rock");

        // 2. Boomers
        Artist theBeatles = new Artist("tb", "The Beatles", "Rock");
        Artist rollingStones = new Artist("rs", "Rolling Stones", "Rock");

        // 3. Generation X
        Artist queen = new Artist("qu", "Queen", "Rock");
        Artist michaelJackson = new Artist("mj", "Michael Jackson", "Pop");

        // 4. Millennials
        Artist linkinPark = new Artist("lp", "Linkin Park", "Rock");
        Artist bowlingForSoup = new Artist("bfs", "Bowling for Soup", "Punk Rock");

        // 5. Generation Z
        Artist playboiCarti = new Artist("pc", "Playboi Carti", "Hip Hop & Rap");
        Artist postMalone = new Artist("pm", "Post Malone", "Pop");

        // 6. Generation Alpha
        Artist richAmiri = new Artist("ra", "Rich Amiri", "Hip Hop & Rap");
        Artist kendrickLamar = new Artist("kl", "Kendrick Lamar", "Hip Hop & Rap");

        // insert all the artists
        artistInputTopic.pipeInput(frankSinatra.id(), frankSinatra);
        artistInputTopic.pipeInput(elvisPresley.id(), elvisPresley);
        artistInputTopic.pipeInput(theBeatles.id(), theBeatles);
        artistInputTopic.pipeInput(rollingStones.id(), rollingStones);
        artistInputTopic.pipeInput(queen.id(), queen);
        artistInputTopic.pipeInput(michaelJackson.id(), michaelJackson);
        artistInputTopic.pipeInput(linkinPark.id(), linkinPark);
        artistInputTopic.pipeInput(bowlingForSoup.id(), bowlingForSoup);
        artistInputTopic.pipeInput(playboiCarti.id(), playboiCarti);
        artistInputTopic.pipeInput(postMalone.id(), postMalone);
        artistInputTopic.pipeInput(richAmiri.id(), richAmiri);
        artistInputTopic.pipeInput(kendrickLamar.id(), kendrickLamar);

        // create all the customers and their addresses
        List<String> customerIds = new ArrayList<>();
        HashMap<String, List<Customer>> generationToCustomers = new HashMap<>();
        HashMap<String, Address> customerIdToAddressMap = new HashMap<>();
        for (int i = 0; i < 18; i++) {
            Customer pseudoCustomer = DataFaker.CUSTOMERS.generate();
            // create customer birthday based on this format '2011-12-03'
            String modifiedBirthday = birthdays[i % 6] + pseudoCustomer.birthdt().substring(4);
            Customer c = new Customer(
                pseudoCustomer.id(),
                pseudoCustomer.type(),
                pseudoCustomer.gender(),
                pseudoCustomer.fname(),
                pseudoCustomer.mname(),
                pseudoCustomer.lname(),
                pseudoCustomer.fullname(),
                pseudoCustomer.suffix(),
                pseudoCustomer.title(),
                modifiedBirthday, // index into birthdays list and grab a valid birthday
                pseudoCustomer.joindt()
            );
            customerIds.add(c.id());
            // add this customer to the add bucket
            List<Customer> list = generationToCustomers.computeIfAbsent(generations[i % 6], k -> new ArrayList<>());
            list.add(c);
            generationToCustomers.put(generations[i % 6], list);
            // add customer
            customerInputTopic.pipeInput(c.id(), c);

            // create addresses
            Address pseudoAddress = DataFaker.ADDRESSES.generateCustomerAddress(c.id());
            Address address = new Address(
                pseudoAddress.id(),
                pseudoAddress.customerid(),
                pseudoAddress.formatcode(),
                pseudoAddress.type(),
                pseudoAddress.line1(),
                pseudoAddress.line2(),
                pseudoAddress.citynm(),
                "Minnesota",
                pseudoAddress.zip5(),
                pseudoAddress.zip4(),
                pseudoAddress.countrycd(),
                pseudoAddress.latitude(),
                pseudoAddress.longitude()
            );
            customerIdToAddressMap.put(c.id(), address);
            addressInputTopic.pipeInput(address.id(), address);
        }

        // new time to create all the streams for each generation and customers
        Instant ts = Instant.now();

        // Pre-boomers
        var preboomers = generationToCustomers.get(TrendingArtistByGeneration.PRE_BOOMER);
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(preboomers.get(0).id(), frankSinatra.id()), ts);
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(preboomers.get(1).id(), elvisPresley.id()), ts);
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(preboomers.get(2).id(), elvisPresley.id()), ts);

        // Boomers
        var boomers = generationToCustomers.get(TrendingArtistByGeneration.BOOMER);
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(boomers.get(0).id(), theBeatles.id()), ts);
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(boomers.get(1).id(), rollingStones.id()), ts);
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(boomers.get(2).id(), rollingStones.id()), ts);

        // GenX
        var genX = generationToCustomers.get(TrendingArtistByGeneration.GENERATION_X);
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(genX.get(0).id(), queen.id()), ts);
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(genX.get(1).id(), michaelJackson.id()), ts);
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(genX.get(2).id(), michaelJackson.id()), ts);

        // millennials
        var millennials = generationToCustomers.get(TrendingArtistByGeneration.MILLENNIAL);
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(millennials.get(0).id(), linkinPark.id()), ts);
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(millennials.get(1).id(), bowlingForSoup.id()), ts);
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(millennials.get(2).id(), bowlingForSoup.id()), ts);

        // genZ
        var genZ = generationToCustomers.get(TrendingArtistByGeneration.GENERATION_Z);
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(genZ.get(0).id(), playboiCarti.id()), ts);
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(genZ.get(1).id(), postMalone.id()), ts);
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(genZ.get(2).id(), postMalone.id()), ts);

        // gen alpha
        var genAlpha = generationToCustomers.get(TrendingArtistByGeneration.GENERATION_ALPHA);
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(genAlpha.get(0).id(), richAmiri.id()), ts);
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(genAlpha.get(1).id(), kendrickLamar.id()), ts);
        streamInputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate(genAlpha.get(2).id(), kendrickLamar.id()), ts);


        // Verify Round #1 results
        var outputRecords = outputTopic.readRecordsToList();

        // we should have 18 records
        assertEquals(18, outputRecords.size());
        // for all 6 generation read results, should be in every 3 streams

        // pre-boomers
        var record = outputRecords.get(2);
        assertEquals(TrendingArtistByGeneration.PRE_BOOMER, record.getKey());
        assertEquals(elvisPresley.name(), record.getValue().getArtistName());
        assertEquals(elvisPresley.id(), record.getValue().getArtistId());
        assertEquals(2, record.getValue().getStreamCount());
        assertEquals("Minnesota", record.getValue().getState());

        // boomers
        record = outputRecords.get(5);
        assertEquals(TrendingArtistByGeneration.BOOMER, record.getKey());
        assertEquals(rollingStones.name(), record.getValue().getArtistName());
        assertEquals(rollingStones.id(), record.getValue().getArtistId());
        assertEquals(2, record.getValue().getStreamCount());
        assertEquals("Minnesota", record.getValue().getState());

        // genx
        record = outputRecords.get(8);
        assertEquals(TrendingArtistByGeneration.GENERATION_X, record.getKey());
        assertEquals(michaelJackson.name(), record.getValue().getArtistName());
        assertEquals(michaelJackson.id(), record.getValue().getArtistId());
        assertEquals(2, record.getValue().getStreamCount());
        assertEquals("Minnesota", record.getValue().getState());

        // millenials
        record = outputRecords.get(11);
        assertEquals(TrendingArtistByGeneration.MILLENNIAL, record.getKey());
        assertEquals(bowlingForSoup.name(), record.getValue().getArtistName());
        assertEquals(bowlingForSoup.id(), record.getValue().getArtistId());
        assertEquals(2, record.getValue().getStreamCount());
        assertEquals("Minnesota", record.getValue().getState());

        // gen z
        record = outputRecords.get(14);
        assertEquals(TrendingArtistByGeneration.GENERATION_Z, record.getKey());
        assertEquals(postMalone.name(), record.getValue().getArtistName());
        assertEquals(postMalone.id(), record.getValue().getArtistId());
        assertEquals(2, record.getValue().getStreamCount());
        assertEquals("Minnesota", record.getValue().getState());

        // gen alpha
        record = outputRecords.getLast();
        assertEquals(TrendingArtistByGeneration.GENERATION_ALPHA, record.getKey());
        assertEquals(kendrickLamar.name(), record.getValue().getArtistName());
        assertEquals(kendrickLamar.id(), record.getValue().getArtistId());
        assertEquals(2, record.getValue().getStreamCount());
        assertEquals("Minnesota", record.getValue().getState());
    }
}