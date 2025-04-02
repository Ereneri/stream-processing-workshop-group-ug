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
import org.msse.demo.mockdata.music.event.Event;
import org.msse.demo.mockdata.music.stream.Stream;
import org.msse.demo.mockdata.music.ticket.Ticket;
import org.msse.demo.mockdata.music.venue.Venue;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MostSharedStreamedArtistTest {
    private final static Serializer<String> stringSerializer = Serdes.String().serializer();
    private final static Deserializer<String> stringDerializer = Serdes.String().deserializer();

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

        // build the RemainingEventTickets topology (by reference)
        MostSharedStreamedArtist.configureTopology(streamsBuilder);

        // build the TopologyTestDriver
        driver = new TopologyTestDriver(streamsBuilder.build(), Streams.buildProperties());

        customerInputTopic = driver.createInputTopic(
            Streams.TOPIC_DATA_DEMO_EVENTS,
            stringSerializer,
            Streams.SERDE_CUSTOMER_JSON.serializer()
        );

        streamInputTopic = driver.createInputTopic(
            Streams.TOPIC_DATA_DEMO_TICKETS,
            stringSerializer,
            Streams.SERDE_STREAM_JSON.serializer()
        );

        artistInputTopic = driver.createInputTopic(
            Streams.TOPIC_DATA_DEMO_VENUES,
            stringSerializer,
            Streams.SERDE_ARTIST_JSON.serializer()
        );

        outputTopic = driver.createOutputTopic(
            MostSharedStreamedArtist.OUTPUT_TOPIC,
            stringDerializer,
            MostSharedStreamedArtist.MOST_PROFITABLE_VENUE_EVENT_JSON_SERDE.deserializer()
        );
    }

    @AfterEach
    void cleanup() {
        driver.close();
    }

    @Test
    @DisplayName("most profitable venue")
    void testMostProfitableVenue() {
        /**
         * Testing Idea:
         * create 2 Venues, v0, and v1 => v1 will be our more profitable
         * create 4 events, two per venue => used so we can attach tickets to a venue
         * create 2 fake tickets for per event, there for 8 total tickets
         *   - manually set the tickets s.t. event, e0, to be + 1 than e1 tickets
         *   - This can be done by looping 4 times and creating 4 tickets, but splitting into e0 and e1 but have e0 + 1 the ticket price
         */

        String venue0 = "venue-0";
        String venue1 = "venue-1";
        Integer maxCap = 1000;

        // Create venues
        String[] venueIds = {venue0, venue1};
        for (int i = 0; i < venueIds.length; i++) {
            String venueId = venueIds[i];
            venueInputTopic.pipeInput(venueId, new Venue(venueId, "venue-address", venueId + "-name", maxCap));
        }

        // Create 2 events for each venue
        for (int i = 0; i < 4; i++) {
            String eventId = "event-" + i;
            String artistId = "artist-" + i;
            // for i over 2 its for venue, therefore, 2,3 are for venue 1 therefore venue one will have the greater ticket prices
            String venueId = (i < 2) ? venue0 : venue1;
            eventInputTopic.pipeInput(DataFaker.EVENTS.generate(eventId, artistId, venueId, maxCap));
        }

        // Create tickets - 2 per event
        double[] prices = {10.0, 10.0, 20.0, 20.0};
        for (int batch = 0; batch < 2; batch++) {
            for (int i = 0; i < 4; i++) {
                String eventId = "event-" + i;
                double price = prices[i]; // this works because events 2-3 will be $20
                Ticket ticket = new Ticket(DataFaker.TICKETS.randomId(), "customer", eventId, price);
                ticketInputTopic.pipeInput(ticket);
            }
        }

        // reading out records
        var outputRecords = outputTopic.readRecordsToList();

        // expected records => 2 + 4 + 8 = 14
        assertEquals(12, outputRecords.size());

        // string will be the venueId
        TestRecord<String, MostProfitableVenue.MostProfitableVenueEvent> mostProfitableVenueEvent = outputRecords.getLast();

        assertEquals(venue1, mostProfitableVenueEvent.key());
        assertEquals(80.0, mostProfitableVenueEvent.value().getTotalVenueRevenue());
        assertEquals(venue1, mostProfitableVenueEvent.value().getVenueId());
        assertEquals("venue1-name", mostProfitableVenueEvent.value().getVenueName());

        // now we can add 4 more tickets so venue 0 will have a greater value
        eventInputTopic.pipeInput(DataFaker.EVENTS.generate("event-5", "artist-5", venue0, maxCap));
        for (int i = 0; i < 4; i++) {
            String eventId = "event-1" + i;
            Ticket ticket = new Ticket(DataFaker.TICKETS.randomId(), "customer", eventId, 20.0);
            ticketInputTopic.pipeInput(ticket);
        }

        var latestRecords = outputTopic.readRecordsToList();

        // should be the new event and then 4 new tickets
        assertEquals(17, outputRecords.size());

        // string will be the venueId
        TestRecord<String, MostProfitableVenue.MostProfitableVenueEvent> latestMostProfitableVenueEvent = outputRecords.getLast();

        assertEquals(venue1, latestMostProfitableVenueEvent.key());
        assertEquals(120.0, latestMostProfitableVenueEvent.value().getTotalVenueRevenue());
        assertEquals(venue1, latestMostProfitableVenueEvent.value().getVenueId());
        assertEquals("venue0-name", latestMostProfitableVenueEvent.value().getVenueName());
    }
}
