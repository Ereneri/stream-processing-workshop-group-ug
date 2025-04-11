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
import org.msse.demo.mockdata.music.event.Event;
import org.msse.demo.mockdata.music.ticket.Ticket;
import org.msse.demo.mockdata.music.venue.Venue;

import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;


public class MostProfitableVenueTest {
    private final static Serializer<String> stringSerializer = Serdes.String().serializer();
    private final static Deserializer<String> stringDerializer = Serdes.String().deserializer();

    private TopologyTestDriver driver;

    // inputs
    private TestInputTopic<String, Event> eventInputTopic;
    private TestInputTopic<String, Ticket> ticketInputTopic;
    private TestInputTopic<String, Venue> venueInputTopic;

    // outputs
    private TestOutputTopic<String, MostProfitableVenue.MostProfitableVenueEvent> outputTopic;

    @BeforeEach
    void setUp() {
        // instantiate new builder
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // build the RemainingEventTickets topology (by reference)
        MostProfitableVenue.configureTopology(streamsBuilder);

        // build the TopologyTestDriver
        driver = new TopologyTestDriver(streamsBuilder.build(), Streams.buildProperties());

        eventInputTopic = driver.createInputTopic(
            Streams.TOPIC_DATA_DEMO_EVENTS,
            stringSerializer,
            Streams.SERDE_EVENT_JSON.serializer()
        );

        ticketInputTopic = driver.createInputTopic(
            Streams.TOPIC_DATA_DEMO_TICKETS,
            stringSerializer,
            Streams.SERDE_TICKET_JSON.serializer()
        );

        venueInputTopic = driver.createInputTopic(
            Streams.TOPIC_DATA_DEMO_VENUES,
            stringSerializer,
            Streams.SERDE_VENUE_JSON.serializer()
        );

        outputTopic = driver.createOutputTopic(
            MostProfitableVenue.OUTPUT_TOPIC,
            stringDerializer,
            MostProfitableVenue.MOST_PROFITABLE_VENUE_EVENT_JSON_SERDE.deserializer()
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

        // Create 2 venues
        venueInputTopic.pipeInput(venue0, new Venue(venue0, "venue-address", "venue-0-name", maxCap));
        venueInputTopic.pipeInput(venue1, new Venue(venue1, "venue-address", "venue-1-name", maxCap));

        // Create 2 events for each venue => therefore 4 events total
        for (int i = 0; i < 4; i++) {
            String eventId = "event-" + i;
            String artistId = "artist-" + i;
            // for i over 2 its for venue, therefore, 2,3 are for venue 1 therefore venue one will have the greater ticket prices
            String venueId = (i < 2) ? venue0 : venue1;
            eventInputTopic.pipeInput(eventId, DataFaker.EVENTS.generate(eventId, artistId, venueId, maxCap));
        }

        // Create tickets - 2 per event for => 8 tickets total
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

        // expect 8 ticket records
        assertEquals(8, outputRecords.size());

        // string will be the venueId
        TestRecord<String, MostProfitableVenue.MostProfitableVenueEvent> mostProfitableVenueEvent = outputRecords.getLast();

        assertEquals(venue1, mostProfitableVenueEvent.key());
        assertEquals(80, mostProfitableVenueEvent.value().getCurrentMaxTotalVenueRevenue());
        assertEquals(venue1, mostProfitableVenueEvent.value().getCurrentMaxVenueId());
        assertEquals(80.0, mostProfitableVenueEvent.value().getCurrentMaxTotalVenueRevenue());
        assertEquals("venue-1-name", mostProfitableVenueEvent.value().getCurrentMaxVenueName());

        // now we can add 4 more tickets so venue 0 will have a greater value => 4 * 20 => $80 for venue0 + $40 = 120
        for (int i = 0; i < 4; i++) {
            String eventId = "event-1" + i;
            eventInputTopic.pipeInput(eventId, DataFaker.EVENTS.generate(eventId, "artist", venue0, maxCap));
            Ticket ticket = new Ticket(DataFaker.TICKETS.randomId(), "customer", eventId, 20.0);
            ticketInputTopic.pipeInput(ticket);
        }

        // Process all events
        driver.advanceWallClockTime(java.time.Duration.ofMillis(100));

        // Read the latest records
        var latestRecords = outputTopic.readRecordsToList();

        // should be the 4 new tickets
        assertEquals(4, latestRecords.size());

        // string will be the venueId
        TestRecord<String, MostProfitableVenue.MostProfitableVenueEvent> latestMostProfitableVenueEvent = latestRecords.getLast();

        assertEquals(venue0, latestMostProfitableVenueEvent.key());
        assertEquals(120.0, latestMostProfitableVenueEvent.value().getCurrentMaxTotalVenueRevenue());
        assertEquals(venue0, latestMostProfitableVenueEvent.value().getCurrentMaxVenueId());
        assertEquals("venue-0-name", latestMostProfitableVenueEvent.value().getCurrentMaxVenueName());

        // Check venues map
        var venueRevenueMap = latestMostProfitableVenueEvent.value().getVenueRevenueMap();
        assertNotNull(venueRevenueMap);
        assertEquals(2, venueRevenueMap.size());
        assertEquals(120.0, venueRevenueMap.get(venue0));
        assertEquals(80.0, venueRevenueMap.get(venue1));
    }

    @Test
    @DisplayName("dueling most profitable venue")
    void testDeulingMostProfitableVenue() {
        /**
         * 3 venues will duel it out for place
         * Venue 1 and Venue 2 will have similar results as prior test
         * Venue 3 will blow them out of the water then venue 1 will return to be winner
         */

        String venue0 = "venue-0";
        String venue1 = "venue-1";
        String venue2 = "venue-2";
        Integer maxCap = 1000;

        // Create 2 venues
        venueInputTopic.pipeInput(venue0, new Venue(venue0, "venue-address", "venue-0-name", maxCap));
        venueInputTopic.pipeInput(venue1, new Venue(venue1, "venue-address", "venue-1-name", maxCap));
        venueInputTopic.pipeInput(venue2, new Venue(venue2, "venue-address", "venue-2-name", maxCap));

        // Create 3 events for each venue => therefore 9 events total
        eventInputTopic.pipeInput("event-0", DataFaker.EVENTS.generate("event-0", "artist-0", venue0, maxCap));
        eventInputTopic.pipeInput("event-1", DataFaker.EVENTS.generate("event-1", "artist-1", venue0, maxCap));
        eventInputTopic.pipeInput("event-2", DataFaker.EVENTS.generate("event-2", "artist-2", venue0, maxCap));

        eventInputTopic.pipeInput("event-3", DataFaker.EVENTS.generate("event-3", "artist-3", venue1, maxCap));
        eventInputTopic.pipeInput("event-4", DataFaker.EVENTS.generate("event-4", "artist-4", venue1, maxCap));
        eventInputTopic.pipeInput("event-5", DataFaker.EVENTS.generate("event-5", "artist-5", venue1, maxCap));

        eventInputTopic.pipeInput("event-6", DataFaker.EVENTS.generate("event-6", "artist-6", venue2, maxCap));
        eventInputTopic.pipeInput("event-7", DataFaker.EVENTS.generate("event-7", "artist-7", venue2, maxCap));
        eventInputTopic.pipeInput("event-8", DataFaker.EVENTS.generate("event-8", "artist-8", venue2, maxCap));

        // create 2 tickets for each venue => 9 * 2 => 18

        // venue 0 => 15 * 6 => 90
        ticketInputTopic.pipeInput(new Ticket(DataFaker.TICKETS.randomId(), "customer", "event-0", 15.0));
        ticketInputTopic.pipeInput(new Ticket(DataFaker.TICKETS.randomId(), "customer", "event-1", 15.0));
        ticketInputTopic.pipeInput(new Ticket(DataFaker.TICKETS.randomId(), "customer", "event-2", 15.0));

        ticketInputTopic.pipeInput(UUID.randomUUID().toString(), new Ticket(DataFaker.TICKETS.randomId(), "customer", "event-0", 15.0));
        ticketInputTopic.pipeInput(UUID.randomUUID().toString(), new Ticket(DataFaker.TICKETS.randomId(), "customer", "event-1", 15.0));
        ticketInputTopic.pipeInput(UUID.randomUUID().toString(), new Ticket(DataFaker.TICKETS.randomId(), "customer", "event-2", 15.0));

        // venue 1 => $77
        ticketInputTopic.pipeInput(new Ticket(DataFaker.TICKETS.randomId(), "customer", "event-3", 15.6));
        ticketInputTopic.pipeInput(new Ticket(DataFaker.TICKETS.randomId(), "customer", "event-4", 15.6));
        ticketInputTopic.pipeInput(new Ticket(DataFaker.TICKETS.randomId(), "customer", "event-5", 15.6));

        ticketInputTopic.pipeInput(new Ticket(DataFaker.TICKETS.randomId(), "customer", "event-3", 15.6));
        ticketInputTopic.pipeInput(new Ticket(DataFaker.TICKETS.randomId(), "customer", "event-4", 15.6));
        ticketInputTopic.pipeInput(new Ticket(DataFaker.TICKETS.randomId(), "customer", "event-5", 12.0));
        // At this point venue 0 and venue 1 are tied

        // venue 2
        ticketInputTopic.pipeInput(new Ticket(DataFaker.TICKETS.randomId(), "customer", "event-6", 15.4));
        ticketInputTopic.pipeInput(new Ticket(DataFaker.TICKETS.randomId(), "customer", "event-7", 15.4));
        ticketInputTopic.pipeInput(new Ticket(DataFaker.TICKETS.randomId(), "customer", "event-8", 15.4));

        ticketInputTopic.pipeInput(new Ticket(DataFaker.TICKETS.randomId(), "customer", "event-6", 15.4));
        ticketInputTopic.pipeInput(new Ticket(DataFaker.TICKETS.randomId(), "customer", "event-7", 15.4));
        ticketInputTopic.pipeInput(new Ticket(DataFaker.TICKETS.randomId(), "customer", "event-8", 13.0));

        // reading out records
        var outputRecords = outputTopic.readRecordsToList();

        // expect 8 ticket records
        assertEquals(18, outputRecords.size());

        // string will be the venueId
        TestRecord<String, MostProfitableVenue.MostProfitableVenueEvent> latestMostProfitableVenueEvent = outputRecords.getLast();

        assertEquals(venue0, latestMostProfitableVenueEvent.key());
        assertEquals(90.0, latestMostProfitableVenueEvent.value().getCurrentMaxTotalVenueRevenue());
        assertEquals(venue0, latestMostProfitableVenueEvent.value().getCurrentMaxVenueId());
        assertEquals("venue-0-name", latestMostProfitableVenueEvent.value().getCurrentMaxVenueName());

        // new venue 2 will get a few more sales
        ticketInputTopic.pipeInput(new Ticket(DataFaker.TICKETS.randomId(), "customer", "event-6", 15.4));
        ticketInputTopic.pipeInput(new Ticket(DataFaker.TICKETS.randomId(), "customer", "event-6", 15.4));

        ticketInputTopic.pipeInput(new Ticket(DataFaker.TICKETS.randomId(), "customer", "event-7", 15.4));
        ticketInputTopic.pipeInput(new Ticket(DataFaker.TICKETS.randomId(), "customer", "event-7", 15.4));

        ticketInputTopic.pipeInput(new Ticket(DataFaker.TICKETS.randomId(), "customer", "event-8", 15.4));
        ticketInputTopic.pipeInput(new Ticket(DataFaker.TICKETS.randomId(), "customer", "event-8", 13.0));

        outputRecords = outputTopic.readRecordsToList();

        // should be the 6 new tickets
        assertEquals(6, outputRecords.size());

        // string will be the venueId
        latestMostProfitableVenueEvent = outputRecords.getLast();

        assertEquals(venue2, latestMostProfitableVenueEvent.key());
        assertEquals(180.0, latestMostProfitableVenueEvent.value().getCurrentMaxTotalVenueRevenue());
        assertEquals(venue2, latestMostProfitableVenueEvent.value().getCurrentMaxVenueId());
        assertEquals("venue-2-name", latestMostProfitableVenueEvent.value().getCurrentMaxVenueName());

        // new venue 0 will get some more, same with venue 1, they will both beat venue 3 but venue will have slightly more

        // new event for venue 1
        eventInputTopic.pipeInput("event-9", DataFaker.EVENTS.generate("event-3", "artist-3", venue1, maxCap));
        eventInputTopic.pipeInput("event-10", DataFaker.EVENTS.generate("event-4", "artist-4", venue1, maxCap));

        // venue 0 => $200
        ticketInputTopic.pipeInput(new Ticket(DataFaker.TICKETS.randomId(), "customer", "event-0", 55.));
        ticketInputTopic.pipeInput(new Ticket(DataFaker.TICKETS.randomId(), "customer", "event-0", 55.0));

        // venue 1 => $222
        ticketInputTopic.pipeInput(new Ticket(DataFaker.TICKETS.randomId(), "customer", "event-9", 33.0));
        ticketInputTopic.pipeInput(new Ticket(DataFaker.TICKETS.randomId(), "customer", "event-9", 33.0));

        ticketInputTopic.pipeInput(new Ticket(DataFaker.TICKETS.randomId(), "customer", "event-10", 33.0));
        ticketInputTopic.pipeInput(new Ticket(DataFaker.TICKETS.randomId(), "customer", "event-10", 33.0));

        outputRecords = outputTopic.readRecordsToList();

        // should be the 6 new tickets
        assertEquals(6, outputRecords.size());

        // string will be the venueId
        latestMostProfitableVenueEvent = outputRecords.getLast();

        assertEquals(venue1, latestMostProfitableVenueEvent.key());
        assertEquals(222.0, latestMostProfitableVenueEvent.value().getCurrentMaxTotalVenueRevenue());
        assertEquals(venue1, latestMostProfitableVenueEvent.value().getCurrentMaxVenueId());
        assertEquals("venue-1-name", latestMostProfitableVenueEvent.value().getCurrentMaxVenueName());
    }
}