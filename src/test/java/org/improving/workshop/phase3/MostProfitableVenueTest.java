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

import static org.junit.jupiter.api.Assertions.assertEquals;


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
        assertEquals(120.0, mostProfitableVenueEvent.value().getCurrentMaxTotalVenueRevenue());
        assertEquals(venue1, mostProfitableVenueEvent.value().getCurrentMaxVenueId());
        assertEquals("venue-1-name", mostProfitableVenueEvent.value().getCurrentMaxVenueName());

        // now we can add 4 more tickets so venue 0 will have a greater value => 4 * 20 => $80 for venue0 + $40 = 120
        for (int i = 0; i < 4; i++) {
            String eventId = "event-1" + i;
            eventInputTopic.pipeInput(eventId, DataFaker.EVENTS.generate(eventId, "artist", venue0, maxCap));
            Ticket ticket = new Ticket(DataFaker.TICKETS.randomId(), "customer", eventId, 20.0);
            ticketInputTopic.pipeInput(ticket);
        }

        var latestRecords = outputTopic.readRecordsToList();

        // should be the 4 new tickets
        assertEquals(4, latestRecords.size());

        // string will be the venueId
        TestRecord<String, MostProfitableVenue.MostProfitableVenueEvent> latestMostProfitableVenueEvent = latestRecords.getLast();

        assertEquals(venue0, latestMostProfitableVenueEvent.key());
        assertEquals(140.0, latestMostProfitableVenueEvent.value().getCurrentMaxTotalVenueRevenue());
        assertEquals(venue0, latestMostProfitableVenueEvent.value().getCurrentMaxVenueId());
        assertEquals("venue-0-name", latestMostProfitableVenueEvent.value().getCurrentMaxVenueName());
    }

    @Test
    @DisplayName("test venue revenue update as new tickets are added")
    void testVenueRevenueUpdate() {
        /**
         * Testing Idea:
         * Create 3 venues and track how the most profitable venue changes as tickets are added:
         * - Phase 1: Venue0 starts with highest revenue
         * - Phase 2: Venue1 takes lead when more tickets are added
         * - Phase 3: Venue2 becomes most profitable with premium tickets
         * - Phase 4: Venue0 reclaims top spot with a surge of high-price tickets
         */

        String venue0 = "venue-0";
        String venue1 = "venue-1";
        String venue2 = "venue-2";
        Integer maxCap = 1000;

        // Create 3 venues
        venueInputTopic.pipeInput(venue0, new Venue(venue0, "venue-address-0", "venue-0-name", maxCap));
        venueInputTopic.pipeInput(venue1, new Venue(venue1, "venue-address-1", "venue-1-name", maxCap));
        venueInputTopic.pipeInput(venue2, new Venue(venue2, "venue-address-2", "venue-2-name", maxCap));

        // Create 2 events for each venue (6 events total)
        for (int i = 0; i < 6; i++) {
            String eventId = "event-" + i;
            String artistId = "artist-" + i;
            String venueId = (i < 2) ? venue0 : (i < 4) ? venue1 : venue2; // First 2 for venue0, next 2 for venue1, last 2 for venue2
            eventInputTopic.pipeInput(eventId, DataFaker.EVENTS.generate(eventId, artistId, venueId, maxCap));
        }

        // Phase 1: Venue0 starts with highest revenue (4 tickets at $50 each = $200)
        for (int i = 0; i < 4; i++) {
            Ticket ticket = new Ticket(DataFaker.TICKETS.randomId(), "customer", "event-0", 50.0);
            ticketInputTopic.pipeInput(ticket);
        }
        
        // Add some tickets to other venues too
        // Venue1: 2 tickets at $40 each = $80
        for (int i = 0; i < 2; i++) {
            Ticket ticket = new Ticket(DataFaker.TICKETS.randomId(), "customer", "event-2", 40.0);
            ticketInputTopic.pipeInput(ticket);
        }
        
        // Venue2: 1 ticket at $30 = $30
        Ticket ticket = new Ticket(DataFaker.TICKETS.randomId(), "customer", "event-4", 30.0);
        ticketInputTopic.pipeInput(ticket);

        // Verify Phase 1 results
        var phase1Records = outputTopic.readRecordsToList();
        TestRecord<String, MostProfitableVenue.MostProfitableVenueEvent> phase1Result = phase1Records.getLast();

        assertEquals(venue0, phase1Result.key());
        assertEquals(200.0, phase1Result.value().getCurrentMaxTotalVenueRevenue());
        assertEquals(venue0, phase1Result.value().getCurrentMaxVenueId());
        assertEquals("venue-0-name", phase1Result.value().getCurrentMaxVenueName());

        // Phase 2: Venue1 takes the lead with 5 more tickets at $60 each = $300 + $80 = $380
        for (int i = 0; i < 5; i++) {
            Ticket ticket2 = new Ticket(DataFaker.TICKETS.randomId(), "customer", "event-3", 60.0);
            ticketInputTopic.pipeInput(ticket2);
        }

        // Verify Phase 2 results
        var phase2Records = outputTopic.readRecordsToList();
        TestRecord<String, MostProfitableVenue.MostProfitableVenueEvent> phase2Result = phase2Records.getLast();

        assertEquals(venue1, phase2Result.key());
        assertEquals(380.0, phase2Result.value().getCurrentMaxTotalVenueRevenue());
        assertEquals(venue1, phase2Result.value().getCurrentMaxVenueId());
        assertEquals("venue-1-name", phase2Result.value().getCurrentMaxVenueName());

        // Phase 3: Venue2 becomes most profitable with 3 premium tickets at $150 each = $450
        for (int i = 0; i < 3; i++) {
            Ticket ticket3 = new Ticket(DataFaker.TICKETS.randomId(), "customer", "event-5", 150.0);
            ticketInputTopic.pipeInput(ticket3);
        }

        // Verify Phase 3 results
        var phase3Records = outputTopic.readRecordsToList();
        TestRecord<String, MostProfitableVenue.MostProfitableVenueEvent> phase3Result = phase3Records.getLast();

        assertEquals(venue2, phase3Result.key());
        assertEquals(480.0, phase3Result.value().getCurrentMaxTotalVenueRevenue()); // $30 + $450 = $480
        assertEquals(venue2, phase3Result.value().getCurrentMaxVenueId());
        assertEquals("venue-2-name", phase3Result.value().getCurrentMaxVenueName());

        // Phase 4: Venue0 reclaims top spot with 6 tickets at $50 each = $300 + $200 = $500
        for (int i = 0; i < 6; i++) {
            Ticket ticket4 = new Ticket(DataFaker.TICKETS.randomId(), "customer", "event-1", 50.0);
            ticketInputTopic.pipeInput(ticket4);
        }

        // Verify Phase 4 results
        var phase4Records = outputTopic.readRecordsToList();
        TestRecord<String, MostProfitableVenue.MostProfitableVenueEvent> phase4Result = phase4Records.getLast();

        assertEquals(venue0, phase4Result.key());
        assertEquals(500.0, phase4Result.value().getCurrentMaxTotalVenueRevenue());
        assertEquals(venue0, phase4Result.value().getCurrentMaxVenueId());
        assertEquals("venue-0-name", phase4Result.value().getCurrentMaxVenueName());
        
        // Verify the revenue map contains correct values for all venues
        assertEquals(500.0, phase4Result.value().getVenueRevenueMap().get(venue0));
        assertEquals(380.0, phase4Result.value().getVenueRevenueMap().get(venue1));
        assertEquals(480.0, phase4Result.value().getVenueRevenueMap().get(venue2));
    }
}
