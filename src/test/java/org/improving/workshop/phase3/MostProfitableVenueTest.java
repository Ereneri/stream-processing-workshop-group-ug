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
}
