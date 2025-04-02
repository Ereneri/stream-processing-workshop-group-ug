package org.improving.workshop.phase3;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.improving.workshop.samples.PurchaseEventTicket;
import org.msse.demo.mockdata.music.event.Event;
import org.msse.demo.mockdata.music.venue.Venue;
import org.springframework.kafka.support.serializer.JsonSerde;


import static org.apache.kafka.streams.state.Stores.persistentKeyValueStore;
import static org.improving.workshop.Streams.*;

@Slf4j
public class MostProfitableVenue {
    // MUST BE PREFIXED WITH "kafka-workshop-"
    public static final String OUTPUT_TOPIC = "kafka-workshop-ticket-response"; // TODO revisit

    public static final JsonSerde<MostProfitableVenue.MostProfitableVenueEvent> MOST_PROFITABLE_VENUE_EVENT_JSON_SERDE = new JsonSerde<>(MostProfitableVenueEvent.class);
    public static final JsonSerde<MostProfitableVenue.EventProfit> EVENT_PROFIT_JSON_SERDE = new JsonSerde<>(EventProfit.class);

    /**
     * Streams app launched via main => this should implement the Topology for 'finding the most profitable venue'
     */
    public static void main(final String[] args) {
        final StreamsBuilder builder = new StreamsBuilder();

        // configure the processing topology
        configureTopology(builder);
    }

    static void configureTopology(final StreamsBuilder builder) {
        // store the events in a table so they can be joined
        KTable<String, Event> eventsTable = builder
            .table(
                TOPIC_DATA_DEMO_EVENTS,
                Materialized
                    .<String, Event>as(persistentKeyValueStore("events"))
                    .withKeySerde(Serdes.String())
                    .withValueSerde(SERDE_EVENT_JSON)
            );

        // store the venues in a table so they can be joined later
        KTable<String, Venue> venueTable = builder
            .table(
                TOPIC_DATA_DEMO_VENUES,
                Materialized
                    .<String, Venue>as(persistentKeyValueStore("venues"))
                    .withKeySerde(Serdes.String())
                    .withValueSerde(SERDE_VENUE_JSON)
            );

        builder
            .stream(TOPIC_DATA_DEMO_EVENTS, Consumed.with(Serdes.String(), SERDE_TICKET_JSON))
            .peek((ticketId, ticketRequest) -> log.info("Ticket Requested: {}", ticketRequest))

            // need to rekey and group them by eventId
            .selectKey((ticketId, ticketRequest) -> ticketRequest.eventid())

            // join tickets to events, so we have the ticket and event information all at once
            .join(
                eventsTable,
                (eventId, ticket, event) -> new PurchaseEventTicket.EventTicket(ticket, event)
            )
            .peek((ticketId, ticket) -> log.info("Ticket Requested JOINED: {}", ticket))

            .groupByKey()

            // time to aggregate the tickets
            .aggregate(
                EventProfit::new,

                (eventId, eventTicket, eventProfit) -> {
                    if (!eventProfit.initialized) {
                        // have event profit save the eventid and venueid
                        eventProfit.initialize(eventId, eventTicket.getEvent().venueid());
                    }

                    // add the ticket revenue
                    eventProfit.addTicketPrice(eventTicket.getTicket().price());
                    return eventProfit;
                },

                // save the eventTicket into a ktable
                Materialized
                    .<String, EventProfit>as(persistentKeyValueStore("event-revenue-table"))
                    .withKeySerde(Serdes.String())
                    .withValueSerde(EVENT_PROFIT_JSON_SERDE)
            )

            // Rekey EventProfit by venueId, get that from the Event.getVenueId()
            .toStream()

            .map((eventId, eventProfit) -> KeyValue.pair(eventProfit.venueId, eventProfit))

            // Group by venue
            .groupByKey()

            // Aggregate the grouped EventTickets, to get aggregate of MostProfitableVenueEvent
            .aggregate(
                MostProfitableVenueEvent::new,

                (venueId, eventProfit, venueEvent) -> {
                    if (!venueEvent.initialized) {
                        // TODO bc fuck idk how we get the venue Name here?
                        venueEvent.initialize(venueId);
                    }

                    // Add the event revenue to the venue total
                    venueEvent.addEventRevenue(eventProfit.getProfit());
                    return venueEvent;
                },

                Materialized
                    .<String, MostProfitableVenueEvent>as(persistentKeyValueStore("venue-revenue-table"))
                    .withKeySerde(Serdes.String())
                    .withValueSerde(MOST_PROFITABLE_VENUE_EVENT_JSON_SERDE)
            )

            .toStream()

            .peek((venueId, mostProfitableVenue) -> log.info("Venue '{}' has most revenue of ${}", mostProfitableVenue.venueName, mostProfitableVenue.totalVenueRevenue))

            .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), MOST_PROFITABLE_VENUE_EVENT_JSON_SERDE));
    }


    @Data
    @Builder
    @AllArgsConstructor
    public static class MostProfitableVenueEvent {
        private boolean initialized;
        private String venueId;
        private String venueName;
        private double totalVenueRevenue;

        // constructor not init
        public MostProfitableVenueEvent() { initialized = false; }

        // init constructor
        public void initialize(String venueId) {
            this.initialized = true;
            this.venueId = venueId;
            this.venueName = venueId;
            this.totalVenueRevenue = 0.0;
        }

        public void addEventRevenue(double eventRevenue) {
            this.totalVenueRevenue += eventRevenue;
        }
    }

    @Data
    @AllArgsConstructor
    public static class EventProfit {
        private boolean initialized;
        private String eventId;
        private String venueId;
        private double profit;

        // constructor not init
        public EventProfit() { initialized = false; }

        // init constructor
        public void initialize(String eventId, String venueId) {
            this.initialized = true;
            this.eventId = eventId;
            this.venueId = venueId;
            this.profit = 0.0;
        }

        public void addTicketPrice(double ticketPrice) {
            this.profit += ticketPrice;
        }
    }
}
