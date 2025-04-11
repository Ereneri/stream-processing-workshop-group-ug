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
import org.msse.demo.mockdata.music.ticket.Ticket;
import org.msse.demo.mockdata.music.venue.Venue;
import org.springframework.kafka.support.serializer.JsonSerde;


import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.kafka.streams.state.Stores.persistentKeyValueStore;
import static org.improving.workshop.Streams.*;

@Slf4j
public class MostProfitableVenue {
    // MUST BE PREFIXED WITH "kafka-workshop-"
    public static final String OUTPUT_TOPIC = "kafka-workshop-most-profitable-venue"; // TODO revisit

    public static final JsonSerde<MostProfitableVenue.MostProfitableVenueEvent> MOST_PROFITABLE_VENUE_EVENT_JSON_SERDE = new JsonSerde<>(MostProfitableVenueEvent.class);
    public static final JsonSerde<MostProfitableVenue.EventProfit> EVENT_PROFIT_JSON_SERDE = new JsonSerde<>(EventProfit.class);

    /**
     * Streams app launched via main => this should implement the Topology for 'finding the most profitable venue'
     */
    public static void main(final String[] args) {
        final StreamsBuilder builder = new StreamsBuilder();

        // configure the processing topology
        configureTopology(builder);

        // fire up engine
        startStreams(builder);
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
            .stream(TOPIC_DATA_DEMO_TICKETS, Consumed.with(Serdes.String(), SERDE_TICKET_JSON))
            .peek((ticketId, ticketRequest) -> log.info("Ticket Requested: {}", ticketRequest))

            // need to rekey and group them by eventId
            .selectKey((ticketId, ticketRequest) -> ticketRequest.eventid())

            // join tickets to events, so we have the ticket and event information all at once
            .join(
                eventsTable,
                (eventId, ticket, event) -> new PurchaseEventTicket.EventTicket(ticket, event)
            )
            .peek((ticketId, ticket) -> log.info("Ticket {} Requested JOINED: {}", ticketId, ticket))

            // group by eventId => already keyed by groupId
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

            // rekey venueId
            .selectKey((eventId, eventProfit) -> "global")

            // Group by venueId from our rekey above
            .groupByKey()

            // Aggregate the grouped EventTickets, to get aggregate of MostProfitableVenueEvent
            .aggregate(
                MostProfitableVenueEvent::new,

                (globalKey, eventProfit, venueEvent) -> {
                    // Add the event revenue to the venue total
                    venueEvent.addEventRevenue(eventProfit.getProfit(), eventProfit.venueId);
                    log.info("Venue '{}' has current profit of '${}'", eventProfit.venueId, venueEvent.getVenueRevenueMap().get(eventProfit.venueId));
                    return venueEvent;
                },

                Materialized
                    .<String, MostProfitableVenueEvent>as(persistentKeyValueStore("venue-revenue-table"))
                    .withKeySerde(Serdes.String())
                    .withValueSerde(MOST_PROFITABLE_VENUE_EVENT_JSON_SERDE)
            )

            .toStream()

            .selectKey((globalKey, venueEvent) -> venueEvent.getCurrentMaxVenueId())

            // join out MostProfitableVenueEvent to a Venue => already rekeyed to be venueId
            .peek((venueId, mostProfitableVenueEvent) -> log.info("Venue {} has {}", venueId, mostProfitableVenueEvent))
            .join(
                venueTable,
                (mostProfitableVenueEvent, venue) -> {
                    // Update venue name from actual venue object
                    mostProfitableVenueEvent.setCurrentMaxVenueName(venue.name());
                    return mostProfitableVenueEvent;
                }
            )
            .peek((venueId, eventVenue) -> log.info("Venue {} has JOINED {}", venueId, eventVenue))

            // rekey our venueId to be the max
            .selectKey((venueId, mostProfitableVenue) -> mostProfitableVenue.getCurrentMaxVenueId())

            .peek((venueId, mostProfitableVenue) -> log.info("Venue {} has most revenue of ${}", venueId, mostProfitableVenue.getCurrentMaxTotalVenueRevenue()))

            .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), MOST_PROFITABLE_VENUE_EVENT_JSON_SERDE));
    }


    @Data
    @Builder
    @AllArgsConstructor
    // TODO update so we save a map of the venues and a current max
    public static class MostProfitableVenueEvent {
        private String currentMaxVenueId;
        private String currentMaxVenueName;
        private double currentMaxTotalVenueRevenue;
        // store a map from venueId to its revenue amount
        private Map<String, Double> venueRevenueMap;

        // constructor not init
        public MostProfitableVenueEvent() {
            this.currentMaxVenueId = null;
            this.currentMaxVenueName = null;
            this.currentMaxTotalVenueRevenue = 0.0;
            this.venueRevenueMap = new HashMap<>();
        }

        public void addEventRevenue(double eventRevenue, String venueId) {
            // check if this venueId exists, else put it into map
            if (!venueRevenueMap.containsKey(venueId)) {
                venueRevenueMap.put(venueId, 0.0);
            }
            // index into map, update venue revenue map
            BigDecimal roundedDouble = new BigDecimal((venueRevenueMap.get(venueId) + eventRevenue)).setScale(2, RoundingMode.HALF_UP);
            venueRevenueMap.replace(venueId, roundedDouble.doubleValue());

            // check if it's greater than current max then update accordingly
            if (this.currentMaxTotalVenueRevenue < venueRevenueMap.get(venueId)) {
                this.currentMaxTotalVenueRevenue = venueRevenueMap.get(venueId);
                this.currentMaxVenueId = venueId;
            }
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
            this.profit = ticketPrice;
        }
    }
}
