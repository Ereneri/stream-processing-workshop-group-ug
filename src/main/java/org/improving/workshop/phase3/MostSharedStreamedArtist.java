package org.improving.workshop.phase3;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.msse.demo.mockdata.customer.profile.Customer;
import org.msse.demo.mockdata.music.artist.Artist;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.List;

import static org.improving.workshop.Streams.startStreams;

@Slf4j
public class MostSharedStreamedArtist {
    // MUST BE PREFIXED WITH "kafka-workshop-"
    public static final String OUTPUT_TOPIC = "kafka-workshop-ticket-response"; // TODO revisit

    public static final JsonSerde<MostSharedStreamedArtist.MostSharedStreamedArtistResult> MOST_PROFITABLE_VENUE_EVENT_JSON_SERDE = new JsonSerde<>(MostSharedStreamedArtistResult.class);

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

    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class MostSharedStreamedArtistResult {
        // key
        private String artistId;
        // values
        private Artist artist;
        private List<Customer> customerList;
    }

    // TODO make classes for merging joins
}
