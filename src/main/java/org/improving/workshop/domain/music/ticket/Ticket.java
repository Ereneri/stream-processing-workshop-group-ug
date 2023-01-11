package org.improving.workshop.domain.music.ticket;

import java.io.Serializable;

public record Ticket(
        String id,
        String customerid,
        String eventid,
        Double price) implements Serializable {}
