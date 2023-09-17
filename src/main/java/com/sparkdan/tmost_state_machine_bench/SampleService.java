package com.sparkdan.tmost_state_machine_bench;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.Instant;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import static com.sparkdan.tmost_state_machine_bench.RoomMediaSessionState.ARCHIVED;
import static com.sparkdan.tmost_state_machine_bench.RoomMediaSessionState.CONNECTED;
import static com.sparkdan.tmost_state_machine_bench.RoomMediaSessionState.CREATED;
import static com.sparkdan.tmost_state_machine_bench.RoomMediaSessionState.DISCONNECTED;
import static com.sparkdan.tmost_state_machine_bench.RoomMediaSessionState.FIRST_OFFER_RECEIVED;

@Service
@Slf4j
public class SampleService {

    @Autowired
    MeterRegistry meterRegistry;

    @Autowired
    RoomMediaSessionDao roomMediaSessionDao;

    private Counter callsConnectedCounter;
    private Counter upsertRequestsCounter;
    private Timer upsertRequestsTimer;

    @PostConstruct
    protected void registerMeters() {
        callsConnectedCounter = meterRegistry.counter("sampleservice.callsConnected");
        upsertRequestsCounter = meterRegistry.counter("sampleservice.upsertRequestsCounter");

        upsertRequestsTimer = Timer.builder("sampleservice.upsertRequestTimer")
                .publishPercentiles(0.5, 0.9, 0.99)
                .publishPercentileHistogram()
                .distributionStatisticExpiry(Duration.ofSeconds(30))
                .register(meterRegistry);
    }

    private boolean isStaleRoomSession(String roomId, String roomSessionId) {
        String currentRoomSessionId = roomMediaSessionDao.getLatestRoomSessionId(roomId);
        if (currentRoomSessionId != null && roomSessionId != null
            && !StringUtils.equals(roomSessionId, currentRoomSessionId)) {
            //this one will almost always return empty set of rows because same check is performed
            // in RoomMediaSessionService outside the transaction before this one
            return !roomMediaSessionDao.isBrandNewRoomSession(roomId, roomSessionId);
        }

        return false;
    }

    public void createSession(String roomId, String peerId) {
        roomMediaSessionDao.created(roomId, peerId);
    }

    private boolean isRoomSessionLive(String roomId, String peerId, String roomSessionId) {
        String latestRoomSessionId = roomMediaSessionDao.getLatestRoomSessionId(roomId);
        if (latestRoomSessionId != null && roomSessionId != null
            && !latestRoomSessionId.equals(roomSessionId)) {
            //in case offer is for a session that has never been seen before,
            // assume that bridge session was changed for some reason.
            // allow connections only to the new sesson
            boolean brandNew = roomMediaSessionDao.isBrandNewRoomSession(roomId, roomSessionId);
            log.trace("Room session {} is different from the database version {}. brand new: {}",
                    roomSessionId,
                    latestRoomSessionId,
                    brandNew
            );
            return brandNew;
        }
        return true;
    }


    public boolean offerReceived(String roomId, String peerId, String roomSessionId) {
        long start = System.nanoTime();

        boolean accepted = isRoomSessionLive(roomId, peerId, roomSessionId);
        if (accepted) {
            disconnectOtherSessions(roomId, roomSessionId);
            accepted = persistFirstOfferEvent(roomId, peerId, roomSessionId);
        }

        log.trace("Acknowledged offer from room_id={}, peer_id={}, bridge_session_id={}. " +
                  "Accepted: {}",
                roomId, peerId, roomSessionId, accepted
        );

        upsertRequestsTimer.record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        upsertRequestsCounter.increment();

        return accepted;
    }

    protected void disconnectOtherSessions(String roomId, String roomSessionIdToKeep) {
        Collection<String> stalledRoomMediaSessions = roomMediaSessionDao.findOtherActiveRoomSessions(roomId,
                roomSessionIdToKeep);
        log.trace("Disconnecting stalled room sessions {}", stalledRoomMediaSessions);
        Collection<RoomMediaSessionDto> disconnectedMediaSessionIds =
                roomMediaSessionDao.disconnectAllInRoomAndRecreate(
                        stalledRoomMediaSessions,
                        roomSessionIdToKeep
                );
    }

    public boolean persistFirstOfferEvent(String roomId, String peerId, String roomSessionId) {
        Instant now = Instant.now();

        return upsertTransactionally(UpsertRMSRequest.builder()
                .roomId(roomId)
                .peerId(peerId)
                .roomSessionId(roomSessionId)
                .newState(FIRST_OFFER_RECEIVED)
                .updatedStates(List.of(CREATED, ARCHIVED, FIRST_OFFER_RECEIVED))
                .newCreatedAt(now)
                .newFirstOfferAt(now)
                .build());
    }

    public boolean connected(String roomId, String peerId, String roomSessionId) {
        long start = System.nanoTime();
        Instant now = Instant.now();
        boolean result = upsertTransactionally(
                UpsertRMSRequest.builder()
                        .roomId(roomId)
                        .roomSessionId(roomSessionId)
                        .peerId(peerId)
                        .updatedStates(List.of(ARCHIVED, CREATED, FIRST_OFFER_RECEIVED, CONNECTED))
                        .newState(CONNECTED)
                        .newCreatedAt(now)
                        .newFirstOfferAt(now)
                        .newConnectedAt(now)
                        .build()
        );

        upsertRequestsTimer.record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        upsertRequestsCounter.increment();
        callsConnectedCounter.increment();

        return result;
    }

    public boolean disconnected(String roomId, String peerId, String roomSessionId) {
        long start = System.nanoTime();

        Instant now = Instant.now();
        boolean result = upsertTransactionally(
                UpsertRMSRequest.builder()
                        .roomId(roomId)
                        .roomSessionId(roomSessionId)
                        .peerId(peerId)
                        .updatedStates(List.of(ARCHIVED, CREATED, FIRST_OFFER_RECEIVED, CONNECTED, DISCONNECTED))
                        .newState(DISCONNECTED)
                        .newCreatedAt(now)
                        .newFirstOfferAt(now)
                        .newConnectedAt(now)
                        .newDisconnectedAt(now)
                        .build()
        );

        upsertRequestsTimer.record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        upsertRequestsCounter.increment();

        return result;
    }

    private boolean upsertTransactionally(UpsertRMSRequest upsertRMSRequest) {
        String roomId = upsertRMSRequest.getRoomId();
        String roomSessionId = upsertRMSRequest.getRoomSessionId();

        if (isStaleRoomSession(roomId, roomSessionId)) {
            return false;
        }

        int updated;
        updated = roomMediaSessionDao.updateByRoomSessionId(upsertRMSRequest);
        if (updated == 0) {
            updated = roomMediaSessionDao.updateCreatedRoomSession(upsertRMSRequest);
            if (updated == 0) {
                updated = roomMediaSessionDao.insertOrDoNothing(upsertRMSRequest);
                if (updated == 0) {
                    updated = roomMediaSessionDao.updateByRoomSessionId(upsertRMSRequest);
                }
            }
        }

        if (updated == 0) {
            throw new RuntimeException(String.format(
                    "Failed to update info on room media sessions. 0 rows updated. request: %s",
                    upsertRMSRequest
            ));
        }

        //has to be done last not to hold a lock on a neighbour transaction for the whole duration of the
        // transaction
        roomMediaSessionDao.setLatestRoomSessionId(roomId, roomSessionId);

        return true;
    }

}
