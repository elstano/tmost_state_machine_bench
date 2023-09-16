package com.sparkdan.tmost_state_machine_bench;

import java.util.List;

import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.Instant;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import static com.sparkdan.tmost_state_machine_bench.RoomMediaSessionState.*;

@Service
public class SampleService {

    @Autowired
    RoomMediaSessionDao roomMediaSessionDao;

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

    public boolean offerReceived(String roomId, String peerId, String roomSessionId) {
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
        Instant now = Instant.now();
        return upsertTransactionally(
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
    }

    public boolean disconnected(String roomId, String peerId, String roomSessionId) {
        Instant now = Instant.now();
        return upsertTransactionally(
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
