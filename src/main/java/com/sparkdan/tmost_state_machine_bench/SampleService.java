package com.sparkdan.tmost_state_machine_bench;

import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class SampleService {

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
