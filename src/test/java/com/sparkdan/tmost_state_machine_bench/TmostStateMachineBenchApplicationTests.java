package com.sparkdan.tmost_state_machine_bench;

import java.util.UUID;

import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static com.sparkdan.tmost_state_machine_bench.RoomMediaSessionDao.UNKNOWN_ROOM_SESSION_ID;
import static com.sparkdan.tmost_state_machine_bench.RoomMediaSessionState.CREATED;
import static org.junit.jupiter.api.Assertions.*;


@SpringBootTest
class TmostStateMachineBenchApplicationTests {

	@Autowired
	protected SampleService sampleService;

	@Autowired
	protected RoomMediaSessionDao dao;

	@Test
	void persistWorks() {
		String roomId = UUID.randomUUID().toString();
		String peerId = UUID.randomUUID().toString();
		String roomSessionId = UUID.randomUUID().toString();
		dao.createRoom(roomId);
		sampleService.createSession(roomId, peerId);

		RoomMediaSessionDto dto = dao.findByPrimaryKey(UNKNOWN_ROOM_SESSION_ID, peerId);
		assertNotNull(dto);
		assertEquals(CREATED, dto.getState());
	}



}
