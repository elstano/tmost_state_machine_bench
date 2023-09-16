package com.sparkdan.tmost_state_machine_bench;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class RunTests {

    @Value("${num.peers}")
    public int numPeers;

    @Value("${test.duration.millis}")
    public long testDurationMillis;

    @Value("${test.concurrency}")
    public long testConcurrency;

    @Autowired
    protected SampleService sampleService;

    @Autowired
    protected RoomMediaSessionDao dao;

    private ExecutorService executor = Executors.newCachedThreadPool();

    @RequiredArgsConstructor
    static class Conf {
        private final String roomId;
        private final List<String> peerIDs = Collections.synchronizedList(new ArrayList<>());
        private final List<Future<String>> hangingOffers = Collections.synchronizedList(new ArrayList<>());
    }

    public void runTests() throws InterruptedException {
        log.info("Running tests");
        launchCycles().join();
        log.info("tests finished");
    }

    public CompletableFuture<Void> launchCycles() {
        long start = System.currentTimeMillis();
        List<CompletableFuture<Void>> cycles = new ArrayList<>();
        for(int i=0; i < testConcurrency; i++) {
            cycles.add(CompletableFuture.runAsync(() -> {
                while (System.currentTimeMillis() - start < testDurationMillis) {
                    mainOneCycle();
                }
            }, executor));
        }

        return CompletableFuture.allOf(cycles.toArray(new CompletableFuture[0]));
    }

    public void mainOneCycle() {
        try {
            String roomId = UUID.randomUUID().toString();
            dao.createRoom(roomId);
            log.info("Room created: {}", roomId);
            Conf conf = new Conf(roomId);

            String roomSession1 = UUID.randomUUID().toString();
            everyoneJoins(conf, roomSession1);

            String roomSession2 = UUID.randomUUID().toString();

            sampleService.offerReceived(conf.roomId, conf.peerIDs.get(0), roomSession2);
            sampleService.connected(conf.roomId, conf.peerIDs.get(0), roomSession2);

            //when everyone idles in first media session, they're going to receive non-legitimate offers
            CompletableFuture<Void> allDisconnected = everyoneIdlyDisconnectsAsync(conf, roomSession1);
            conf.peerIDs.clear();

            everyoneJoins(conf, roomSession2);
            allDisconnected.join();
        } catch (Exception e) {
            log.error("Exception in test cycle", e);
        }
    }

    public CompletableFuture<Void> everyoneIdlyDisconnectsAsync(Conf conf, String roomSessionId) {
        List<CompletableFuture<Void>> leaves = new ArrayList<>();
        List<String> localPeerIDs = new ArrayList<>(conf.peerIDs);
        for(String peerId: localPeerIDs) {
            leaves.add(CompletableFuture.runAsync(() -> {
                        sampleService.offerReceived(conf.roomId, peerId, roomSessionId);
                        sampleService.disconnected(conf.roomId, peerId, roomSessionId);
                    },
                    executor
            ));
        }
        return CompletableFuture.allOf(leaves.toArray(new CompletableFuture[0]));
    }

    public void everyoneJoins(Conf conf, String roomSessionId) {
        List<Future<String>> joins = new ArrayList<>();
        for (int i = 0; i < numPeers; i++) {
            joins.add(joinPeer(conf, roomSessionId));
        }
        waitFutures(joins);
        waitFutures(conf.hangingOffers);
        conf.hangingOffers.clear();
    }

    private <T> void waitFutures(List<Future<T>> futures) {
        for (Future<?> f : futures) {
            try {
                f.get();
            } catch (ExecutionException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public Future<String> joinPeer(Conf conf, String roomSessionId) {
        return executor.submit(() -> {
            String peerId = UUID.randomUUID().toString();
            sampleService.createSession(conf.roomId, peerId);
            sampleService.offerReceived(conf.roomId, peerId, roomSessionId);

            offerToAllAsync(conf, roomSessionId);

            sampleService.connected(conf.roomId, peerId, roomSessionId);
            conf.peerIDs.add(peerId);
            return peerId;
        });
    }

    public void offerToAllAsync(Conf conf, String roomSessionId) {
        List<String> presentPeers = new ArrayList<>(conf.peerIDs);
        for (String presentPeer : presentPeers) {
            conf.hangingOffers.add(executor.submit(() -> {
                sampleService.offerReceived(conf.roomId, presentPeer, roomSessionId);
                return "";
            }));
        }

    }


}
