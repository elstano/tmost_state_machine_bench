package com.sparkdan.tmost_state_machine_bench;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class RunTests {
    public void runTests() throws InterruptedException {
        log.info("Running tests");
        Thread.sleep(10000L);
    }
}
