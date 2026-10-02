package com.pulseq.server;

import com.pulseq.core.QueueManager;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Verifies backpressure behaviour on the publish endpoint with a deliberately tiny queue, so a
 * full topic is reached in a test rather than after a thousand messages.
 */
@SpringBootTest(properties = {
        "pulseq.capacity=2",
        "pulseq.publish-timeout-ms=50"
})
@AutoConfigureMockMvc
class PublishBackpressureTest {

    @Autowired
    private MockMvc mvc;

    @Autowired
    private QueueManager queueManager;

    @Test
    void fullTopicReturnsTooManyRequestsInsteadOfBlockingTheRequestThread() throws Exception {
        String topic = "saturated";
        for (int i = 0; i < 2; i++) {
            mvc.perform(post("/publish/" + topic)
                            .contentType(MediaType.APPLICATION_JSON)
                            .content("{\"payload\":\"m" + i + "\"}"))
                    .andExpect(status().isOk());
        }

        long startedAt = System.nanoTime();
        mvc.perform(post("/publish/" + topic)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"payload\":\"overflow\"}"))
                .andExpect(status().isTooManyRequests())
                .andExpect(jsonPath("$.error").exists());
        long elapsedMillis = (System.nanoTime() - startedAt) / 1_000_000;

        assertEquals(2, queueManager.getQueue(topic).size(), "the refused message must not be stored");
        assertEquals(1, queueManager.getMetrics().getBackpressure(topic));
        assertEquals(0, queueManager.getMetrics().getDuplicate(topic),
                "a full queue is backpressure, not a duplicate");
        org.junit.jupiter.api.Assertions.assertTrue(elapsedMillis < 5_000,
                "publish must answer within the backpressure window, took " + elapsedMillis + "ms");
    }
}