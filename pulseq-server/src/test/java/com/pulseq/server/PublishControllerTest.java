package com.pulseq.server;

import com.pulseq.core.QueueManager;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest
@AutoConfigureMockMvc
class PublishControllerTest {

    @Autowired
    private MockMvc mvc;

    @Autowired
    private QueueManager queueManager;

    @Test
    void publishAcceptsPayloadAndReturnsMessageId() throws Exception {
        mvc.perform(post("/publish/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"payload\":\"hello\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.messageId").isNotEmpty());

        assertTrue(queueManager.hasQueue("orders"));
        assertEquals(1, queueManager.getQueue("orders").size());
    }

    @Test
    void emptyPayloadIsRejected() throws Exception {
        mvc.perform(post("/publish/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"payload\":\"\"}"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void missingBodyIsRejected() throws Exception {
        mvc.perform(post("/publish/orders")
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isBadRequest());
    }

    @Test
    void healthAndMetricsAreExposed() throws Exception {
        mvc.perform(get("/health"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("UP"));
        mvc.perform(get("/metrics"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.queueDepths").exists());
    }
}
