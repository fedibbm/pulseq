package com.pulseq.server;

import com.pulseq.core.Dispatcher;
import com.pulseq.core.Message;
import com.pulseq.core.MessageStatus;
import com.pulseq.core.QueueManager;
import com.pulseq.core.Reason;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class DlqControllerTest {

    @Autowired
    private MockMvc mvc;

    @Autowired
    private QueueManager queueManager;

    @Autowired
    private Dispatcher dispatcher;

    @Test
    void listReturnsDeadLetteredMessages() throws Exception {
        Message rejected = new Message("dlq-1", "dlq-list", "boom".getBytes());
        rejectViaDispatcher(rejected);

        mvc.perform(get("/dlq/dlq-list"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].id").value("dlq-1"))
                .andExpect(jsonPath("$[0].status").value(MessageStatus.DEAD_LETTERED.name()))
                .andExpect(jsonPath("$[0].payload").isNotEmpty());
    }

    @Test
    void replayReturnsDeadLetteredMessagesToMainQueue() throws Exception {
        Message rejected = new Message("dlq-2", "dlq-replay", "boom".getBytes());
        rejectViaDispatcher(rejected);

        mvc.perform(post("/dlq/dlq-replay/replay"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.replayed").value(1));

        mvc.perform(get("/dlq/dlq-replay"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$").isEmpty());
        mvc.perform(get("/metrics"))
                .andExpect(status().isOk());
    }

    @Test
    void unknownTopicReturnsNotFound() throws Exception {
        mvc.perform(get("/dlq/nope"))
                .andExpect(status().isNotFound());
        mvc.perform(post("/dlq/nope/replay"))
                .andExpect(status().isNotFound());
    }

    /** Delivers the message through the dispatcher to a listener that immediately rejects it. */
    private void rejectViaDispatcher(Message message) throws InterruptedException {
        CountDownLatch delivered = new CountDownLatch(1);
        String topic = message.getTopic();
        com.pulseq.core.MessageListener listener = m -> {
            queueManager.getQueue(topic).nack(m.getId(), Reason.REJECTED);
            delivered.countDown();
        };
        dispatcher.subscribe(topic, listener);
        queueManager.publish(topic, message);
        assertTrue(delivered.await(5, TimeUnit.SECONDS), "message should have been delivered");
        dispatcher.unsubscribe(topic, listener);
    }
}
