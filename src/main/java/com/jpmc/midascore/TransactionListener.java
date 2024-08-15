package com.jpmc.midascore;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import com.jpmc.midascore.foundation.Transaction;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@Component
public class TransactionListener {
    // this queue is used by TaskTwoTests to verify received messages
    public static final BlockingQueue<Transaction> MESSAGES = new LinkedBlockingQueue<>();

    @KafkaListener(
            topics  = "${general.kafka-topic}",
            groupId = "${spring.kafka.consumer.group-id}"
    )
    public void onMessage(Transaction transaction) {
        MESSAGES.add(transaction);
    }
}