package io.dmendezg.twitterconsumer.twitter

import com.twitter.hbc.core.Client
import org.springframework.boot.CommandLineRunner
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.kafka.support.SendResult
import org.springframework.messaging.support.GenericMessage
import org.springframework.stereotype.Component
import org.springframework.util.concurrent.ListenableFutureCallback
import java.util.concurrent.LinkedBlockingQueue

@Component
class TwitterConsumer(
    val client: Client,
    val template: KafkaTemplate<*, *>,
    val msgQueue: LinkedBlockingQueue<String>
) : CommandLineRunner {
    override fun run(vararg args: String?) {
        client.connect()

        while (!client.isDone) {
            val msg = msgQueue.take()
            sendToKafka(msg)
        }
    }

    fun sendToKafka(data: String) {
        val msg = GenericMessage(
            data, mapOf(
                KafkaHeaders.TOPIC to "test",
                KafkaHeaders.PARTITION_ID to 0
            )
        )
        val future = template.send(msg)
        future.addCallback(Callback(data))
    }
}

class Callback(val data: String) : ListenableFutureCallback<SendResult<*, *>> {
    override fun onSuccess(p0: SendResult<*, *>?) {
        println(data)
        println(p0)
    }

    override fun onFailure(p0: Throwable) {
        println(data)
        p0.printStackTrace()
    }
}