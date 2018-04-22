package io.dmendezg.twitterconsumer

import com.twitter.hbc.core.Client
import io.dmendezg.twitterconsumer.analysis.NlpAnalysis
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.kafka.support.SendResult
import org.springframework.messaging.support.GenericMessage
import org.springframework.stereotype.Component
import org.springframework.util.concurrent.ListenableFutureCallback
import twitter4j.JSONObject
import java.util.*
import java.util.concurrent.CountDownLatch
import java.util.concurrent.LinkedBlockingQueue


@SpringBootApplication
class TwitterConsumerApplication

fun main(args: Array<String>) {
    runApplication<TwitterConsumerApplication>(*args)
}

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
            println(msg)
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

@Component
class KafkaProcessor(
    val nlpAnalysis: NlpAnalysis
) : CommandLineRunner {
    override fun run(vararg args: String?) {
        val props = Properties()
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe")
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().javaClass)
        props.put(
            StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
            Serdes.String().javaClass
        )

        val builder = StreamsBuilder()

        builder.stream<Any, Any>("test")
            .map({ i, s ->
                KeyValue(i, JSONObject(s.toString()))
            })
            .filter({ _, s ->
                s.has("text") && s.getString("lang") == "en"
            })
            .map({ i, s ->
                val tweet = s.getString("text")
                val sentiment = nlpAnalysis.findSentiment(tweet)
                KeyValue(i, "$sentiment: $tweet")
            })
            .to("test-count")

        val topology = builder.build()

        val streams = KafkaStreams(topology, props)
        val latch = CountDownLatch(1)

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(object : Thread("streams-shutdown-hook") {
            override fun run() {
                streams.close()
                latch.countDown()
            }
        })

        streams.start()
    }
}
