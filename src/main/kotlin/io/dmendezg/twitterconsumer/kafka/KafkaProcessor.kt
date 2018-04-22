package io.dmendezg.twitterconsumer.kafka

import io.dmendezg.twitterconsumer.analysis.NlpAnalysis
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.springframework.boot.CommandLineRunner
import org.springframework.stereotype.Component
import twitter4j.JSONObject
import java.text.SimpleDateFormat
import java.util.*
import java.util.concurrent.CountDownLatch

@Component
class KafkaProcessor(
    val nlpAnalysis: NlpAnalysis
) : CommandLineRunner {

    init {
        sf.isLenient = true
    }

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
                try {
                    KeyValue(i, JSONObject(s.toString()))
                } catch (e: Exception) {
                    KeyValue(i, JSONObject())
                }
            })
            .filter({ _, s ->
                s.has("text")
            })
            .map({ i, s ->
                val tweet = s.getString("text")
                val date = sf.parse(s.getString("created_at"))
                val sentiment = nlpAnalysis.findSentiment(tweet)
                KeyValue(i, "$sentiment - $date - $tweet")
            })
            .to("test-count")

        val topology = builder.build()

        val streams = KafkaStreams(topology, props)
        val latch = CountDownLatch(1)

        Runtime.getRuntime().addShutdownHook(object : Thread("streams-shutdown-hook") {
            override fun run() {
                streams.close()
                latch.countDown()
            }
        })

        streams.start()
    }

    companion object {
        val sf = SimpleDateFormat("EEE MMM dd HH:mm:ss ZZZZZ yyyy")
    }

}
