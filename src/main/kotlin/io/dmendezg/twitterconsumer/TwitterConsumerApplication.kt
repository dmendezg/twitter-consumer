package io.dmendezg.twitterconsumer

import com.twitter.hbc.core.Client
import com.twitter.hbc.httpclient.BasicClient
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.stereotype.Component
import java.util.concurrent.LinkedBlockingQueue

@SpringBootApplication
class TwitterConsumerApplication

fun main(args: Array<String>) {
    runApplication<TwitterConsumerApplication>(*args)
}

@Component
class TwitterConsumer(
    val client: Client,
    val msgQueue: LinkedBlockingQueue<String>
) : CommandLineRunner {
    override fun run(vararg args: String?) {
        client.connect()

        while (!client.isDone) {
            val msg = msgQueue.take()
            println(msg)
        }
    }
}
