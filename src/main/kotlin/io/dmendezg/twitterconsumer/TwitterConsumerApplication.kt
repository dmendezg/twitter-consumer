package io.dmendezg.twitterconsumer

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class TwitterConsumerApplication

fun main(args: Array<String>) {
    runApplication<TwitterConsumerApplication>(*args)
}
