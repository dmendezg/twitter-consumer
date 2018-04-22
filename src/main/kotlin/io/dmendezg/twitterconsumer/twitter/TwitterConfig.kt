package io.dmendezg.twitterconsumer.twitter

import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.core.Client
import com.twitter.hbc.core.Constants
import com.twitter.hbc.core.HttpHosts
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint
import com.twitter.hbc.core.event.Event
import com.twitter.hbc.core.processor.StringDelimitedProcessor
import com.twitter.hbc.httpclient.auth.OAuth1
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.beans.ConstructorProperties
import java.util.concurrent.LinkedBlockingQueue

@Configuration
class TwitterConfig {

    @Bean
    fun msgQueue(): LinkedBlockingQueue<String> {
        return LinkedBlockingQueue(100000)
    }

    @Bean
    fun oAuth1(twitterProperties: TwitterProperties): OAuth1 {
        return OAuth1(
            twitterProperties.consumerKey,
            twitterProperties.consumerSecret,
            twitterProperties.token,
            twitterProperties.tokenSecret
        )
    }

    @Bean
    fun config(oAuth1: OAuth1, msgQueue: LinkedBlockingQueue<String>): Client {
        val eventQueue = LinkedBlockingQueue<Event>(1000)

        val hosebirdHosts = HttpHosts(Constants.STREAM_HOST)
        val hosebirdEndpoint = StatusesFilterEndpoint()
        val terms = listOf("Espana", "David", "twitter", "api")
        hosebirdEndpoint.trackTerms(terms)

        val builder = ClientBuilder()
            .name("Hosebird-Client-01")
            .hosts(hosebirdHosts)
            .authentication(oAuth1)
            .endpoint(hosebirdEndpoint)
            .processor(StringDelimitedProcessor(msgQueue))
            .eventMessageQueue(eventQueue)

        return builder.build()
    }

}

@Configuration
@ConfigurationProperties("twitter")
class TwitterProperties {
    var consumerKey: String? = null
    var consumerSecret: String? = null
    var token: String? = null
    var tokenSecret: String? = null
}