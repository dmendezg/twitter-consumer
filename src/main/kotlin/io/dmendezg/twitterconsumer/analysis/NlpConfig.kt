package io.dmendezg.twitterconsumer.analysis

import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import org.springframework.context.annotation.Bean
import org.springframework.stereotype.Component


@Component
class NlpAnalysis {

    val pipeline: StanfordCoreNLP = StanfordCoreNLP("nlp.properties")

    fun findSentiment(tweet: String?): Int {
        var mainSentiment = 0
        if (tweet != null && tweet.length > 0) {
            var longest = 0
            val annotation = pipeline.process(tweet)
            for (sentence in annotation
                .get(CoreAnnotations.SentencesAnnotation::class.java)) {
                val tree = sentence
                    .get(SentimentCoreAnnotations.SentimentAnnotatedTree::class.java)
                val sentiment = RNNCoreAnnotations.getPredictedClass(tree)
                val partText = sentence.toString()
                if (partText.length > longest) {
                    mainSentiment = sentiment
                    longest = partText.length
                }

            }
        }
        return mainSentiment
    }

}