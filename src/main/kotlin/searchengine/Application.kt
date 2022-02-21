package searchengine

import co.elastic.clients.elasticsearch.core.search.Hit
import kotlinx.coroutines.*
import libraries.*


fun elasticRank(): ElasticPagerank {
    val esOld = Elastic(Credentials("elastic", "testerino"), Address("localhost", 9200), "search")
    val esNew = Elastic(Credentials("elastic", "testerino"), Address("localhost", 9200), "search${System.currentTimeMillis()}")
    return ElasticPagerank(esOld, esNew, "search")
}

suspend fun main() = runBlocking {
    println("Starting...") // single iteration
    println("This might take a while...")


    elasticRank().transferNormalizedDocs()

    for (i in 0..5) {
        println("\nIteration $i")
        elasticRank().doPagerankIteration()
    }

    println("done")
}
