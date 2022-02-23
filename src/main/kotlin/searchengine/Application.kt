package searchengine

import kotlinx.coroutines.*
import libraries.*
import kotlin.system.measureTimeMillis

fun elasticRank(): ElasticPagerank {
    val esOld = Elastic(Credentials("elastic", "testerino"), Address("localhost", 9200), "search")
    val esNew = Elastic(Credentials("elastic", "testerino"), Address("localhost", 9200), "search${System.currentTimeMillis()}")
    return ElasticPagerank(esOld, esNew, "search", 1200)
}

suspend fun main() = runBlocking {
    println("Starting...") // single iteration
    println("This might take a while...")
    val esOld = Elastic(Credentials("elastic", "testerino"), Address("localhost", 9200), "search")
    println(esOld.getAllDocsCount())

//    elasticRank().normalizeDocs()

    for (i in 0..10) {
        println("\nIteration $i")
        val time = measureTimeMillis {
            elasticRank().doPagerankIteration()
        }
        println("Iteration took ${time / 1000}s")
    }

    println("done")
}
