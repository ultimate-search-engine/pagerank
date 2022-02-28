package searchengine

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import libraries.Address
import libraries.Credentials
import libraries.Elastic
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

const val INDEX_NAME = "search"
const val PRECISION = 0.001 // lower is more precise

fun elasticRank(): ElasticPagerank {
    val esOld = Elastic(Credentials("elastic", "testerino"), Address("localhost", 9200), INDEX_NAME)
    val esNew =
        Elastic(
            Credentials("elastic", "testerino"),
            Address("localhost", 9200),
            "$INDEX_NAME${System.currentTimeMillis()}"
        )
    return ElasticPagerank(esOld, esNew, INDEX_NAME, 200)
}

@OptIn(ExperimentalTime::class)
suspend fun main() = runBlocking {
    println("Starting...")
    println("This might take a while...")
    val es = Elastic(Credentials("elastic", "testerino"), Address("localhost", 9200), INDEX_NAME)

    val globalSinkRank = withContext(Dispatchers.Default) { es.getGlobalSinkRank() }
    val allDocsCount = withContext(Dispatchers.Default) { es.getAllDocsCount() }

    println("$allDocsCount documents in total")
    println("Targeted maximal pagerank difference: ${(1.0 / allDocsCount) * PRECISION}\n")

    elasticRank().normalizeDocs(allDocsCount, globalSinkRank)
    var i = 0

    val time = measureTimedValue {
        do {
            i += 1
            val (maxPagerankDiff: Double, time: Duration) = measureTimedValue {
                elasticRank().doPagerankIteration(allDocsCount, globalSinkRank)
            }
            println("\nMaximal pagerank difference: $maxPagerankDiff")
            println("Iteration took ${time.inWholeMinutes}min ${time.inWholeSeconds % 60}s\n\n")
        } while (maxPagerankDiff > (1.0 / allDocsCount) * PRECISION)
    }

    println("done, $i iterations, took: ${time.duration.inWholeMinutes}min ${time.duration.inWholeSeconds % 60}s")
}
