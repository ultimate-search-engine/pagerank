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
const val PRECISION = 0.01 // lower means higher precision

fun elasticRank(allDocsCount: Long): ElasticPagerank {
    val esOld = Elastic(Credentials("elastic", "testerino"), Address("localhost", 9200), INDEX_NAME)
    val esNew = Elastic(Credentials("elastic", "testerino"), Address("localhost", 9200), "$INDEX_NAME${System.currentTimeMillis()}")
    return ElasticPagerank(esOld, esNew, INDEX_NAME, allDocsCount, 200)
}

@OptIn(ExperimentalTime::class)
suspend fun main() = runBlocking {
    println("Starting...")

    val es = Elastic(Credentials("elastic", "testerino"), Address("localhost", 9200), INDEX_NAME)
    val allDocsCount = withContext(Dispatchers.Default) { es.getAllDocsCount() }

    println("$allDocsCount documents in total")
    val targetDeviation = (1.0 / allDocsCount) * PRECISION
    println("Targeted pagerank precision: $targetDeviation\n")

    elasticRank(allDocsCount).normalizeDocs()

    var i = 0
    val time = measureTimedValue {
        do {
            val (maxPagerankDiff: Double, time: Duration) = measureTimedValue {
                elasticRank(allDocsCount).doPagerankIteration()
            }
            println("\nHighest pagerank deviation: $maxPagerankDiff / $targetDeviation, (${((maxPagerankDiff / targetDeviation) * 100).round(2)}%)")
            println("Iteration $i took ${time.inWholeMinutes}min ${time.inWholeSeconds % 60}s\n\n")
            i += 1
        } while (maxPagerankDiff > (1.0 / allDocsCount) * PRECISION)
    }

    println("done, $i iterations, took: ${time.duration.inWholeMinutes}min ${time.duration.inWholeSeconds % 60}s")
}
