package searchengine

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import libraries.Address
import libraries.Credentials
import libraries.Elastic
import kotlin.time.ExperimentalTime
import kotlin.time.TimedValue
import kotlin.time.measureTimedValue

const val INDEX_NAME = "search"
const val PRECISION = 0.042 // lower means higher precision
const val PASSWORD = "0Se+4Stcs4VGWYBzLKip"

fun elasticRank(allDocsCount: Long): ElasticPagerank {
    val esOld = Elastic(Credentials("elastic", PASSWORD), Address("10.0.0.33", 9200), INDEX_NAME)
    val esNew = Elastic(
        Credentials("elastic", PASSWORD),
        Address("10.0.0.33", 9200),
        "$INDEX_NAME${System.currentTimeMillis()}"
    )
    return ElasticPagerank(esOld, esNew, INDEX_NAME, allDocsCount, 10_000)
}

@OptIn(ExperimentalTime::class)
suspend fun main() = runBlocking {
    println("Starting...")

    val es = Elastic(Credentials("elastic", PASSWORD), Address("10.0.0.33", 9200), INDEX_NAME)
    val allDocsCount = withContext(Dispatchers.Default) { es.getAllDocsCount() }

    println("$allDocsCount documents in total")
    val targetDeviation = (1.0 / allDocsCount) * PRECISION
    println("Targeted pagerank precision: $targetDeviation\n")

    val time: TimedValue<Unit> = measureTimedValue {
        elasticRank(allDocsCount).computePagerank(ElasticPagerank.ComputeMethod.InMemory)
    }
    println("Done, took: ${time.duration.inWholeMinutes}min ${time.duration.inWholeSeconds % 60}s")
}
