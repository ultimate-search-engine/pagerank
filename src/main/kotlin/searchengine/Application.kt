package searchengine

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import libraries.Address
import libraries.Credentials
import libraries.Elastic
import kotlin.system.exitProcess
import kotlin.time.ExperimentalTime
import kotlin.time.TimedValue
import kotlin.time.measureTimedValue

val Precision = System.getenv("Precision").toDoubleOrNull() ?: 0.042 // lower means higher precision
val IndexName = System.getenv("IndexName") ?: "search"
val Host = System.getenv("ElasticHost") ?: "localhost"
val Password = System.getenv("ELASTIC_PASSWORD") ?: throw(Exception("ElasticPassword not set in env variable"))
val esPort = System.getenv("ES_PORT").toIntOrNull() ?: 9200


fun elasticRank(allDocsCount: Long): ElasticPagerank {
    val esOld = Elastic(Credentials("elastic", Password), Address(Host, esPort), IndexName)
    val esNew = Elastic(
        Credentials("elastic", Password),
        Address(Host, esPort),
        "$IndexName${System.currentTimeMillis()}"
    )
    return ElasticPagerank(esOld, esNew, IndexName, allDocsCount, 10_000)
}

@OptIn(ExperimentalTime::class)
suspend fun main(): Unit = runBlocking {
    println("Starting...")

    val es = Elastic(Credentials("elastic", Password), Address(Host, esPort), IndexName)
    val allDocsCount = withContext(Dispatchers.Default) { es.getAllDocsCount() }

    println("$allDocsCount documents in total")
    val targetDeviation = (1.0 / allDocsCount) * Precision
    println("Targeted pagerank precision: $targetDeviation\n")

    val time: TimedValue<Unit> = measureTimedValue {
        elasticRank(allDocsCount).computePagerank(ElasticPagerank.ComputeMethod.InMemory)
    }
    println("Done, took: ${time.duration.inWholeMinutes}min ${time.duration.inWholeSeconds % 60}s")
    exitProcess(0)
}
