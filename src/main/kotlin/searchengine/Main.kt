package searchengine

import kotlinx.coroutines.runBlocking
import libraries.Address
import libraries.Credentials
import libraries.Elastic
import libraries.PageRepository
import searchengine.pagerank.handlePagerankBuild
import searchengine.searchIndexer.ElasticIndexer
import kotlin.system.exitProcess
import kotlin.time.ExperimentalTime
import kotlin.time.TimedValue
import kotlin.time.measureTimedValue

const val mongoAddress = "mongodb://localhost:27017"
const val mongoIndexName = "web1"
val elasticAddress = Address("localhost", 9200)
val elasticCredentials = Credentials("elastic", "changeme")
const val elasticIndexName = "web1"

const val d = 0.85
const val Precision = 0.0 // lower is better, iterations are limited to 500
const val DocCount = Int.MAX_VALUE
const val DocBatchSize = 400


@OptIn(ExperimentalTime::class)
suspend fun main(): Unit = runBlocking {
    val dbClient = PageRepository.MongoClient(mongoIndexName, mongoAddress)
    val elastic = Elastic(elasticCredentials, elasticAddress, elasticIndexName)

    val time: TimedValue<Unit> = measureTimedValue {
        val pagerank = handlePagerankBuild(dbClient, useUncrawledLinks = false)
//        pagerank.getAll().sortedByDescending { it.rank.last() }.take(30).forEach { if (!it.doesForward) println(it.url) }
        println(pagerank.getAll().sumOf { it.rank.last() }) // checks out

        ElasticIndexer(pagerank, dbClient, elastic).index()
    }

    println("Done, took: ${time.duration.inWholeMinutes}min ${time.duration.inWholeSeconds % 60}s")
    exitProcess(0)
}

