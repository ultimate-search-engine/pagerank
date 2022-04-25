package searchengine

import kotlinx.coroutines.*
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


const val d = 0.85
const val Precision = 0.0 // lower is better, iterations are limited to 200
const val DocCount = 20_000
const val DocBatchSize = 200


@OptIn(ExperimentalTime::class)
suspend fun main(): Unit = runBlocking {
    val dbClient = PageRepository.MongoClient("wiki2", "mongodb://10.0.0.35:27017")
    val elastic = Elastic(Credentials("elastic", ""), Address("10.0.0.35", 9200), "experimental")

//    val dbClient = PageRepository.MongoClient("wiki2", "mongodb://localhost:27017")

    val time: TimedValue<Unit> = measureTimedValue {
        val pagerank = handlePagerankBuild(dbClient, useUncrawledLinks = false)
//        pagerank.getAll().sortedByDescending { it.rank.last() }.take(30).forEach { if (!it.doesForward) println(it.url) }
        println(pagerank.getAll().sumOf { it.rank.last() }) // checks out

        ElasticIndexer(pagerank, dbClient, elastic).index()
    }

    println("Done, took: ${time.duration.inWholeMinutes}min ${time.duration.inWholeSeconds % 60}s")
    exitProcess(0)
}

