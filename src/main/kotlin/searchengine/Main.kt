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
import io.github.cdimascio.dotenv.dotenv


val dotenv = dotenv()

val dbHost: String = (dotenv["DB_HOST"] ?: "").ifEmpty { "localhost" }
val dbPort: String = (dotenv["DB_PORT"] ?: "").ifEmpty { "27017" }
val dbUsername = (dotenv["DB_USERNAME"] ?: "").ifEmpty { "production_user" }
val dbPassword = (dotenv["DB_PASSWORD"] ?: "").ifEmpty { "use235" }

val esHost: String = (dotenv["ES_HOST"] ?: "").ifEmpty { "localhost" }
val esPort: Int = (dotenv["ES_PORT"] ?: "").ifEmpty { "9200" }.toInt()
val esUsername: String = (dotenv["ES_USERNAME"] ?: "").ifEmpty { "elastic" }
val esPassword: String = (dotenv["ES_PASSWORD"] ?: "").ifEmpty { "changeme" }

val BacklinkAnchorTextMaxCount: Int = (dotenv["BACKLINK_ANCHOR_TEXT_MAX_COUNT"] ?: "").ifEmpty { "5" }.toInt()


val mongoAddress = if (dbUsername.isNotEmpty()) "mongodb://$dbUsername:$dbPassword@$dbHost:$dbPort" else "mongodb://$dbHost:$dbPort"
const val mongoIndexName = "ency"
val elasticAddress = Address(esHost, esPort)
val elasticCredentials = Credentials(esUsername, esPassword)
const val elasticIndexName = "ency"

const val d = 0.85
const val Precision = 0.0 // lower is better, iterations are limited to 300
const val DocCount = Int.MAX_VALUE
const val DocBatchSize = 200


@OptIn(ExperimentalTime::class)
suspend fun main(): Unit = runBlocking {
    listOf(
        "britannica",
        "deletionpedia",
        "goodreads",
        "infoplease",
        "ncatlab",
        "scholarpedia",
        "wikipedia"
    ).forEachIndexed { index, it ->

        val dbClient = PageRepository.MongoClient(mongoIndexName, mongoAddress, it)
        val elastic = Elastic(elasticCredentials, elasticAddress, elasticIndexName)

        val time: TimedValue<Unit> = measureTimedValue {
            val pagerank = handlePagerankBuild(dbClient, useUncrawledLinks = false)
//        pagerank.getAll().sortedByDescending { it.rank.last() }.take(30).forEach { if (!it.doesForward) println(it.url) }
            println(pagerank.getAll().sumOf { it.rank.last() }) // checks out

            ElasticIndexer(pagerank, dbClient, elastic).index(index == 0)
        }
        println("Done with $it, took: ${time.duration.inWholeMinutes}min ${time.duration.inWholeSeconds % 60}s")
    }
    exitProcess(0)
}

