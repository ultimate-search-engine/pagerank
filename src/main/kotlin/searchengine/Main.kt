package searchengine

import io.ktor.http.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.channels.produce
import libraries.Address
import libraries.Credentials
import libraries.Elastic
import libraries.PageRepository
import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import searchengine.searchIndexer.ElasticIndexer
import kotlin.math.roundToInt
import kotlin.system.exitProcess
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.TimedValue
import kotlin.time.measureTimedValue


const val d = 0.85
const val Precision = 0.0


@OptIn(ExperimentalTime::class)
suspend fun main(): Unit = runBlocking {
    val dbClient = PageRepository.MongoClient("wiki2", "mongodb://10.0.0.35:27017")
    val elastic = Elastic(Credentials("elastic", ""), Address("10.0.0.35", 9200), "experimental")

//    val dbClient = PageRepository.MongoClient("wiki2", "mongodb://localhost:27017")

    val time: TimedValue<Unit> = measureTimedValue {
        val pagerank = handlePagerankBuild(dbClient)
        pagerank.getAll().sortedByDescending { it.rank.last() }.take(30).forEach { if (!it.doesForward) println(it) }
        println(pagerank.getAll().sumOf { it.rank.last() })

        elastic.deleteIndex()
        elastic.putMapping()
        val elasticIndexer = ElasticIndexer(pagerank, dbClient, elastic)
        elasticIndexer.index()
    }

    println("Done, took: ${time.duration.inWholeMinutes}min ${time.duration.inWholeSeconds % 60}s")
    exitProcess(0)
}


suspend fun handlePagerankBuild(dbClient: PageRepository.Client) = coroutineScope {
    val everyLink = mutableSetOf<String>()

    repositoryDocs(dbClient).consumeEach {
        val parse = Jsoup.parse(it.content)
        val links = parse.pageLinks(Url(it.finalUrl))

        if (links.size > 5000) println("${it.finalUrl} has ${links.size} links")

        everyLink.add(Url(it.finalUrl).cUrl())
        links.forEach(everyLink::add)
        it.targetUrl.forEach(everyLink::add)
    }
    println("Found ${everyLink.size} total items")

    val pagerank = Pagerank(everyLink)
    pagerank.entangleDocs(dbClient)
    pagerank.run()
    return@coroutineScope pagerank
}

fun Document.pageLinks(url: Url): List<String> {
    val links = this.select("a")

    return links.mapNotNull {
        val href = it.attr("href")
        try {
            if (href.startsWith("https")) {
                Url(href).cUrl()
            } else if (href.startsWith("/") && !href.startsWith("//")) {
                Url("${url.protocol.name}://${url.host}$href").cUrl()
            } else {
                null
            }
        } catch (e: Exception) {
            println("Error parsing $href")
            null
        }
    }
}


@OptIn(ExperimentalCoroutinesApi::class, ExperimentalTime::class)
fun CoroutineScope.repositoryDocs(client: PageRepository.Client): ReceiveChannel<PageRepository.Page> =
    produce(capacity = 300) {
        val limit = 250
        val batch = 250

        var lastUrl: String? = null
        var ctr = 0

        while (ctr < limit) {
            val (finds: List<PageRepository.Page>, duration: Duration) = measureTimedValue {
                client.findAfter(lastUrl, batch, code = 200)
            }
            if (finds.isEmpty()) break
            finds.forEach { send(it) }
            lastUrl = finds.last().finalUrl
            ctr += finds.size
            println("${(ctr.toDouble() / limit.toDouble() * 100.0).roundToInt()}%, took: ${duration.inWholeMinutes}min ${duration.inWholeSeconds % 60}s")
        }

    }

