package searchengine.pagerank

import io.ktor.http.*
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.coroutineScope
import libraries.PageRepository
import org.jsoup.Jsoup
import searchengine.Precision
import searchengine.d
import searchengine.utilities.pageLinks
import searchengine.utilities.repositoryDocs
import kotlin.math.abs
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.TimedValue
import kotlin.time.measureTimedValue


suspend fun handlePagerankBuild(
    dbClient: PageRepository.Client,
    useUncrawledLinks: Boolean = false // turned out to require a funny amount of memory
) = coroutineScope {
    val everyLink = mutableSetOf<String>()

    repositoryDocs(dbClient).consumeEach {
        if (useUncrawledLinks) {
            val parse = Jsoup.parse(it.content)
            val links = parse.pageLinks(Url(it.finalUrl))
//            if (links.size > 5000) println("${it.finalUrl} has ${links.size} links")
            links.forEach(everyLink::add)
        }

        everyLink.add(Url(it.finalUrl).cUrl())
        it.targetUrl.forEach(everyLink::add)
        everyLink.add(it.finalUrl)
    }
    println("Found ${everyLink.size} total items")

    val pagerank = Pagerank(everyLink)
    pagerank.entangleDocs(dbClient)
    pagerank.run()
    return@coroutineScope pagerank
}

class Pagerank(everyLink: Set<String>) {

    private val allLinks = everyLink.map { link ->
        val initialRank = 1.0 / everyLink.size.toDouble()
        val page = PagerankPage(link, mutableListOf(), DoubleArray(2), 0, false)
        page.rank[0] = initialRank
        page.rank[1] = initialRank
        page
    }.sortedBy { it.url }.toTypedArray()


    private fun binarySearchLinks(
        element: String,
    ): Int? {
        var index = 0
        var end = allLinks.size - 1

        while (index <= end) {

            val center: Int = (index + end) / 2

            if (element == allLinks[center].url) {
                return center
            } else if (element < allLinks[center].url) {
                end = center - 1
            } else if (element > allLinks[center].url) {
                index = center + 1
            }
        }
        return null
    }

    @OptIn(ExperimentalTime::class)
    suspend fun entangleDocs(db: PageRepository.Client) = coroutineScope {
        repositoryDocs(db).consumeEach { doc ->
            measureTimedValue {
                val parsed = Jsoup.parse(doc.content)
                val links = parsed.pageLinks(Url(doc.finalUrl))

                val finalLinkObj = get(doc.finalUrl) ?: return@measureTimedValue

                val finalUrl = Url(doc.finalUrl).cUrl()
                doc.targetUrl.forEach { targetUrl ->
                    if (Url(targetUrl).cUrl() != finalUrl) {
                        val targetPage = get(targetUrl) ?: return@measureTimedValue

                        targetPage.doesForward = true
                        targetPage.forwardLinkCount += 1
                        addBacklink(finalLinkObj, targetPage)
                    }
                }

                links.forEach { link ->
                    val linkedPage = get(link)
                    if (linkedPage != null) {
                        finalLinkObj.forwardLinkCount += 1
                        addBacklink(linkedPage, finalLinkObj)
                    }
                }
            }
        }
    }


    private fun addBacklink(page: PagerankPage, link: PagerankPage) = page.backLinks.add(link)


    infix fun get(url: String): PagerankPage? {
        val index = binarySearchLinks(url) ?: return null
        return if (0 <= index && index < allLinks.size) allLinks[index] else null
    }

    fun getAll() = allLinks

    private fun globalSinkRank(): Double =
        allLinks.sumOf { if (it.forwardLinkCount == 0) it.rank[0] else 0.0 }

    private fun computePagerankOnDoc(doc: PagerankPage, sinkRank: Double): Double =
        (1 - d) / allLinks.size + d * (doc.backLinks.sumOf { it.rank[0] / it.forwardLinkCount } + sinkRank / allLinks.size)


    private fun computePagerankIteration(): Double {
        val sinkRank = globalSinkRank()
        var highestDeviation = 0.0
        allLinks.forEach {
            val rank = computePagerankOnDoc(it, sinkRank)
            if (rank == Double.POSITIVE_INFINITY) throw Exception("Rank is infinite")

            val deviation = abs(it.rank.last() - rank)
            if (deviation > highestDeviation) highestDeviation = deviation
            it.rank[1] = rank
        }
        moveCalculatedRank()
        return highestDeviation
    }

    private fun moveCalculatedRank() {
        allLinks.forEach {
            it.rank[0] = it.rank[1]
        }
    }

    @OptIn(ExperimentalTime::class)
    tailrec fun run(prevMetrics: PagerankMetrics? = null): PagerankMetrics {
        val (deviation: Double, duration: Duration) = measureTimedValue {
            computePagerankIteration()
        }
        val metrics = PagerankMetrics(deviation, (prevMetrics?.iteration ?: 0) + 1, duration)
        println("Iteration ${metrics.iteration} deviation: ${metrics.highestDeviation} duration: ${metrics.time.inWholeMilliseconds}ms")
        return if (deviation > Precision && metrics.iteration < 300) run(metrics) else metrics
    }

    data class PagerankMetrics(
        val highestDeviation: Double, val iteration: Int, val time: Duration
    )

    class PagerankPage(
        val url: String,
        val backLinks: MutableList<PagerankPage>,
        var rank: DoubleArray = DoubleArray(2),
        var forwardLinkCount: Int,
        var doesForward: Boolean,
    )

}


fun Url.cUrl(): String = "${this.protocol.name}://${this.host}${this.encodedPath}"
