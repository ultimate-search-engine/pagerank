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
            if (links.size > 5000) println("${it.finalUrl} has ${links.size} links")
            links.forEach(everyLink::add)
        }

        everyLink.add(Url(it.finalUrl).cUrl())
        it.targetUrl.forEach(everyLink::add)
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


    private tailrec fun Array<PagerankPage>.binarySearchLinks(
        url: Url,
        x: Int = (allLinks.size - 1) / 2,
        from: Int = 0,
        to: Int = allLinks.size - 1
    ): Int? {
        if (x >= allLinks.size) return null
        val comparable = url.cUrl().compareTo(Url(allLinks[x].url).cUrl())
//        println("comparable: $comparable, x: $x, from: $from, to: $to")

        return if (from > to) null
        else if (comparable < 0) binarySearchLinks(url, (from + x) / 2, from, x - 1)
        else if (comparable > 0) binarySearchLinks(url, (x + to) / 2 + 1, x + 1, to)
        else if (url.cUrl() == allLinks[x].url) x
        else {
            println("null")
            null
        }
    }

    @OptIn(ExperimentalTime::class)
    suspend fun entangleDocs(db: PageRepository.Client) = coroutineScope {
        repositoryDocs(db).consumeEach { doc ->
            val time: TimedValue<Unit> = measureTimedValue {

                val parsed = Jsoup.parse(doc.content)
                val links = parsed.pageLinks(Url(doc.finalUrl))

                val finalLinkObj = allLinks[allLinks.binarySearchLinks(Url(doc.finalUrl))!!]
                assert(finalLinkObj.url == doc.finalUrl)

                doc.targetUrl.forEach { targetUrl ->
                    if (Url(targetUrl).cUrl() != Url(doc.finalUrl).cUrl()) {
                        val targetPage = get(targetUrl)!!
                        targetPage.doesForward = true
                        targetPage.forwardLinkCount += 1
                        addBacklink(finalLinkObj, targetPage)
                    }
                }

                links.forEach { link ->
                    val linkedPage = get(link)
                    if (linkedPage != null) {
                        assert(linkedPage.url == link)
                        finalLinkObj.forwardLinkCount += 1
                        addBacklink(linkedPage, finalLinkObj)
                    }
                }
            }
            if (time.duration.inWholeSeconds > 0) println("${doc.finalUrl} took ${time.duration}")
        }
    }


    private fun addBacklink(page: PagerankPage, link: PagerankPage) = page.backLinks.add(link)


    infix fun get(url: String): PagerankPage? {
        val index = allLinks.binarySearchLinks(Url(url)) ?: return null
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
            if (rank == Double.POSITIVE_INFINITY) {
                throw Exception("Rank is infinite")
            }

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
        return if (deviation > Precision && metrics.iteration < 200) run(metrics) else metrics
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
