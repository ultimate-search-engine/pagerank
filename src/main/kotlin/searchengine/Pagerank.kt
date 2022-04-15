package searchengine

import io.ktor.http.*
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.coroutineScope
import libraries.PageRepository
import org.jsoup.Jsoup
import kotlin.math.abs
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.TimedValue
import kotlin.time.measureTimedValue


class Pagerank(everyLink: Set<String>) {

    private val allLinks = everyLink.map { link ->
        PagerankPage(link, mutableSetOf(), mutableListOf(1.0 / everyLink.size.toDouble()), 0, false)
    }.sortedBy { it.url }


    @OptIn(ExperimentalTime::class)
    suspend fun entangleDocs(db: PageRepository.Client) = coroutineScope {
        repositoryDocs(db).consumeEach { doc ->
            val time: TimedValue<Unit> = measureTimedValue {

                val parsed = Jsoup.parse(doc.content)
                val links = parsed.pageLinks(Url(doc.finalUrl)).toSet()

                val finalLinkObj = allLinks[allLinks.binarySearch {
                    Url(it.url).cUrl().compareTo(Url(doc.finalUrl).cUrl())
                }].also {
                    it.forwardLinkCount = links.size
                }

                doc.targetUrl.forEach { targetUrl ->
                    if (Url(targetUrl).cUrl() != Url(doc.finalUrl).cUrl()) {
                        val targetPage = get(targetUrl)!!
                        targetPage.doesForward = true
                        targetPage.forwardLinkCount += 1
                        finalLinkObj.backLinks.add(targetPage)
                    }
                }

                links.forEach { link ->
                    val linkedPage = get(link)!!
                    linkedPage.backLinks.add(finalLinkObj)
                }
            }
            if (time.duration.inWholeSeconds > 0) println("${doc.finalUrl} took ${time.duration}")
        }
    }


    infix fun get(url: String): PagerankPage? {
        val index = allLinks.binarySearch { Url(it.url).cUrl().compareTo(Url(url).cUrl()) }
        return if (0 <= index && index < allLinks.size) allLinks[index] else null
    }

    fun getAll() = allLinks

    private fun globalSinkRank(iteration: Int): Double =
        allLinks.sumOf { if (it.forwardLinkCount == 0) it.rank[iteration - 1] else 0.0 }

    private fun computePagerankOnDoc(doc: PagerankPage, sinkRank: Double, iteration: Int): Double =
        (1 - d) / allLinks.size + d * (doc.backLinks.sumOf { it.rank[iteration - 1] / it.forwardLinkCount } + sinkRank / allLinks.size)


    private fun computePagerankIteration(iteration: Int): Double {
        val sinkRank = globalSinkRank(iteration)
        var highestDeviation = 0.0
        allLinks.forEach {
            val rank = computePagerankOnDoc(it, sinkRank, iteration)
            if (rank == Double.POSITIVE_INFINITY) {
                throw Exception("Rank is infinite")
            }

            val deviation = abs(it.rank.last() - rank)
            if (deviation > highestDeviation) highestDeviation = deviation
            it.rank.add(rank)
        }
        return highestDeviation
    }

    @OptIn(ExperimentalTime::class)
    tailrec fun run(prevMetrics: PagerankMetrics? = null): PagerankMetrics {
        val (deviation: Double, duration: Duration) = measureTimedValue {
            computePagerankIteration(if (prevMetrics != null) prevMetrics.iteration + 1 else 1)
        }
        val metrics = PagerankMetrics(deviation, (prevMetrics?.iteration ?: 0) + 1, duration, prevMetrics)
        println("Iteration ${metrics.iteration} deviation: ${metrics.highestDeviation} duration: ${metrics.time.inWholeMilliseconds}")
        return if (deviation > Precision && metrics.iteration < 200) run(metrics) else metrics
    }

    data class PagerankMetrics(
        val highestDeviation: Double, val iteration: Int, val time: Duration, val previous: PagerankMetrics?
    )

    class PagerankPage(
        val url: String,
        val backLinks: MutableSet<PagerankPage>,
        var rank: MutableList<Double>,
        var forwardLinkCount: Int,
        var doesForward: Boolean,
    ) {
        operator fun compareTo(other: PagerankPage) = url.compareTo(other.url)

        override fun toString() = "$rank: $url"
    }

}


fun Url.cUrl(): String = "${this.protocol.name}://${this.host}${this.encodedPath}"
