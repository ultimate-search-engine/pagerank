package searchengine

import co.elastic.clients.elasticsearch.core.search.Hit
import kotlinx.coroutines.*
import libraries.Elastic
import libraries.PageType
import searchengine.pagerank.PagerankCompute

val elastic = Elastic("se14")

val pagerankCompute = PagerankCompute(elastic)

val computeId = (0..Long.MAX_VALUE).random()
const val batchSize: Long = 1984
const val elasticIndexDelay: Long = 4000

suspend fun main() = runBlocking {
    println("Starting...") // single iteration
    println("This might take a while... (depending on your batch size: $batchSize)")
    val globalSinkRankAsync = async { elastic.getGlobalSinkRank() }
    val allDocCountAsync = async { elastic.getAllDocsCount() }
    val globalSinkRank = globalSinkRankAsync.await()
    val allDocCount = allDocCountAsync.await()
    do {
        val res = async { elastic.getDocsThatDoesNotMatchComputeRoundId(computeId, batchSize) }
        val docs = res.await()

        docs.mapNotNull { doc ->
            val sauce = doc.source()
            if (sauce != null) {
                RankAndDocDeffered(doc, async { pagerankCompute.getPagerank(sauce, globalSinkRank, allDocCount) })
            } else null
        }.map {
            val pagerank = it.pagerank.await()
            RankAndDoc(it.doc, pagerank)
        }.mapNotNull {
            it.doc.source()?.let { sauce ->
                sauce.inferredData.ranks.pagerank = it.pagerank
                sauce.inferredData.ranks.computeRoundId = computeId
                println("doc id: ${it.doc.id()}, pagerank: ${sauce.inferredData.ranks.pagerank}, url: ${sauce.address.url}")

                // TODO: This re-indexes the whole doc, not just the pagerank - should be optimized
                async { elastic.indexPage(sauce, it.doc.id()) }
            }
        }.forEach {
            it.await()
        }
        println("batch complete, delay: $elasticIndexDelay ms")
        delay(elasticIndexDelay)
    } while (docs.isNotEmpty())
    println("done")
}

data class RankAndDocDeffered(val doc: Hit<PageType>, val pagerank: Deferred<Double>)
data class RankAndDoc(val doc: Hit<PageType>, val pagerank: Double)
