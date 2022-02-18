package searchengine.pagerank

import co.elastic.clients.elasticsearch.core.search.Hit
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import searchengine.elastic.Elastic
import searchengine.types.BackLink
import searchengine.types.PageType

class PagerankCompute(private val elastic: Elastic) {
    private suspend fun getBackDocs(backLinks: List<BackLink>) = coroutineScope {
        val backDocsAsync = backLinks.map {
            async {
                elastic.docsByUrlOrNull(it.source)
            }
        }
        return@coroutineScope backDocsAsync.mapNotNull {
            it.await()?.firstOrNull()
        }
    }

    private fun backRank(doc: PageType, backDocks: List<PageType>): Double {
        return backDocks.sumOf { backDoc ->
            val linkCount = backDoc.body.links.internal.size + backDoc.body.links.external.size
            val sameLinkCount = backDoc.body.links.internal.count { it.href == doc.address.url } + backDoc.body.links.external.count { it.href == doc.address.url }
            (backDoc.inferredData.ranks.pagerank / linkCount) * sameLinkCount
        }
    }

    private suspend fun getGlobalPagerankInfluenceOnDoc(doc: PageType): Double = coroutineScope {
        val globalSinkRank = async { elastic.getGlobalSinkRank() }
        val allDocCount = async { elastic.getAllDocsCount() }
        val isSinkPage = doc.body.links.internal.isEmpty() && doc.body.links.external.isEmpty()
        return@coroutineScope (globalSinkRank.await() - (if (isSinkPage) doc.inferredData.ranks.pagerank else 0.0)) / (allDocCount.await() - (if (isSinkPage) 1 else 0))
    }

    suspend fun getPagerank(doc: PageType): Double = coroutineScope {
            val backDocsAsync =
                async { doc.inferredData.backLinks.let { it1 -> getBackDocs(it1) } }
            val globalInfluence = async { getGlobalPagerankInfluenceOnDoc(doc) }
            val backDocs = backDocsAsync.await()
            val backRank = backRank(doc, backDocs.mapNotNull { it.source() })
            val pagerank = backRank + globalInfluence.await()
            println("pagerank: $pagerank")
            return@coroutineScope pagerank
    }

}