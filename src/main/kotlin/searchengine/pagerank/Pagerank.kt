package searchengine.pagerank

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.withContext
import libraries.Elastic
import libraries.Page

class PagerankCompute(private val elastic: Elastic) {

    private suspend fun getBackDocs(backLinks: List<Page.BackLink>) = coroutineScope {
        withContext(Dispatchers.Unconfined) {
            elastic.docsByUrlOrNullBulk(backLinks.map { it.source }, 10)
        }
    }

    private fun backRank(doc: Page.PageType, backDocs: List<Page.PageType>): Double {
        return backDocs.sumOf { backDoc ->
            val linkCount = backDoc.body.links.internal.count() + backDoc.body.links.external.count()
            val sameLinkCount =
                backDoc.body.links.internal.count { it.href == doc.address.url } + backDoc.body.links.external.count { it.href == doc.address.url }
            (backDoc.inferredData.ranks.pagerank / linkCount) * sameLinkCount
        }
    }

    private fun getGlobalPagerankInfluenceOnDoc(doc: Page.PageType, globalSinkRank: Double, allDocCount: Long): Double {
        val isSinkPage = doc.body.links.internal.isEmpty() && doc.body.links.external.isEmpty()
        return (globalSinkRank - (if (isSinkPage) doc.inferredData.ranks.pagerank else 0.0)) /
                (allDocCount - (if (isSinkPage) 1 else 0))
    }

    suspend fun getPagerank(doc: Page.PageType, globalSinkRank: Double, allDocCount: Long): Double = coroutineScope {
        val backDocs = doc.inferredData.backLinks.let {
            if (it.isNotEmpty()) getBackDocs(it) else null
            }?.responses()?.mapNotNull { it.result().hits().hits().firstOrNull() } ?: listOf()
        val globalInfluence = getGlobalPagerankInfluenceOnDoc(doc, globalSinkRank, allDocCount)
        val backRank = backRank(doc, backDocs.mapNotNull { it.source() })
        return@coroutineScope backRank + globalInfluence
    }

//    fun getSmartRank(doc: PageType, backDocks: List<PageType>): Double {
//        return 0.0
//    }

}