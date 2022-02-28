package searchengine

import co.elastic.clients.elasticsearch.core.search.Hit
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import libraries.Elastic
import libraries.Page
import searchengine.pagerank.PagerankCompute


fun Double.round(decimals: Int): Double {
    var multiplier = 1.0
    repeat(decimals) { multiplier *= 10 }
    return kotlin.math.round(this * multiplier) / multiplier
}

class ElasticPagerank(
    private val oldElastic: Elastic,
    private val newElastic: Elastic,
    private val defaultIndex: String,
    private val batchSize: Long = 400
) {

    private suspend fun switchAliasesAndDeleteOldIndex() {
        val oldAliasIndex = oldElastic.alias.getIndexByAlias(defaultIndex)

        try {
            oldElastic.alias.delete(defaultIndex)
        } catch (e: Exception) {
            println("Alias not found")
        }

        delay(1000)

        oldAliasIndex.forEach { oldElastic.deleteIndex(it) }
        newElastic.alias.create(defaultIndex)
    }

    suspend fun normalizeDocs(allDocsCount: Long, globalSinkRank: Double) = coroutineScope {
        var pagination: Long = 0
        var lastUrl: String? = null

        newElastic.putMapping()

        suspend fun doBatch(docs: List<Hit<Page.PageType>>): List<Page.PageType> {
            return docs.mapNotNull {
                val source = it.source()
                if (source != null) {
                    async(Dispatchers.Unconfined) {
                        source.inferredData.ranks.pagerank = 1.0 / allDocsCount
                        source.inferredData.ranks.smartRank = 1.0 / allDocsCount
                        source
                    }
                } else null
            }.map { it.await() }
        }

        do {
            println(
                "Normalization batch: $pagination - ~${((pagination * batchSize).toDouble() / allDocsCount * 100).round(2)}%")

            val res = async { oldElastic.searchAfterUrl(batchSize, lastUrl) }
            val docs = res.await().hits().hits()
            val doneDocs = doBatch(docs)

            if (doneDocs.isNotEmpty()) {
                newElastic.indexDocsBulk(doneDocs)
            }

            lastUrl = docs?.lastOrNull()?.source()?.address?.url

            pagination += 1
        } while (docs?.isNotEmpty() == true)

        switchAliasesAndDeleteOldIndex()

        println("Normalization done\n")
    }


    suspend fun doPagerankIteration(allDocsCount: Long, globalSinkRank: Double) = coroutineScope {
        val newMapping = async { newElastic.putMapping(4) }
        newMapping.await()

        val pagerankCompute = PagerankCompute(oldElastic)
        var pagination: Long = 0

        var lastUrl: String? = null
        var maxPagerankDiff = 0.0

        do {
            val doneFrom = (pagination * batchSize).toDouble() / allDocsCount
            println("Pagerank batch: $pagination - ~${(doneFrom * 100).round(2)}%")

            val res = async { oldElastic.searchAfterUrl(batchSize, lastUrl) }
            val docs = res.await().hits().hits()

            lastUrl = docs?.lastOrNull()?.source()?.address?.url

            val doneDocs = docs?.mapNotNull { doc ->
                async(Dispatchers.Unconfined) {
                    val source = doc.source()
                    if (source != null) {
                        var pagerank = pagerankCompute.getPagerank(source, globalSinkRank, allDocsCount)

                        if (pagerank.isNaN()) {
                            // cause most likely is that the backlink is indexed more than once
                            println("Pagerank of ${source.address.url} is NaN, substituting with previous value ${source.inferredData.ranks.pagerank}")
                            pagerank = source.inferredData.ranks.pagerank
                        }
                        val pagerankDiff = kotlin.math.abs(pagerank - source.inferredData.ranks.pagerank)

                        source.inferredData.ranks.pagerank = pagerank
                        source.inferredData.ranks.smartRank = pagerank * allDocsCount

                        PageAndPagerankDifference(source, pagerankDiff)
                    } else null
                }
            }?.mapNotNull { it.await() }

            if (doneDocs != null && doneDocs.isNotEmpty()) {
                newElastic.indexDocsBulk(doneDocs.map { it.page })
                maxPagerankDiff = listOf(doneDocs.maxOf { it.pagerankDifference }).maxOf { it }
            }

            pagination += 1
        } while (docs?.isNotEmpty() == true)

        switchAliasesAndDeleteOldIndex()

        return@coroutineScope maxPagerankDiff
    }

    data class PageAndPagerankDifference(val page: Page.PageType, val pagerankDifference: Double)

}