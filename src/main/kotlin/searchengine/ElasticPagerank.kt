package searchengine

import co.elastic.clients.elasticsearch.core.search.Hit
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
    private val allDocsCount: Long,
    private val batchSize: Long = 400
) {

    private suspend fun switchAliasesAndDeleteOldIndex() {
        val oldIndex = oldElastic.alias.getIndexByAlias(defaultIndex)

        try {
            oldElastic.alias.delete(defaultIndex)
        } catch (e: Exception) {
            println("Alias not found")
        }

        delay(1500)

        oldIndex.forEach { oldElastic.deleteIndex(it) }
        newElastic.alias.create(defaultIndex)
    }


    suspend fun normalizeDocs() = coroutineScope {
        newElastic.putMapping()

        fun doBatch(docs: List<Hit<Page.PageType>>): List<Page.PageType> {
            return docs.mapNotNull {
                val source = it.source()
                if (source != null) {
                    source.inferredData.ranks.pagerank = 1.0 / allDocsCount
                    source.inferredData.ranks.smartRank = 1.0 / allDocsCount
                    source
                } else null
            }
        }

        iterateDocsInBatch { docs, pagination ->
            val done = ((pagination * batchSize).toDouble() / allDocsCount * 100).round(2)
            println("Normalization batch: $pagination - ~$done%")

            val doneDocs = doBatch(docs)
            if (doneDocs.isNotEmpty()) newElastic.indexDocsBulk(doneDocs)
        }

        switchAliasesAndDeleteOldIndex()
        println("Normalization done\n")
    }


    suspend fun doPagerankIteration() = coroutineScope {
        val newMapping = async { newElastic.putMapping(6) }
        val globalSinkRankAsync = async { oldElastic.getGlobalSinkRank() }
        newMapping.await()
        val globalSinkRank = globalSinkRankAsync.await()

        val pagerankCompute = PagerankCompute()

        val maxDiff: List<Double> = iterateDocsInBatch { docs, pagination ->
            val done = ((pagination * batchSize).toDouble() / allDocsCount * 100).round(2)
            println("Batch: $pagination - ~$done%")

            val backDocs = getBatchBackDocsAsMap(docs)

            val doneDocs = docs.mapNotNull { doc ->
                val source = doc.source()
                if (source != null) {
                    val pagerank = pagerankCompute.getPagerank(source, backDocs, globalSinkRank, allDocsCount)

                    if (pagerank.isNaN()) {
                        // cause most likely is that the backlink is indexed more than once
                        println("Pagerank of ${source.address.url} is NaN, the database is probably corrupted")
                    }
                    val pagerankDiff = kotlin.math.abs(pagerank - source.inferredData.ranks.pagerank)

                    source.inferredData.ranks.pagerank = pagerank
                    source.inferredData.ranks.smartRank = pagerank * allDocsCount

                    PageAndPagerankDifference(source, pagerankDiff)
                } else null
            }

            if (doneDocs.isNotEmpty()) newElastic.indexDocsBulk(doneDocs.map { it.page })
            return@iterateDocsInBatch doneDocs.maxOf { it.pagerankDifference }
        }

        switchAliasesAndDeleteOldIndex()
        return@coroutineScope maxDiff.maxOf { it }
    }

    private fun getBatchBackDocsAsMap(docs: List<Hit<Page.PageType>>): Map<String, Page.PageType> {
        val links =
            docs.mapNotNull { doc -> doc.source()?.inferredData?.backLinks?.map { it.source } }.flatten().distinct()
        val backDocs = oldElastic.docsByUrlOrNullBulk(links)
        val responses =
            backDocs?.responses()?.mapNotNull { it.result().hits().hits().firstOrNull()?.source() } ?: emptyList()
        return responses.associateBy { it.address.url }
    }

    data class PageAndPagerankDifference(val page: Page.PageType, val pagerankDifference: Double)


    private suspend fun <T> iterateDocsInBatch(function: suspend (docs: List<Hit<Page.PageType>>, pagination: Long) -> T): List<T> =
        coroutineScope {
            var lastUrl: String? = null
            var pagination: Long = 0
            val results = mutableListOf<T>()

            do {
                val res = async { oldElastic.searchAfterUrl(batchSize, lastUrl) }
                val docs = res.await().hits().hits()

                lastUrl = docs?.lastOrNull()?.source()?.address?.url
                if (docs.isNotEmpty()) results.add(function(docs, pagination))

                pagination += 1
            } while (docs?.isNotEmpty() == true)

            return@coroutineScope results
        }
}
