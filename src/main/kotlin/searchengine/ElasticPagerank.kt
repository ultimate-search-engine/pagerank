package searchengine

import kotlinx.coroutines.*
import libraries.Elastic
import searchengine.pagerank.PagerankCompute


fun Double.round(decimals: Int): Double {
    var multiplier = 1.0
    repeat(decimals) { multiplier *= 10 }
    return kotlin.math.round(this * multiplier) / multiplier
}

class ElasticPagerank(
    private val oldElastic: Elastic, private val newElastic: Elastic, private val defaultIndex: String, private val batchSize: Long = 400
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

    suspend fun transferNormalizedDocs() = coroutineScope {
        var pagination: Long = 0
        var lastUrl: String? = null

        val allDocsCount = oldElastic.getAllDocsCount()
        newElastic.putMapping()

        do {
            println(
                "Normalization batch: $pagination - ~${
                    ((pagination * batchSize).toDouble() / allDocsCount * 100).round(2)
                }%"
            )

            val res = async { oldElastic.searchAfterUrl(batchSize, lastUrl) }
            val docs = res.await().hits().hits()

            lastUrl = docs?.lastOrNull()?.source()?.address?.url

            docs?.mapNotNull {
                val source = it.source()
                if (source != null) {
                    launch(Dispatchers.Unconfined) {
                        source.inferredData.ranks.pagerank = 1.0 / allDocsCount
                        newElastic.indexPage(source)
                    }
                } else null
            }?.map { it.join() }

            pagination += 1
        } while (docs?.isNotEmpty() == true)


        switchAliasesAndDeleteOldIndex()

        println("Normalization done\n")
    }

    suspend fun doPagerankIteration() = coroutineScope {
        val globalSinkRankAsync = async { oldElastic.getGlobalSinkRank() }
        val allDocsCountAsync = async { oldElastic.getAllDocsCount() }
        val newMapping = async { newElastic.putMapping(3) }
        val globalSinkRank = globalSinkRankAsync.await()
        val allDocsCount = allDocsCountAsync.await()
        newMapping.await()

        val pagerankCompute = PagerankCompute(oldElastic)
        var pagination: Long = 0

        var lastUrl: String? = null

        do {
            println("Pagerank batch: $pagination - ~${((pagination * batchSize).toDouble() / allDocsCount * 100).round(2)}%")

            val res = async { oldElastic.searchAfterUrl(batchSize, lastUrl) }
            val docs = res.await().hits().hits()

            lastUrl = docs?.lastOrNull()?.source()?.address?.url

            docs?.mapNotNull { doc ->
                val source = doc.source()
                if (source != null) {
                    launch(Dispatchers.Unconfined) {
                        // print("${source.inferredData.ranks.pagerank} - ${source.address.url} ")
                        var pagerank = pagerankCompute.getPagerank(source, globalSinkRank, allDocsCount)

                        if (pagerank.isNaN()) {
                            // cause most likely is that the backlink is indexed more than once and the integrity is broken
                            println("Pagerank of ${source.address.url} is NaN, substituting with previous value ${source.inferredData.ranks.pagerank}")
                            pagerank = source.inferredData.ranks.pagerank
                        }
                        source.inferredData.ranks.pagerank = pagerank
                        source.inferredData.ranks.smartRank = pagerank * allDocsCount
//                        println("doc id: ${doc.id()} pagerank: $pagerank, url: ${source.address.url}")
                        // println("pagerank: $pagerank")
                        newElastic.indexPage(source)
                    }
                } else null
            }?.forEach { it.join() }
            pagination += 1
        } while (docs?.isNotEmpty() == true)

        switchAliasesAndDeleteOldIndex()
    }

}