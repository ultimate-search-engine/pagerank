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
    private val oldElastic: Elastic, private val newElastic: Elastic, private val defaultIndex: String
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
        val batchSize: Long = 100
        var pagination: Long = 0

        val allDocsCount = oldElastic.getAllDocsCount()
        newElastic.putMapping()

        do {
            println(
                "Normalization batch: $pagination - ~${
                    ((pagination * batchSize).toDouble() / allDocsCount * 100).round(
                        2
                    )
                }%"
            )

            val docs = oldElastic.getDocs(batchSize, pagination * batchSize)

            docs?.mapNotNull {
                val source = it.source()
                if (source != null) {
                    launch(Dispatchers.Unconfined) {
                        source.inferredData.ranks.pagerank = 1.0 / allDocsCount
                        launch(Dispatchers.Unconfined) { newElastic.indexPage(source) }.join()
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
        val batchSize: Long = 400
        var pagination: Long = 0

        (0..(allDocsCount / batchSize)).map {
            println("Pagerank batch: $pagination - ~${((pagination * batchSize).toDouble() / allDocsCount * 100).round(2)}%")

            val res = async { oldElastic.getDocs(batchSize, pagination * batchSize) }
            val docs = res.await()

            docs?.mapNotNull { doc ->
                val source = doc.source()
                if (source != null) {
                    launch(Dispatchers.Unconfined) {
                        // print("${source.inferredData.ranks.pagerank} - ${source.address.url} ")
                        var pagerank = withContext(Dispatchers.Default) {
                            pagerankCompute.getPagerank(source, globalSinkRank, allDocsCount)
                        }
                        if (pagerank.isNaN()) {
                            // cause most likely is that the backlink is indexed more than once
                            println("Pagerank of ${source.address.url} is NaN, substituting with previous value ${source.inferredData.ranks.pagerank}")
                            pagerank = source.inferredData.ranks.pagerank
                        }
                        source.inferredData.ranks.pagerank = pagerank
                        source.inferredData.ranks.smartRank = pagerank
//                        println("doc id: ${doc.id()} pagerank: $pagerank, url: ${source.address.url}")
                        // println("pagerank: $pagerank")
                        withContext(Dispatchers.Default) {
                            newElastic.indexPage(source)
                        }
                    }
                } else null
            }?.forEach { it.join() }
            pagination += 1
        }

        switchAliasesAndDeleteOldIndex()
    }

}