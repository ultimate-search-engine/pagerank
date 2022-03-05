package searchengine

import co.elastic.clients.elasticsearch.core.search.Hit
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import libraries.Elastic
import libraries.Page
import searchengine.pagerank.PagerankCompute
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue


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

    private val targetDeviation = (1.0 / allDocsCount) * PRECISION

    enum class ComputeMethod {
        BatchDocs,
        InMemory
    }

    suspend fun computePagerank(method: ComputeMethod) {
        when (method) {
            ComputeMethod.BatchDocs -> chunkedPagerankIteration()
            ComputeMethod.InMemory -> doWholePagerankCalculationInMemory()
        }
    }


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


    private suspend fun normalizeDocs() = coroutineScope {
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
            println("Normalization batch: $pagination")

            val doneDocs = doBatch(docs)
            if (doneDocs.isNotEmpty()) newElastic.indexDocsBulk(doneDocs)
        }
        switchAliasesAndDeleteOldIndex()
        println("Normalization done\n")
    }


    @OptIn(ExperimentalTime::class)
    private suspend fun chunkedPagerankIteration() { // super slow smh
        normalizeDocs()
        var i = 0
        do {
            val (maxPagerankDiff: Double, time: Duration) = measureTimedValue {
                elasticRank(allDocsCount).doChunkedPagerankIteration()
            }
            println(
                "\nHighest pagerank deviation: $maxPagerankDiff / $targetDeviation, (${
                    ((maxPagerankDiff / targetDeviation) * 100).round(2)
                }%)"
            )
            println("Iteration $i took ${time.inWholeMinutes}min ${time.inWholeSeconds % 60}s\n\n")
            i += 1
        } while (maxPagerankDiff > (1.0 / allDocsCount) * PRECISION)

    }


    private suspend fun doChunkedPagerankIteration() = coroutineScope {
        val newMapping = async { newElastic.putMapping(6) }
        val globalSinkRankAsync = async { oldElastic.getGlobalSinkRank() }
        newMapping.await()

        val globalSinkRank = globalSinkRankAsync.await()
        val pagerankCompute = PagerankCompute()

        val maxDiff: List<Double> = iterateDocsInBatch { docs, pagination ->
            println("Batch: $pagination")

            val backDocs = getBatchBackDocsAsMap(docs)
            val doneDocs = docs.mapNotNull { doc ->
                val source = doc.source()
                if (source != null) {
                    rankPage(source, pagerankCompute, backDocs, globalSinkRank)
                } else null
            }

            if (doneDocs.isNotEmpty()) newElastic.indexDocsBulk(doneDocs.map { it.page })
            return@iterateDocsInBatch doneDocs.maxOf { it.pagerankDifference }
        }
        switchAliasesAndDeleteOldIndex()
        return@coroutineScope maxDiff.maxOf { it }
    }


    private fun rankPage(
        source: Page.PageType,
        pagerankCompute: PagerankCompute,
        backDocs: Map<String, Page.PageType>,
        globalSinkRank: Double
    ): PageAndPagerankDifference {
        val pagerank = pagerankCompute.getPagerank(source, backDocs, globalSinkRank, allDocsCount)

        if (pagerank.isNaN()) {
            // cause most likely is that the backlink is indexed more than once
            println("Pagerank of ${source.address.url} is NaN, the database is probably corrupted")
        }
        val pagerankDiff = kotlin.math.abs(pagerank - source.inferredData.ranks.pagerank)

        source.inferredData.ranks.pagerank = pagerank
        source.inferredData.ranks.smartRank = pagerank * allDocsCount

        return PageAndPagerankDifference(source, pagerankDiff)
    }


    private suspend fun doWholePagerankCalculationInMemory() = coroutineScope {
        val newMapping = async { newElastic.putMapping(6) }
        newMapping.await()

        val pagerankCompute = PagerankCompute()
        var docsMap: MutableMap<String, Page.PageType> = mutableMapOf()

        iterateDocsInBatch { docs, _ ->
            docs.forEach {
                // could request only for important fields to save memory
                val source = it.source()
                if (source != null) {
                    source.inferredData.ranks.pagerank = 1.0 / allDocsCount
                    docsMap[source.address.url] = source
                }
            }
        }

        var maxDiff: Double
        var iteration = 0

        do {
            iteration += 1
            maxDiff = 0.0
            val globalSinkRank =
                docsMap.values.sumOf { if (it.body.links.external.isEmpty() && it.body.links.internal.isEmpty()) it.inferredData.ranks.pagerank else 0.0 } / allDocsCount

            docsMap = docsMap.mapNotNull { (_, source) ->
                val rankedSrc = rankPage(source, pagerankCompute, docsMap, globalSinkRank)
                if (rankedSrc.pagerankDifference > maxDiff) maxDiff = rankedSrc.pagerankDifference
                rankedSrc.page.address.url to rankedSrc.page
            }.toMap().toMutableMap()

            println("Iteration $iteration: Highest pagerank deviation: $maxDiff, ${((maxDiff / targetDeviation) * 100).round(2)}%)")

        } while (maxDiff > targetDeviation)

        docsMap.toList().map { it.second }.chunked(10_000).forEachIndexed { i, it ->
            println("Indexed ${it.size * i} - ${((it.size * i).toDouble() / allDocsCount * 100).round(2)}% docs")
            newElastic.indexDocsBulk(it)
        }
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
                println("At: ${(pagination.toDouble() * batchSize / allDocsCount * 100).round(2)}%")
                val res = async { oldElastic.searchAfterUrl(batchSize, lastUrl) }
                val docs = res.await().hits().hits()

                lastUrl = docs?.lastOrNull()?.source()?.address?.url
                if (docs.isNotEmpty()) results.add(function(docs, pagination))

                pagination += 1
            } while (docs?.isNotEmpty() == true)

            return@coroutineScope results
        }
}
