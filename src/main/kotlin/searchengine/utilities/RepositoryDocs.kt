package searchengine.utilities

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.produce
import libraries.PageRepository
import searchengine.DocBatchSize
import searchengine.DocCount
import kotlin.math.roundToInt
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

@OptIn(ExperimentalCoroutinesApi::class, ExperimentalTime::class)
fun CoroutineScope.repositoryDocs(client: PageRepository.Client): ReceiveChannel<PageRepository.Page> =
    produce(capacity = 300) {
        val limit = DocCount

        var lastUrl: String? = null
        var ctr = 0

        while (ctr < limit) {
            val (finds: List<PageRepository.Page>, duration: Duration) = measureTimedValue {
                client.findAfter(lastUrl, DocBatchSize, code = 200)
            }
            if (finds.isEmpty()) break
            finds.forEach { send(it) }
            lastUrl = finds.last().finalUrl
            ctr += finds.size
            println("$ctr docs - ${(ctr.toDouble() / limit.toDouble() * 100.0).roundToInt()}%, took: ${duration.inWholeMinutes}min ${duration.inWholeSeconds % 60}s")
        }

    }