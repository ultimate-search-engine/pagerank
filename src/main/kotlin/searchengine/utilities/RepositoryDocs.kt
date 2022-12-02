package searchengine.utilities

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.delay
import libraries.PageRepository
import searchengine.DocBatchSize
import searchengine.DocCount
import kotlin.math.roundToInt
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.TimedValue
import kotlin.time.measureTimedValue

@OptIn(ExperimentalCoroutinesApi::class, ExperimentalTime::class)
fun CoroutineScope.repositoryDocs(client: PageRepository.Client): ReceiveChannel<PageRepository.Page> =
    produce(capacity = DocBatchSize + 1) {
        val limit = DocCount

        var lastUrl: String? = null
        var ctr = 0

        while (ctr < limit) {
            val value: TimedValue<Unit> = measureTimedValue {
                try {
                    val finds: List<PageRepository.Page> = client.findAfter(lastUrl, DocBatchSize)
                    if (finds.isEmpty()) return@produce
                    finds.forEach { if (it.statusCode == 200) send(it) }
                    lastUrl = finds.last().finalUrl
                    ctr += finds.size
                } catch (e: Exception) {
                    println("Error: $e")
                    delay(10_000)
                }
            }
            println(
                "$ctr docs, took: ${value.duration.inWholeMinutes}min ${value.duration.inWholeSeconds % 60}s"
            )
        }

    }