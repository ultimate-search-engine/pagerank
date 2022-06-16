package searchengine.utilities

import io.ktor.http.*
import org.jsoup.nodes.Document
import searchengine.pagerank.cUrl

fun Document.pageLinks(url: Url): List<String> {
    val links = this.select("a")

    return links.mapNotNull {
        val href = it.attr("href")
        try {
            if (href.startsWith("https")) {
                Url(href).cUrl()
            } else if (href.startsWith("/") && !href.startsWith("//")) {
                Url("${url.protocol.name}://${url.host}$href").cUrl()
            } else {
                null
            }
        } catch (e: Exception) {
//            println("Error parsing $href")
            null
        }
    }
}