package searchengine.searchIndexer

import io.ktor.http.*
import io.ktor.util.*
import libraries.Page
import libraries.PageRepository
import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import searchengine.BacklinkAnchorTextMaxCount
import searchengine.pagerank.Pagerank
import searchengine.pagerank.cUrl
import searchengine.utilities.meldSimilar

class ComputeDoc(
    private val pagerankPage: Pagerank.PagerankPage,
    repoDoc: PageRepository.Page,
    private val dbClient: PageRepository.Client,
    private val totalDocsCount: Int
) {
    private val parse = try {
        Jsoup.parse(repoDoc.content)
    } catch (e: Exception) {
        throw Exception("Error parsing page content: ${repoDoc.finalUrl}")
    }
    private val targetUrls = pagerankPage.backLinks.map { it.url } + pagerankPage.url

    private suspend fun getBacklinkAnchors(): List<Anchor> {
        var ctr = 0
        return backDocs().sortedByDescending { it.rank.last() }.mapNotNull { pagerankPage ->
            if (ctr >= BacklinkAnchorTextMaxCount) null
            else {
                val dbDoc = try {
                    dbClient.find(pagerankPage.url)
                } catch (e: Exception) {
                    null
                } ?: return@mapNotNull null

                val anchorText = anchorTextOnDoc(Jsoup.parse(dbDoc.content)).filter { it.anchor.count() in 3..72 }
                ctr += anchorText.size
                anchorText
            }
        }.flatten()
    }

    private fun backDocs(): List<Pagerank.PagerankPage> = pagerankPage.backLinks.map {
        if (it.doesForward) it.backLinks else mutableListOf(it)
    }.flatten().distinctBy { it.url }

    private fun anchorTextOnDoc(doc: Document): List<Anchor> {
        val links = doc.select("a")
        return links.mapNotNull {
            try {
                val url = Url(it.attr("href"))
                if (url.cUrl() in targetUrls) Anchor(url, it.text()) else null
            } catch (e: Exception) {
                null
            }
        }
    }

    data class Anchor(val url: Url, val anchor: String)


    suspend fun compute(): Page.Page {
        val url = Url(pagerankPage.url)
        val backlinkAnchors = getBacklinkAnchors()
        val backlinkParameters = backlinkAnchors.map { it.url.parameters.toMap().values.flatten() }.flatten()
        val urlParametersUnique = meldSimilar(backlinkParameters.distinct())

        return Page.Page(Page.Address(
            pagerankPage.url,
            meldSimilar(url.pathSegments.filter { it.count() >= 3 }.map { it.split(" ", "-", "_") }.flatten(), .80),
            url.host,
        ),
            Page.Ranks(
                pagerankPage.rank.last(),
                pagerankPage.rank.last() * totalDocsCount,
                urlLength = url.cUrl().count(),
                urlPathLength = url.fullPath.count(),
                urlSegmentsCount = url.pathSegments.count(),
                urlParameterCount = backlinkParameters.count() + 1,
                urlParameterCountUnique = urlParametersUnique.size + 1,
                urlParameterCountUniquePercent = (urlParametersUnique.size.toDouble() + 1) / (backlinkParameters.count() + 1),
                totalDocsCount
            ),
            Page.Content(
                parse?.title() ?: "",
                parse?.select("meta[name=description]")?.attr("content") ?: "",
                listOf(), // possibly more words that may be relevant
                backlinkAnchors.map {it.anchor}, // inner text of backlinks to this page
                parse?.select("b")?.map { it.text() } ?: listOf(),
                Page.Headings(meldSimilar(parse?.select("h1")?.map { it.text() } ?: listOf(), .80),
                    meldSimilar(parse?.select("h2")?.map { it.text() } ?: listOf(), .80),
                    meldSimilar(parse?.select("h3")?.map { it.text() } ?: listOf(), .80),
                    meldSimilar(parse?.select("h4")?.map { it.text() } ?: listOf(), .80),
                    meldSimilar(parse?.select("h5")?.map { it.text() } ?: listOf(), .80),
                    meldSimilar(parse?.select("h6")?.map { it.text() } ?: listOf(), .80)),
                meldSimilar(parse?.select("p")?.mapNotNull { if (it.text().count() > 40) it.text() else null }
                    ?: listOf(), .80),
            ))
    }


}