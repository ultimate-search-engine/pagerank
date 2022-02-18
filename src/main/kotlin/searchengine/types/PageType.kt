package searchengine.types

@kotlinx.serialization.Serializable
open class Metadata(
    open val title: String,
    open val description: String,
    open val openGraphImgURL: String,
    open val openGraphTitle: String,
    open val openGraphDescription: String,
    open val type: String,
    open val tags: List<String>,
) {
    constructor() : this("", "", "", "", "", "", listOf())
}

@kotlinx.serialization.Serializable
open class Headings(
    open val h1: List<String>,
    open val h2: List<String>,
    open val h3: List<String>,
    open val h4: List<String>,
    open val h5: List<String>,
    open val h6: List<String>,
) {
    constructor() : this(listOf(), listOf(), listOf(), listOf(), listOf(), listOf())
}

@kotlinx.serialization.Serializable
open class ForwardLink(
    open val text: String,
    open val href: String,
) {
    @Suppress("unused")
    constructor() : this("", "")
}

@kotlinx.serialization.Serializable
open class BodyLinks(
    open val internal: List<ForwardLink>,
    open val external: List<ForwardLink>,
) {
    constructor() : this(listOf(), listOf())
}

@kotlinx.serialization.Serializable
open class Body(
    open val headings: Headings,
    open val boldText: List<String>,
    open val article: List<String>,
    open val links: BodyLinks,
) {
    constructor() : this(Headings(), listOf(), listOf(), BodyLinks())
}

@kotlinx.serialization.Serializable
open class BackLink(
    open val text: String,
    open val source: String,
) {
    @Suppress("unused")
    constructor() : this("", "")
}

@kotlinx.serialization.Serializable
open class Ranks(
    @Suppress("unused")
    open var pagerank: Double,
    @Suppress("unused")
    open var computeRoundId: Long?,
    @Suppress("unused")
    open val smartRank: Double?,
) {
    constructor() : this(0.0, null, null)
}

@kotlinx.serialization.Serializable
open class InferredData(
    open var backLinks: List<BackLink>,
    open val ranks: Ranks,
    open var domainName: String,
) {
    constructor() : this(listOf(), Ranks(), "")
//    constructor(backLinks: List<BackLink>) : this(backLinks, Ranks(), null, "")
}

enum class CrawlerStatus {
    Crawled,
    NotCrawled,
    AwaitingPagerank,
    DoesNotExist,
    Error,
}

@kotlinx.serialization.Serializable
open class Address(
    @Suppress("unused")
    open val url: String,
    @Suppress("unused")
    open var urlAsText: List<String>,
) {
    constructor() : this("", listOf())
}

@kotlinx.serialization.Serializable
open class PageType(
    open val metadata: Metadata,
    open val body: Body,
    open val inferredData: InferredData,

    open val address: Address,
    open val crawlerTimestamp: Long = System.currentTimeMillis(),
    open val crawlerStatus: CrawlerStatus,
) {
    constructor() : this(
        Metadata(),
        Body(),
        InferredData(),
        Address(),
        System.currentTimeMillis(),
        CrawlerStatus.NotCrawled
    )
}

