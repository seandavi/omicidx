# Comparable resource papers — models for an OmicIDX paper

Research note. Ten published papers describing resources similar to OmicIDX,
compared for structure, figures, evidence and availability practice, ending
with a recommended outline for an OmicIDX paper. Intended home in the repo:
`docs/research/comparable-resource-papers.md`.

Sources: every citation was checked against Europe PMC's REST API (PMID/PMCID/DOI)
or the publisher/arXiv record. Full text was read from PMC, Europe PMC JATS XML,
nature.com, genomebiology.biomedcentral.com and arxiv.org. Venue rules were read
from the journals' author-guideline pages where reachable. OUP pages
(`academic.oup.com`) returned HTTP 403 to direct fetch, so OUP guideline text
comes from search-engine extracts of those pages and is marked *(extract)*.
Researched 2026-10-31.

Labels used below: **[read]** = stated in the source; **[inferred]** = my
interpretation, not stated in the source; **[unverified]** = could not open or
confirm.

---

## Bottom line

- **No prior OmicIDX paper or preprint exists.** Europe PMC full-text search for
  "OmicIDX" returns one hit, and OpenAlex full-text search returns one. Neither
  describes OmicIDX; both are downstream users (see §3). The closest thing to a
  prior description is the 2018 Bioconductor workshop chapter, which mentions
  `Omicidx` as an entry point in `SRAdbV2`. That chapter is not a journal
  article.
- **The baseline every reviewer will expect:** a "repository X is hard to
  query" motivation, a named list of predecessors, an architecture or pipeline
  figure, a schema/data-model figure, worked example queries, scale numbers,
  and an Availability section (URL, license, code, update cadence). All ten
  papers have most of these.
- **What sets the strong papers apart:** a quantitative comparison table
  against alternatives (ARCHS4 Table 1), cost and throughput tables (recount3
  Tables 1–2, ARCHS4 Fig. 4), validation against a ground truth (MetaSRA,
  CELLxGENE), named downstream uses (CELLxGENE), archival DOIs for code and data
  (recount3, pysradb), and an explicit statement about schema versioning
  (CELLxGENE).
- **None of the ten papers offers what OmicIDX can:** immutable dated
  snapshots, each with a provenance manifest, queryable anonymously over HTTPS
  with no server, account or billing. The official cloud metadata (SRA on
  BigQuery) needs a Google Cloud account and project, and the user pays for
  queries. **This is the paper's novelty claim; the evidence below should be
  built around it.**
- **Recommended venue:** the *NAR* Database issue if the maintainer can make
  the 1 July presubmission deadline (the next one is 2027 [inferred from the
  annual cycle]). Otherwise a *Bioinformatics* Application Note, which is
  GEOmetadb's own lineage. Constraints are in §4.3.

---

## 1. Summary table

| # | Citation | Venue / type | One line | Why relevant to OmicIDX |
|---|---|---|---|---|
| 1 | Zhu Y, Davis S, Stephens R, Meltzer PS, Chen Y. **GEOmetadb: powerful alternative search engine for the Gene Expression Omnibus.** *Bioinformatics* 2008;24(23):2798–2800. doi:10.1093/bioinformatics/btn520. PMID 18842599, PMC2639278 | *Bioinformatics* **Applications Note** (Data and Text Mining) [read: JATS subject] | GEO metadata parsed into MySQL plus a downloadable SQLite file, with an R package | Direct predecessor; OmicIDX ships `geometadb.*` marts |
| 2 | Zhu Y, Stephens RM, Meltzer PS, Davis SR. **SRAdb: query and use public next-generation sequencing data from within R.** *BMC Bioinformatics* 2013;14:19. doi:10.1186/1471-2105-14-19. PMID 23323543, PMC3560148 | *BMC Bioinformatics*, software-style article (it has an "Availability and requirements" block) [type label inferred] | SRA XML parsed into a regularly updated SQLite file with an R/Bioconductor package | Direct predecessor; OmicIDX ships `sradb.*` marts |
| 3 | Choudhary S. **pysradb: A Python package to query next-generation sequencing metadata and data from NCBI Sequence Read Archive.** *F1000Research* 2019;8:532. doi:10.12688/f1000research.18676.1. PMID 31114675, PMC6505635 | *F1000Research* **Software Tool Article**, v1, 2 peer reviews approved [read] | Python command-line tool built on SRAmetadb.sqlite | Shows what happens downstream when the SQLite build stops; a peer for the comparison table |
| 4 | Gálvez-Merchán Á, Min KHJ, Pachter L, Booeshaghi AS. **Metadata retrieval from sequence databases with ffq.** *Bioinformatics* 2023;39(1):btac667. doi:10.1093/bioinformatics/btac667. PMID 36610997, PMC9883619 | *Bioinformatics*; Europe PMC classes it "brief-report" (an Application Note-sized paper) [type label inferred] | Per-accession metadata fetcher over live APIs, returning JSON | The live-API alternative; it argues directly against "regular database builds" |
| 5 | Bernstein MN, Doan A, Dewey CN. **MetaSRA: normalized human sample-specific metadata for the Sequence Read Archive.** *Bioinformatics* 2017;33(18):2914–2923. doi:10.1093/bioinformatics/btx334. PMID 28535296, PMC5870770 | *Bioinformatics* **Original Paper** (Data and Text Mining) [read: JATS subject] | Ontology-normalized sample metadata built from an SRAdb snapshot | A consumer of SRAdb; a model for validation; complements OmicIDX's "faithful parse, no curation" stance |
| 6 | Lachmann A, Torre D, Keenan AB, et al., Ma'ayan A. **Massive mining of publicly available RNA-seq data from human and mouse.** *Nat Commun* 2018;9:1366. doi:10.1038/s41467-018-03751-6. PMID 29636450, PMC5893633 | *Nature Communications* Article | ARCHS4: uniformly reprocessed GEO/SRA RNA-seq, with bulk H5 downloads and a web UI | The best comparison-table and cost-evidence model in the set |
| 7 | Wilks C, Zheng SC, Chen FY, et al., Langmead B. **recount3: summaries and queries for large-scale RNA-seq expression and splicing.** *Genome Biol* 2021;22:323. doi:10.1186/s13059-021-02533-6. PMID 34844637, PMC8628444 | *Genome Biology* (Europe PMC: research-article; section label not verified) | 750k+ reprocessed SRA/GTEx/TCGA runs, with R packages and a query service | Model for throughput/cost tables, archival DOIs and "reproducible from raw" claims |
| 8 | Katz K, Shutov O, Lapoint R, Kimelman M, Brister JR, O'Sullivan C. **The Sequence Read Archive: a decade more of explosive growth.** *Nucleic Acids Res* 2022;50(D1):D387–D390. doi:10.1093/nar/gkab1053. PMID 34850094, PMC8728234 | *NAR* **Database issue** update, 4 pages | NCBI's own update: SRA data in the cloud, with metadata in BigQuery and Athena | The official "cloud metadata" alternative; also shows how short a NAR DB update can be |
| 9 | Priem J, Piwowar H, Orr R. **OpenAlex: A fully-open index of scholarly works, authors, venues, institutions, and concepts.** arXiv:2205.01833 (2022); STI 2022 conference, Granada. doi:10.48550/arXiv.2205.01833 | Conference paper / preprint of record, 5 pages | Fully open replacement for the discontinued Microsoft Academic Graph: full dump, API, no registration | The same story from another field: a successor to a discontinued resource, with an open snapshot and no login |
| 10 | CZI Cell Science Program, Abdulla S, Aevermann B, et al. **CZ CELLxGENE Discover: a single-cell data platform for scalable exploration, analysis and modeling of aggregated data.** *Nucleic Acids Res* 2025;53(D1):D886–D900. doi:10.1093/nar/gkae1142. PMID 39607691, PMC11701654 | *NAR* **Database issue**, 15 pages | Curated single-cell corpus plus "Census", a cloud-hosted data object queried from R and Python | Closest to an analysis-ready cloud snapshot queried without a server; shows schema-versioning and downstream-use practice |

**Not counted in the ten, but should be cited in an OmicIDX paper:**

- **Thalhath N. "Revisiting SRAmetadb.sqlite."** BioHackrXiv preprint, 2024.
  doi:10.37044/osf.io/xvrcm (Europe PMC PPR904222). I read the abstract only.
  It says SRAmetadb.sqlite "has not been regularly updated, with the last
  refresh occurring in late 2023", that "no public tools exist to rebuild or
  update this database", and it mentions DuckDB integration. **This is the
  most direct independent evidence of the gap OmicIDX fills.**
- **pysradb HISTORY.md, 3.0.0 (unreleased):** "All local SQLite database
  support has been removed … SRAweb (API-based) provides better, real-time
  data" (https://raw.githubusercontent.com/saketkc/pysradb/develop/HISTORY.md).
  This is not a paper, but it documents that the SQLite path is dead
  downstream as well.
- **Davis S, Meltzer PS. GEOquery.** *Bioinformatics* 2007;23(14):1846–1847.
  doi:10.1093/bioinformatics/btm254, PMID 17496320. Only the Europe PMC
  metadata record was opened; the full text is not open access. It is listed
  as lineage only, and nothing below is extracted from it.

---

## 2. Per-paper extraction

### 2.1 GEOmetadb (Zhu et al. 2008) — doi:10.1093/bioinformatics/btn520

- **Venue/type:** *Bioinformatics* Applications Note, 3 printed pages
  (2798–2800) [read, from the JATS `<subject>` and page range]. The current
  limit for this article type is 4 pages, about 2,600 words *(extract,
  Bioinformatics author guidelines)*.
- **Sections:** Abstract (with Availability and Contact) → 1 Introduction →
  2 Results (2.1 GEO metadata parsing; 2.2 GEOmetadb Bioconductor package;
  2.3 The GEOmetadb online search tool) → 3 Conclusions → Acknowledgements
  (Funding) [read].
- **Figures/tables:** Fig. 1 is a diagram of GEO entity relationships (an ERD).
  Fig. 2 is a screenshot of the combined GSE-GPL-GSM web search. Two code
  examples are inline images (`geoConvert`, and a `dbGetQuery` that finds
  Affymetrix samples with CEL supplementary files) [read].
- **How value is shown:** worked queries only. There are no benchmarks, usage
  numbers or comparison table. The claim is qualitative: "more flexible and
  efficient" than NCBI web search and eUtils [read, §3].
- **Availability/cadence/funding:** web interface and SQLite download at
  `gbnci.abcc.ncifcrf.gov/geo/`; the package is on Bioconductor; a MATLAB
  version is also offered. The SQLite file is one "we update regularly",
  without a stated cadence [read, §1]. Funding: NIH/NCI Intramural Research
  Program [read]. No data license is stated (the article itself is CC BY-NC).
- **Reproducibility/provenance:** "All data in GEOmetadb are faithfully parsed
  from GEO and no attempt is made to curate, semantically recode, or otherwise
  clean up GEO data. All field names are also taken from GEO records except
  for minor changes"; multi-valued fields are "stored as delimited text"
  (denormalized) [read, §2.1]. The paper has no versioning or snapshot story.
- **Borrow:** reuse the "faithfully parsed, no curation" sentence, now with
  provenance attached. Lead with an ERD. Keep the paper short. **Note for the
  Introduction [inferred from Bioconductor support/GitHub search results, not
  opened in full]:** users report the GEOmetadb SQLite file as outdated (2023)
  and failing to download (404s in 2022–2024).

### 2.2 SRAdb (Zhu et al. 2013) — doi:10.1186/1471-2105-14-19

- **Venue/type:** *BMC Bioinformatics*. Structured abstract (Background /
  Results / Conclusions) and an "Availability and requirements" block, which
  is the BMC software-article layout [type label inferred; Europe PMC tags it
  "product-review"].
- **Sections:** Background → Design → Implementation → Results and discussion
  (Installation; Example usage → Full text search, Arbitrary SQL queries,
  Retrieving data file information; Interface to IGV) → Conclusions →
  Availability and requirements → Competing interests → Authors' contributions
  → Additional file 1 [read].
- **Figures/tables:** no figures. Table 1 lists the IGV-control functions.
  Additional file 1 holds the SQLite column descriptions [read]. Example
  outputs are printed inline (for example, run counts by `library_strategy`).
- **How value is shown:** worked R and SQL examples (FTS3 full-text search,
  `GROUP BY library_strategy`, fastq listing and download). There are no
  benchmarks or usage statistics [read].
- **Availability/cadence:** Artistic-2.0 license; Bioconductor package;
  standalone SQLite at `gbnci.abcc.ncifcrf.gov/backup/SRAmetadb.sqlite.gz`.
  "Since SRA is a continuously growing repository, the SRAdb SQLite file is
  updated regularly" [read]. No cadence, versioning or funding statement is
  given.
- **Reproducibility/provenance:** the same "faithfully parsed … no attempt is
  made to curate" policy and denormalization as GEOmetadb [read,
  Implementation]. The parser was PHP and is not published in the paper.
- **Borrow:** the column-description supplement, which becomes the data
  dictionary. Also SRAdb's point that the file "can be used by any software
  with the ability to connect to a SQLite database" [read, Conclusions];
  OmicIDX's equivalent is "any Parquet/DuckDB client".
- **Lesson [inferred]:** SRAdb and GEOmetadb made no promise about cadence or
  sustainability. When the build host's cron job stalled, downstream tools
  broke (pysradb, MetaSRA's input). An OmicIDX paper should state cadence,
  retention, archive DOIs and the funding or sustainability model explicitly.

### 2.3 pysradb (Choudhary 2019) — doi:10.12688/f1000research.18676.1

- **Venue/type:** *F1000Research* Software Tool Article, v1, "peer review: 2
  approved" [read]. Earlier version on bioRxiv, doi:10.1101/578500 [read].
- **Sections:** Abstract → Introduction → Methods (Implementation; Operation)
  → Use cases (Search; Getting metadata for a SRA project; Getting SRPs from
  GSE; Getting a list of GEO experiments for a GEO study; downloading subsets)
  → Summary → Data availability (underlying data, with identifiers.org links
  to every accession used) → Software availability [read]. This matches the
  F1000 Software Tool template: Introduction / Methods (Implementation,
  Operation) / Results (optional) / Use Cases / Conclusions, with a structured
  abstract of up to 300 words [read, F1000 guidelines page].
- **Figures/tables:** none as figures. All "tables" are command output
  printed inline [read; no "Figure" occurs in the text].
- **How value is shown:** use cases only (for example, Ribo-seq search,
  GSE→SRP conversion, `--expand` of sample attributes into columns).
- **Availability:** BSD 3-Clause; PyPI and bioconda; code archived on Zenodo,
  doi:10.5281/zenodo.2579446; CI on Travis [read]. It pins its data input
  precisely: "SRAmetadb.sqlite with schema version 1.0 and creation timestamp
  2019-01-25 00:38:19" [read].
- **Borrow:** that pinning sentence is exactly what OmicIDX's `manifest.json`
  and DOI'd monthly releases give a user. Show how to write "OmicIDX
  v{date}/manifest X" in a methods section. Also borrow the identifiers.org
  data citations and a Zenodo DOI for the code at submission.

### 2.4 ffq (Gálvez-Merchán et al. 2023) — doi:10.1093/bioinformatics/btac667

- **Venue/type:** *Bioinformatics* 39(1), a short note with a structured
  abstract (Motivation / Results / Availability and implementation) [read].
- **Sections:** 1 Introduction → 2 Description → 3 Usage and documentation →
  4 Discussion → Acknowledgements → Funding → Data availability [read].
- **Figures:** Fig. 1 shows the database hierarchy traversal across
  SRA/ENA/DDBJ/GEO/ENCODE. Fig. 2 is a use-case pipeline (ffq → kb-python →
  count matrix) [read].
- **How value is shown:** code-quality numbers ("58 functions and 2198 lines
  of code across six files … only four software dependencies"; tests "cover
  78% of the code" across five Python versions) and throughput ("ffq can
  download metadata at a rate of 10 s per sample", including deliberate
  delays "to prevent a perceived Denial-of-Service") [read, §2].
- **The argument OmicIDX must answer:** "By leveraging existing APIs, ffq
  offers a lightweight solution for querying data that is guaranteed to be
  more up-to-date than tools that rely on regular database builds" [read,
  §3]. It also lists the failure modes of API dependence: "The reliance on
  these external dependencies can make it challenging to track API updates"
  [read, §2].
- **Availability/funding:** GitHub, pip and conda; NIH U19MH114830 [read].
- **Borrow [inferred]:** answer the up-to-date claim with a measured
  freshness lag (NCBI publish → OmicIDX `latest/`). Contrast "10 s per
  sample" with one HTTPS-range query returning the whole cohort. Report code
  and test-coverage numbers the same way.

### 2.5 MetaSRA (Bernstein, Doan & Dewey 2017) — doi:10.1093/bioinformatics/btx334

- **Venue/type:** *Bioinformatics* Original Paper [read]. Original Papers are
  about 7 pages / 5,000 words *(extract)*.
- **Sections:** 1 Introduction → 2 Data → 3 Task definition → 4 Materials and
  methods → 5 Results (5.1 Evaluation of ontology mappings, …) → 6
  Summarizing the MetaSRA → 7 Discussion and future work → Funding [read].
- **Figures/tables:** Fig. 1 shows raw key-value examples (misspellings,
  negations) and a studies-by-size histogram. Fig. 2 is an example normalized
  sample. Fig. 3 is the pipeline's reasoning-graph data structure. Fig. 4
  gives metric definitions and evaluation results. Fig. 5 is classifier
  calibration. Fig. 6 shows corpus summaries (samples per term, most common
  terms). Table 1 gives counts of samples with each real-valued property
  [read].
- **How value is shown:** a held-out, manually normalized test set of 422
  samples, each from a unique study, drawn from recent submissions not seen
  during development; recall and error-rate metrics; a calibration plot
  [read, §5.1].
- **Provenance:** input pinned to an SRAdb snapshot, "downloaded on
  09/05/2016" [read, §2]. MetaSRA was therefore downstream of SRAdb.
- **Availability/funding:** web interface plus bulk downloads; pipeline on
  GitHub; NIH LM007359, AI117924, HG007019 [read].
- **Borrow:** for OmicIDX's Technical Validation, a held-out, reconciled
  sample. For example: pick N random accessions per source and check OmicIDX
  rows field by field against live NCBI E-utilities/XML [inferred design].
  Also cite MetaSRA as complementary normalization that can be built on
  OmicIDX.

### 2.6 ARCHS4 (Lachmann et al. 2018) — doi:10.1038/s41467-018-03751-6

- **Venue/type:** *Nature Communications* Article.
- **Sections:** Introduction → Results (The ARCHS4 website; RNA-seq alignment
  pipeline speed and cost; STAR vs. Kallisto comparison; Comparison to
  existing RNA-seq pipelines and resources; Read quality across institutions;
  Prediction of biological functions and protein interactions) → Discussion
  → Methods → Data availability [read].
- **Figures/tables:** Fig. 1 is growth of GEO/SRA RNA-seq samples against the
  Affymetrix U133 Plus 2 platform. Fig. 2 is a schematic of the cloud
  pipeline. Fig. 3 covers dimensionality-reduction checks and processing time
  per million reads. Fig. 4 plots total samples against total processing cost
  across resources. Fig. 5 shows percent aligned reads by institution. Fig. 6
  shows prediction AUCs. **Table 1 compares processed RNA-seq resources (size,
  cost per sample, access methods).** Table 2 compares prediction performance
  against CCLE and GTEx [read].
- **How value is shown:** dollar costs ("$0.00982" per SRA file; total $1,745
  against $44,785 for Recount and $25,910 for Toil), a benchmark against an
  alternative aligner, a feature comparison with named resources, and
  biological case studies [read].
- **Availability/licensing:** code under Apache 2.0. The data is "under the
  Creative Commons Attribution 4.0 International License … All data is free
  to use for non-commercial purposes" [read]. These two statements conflict.
  ARCHS4 says the low cost "enables a rolling update", but gives no cadence
  [read].
- **Borrow:** Table 1 (a feature comparison matrix against named
  alternatives) and Fig. 4 (scale vs. cost). **Avoid** its license ambiguity:
  OmicIDX should state one clear data license, separate from the code's MIT
  license [inferred].

### 2.7 recount3 (Wilks et al. 2021) — doi:10.1186/s13059-021-02533-6

- **Venue/type:** *Genome Biology*. Received 12 Jul 2021, accepted 29 Oct
  2021; the review history is published as Additional file 1 [read].
- **Sections:** Abstract → Introduction → Results (The recount3 resource;
  Human and mouse splicing in SRA; … The landscape of transcription) →
  Discussion → Methods (Monorail grid design, analyses) → Appendix (Detailed
  related work; Monorail details; Monorail cost and throughput; dbGaP access
  with Monorail; Comparison of recount2 and recount3; recount3 and snapcount R
  package interfaces) → Availability of data and materials → Funding [read].
- **Figures/tables:** Fig. 1 shows the resource and the
  orchestration/analysis/aggregation grid design. **Table 1 gives size and
  compute cost of Monorail runs. Table 2 gives performance on TACC, AWS and
  MARCC.** Later figures are biological use cases (splice-junction annotation
  status, cumulative junctions over time, PCA) [read].
- **How value is shown:** scale and cost ("990 TB of compressed sequencing
  reads, used approximately 3.3 node-years of computation (29K node-hours),
  and produced over 150 TB of summarized data"), three biological analyses,
  and a dedicated "comparison of recount2 and recount3" appendix [read].
- **Reproducibility/provenance:** "The Monorail system is available both as
  an open source suite of software, and as a self-contained public Docker
  image that produces identical results" [read]. Code is MIT with an archival
  Zenodo DOI (10.5281/zenodo.5576208) [read]. The paper acknowledges
  "incomplete sample metadata from SRA and GEO" and hand-curates a subset for
  its analyses [read].
- **Hosting:** SciServer-Compute and AnVIL ("avoiding any extensive
  downloading") [read].
- **Borrow:** a "changes from the predecessor" appendix (SRAdb/GEOmetadb →
  OmicIDX marts mapping); a throughput/cost table (nightly wall time, bytes
  published, R2 storage and egress cost); a Zenodo DOI for the pipeline at
  submission. recount3's metadata gap is also a use case to cite for OmicIDX
  [inferred].

### 2.8 SRA: a decade more of explosive growth (Katz et al. 2022) — doi:10.1093/nar/gkab1053

- **Venue/type:** *NAR* Database issue update, **4 pages** (D387–D390) [read].
- **Sections:** Abstract → Introduction → STAT Results → STAT challenges
  (reference contamination; viruses; heterologous updates; taxonomic model) →
  STAT opportunities → Conclusion → Data availability → Funding [read].
- **Figures:** a single Fig. 1, SRA growth over the decade (25.6 Pbp, 14.8M
  public runs as of September 2021) [read].
- **What it says about metadata:** a two-year effort made SRA records
  available on two clouds, "along with associated metadata in both Google
  Cloud Platform BigQuery and Amazon Web Services Athena" [read].
- **Access constraints (from NCBI docs, not the paper):** BigQuery "you will
  need to set up a Google cloud account" and a project; "The user pays for
  running queries against public data sets … Big Query provides 1TB per month
  for free" [read, https://www.ncbi.nlm.nih.gov/sra/docs/sra-bigquery/]. NCBI
  calls the service a pilot: "NCBI is piloting this in the cloud-based
  services" [read,
  https://www.ncbi.nlm.nih.gov/sra/docs/sra-cloud-based-examples/].
- **Provenance practice worth noting:** the taxonomy tables carry
  `parser_version` and `aligns_to_version` columns, because reanalysis is
  heterogeneous over time [read].
- **Borrow:** a **per-row or per-table version column** is a cheap provenance
  device. This paper is also the strongest argument for OmicIDX's anonymous,
  no-billing read path. **The comparison table must include "SRA metadata on
  BigQuery/Athena" as an alternative**, rated honestly (official, complete,
  but account plus pay-per-query, and SRA only, with no GEO SOFT fields)
  [last clause inferred; check the NCBI metadata table schema before claiming
  it].

### 2.9 OpenAlex (Priem, Piwowar & Orr 2022) — arXiv:2205.01833

- **Venue/type:** STI 2022 conference paper, 5 pages, posted to arXiv [read].
- **Sections:** Introduction → Schema and IDs → The OpenAlex entities (Works,
  Authors, Venues, Institutions, Concepts) → Open distribution → Limitations
  and future work → Acknowledgements [read].
- **Figures:** a single Fig. 1, a sketch of the graph data model [read].
- **Framing:** launched "as a drop-in replacement for MAG contemporaneously
  with MAG's retirement on January 1st 2022" [read]. This is the same
  successor story as OmicIDX and SRAdb/GEOmetadb [inferred].
- **Availability/cadence/sustainability:** "a full data dump (updated
  fortnightly), a REST API (updated daily), and a web-based GUI … All these
  are free and require no registration or permission"; above 100k requests
  per day, "the data dump is recommended instead". It is a project of a
  nonprofit and an "early adopter of The Principles of Open Scholarly
  Infrastructure (POSI)". Funding comes from Arcadia [read].
- **Value shown:** entity counts only, plus an explicit Limitations section
  ("much work remains to be done in validating … completeness and accuracy")
  [read].
- **Borrow:** a dedicated **"Open distribution"** section that states the
  cadence of each channel (rolling `latest/` daily, dated `v{date}/`, monthly
  DOI'd HuggingFace archive). A sustainability paragraph that names a
  governance principle (POSI or similar). An honest Limitations section. The
  "dump recommended above X load" guidance maps onto OmicIDX's note that HF is
  for archive and R2 is for querying.

### 2.10 CZ CELLxGENE Discover (CZI Cell Science Program et al. 2025) — doi:10.1093/nar/gkae1142

- **Venue/type:** *NAR* 2025 Database issue, 15 pages (D886–D900), with a
  graphical abstract [read].
- **Sections:** Abstract → Graphical Abstract → Introduction → Results
  (minimal cell-level schema; Schema evolution; Submission workflow; community
  resource; navigating the corpus; Explorer; Gene Expression; aggregation and
  batch effects; marker genes; Census) → Discussion → Materials and methods
  (including "TileDB-SOMA development for census") → Data availability →
  Funding ("No external funding") [read].
- **Figures/tables:** Fig. 1 shows corpus growth (unique cells over time) and
  the schema. Fig. 2 breaks the corpus down by modality, organism, tissue and
  demographics, including stated gaps. Fig. 3 shows UI examples. Fig. 4 is an
  ANOVA batch-effect validation. Fig. 5 is marker-gene recall against
  HuBMAP. Fig. 6 is a Census framework diagram. Table 1 lists the included
  assays [read].
- **How value is shown:** growth over time; validation against an external
  reference; laptop-scale claims ("can now be executed in a regular 8 GB
  memory laptop across 65 million cells"); **named downstream uses**
  (scGPT, UCDBase, scTab, CSeQTL); consortium adoption (BRAIN, HTAN, HCA)
  [read].
- **Cloud access:** Census is "a cloud-hosted data object hosted at Amazon's
  Open Data registry along with Python and R packages" and exports to Arrow,
  pandas and SingleCellExperiment [read]. Whether access is anonymous is not
  stated in the text I read [unverified].
- **Versioning/provenance:** a "minimal, versioned schema"; reviewed every 6
  months; "previously submitted data are migrated to meet the new schema";
  "The full CZ CELLxGENE schema changelog is available" [read]. Listed as
  ongoing work: "updating all CZ CELLxGENE features … to present data that is
  derived from the same versioned snapshot" [read]. The Census docs advertise
  dated LTS builds (for example "Census LTS release 2025-11-08") [read, docs
  page https://chanzuckerberg.github.io/cellxgene-census/, not the paper].
- **Borrow:** a growth figure; a "what's in the corpus and what's missing"
  breakdown; a **schema changelog** as a citable artifact; named downstream
  uses; dated LTS builds. OmicIDX's dated `v{date}/` plus `manifest.json` is
  the equivalent, and the provenance is arguably stronger.

---

## 3. OmicIDX's own footprint (prior publication check)

- **No journal article or preprint about OmicIDX was found.** Europe PMC query
  `"OmicIDX"` returns 1 result; OpenAlex full-text search returns 1 result
  [read, both APIs, 2026-10-31].
- **Downstream use #1 (verified):** isomiRdb, *NAR* 2023;51(D1):D179–D185,
  doi:10.1093/nar/gkac884, PMC9825445. Its "Metadata acquisition" section
  says tissue annotations come from several curated resources "and from SRA
  using OmicIDX" [read]. This is citable evidence of use in a NAR Database
  paper.
- **Downstream use #2 (unverified context):** OpenAlex's full-text index
  matches "OmicIDX" in the GenomicSuperSignature bioRxiv preprint,
  doi:10.1101/2021.05.26.445900. I did not open that text to see how it is
  used.
- **Prior non-paper descriptions:** the 2018 Bioconductor Workshop
  compilation describes `Omicidx` within `SRAdbV2` [read, search-result
  summary only]. There is also a 2019 blog post, "OmicIDX on BigQuery"
  (seandavi.github.io), and a PyPI `omicidx` package description that
  mentions BigQuery publication [search snippets only]. **[inferred]** The
  paper should say that earlier OmicIDX incarnations (BigQuery, REST API)
  were retired, and why. The README already says the REST API was retired
  because "almost nobody used it". This is a credible design-evolution story.
- **Suggested action:** run a citation search (Google Scholar / Europe PMC
  full text) for `omicidx.cancerdatasci.org` and `SRAdbV2` to collect more
  downstream uses before submission. That search was not done here.

---

## 4. Synthesis

### 4.1 Common elements (the baseline a reviewer will look for)

| Element | Present in | Notes |
|---|---|---|
| Motivation: the repository is large but hard to query | all 10 | Usually one paragraph, citing the repository's own NAR update |
| Named predecessors and alternatives | 9/10 (not SRA NAR) | ffq and pysradb name SRAdb; recount3 and ARCHS4 name each other |
| Architecture or pipeline figure | GEOmetadb (ERD), ffq, ARCHS4, recount3, CELLxGENE | Usually Fig. 1 or 2 |
| Schema / data model figure or table | GEOmetadb Fig. 1, SRAdb Addl file 1, OpenAlex Fig. 1, CELLxGENE Fig. 1B | |
| Worked usage examples | all tool and resource papers | Code shown inline (SRAdb, pysradb, GEOmetadb) |
| Scale numbers | all | Counts of records, samples, bytes |
| Availability statement (URL, license, code) | all | Bioinformatics and F1000 require one |
| Update cadence statement | GEOmetadb and SRAdb ("regularly"), OpenAlex (fortnightly/daily), ARCHS4 ("rolling") | **Usually vague; OmicIDX can be precise** |
| Funding statement | 8/10 | |

### 4.2 What sets the strong papers apart

1. **A quantitative comparison table against named alternatives:** ARCHS4
   Table 1 and Fig. 4. ffq and pysradb describe alternatives only in prose.
2. **Cost and throughput tables:** recount3 Tables 1–2 and ARCHS4's per-sample
   cost.
3. **Validation against ground truth:** MetaSRA's 422-sample held-out set;
   CELLxGENE's HuBMAP recall and ANOVA.
4. **Growth over time:** SRA NAR Fig. 1, ARCHS4 Fig. 1, CELLxGENE Fig. 1A.
5. **Named downstream uses and adopters:** CELLxGENE.
6. **Archival DOIs for code and data:** recount3 (Zenodo), pysradb (Zenodo).
7. **Explicit versioning and schema-evolution policy:** CELLxGENE (versioned
   schema, changelog, migrations). The other nine have nothing comparable.
8. **Honest limitations and sustainability:** OpenAlex (Limitations, POSI).
   CELLxGENE says openly "No external funding".

**Gaps no paper fills, and which OmicIDX should claim [inferred from the ten]:**

- Immutable, dated snapshots, each with a provenance manifest (source
  partitions, transform git SHA, lake snapshot id).
- Anonymous, serverless querying over HTTPS range requests from DuckDB, R,
  Python and the browser, with no account, key or billing. SRA's BigQuery
  needs an account and the user pays; Census is on AWS Open Data, with
  anonymity unverified.
- Compatibility schemas (`sradb.*`, `geometadb.*`) so that legacy queries keep
  working. recount3's "comparison with recount2" appendix is the nearest
  precedent.

### 4.3 Candidate venues and constraints

| Venue / type | Constraints (source) | Fit for OmicIDX |
|---|---|---|
| ***NAR* Database issue** (new database or update) | Invitation via a presubmission enquiry to the Executive Editor (Prof. Daniel J. Rigden, nardatabase@gmail.com) "by July 1st at the latest"; "Databases must be freely available to all via the web, not require any login or registration" *(extract, NAR "Submitting to the Database Issue")*. Length: precedents run from 4 pages (SRA update) to 15 (CELLxGENE). I found no explicit word limit. | **Strong.** The no-login rule is OmicIDX's design. Next window: presubmission by 1 Jul 2027 for the Jan 2028 issue [inferred from annual cycle]. |
| ***Bioinformatics* Application Note** | "Up to 4 pages; this is approx. 2,600 words or 2,000 words plus one figure"; software or data "freely available to non-commercial users"; must remain available "for a full two years following publication" *(extract, Bioinformatics author guidelines)* | **Good and fast.** GEOmetadb lineage. One figure plus one table: architecture and comparison. Benchmarks and usage would go to supplementary material. |
| ***Scientific Data* Data Descriptor** | Sections: Background & Summary, Methods, Data Records, (Data Overview), Technical Validation, Usage Notes, Data Availability, Code Availability. Abstract ≤170 words recommended. Title ≤110 characters, with **no dataset brand names, self-constructed acronyms or words like "open"**. ≤8 figures and ≤10 tables recommended. Methods unlimited. Usage Notes must not contain case studies. From the second review round, data must be "in a formal data repository that meets our policy" [read, nature.com/sdata/submission-guidelines] | **Possible but awkward.** The data is a *moving* daily product. One dated `v{date}/` release would have to be deposited (Zenodo, or HF if it meets their repository policy [unverified]). The title could not say "OmicIDX". Technical Validation fits the reconciliation checks well. |
| ***F1000Research* Software Tool Article** | Structured abstract ≤300 words; Introduction, Methods (Implementation, Operation), Results (optional), Use Cases, Conclusions; Data and Software Availability [read, F1000 guidelines] | Fits if framed around the client/query layer. pysradb precedent. Post-publication review; fast. |
| *GigaScience* Data Note | 250-word abstract [search-answer summary only; page not opened — unverified]. No other constraints verified. | Not researched further. |
| JOSS | Not researched. OmicIDX is primarily a data product, so likely out of scope [inferred]. | — |

**Recommendation:** target the **NAR Database issue** if the maintainer can
wait for the July 2027 presubmission. The audience matches, the no-login rule
favors OmicIDX, and there is room for the comparison table, growth figure,
benchmark and usage stats. If a sooner publication matters more, write a
**Bioinformatics Application Note** now, and keep the benchmark and usage
evidence for a later NAR update paper.

### 4.4 Recommended outline for an OmicIDX paper

Working title (NAR or Bioinformatics style; Sci Data would forbid the brand
name): *"OmicIDX: daily, versioned, serverless SQL access to NCBI SRA, GEO,
BioSample and BioProject metadata"*.

1. **Abstract.** The problem; what is published (tables, row counts, nightly
   refresh, dated snapshots with manifests, monthly DOI'd archive); how to
   access it (anonymous HTTPS; DuckDB, R, Python, browser); compatibility
   marts; URL; license.
2. **Introduction.**
   - Scale of SRA and GEO (cite the SRA NAR 2022 growth figure).
   - The two-decade lineage: GEOquery → GEOmetadb → SRAdb. Their lapse: the
     Thalhath 2024 preprint says the last SRAmetadb refresh was late 2023
     with no rebuild tooling; pysradb 3.0 removed SQLite support; Bioconductor
     support threads report outdated or 404 GEOmetadb files.
   - The three existing access patterns and their costs:
     (a) per-accession live APIs (ffq ~10 s/sample, pysradb web mode);
     (b) cloud warehouses (SRA BigQuery/Athena: account, pay-per-query, SRA
     only);
     (c) downloadable snapshots (SRAdb/GEOmetadb: stale, unversioned).
   - OmicIDX's position: snapshot-fresh *and* versioned *and* serverless.
3. **Design and implementation.**
   - **Fig. 1, architecture:** NCBI/EBI sources → extract (raw on R2,
     semaphore-gated) → DuckLake lake → SQLMesh transforms → parquet-export →
     publish bundle (`omicidx.duckdb`, `views.sql`, `manifest.json`) →
     Cloudflare Worker (HTTPS range) → clients.
   - Data model: **Fig. 2, an ERD across SRA study, experiment, sample and run
     ↔ BioSample ↔ BioProject ↔ GEO GSE/GSM/GPL ↔ Taxonomy, with join keys.**
     Copy GEOmetadb Fig. 1's role.
   - Parsing policy: the GEOmetadb/SRAdb "faithfully parsed, no curation"
     stance, restated with typed Pydantic models.
   - **Versioning and provenance:** rolling `latest/`, immutable `v{date}/`
     (retention policy), manifest fields, monthly archive with DOIs; how to
     cite a snapshot (pysradb's "schema version + creation timestamp", made
     formal).
   - Compatibility: `sradb.*` / `geometadb.*`. **Table 3, mapping:** legacy
     table and column → OmicIDX, with what changed and what was dropped. This
     is recount3's "comparison with predecessor" appendix applied to OmicIDX.
4. **Usage (worked examples).** Four or five queries, each shown in SQL plus
   one R or Python line. Suggested:
   - human RNA-seq runs by year;
   - GSE with count-matrix supplementary files (from the README);
   - cohort assembly that joins BioSample attributes to runs;
   - organism resolution through Taxonomy;
   - a pinned-snapshot query against `v{date}/` that shows reproducibility.

   Also give a browser (DuckDB-Wasm playground) screenshot or URL.
5. **Evaluation / technical validation.**
   - **Freshness:** distribution of lag between an NCBI record's
     public/update date and its first appearance in OmicIDX. This answers
     ffq's "more up-to-date" claim.
   - **Completeness and fidelity:** row-count reconciliation against NCBI
     totals (SRA_Accessions.tab, GEO counts), plus a MetaSRA-style random
     held-out sample of N accessions per entity, checked field by field
     against live NCBI XML.
   - **Reproducibility:** the "re-run writes zero new data files" smoke test
     from the deliverables spec; manifest round-trip.
   - **Query benchmark (Fig. 4):** cold- and warm-cache wall time and bytes
     transferred for the worked queries over HTTPS range from (i) a laptop
     and (ii) a cloud VM. Compare against the time-to-first-query of
     downloading SRAmetadb.sqlite (historical size, cited), against
     pysradb/ffq per-accession API calls for an equivalent cohort, and,
     optionally, against BigQuery cost for the same query.
   - **Growth (Fig. 3):** records per month from OmicIDX's own date columns
     for SRA runs, GEO samples and BioSamples. It doubles as a demonstration
     query.
6. **Usage statistics (Fig. 5 or a table).** From Cloudflare Worker
   analytics: monthly requests, bytes served, unique client IPs or ASNs,
   breakdown by file (which tables matter), breakdown by user-agent (DuckDB,
   R arrow, Python, browser/Wasm), and HF archive downloads. Add the
   downstream citations (isomiRdb, plus any found).
7. **Comparison with existing resources: Table 1, ARCHS4-style.** Columns:
   SRAdb, GEOmetadb, pysradb, ffq, SRA BigQuery/Athena, MetaSRA (as a
   complementary layer), OmicIDX. Rows: sources covered; update cadence and
   last update; access method; account/key/billing required; versioned
   snapshots; provenance manifest; bulk/cohort query; per-accession lookup;
   languages; license; maintained (yes/no, as of date).
8. **Discussion.**
   - What OmicIDX does not do: no ontology normalization (point to MetaSRA as
     complementary), no reads, no PubMed in the public bundle.
   - Limitations: NCBI parsing quirks; HF anonymous querying gets 429s, so
     the archive is download-then-query; external time travel is still gated
     on manifest faithfulness.
   - Sustainability: cost per month (R2 storage and egress, compute),
     governance, funding, the 2-year availability commitment (Bioinformatics
     requires one), and the retirement history (BigQuery era, REST API
     retired in #196 for low use).
9. **Availability.** URLs (data domain, docs site, playground); data license
   (state one, unambiguously — unlike ARCHS4); code license (MIT); Zenodo DOI
   for the pipeline at submission; HF DOIs for the monthly archive; the
   snapshot used for the paper's figures, pinned by manifest.
10. **Funding / acknowledgements.** NCBI staff acknowledgement (GEOmetadb did
    this).

**Figure and table checklist:** Fig. 1 architecture, Fig. 2 ERD, Fig. 3
growth, Fig. 4 benchmark, Fig. 5 usage; Table 1 comparison, Table 2 table
inventory (file, rows, bytes, source, refresh), Table 3 legacy mapping.
Supplementary: data dictionary (SRAdb's Additional file 1 equivalent) and
manifest schema. For a Bioinformatics Application Note, keep Fig. 1 and Table
1 in the main text and move the rest to supplementary.

**Evidence to gather before writing** (all from data already collected,
per the maintainer's practice):

1. Worker analytics export: 6–12 months, by path and user-agent.
2. HF download counts per monthly release.
3. Freshness lag computed from lake snapshot timestamps against NCBI dates.
4. Reconciliation counts against NCBI totals.
5. Benchmark runs: a script in the repo so the numbers are reproducible.
6. Pipeline cost per month.
7. Citation search for downstream users.

---

## 5. Contradictions and caveats

- **ARCHS4 license:** "CC BY 4.0" and "free to use for non-commercial
  purposes" appear together [read]. Recorded, not resolved.
- **ffq vs. snapshot resources:** ffq claims API tools are "guaranteed to be
  more up-to-date than tools that rely on regular database builds" [read].
  This holds against SRAdb's lapsed builds; against a nightly build it is a
  matter of measured lag. The OmicIDX paper should measure it rather than
  argue it.
- **Article-type labels:** SRAdb (BMC Software), ffq (Application Note) and
  recount3 (Genome Biology section) are inferred from layout and Europe PMC
  pubType, not from a publisher label I could read.

## 6. Missing evidence

- OUP author-guideline pages (NAR Database issue; Bioinformatics Application
  Note) could not be fetched directly (HTTP 403). Their limits come from
  search-engine extracts of the official pages. Confirm on the live pages
  before choosing a venue.
- GigaScience Data Note and JOSS constraints: not verified.
- Whether CELLxGENE Census access is anonymous or credential-free: not
  verified.
- Whether HuggingFace meets *Scientific Data*'s repository policy: not
  checked.
- Full text of GEOquery (not open access) and of the Thalhath preprint
  (abstract only) were not read.
- How the GenomicSuperSignature preprint uses OmicIDX: not read.
- The NCBI BigQuery SRA metadata table's coverage of GEO fields: not checked.
  The comparison-table claim about it is a placeholder until it is checked.
