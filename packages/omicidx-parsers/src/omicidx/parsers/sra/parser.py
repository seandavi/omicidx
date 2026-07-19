"""SRA parsers including:

- study
- sample
- experiment
- run

These parsers each parse XML format files of the format
available from the fullxml api.

The main streaming entry point is `iter_sra_records`, which yields one
parsed dict per record element. `parse_xml_file` / `parse_xml_url` are
thin file/URL adapters over it.

"""

import contextlib
import gzip
import logging
import re
import xml.etree.ElementTree as etree
from collections import defaultdict
from io import BytesIO

import httpx

from . import pydantic_models

logger = logging.getLogger(__name__)


def parse_xml_url(url: str, entity: str | None = None, gz: bool = True):
    """Fetch an SRA XML document over HTTP and yield parsed record dicts.

    Thin fetch adapter over :func:`iter_sra_records`. Records are dispatched by
    XML tag; ``entity`` is accepted for backward compatibility but no longer
    used (dispatch is by tag).
    """
    resp = httpx.get(url, timeout=120, follow_redirects=True)
    resp.raise_for_status()
    content = BytesIO(resp.content)
    with gzip.open(content, "rb") if gz else content as f:
        yield from iter_sra_records(f)


def parse_xml_file(xmlfilename):
    """Parse an NCBI SRA mirroring XML file into an iterator of record dicts.

    Thin file adapter over :func:`iter_sra_records`: opens the file
    (transparently gunzipping ``.gz``) and yields one parsed dict per
    STUDY / SAMPLE / RUN / EXPERIMENT / SUBMISSION element. The entity type is
    detected from the XML tags, so the filename no longer needs to encode it.

    For example:

    wget --mirror -nH --cut-dirs=3 ftp://ftp.ncbi.nlm.nih.gov/sra/reports/Mirroring/NCBI_SRA_Mirroring_20181027/

    >>> import omicidx.parsers.sra.parser as sp
    >>> studies = sp.parse_xml_file("NCBI_SRA_Mirroring_20181027/meta_study_set.xml.gz")
    >>> next(studies)
    ...

    Parameters
    ----------
    xmlfilename : string
        the filename to be parsed. Can be gzipped.

    Returns
    -------
    iterator:
        An iterator of dict records from parsing each xml record.

    """
    open_func = gzip.open if xmlfilename.endswith(".gz") else open
    with open_func(xmlfilename) as f:
        yield from iter_sra_records(f)


def parse_study(xml: etree.Element) -> dict:
    """Parse an SRA xml STUDY element

    Parameters
    ----------
    xml: an xml.etree Element

    Returns
    -------
    A dict object parsed from the XML
    """

    required_keys = [
        "abstract",
        "BioProject",
        "GEO",
        "accession",
        "alias",
        "attributes",
        "center_name",
        "broker_name",
        "description",
        "study_type",
        "study_accession",
        "title",
    ]
    d = dict.fromkeys(required_keys)
    with contextlib.suppress(AttributeError):
        d.update(xml.attrib)
    path_map = {
        "title": (".//STUDY_TITLE", "text"),
        "abstract": (".//STUDY_ABSTRACT", "text"),
        "description": (".//STUDY_DESCRIPTION", "text"),
    }
    d.update(_process_path_map(xml, path_map))
    d.update(_parse_identifiers(xml.find("IDENTIFIERS"), "study"))
    try_update(d, _parse_study_type(xml.find("DESCRIPTOR/STUDY_TYPE")))
    d = try_update(d, _parse_attributes(xml.find("STUDY_ATTRIBUTES")))
    d.update(_parse_links(xml.find("STUDY_LINKS")))
    pubmeds = []
    if "xrefs" in d:
        for xref in d["xrefs"]:
            if xref["db"] == "pubmed" and xref["id"] is not None:
                pubmeds.append(xref["id"])
    d.update({"pubmed_ids": pubmeds})
    return d


def parse_submission(xml: etree.Element) -> dict:
    """Parse an SRA xml SUBMISSION element

    Parameters
    ----------
    xml: xml.etree.ElementTree.Element

    Returns
    -------
    a dict of experiment
    """
    d = {}
    d.update(xml.attrib)
    d.update(_parse_identifiers(xml.find("IDENTIFIERS"), "submission"))
    return d


def dict_from_single_xml(txt):
    """Parse a single standalone SRA XML string into a dict (tag-dispatched)."""
    xml = etree.fromstring(txt)
    vals = _parse_element(xml)
    vals["entity_type"] = xml.tag.lower()
    return vals


def model_from_single_xml(txt):
    """Parse a single standalone SRA XML string into a pydantic model."""
    xml = etree.fromstring(txt)
    entity = xml.tag.lower()
    return getattr(pydantic_models, "Sra" + entity.capitalize())(**_parse_element(xml))


def parse_run(xml: etree.Element) -> dict:
    """Parse an SRA xml RUN element

    Parameters
    ----------
    xml: an xml.etree Element

    Returns
    -------
    A dict object parsed from the XML
    """
    d = {}
    d.update(xml.attrib)
    for k in ["total_spots", "total_bases", "size"]:
        with contextlib.suppress(KeyError, ValueError, TypeError):
            d[k] = int(d[k])
    with contextlib.suppress(Exception):
        d["avg_length"] = float(d["total_bases"]) / d["total_spots"]
    path_map = {
        "experiment_accession": ("EXPERIMENT_REF", "accession"),
        "title": ("TITLE", "text"),
    }

    d = try_update(d, _parse_taxon(xml.find("tax_analysis")))
    # d = try_update(d, _parse_run_reads(xml.find(".//SPOT_DESCRIPTOR")))
    d.update(_process_path_map(xml, path_map))
    d.update(_parse_identifiers(xml.find("IDENTIFIERS"), "run"))
    d = try_update(d, _parse_attributes(xml.find("RUN_ATTRIBUTES")))
    d = try_update(d, _parse_run_files(xml.find("SRAFiles")))
    d = try_update(d, _parse_run_stats(xml.find("Statistics")))
    d = try_update(d, _parse_run_bases(xml.find("Bases")))
    d = try_update(d, _parse_run_qualities(xml))
    with contextlib.suppress(KeyError):
        del d["run_accession"]
    return d


def _parse_run_stats(xml: etree.Element | None) -> dict | None:
    if xml is None:
        return None
    stats = []
    for read in xml.findall("Read"):
        ret = {}
        ret["index"] = int(read.get("index", 0))
        ret["count"] = int(read.get("count", 0))
        ret["mean_length"] = float(read.get("average", 0.0))
        ret["sd_length"] = float(read.get("stdev", 0.0))
        stats.append(ret)
    return {"reads": stats}


def _parse_run_bases(xml: etree.Element | None) -> dict | None:
    if xml is None:
        return None
    ret = []
    for base in xml.findall("Base"):
        ret.append({base.get("value"): int(base.get("count"))})
    return {"base_counts": ret}


def _parse_run_files(xml: etree.Element | None) -> dict | None:
    if xml is None:
        return None
    files = xml.findall("./SRAFile")
    ret = []
    for f in files:
        retfile = {}
        for k in f:
            retfile[k] = f.get(k)
        retfile["alternatives"] = []
        for alt in f.findall("Alternatives"):
            altfile = {}
            for k in alt:
                altfile[k] = alt.get(k)
            retfile["alternatives"].append(altfile)
        ret.append(retfile)
    return {"files": ret}


def parse_experiment(xml: etree.Element) -> dict:
    """Parse an SRA xml EXPERIMENT element

    Parameters
    ----------
    xml: xml.etree.ElementTree.Element

    Returns
    -------
    a dict of experiment
    """
    required_keys = [
        "accession",
        "attributes",
        "alias",
        "center_name",
        "design",
        "description",
        "experiment_accession",
        "identifiers",
        "instrument_model",
        "library_name",
        "library_construction_protocol",
        "library_layout_orientation",
        "library_layout_length",
        "library_layout_sdev",
        "library_strategy",
        "library_source",
        "library_selection",
        "library_layout",
        "xrefs",
        "platform",
        "sample_accession",
        "study_accession",
        "title",
    ]

    d = dict.fromkeys(required_keys)
    try:
        d.update(xml.attrib)
    except (AttributeError, TypeError):
        import xml.etree.ElementTree as et

        et.tostring(xml)

    path_map = {
        "title": ("./TITLE", "text"),
        "study_accession": ("./STUDY_REF/IDENTIFIERS/PRIMARY_ID", "text"),
        "design": ("./DESIGN/DESIGN_DESCRIPTION", "text"),
        "library_name": ("./DESIGN/LIBRARY_DESCRIPTOR/LIBRARY_NAME", "text"),
        "library_strategy": ("./DESIGN/LIBRARY_DESCRIPTOR/LIBRARY_STRATEGY", "text"),
        "library_source": ("./DESIGN/LIBRARY_DESCRIPTOR/LIBRARY_SOURCE", "text"),
        "library_selection": ("./DESIGN/LIBRARY_DESCRIPTOR/LIBRARY_SELECTION", "text"),
        "library_layout": (
            "./DESIGN/LIBRARY_DESCRIPTOR/LIBRARY_LAYOUT",
            "child",
            "tag",
        ),
        "library_layout_orientation": (
            "./DESIGN/LIBRARY_DESCRIPTOR/LIBRARY_LAYOUT/PAIRED",
            "ORIENTATION",
        ),
        "library_layout_length": (
            "./DESIGN/LIBRARY_DESCRIPTOR/LIBRARY_LAYOUT/PAIRED",
            "NOMINAL_LENGTH",
        ),
        "library_layout_sdev": (
            "./DESIGN/LIBRARY_DESCRIPTOR/LIBRARY_LAYOUT/PAIRED",
            "NOMINAL_SDEV",
        ),
        "pooling_stategy": ("./DESIGN/LIBRARY_DESCRIPTOR/POOLING_STRATEGY", "text"),
        "library_construction_protocol": (
            "./DESIGN/LIBRARY_DESCRIPTOR/LIBRARY_CONSTRUCTION_PROTOCOL",
            "text",
        ),
        "platform": ("./PLATFORM", "child", "tag"),
        "sample_accession": (".//SAMPLE_DESCRIPTOR", "accession"),
        "instrument_model": ("./PLATFORM/*/INSTRUMENT_MODEL", "text"),
    }

    d.update(_process_path_map(xml, path_map))
    d.update(_parse_identifiers(xml.find("IDENTIFIERS"), "experiment"))

    d.update(_parse_attributes(xml.find("EXPERIMENT_ATTRIBUTES")))
    d.update(_parse_links(xml.find("EXPERIMENT_LINKS")))
    d = try_update(d, _parse_run_reads(xml.find(".//SPOT_DESCRIPTOR")))
    return d


def parse_sample(xml: etree.Element) -> dict:
    """Parse an SRA xml SAMPLE element

    Parameters
    ----------
    xml: an xml.etree Element

    Returns
    -------
    A dict object parsed from the XML
    """

    d = {}
    d.update(xml.attrib)
    path_map = {
        "title": (".//TITLE", "text"),
        "organism": (".//SCIENTIFIC_NAME", "text"),
        "description": (".//DESCRIPTION", "text"),
    }
    d.update(_process_path_map(xml, path_map))
    d.update(_parse_identifiers(xml.find("IDENTIFIERS"), "sample"))
    d.update(_parse_attributes(xml.find("SAMPLE_ATTRIBUTES")))
    d.update(_parse_links(xml.find("SAMPLE_LINKS")))

    for elem in xml.iter():
        if elem.tag == "TAXON_ID":
            d["taxon_id"] = int(elem.text)

    return d


def _parse_run_reads(node: etree.Element | None) -> dict:
    """Parse reads from runs."""
    d = {}
    try:
        d["spot_length"] = int(node.find(".//SPOT_LENGTH").text)
    except Exception:
        d["spot_length"] = 0
    d["reads"] = []
    if node is None:
        # No read statistics present
        return d
    readrecs = node.findall(".//READ_SPEC")
    d["nreads"] = len(readrecs)

    for read in readrecs:
        r = {}
        with contextlib.suppress(Exception):
            r["read_index"] = int(read.find("./READ_INDEX").text)
        with contextlib.suppress(Exception):
            r["read_class"] = read.find("./READ_CLASS").text
        with contextlib.suppress(Exception):
            r["read_type"] = read.find("./READ_TYPE").text
        with contextlib.suppress(Exception):
            r["base_coord"] = int(read.find("./BASE_COORD").text)
        d["reads"].append(r)

    return d


def _parse_run_qualities(node: etree.Element) -> dict:
    """Parse the quality stats, if available, from RUN"""
    d = {}
    d["qualities"] = []
    qualrecs = node.findall(".//Quality")
    for qual in qualrecs:
        with contextlib.suppress(Exception):
            d["qualities"].append(
                {"quality": int(qual.get("value")), "count": int(qual.get("count"))}
            )

    return d


def _parse_taxon(node: etree.Element | None) -> dict:
    """Parse taxonomy informaiton."""

    def crawl(node, d=None):
        if d is None:
            d = []
        for i in node:
            rank = i.get("rank", "Unkown")
            parent = None
            if node.get("tax_id") is not None:
                parent = int(node.get("tax_id"))
            d.append(
                {
                    "rank": rank,
                    "name": i.get("name").replace(".", "_").replace("$", ""),
                    "parent": parent,
                    "total_count": int(i.get("total_count")),
                    "self_count": int(i.get("self_count")),
                    "tax_id": int(i.get("tax_id")),
                }
            )

            if len(list(i)) > 0:
                d = d + crawl(i)
        return d

    try:
        d = {
            "tax_analysis": {
                "nspot_analyze": node.get("analyzed_spot_count"),
                "total_spots": node.get("total_spot_count"),
                "mapped_spots": node.get("identified_spot_count"),
                "tax_counts": crawl(node),
            }
        }
    except AttributeError:
        # No tax_analysis node
        return {}

    try:
        if d["tax_analysis"]["nspot_analyze"] is not None:
            d["tax_analysis"]["nspot_analyze"] = int(d["tax_analysis"]["nspot_analyze"])
    except (KeyError, ValueError, TypeError):
        logger.debug("Non integer count: nspot_analyze")
        logger.debug(d["tax_analysis"]["nspot_analyze"])
        d["tax_analysis"]["nspot_analyze"] = None

    try:
        if d["tax_analysis"]["total_spots"] is not None:
            d["tax_analysis"]["total_spots"] = int(d["tax_analysis"]["total_spots"])
    except (KeyError, ValueError, TypeError):
        logger.debug("Non integer count: total_spots")
        logger.debug(d["tax_analysis"]["total_spots"])
        d["tax_analysis"]["total_spots"] = None

    try:
        if d["tax_analysis"]["mapped_spots"] is not None:
            d["tax_analysis"]["mapped_spots"] = int(d["tax_analysis"]["mapped_spots"])
    except (KeyError, ValueError, TypeError):
        logger.debug("Non integer count: mapped_spots")
        logger.debug(d["tax_analysis"]["mapped_spots"])
        d["tax_analysis"]["mapped_spots"] = None

    return d


def _safe_add_text_element(d, key, elem):
    """Add text from an xml element to a dict

    Because not all elements have text elements despite
    their existence in the xml tree, this little
    function checks for text existence and then
    adds the text conditionally. If no text is present,
    the key is not created.

    Parameters
    ----------
    d : dict
        Add the text element to this dict
    key : str
        The key to which to add the text element
    elem : lxml.etree.Element
        From where to extract the text
    """
    txt = elem.text
    if txt is not None:
        d[key] = txt.strip()


def _parse_attributes(xml: etree.Element | None) -> dict:
    """Parse attributes from an SRA XML ATTRIBUTES element.

    Parameters
    ----------
    xml: xml.etree.ElementTree.Element or None
        An xml element of level "EXPERIMENT|STUDY|RUN|SAMPLE_ATTRIBUTES"
    """
    if xml is None:
        return {}
    d = defaultdict(list)
    # Iterate over "XXX_ATTRIBUTES"
    for elem in xml:
        try:
            tag = elem.find("./TAG")
            value = elem.find("./VALUE")
            d["attributes"].append({"tag": tag.text, "value": value.text})
        except AttributeError:
            # tag or value missing text, so skip
            pass
    if len(d) == 0:
        d = {}
    return d


def _parse_links(xml: etree.Element | None) -> dict:
    """Parse xref links from an SRA XML LINKS element.

    Parameters
    ----------
    xml: xml.etree.ElementTree.Element or None
        An xml element of level "EXPERIMENT|STUDY|RUN|SAMPLE_LINKS"
    """
    if xml is None:
        return {}
    d = defaultdict(list)
    # Iterate over "XXX_ATTRIBUTES"
    for elem in xml.findall(".//XREF_LINK"):
        try:
            tag = elem.find("./DB")
            value = elem.find("./ID")
            d["xrefs"].append({"db": tag.text, "id": value.text})
        except AttributeError:
            # tag or value missing text, so skip
            pass
    if len(d) == 0:
        d = {}
    return d


def _get_special_ids(id_rec):
    namespace_map = {
        "geo": "GEO",
        "gds": "GEO_Dataset",
        "pubmed": "pubmed",
        "biosample": "BioSample",
        "bioproject": "BioProject",
    }
    # code below from sramongo/sra.py by jtfear
    # https://github.com/jfear/sramongo/blob/master/sramongo/sra.py
    #
    # Make sure fully formed xref
    try:
        _id = id_rec["id"]
        _db = id_rec["namespace"]
    except Exception:
        return False

    if (_id is None) | (_db is None):
        return False

    # normalize db name
    try:
        norm = _db.strip(" ()[].:").lower()
    except Exception:
        norm = ""

    if norm in namespace_map:
        # Normalize the ids a little
        id_norm = (
            re.sub("geo|gds|bioproject|biosample|pubmed|pmid", "", _id.lower())
            .strip(" :().")
            .upper()
        )
        return namespace_map[norm], id_norm
    else:
        return False


def _parse_identifiers(xml: etree.Element, section: str) -> dict:
    """Parse IDENTIFIERS section"""

    d = defaultdict(list)

    for _id in xml:
        if _id.tag == "PRIMARY_ID":
            d[section + "_accession"] = _id.text
        elif _id.tag == "SUBMITTER_ID":
            id_rec = {"namespace": _id.get("namespace"), "id": _id.text}
            d["identifiers"].append(id_rec)
        elif _id.tag == "UUID":
            d["identifiers"].append({"uuid": _id.text})
        else:  # all other id types (secondary, external)
            id_rec = {"namespace": _id.get("namespace"), "id": _id.text}
            d["identifiers"].append(id_rec)
            special = _get_special_ids(id_rec)
            if special:
                d[special[0]] = special[1]
            else:
                d["identifiers"].append(id_rec)
    return d


def _process_path_map(xml: etree.Element, path_map: dict) -> dict:
    d = {}
    for k, v in path_map.items():
        try:
            # use "text" as second tuple value to
            # get the text value
            if v[1] == "text":
                d[k] = xml.find(v[0]).text
                # use the name of the attribute to
                # get a specific attribute
            elif v[1] == "child":
                child = list(xml.find(v[0]))

                if len(child) > 1:
                    raise Exception("There are too many elements")
                elif v[2] == "text":
                    d[k] = child[0].text
                elif v[2] == "tag":
                    d[k] = child[0].tag
            else:
                d[k] = xml.find(v[0]).get(v[1])
        except Exception:
            pass
    return d


def _parse_study_type(xml: etree.Element | None) -> dict:
    d = {}
    if xml is None:
        return d
    if xml.get("existing_study_type"):
        d["study_type"] = xml.get("existing_study_type")
    if xml.get("new_study_type"):
        d["study_type"] = xml.get("new_study_type")
    return d


def try_update(d: dict, value) -> dict:
    try:
        d.update(value)
        return d
    except Exception:
        return d


# ---------------------------------------------------------------------------
# Entity dispatch: one table (XML tag -> pure parse function) replaces the
# scattered globals()/filename-substring/dict dispatch mechanisms.
# ---------------------------------------------------------------------------
_ENTITY_PARSERS = {
    "STUDY": parse_study,
    "SAMPLE": parse_sample,
    "RUN": parse_run,
    "EXPERIMENT": parse_experiment,
    "SUBMISSION": parse_submission,
}


def _parse_element(element: etree.Element) -> dict:
    """Dispatch one SRA element to its parser by tag; raise on unknown tags."""
    parse = _ENTITY_PARSERS.get(element.tag.upper())
    if parse is None:
        raise ValueError(f"No SRA parser for element tag: {element.tag!r}")
    return parse(element)


def iter_sra_records(fileobj):
    """Stream parsed SRA records from an open XML file object.

    The single streaming entry point for SRA XML (NCBI fullxml API output and
    the mirroring ``meta_*_set`` files). Yields one dict per STUDY / SAMPLE /
    RUN / EXPERIMENT / SUBMISSION element, dispatched by tag; elements with any
    other tag are ignored.

    Parameters
    ----------
    fileobj:
        An open binary file-like object of SRA XML (e.g. an already-gunzipped
        stream). Parsed incrementally via ``iterparse``.

    Yields
    ------
    dict
        One parsed record per recognized element.
    """
    n = 0
    for event, element in etree.iterparse(fileobj):
        if event != "end":
            continue
        parse = _ENTITY_PARSERS.get(element.tag.upper())
        if parse is None:
            continue
        rec = parse(element)
        element.clear()
        n += 1
        if (n % 100000) == 0:
            logger.info(f"parsed {n} SRA records")
        yield rec
    logger.info(f"parsed {n} SRA records")


class SraRecord:
    """Back-compat wrapper exposing a parsed record dict as ``.data``.

    Retained only for :func:`sra_object_generator`; new code should use
    :func:`iter_sra_records`, which yields dicts directly.
    """

    def __init__(self, data: dict):
        self.data = data


def sra_object_generator(fh):
    """Deprecated: iterate SRA objects exposing parsed data via ``.data``.

    Prefer :func:`iter_sra_records`, which yields the dicts directly. Kept as a
    thin shim so existing callers of the ``.data`` contract keep working.
    """
    for rec in iter_sra_records(fh):
        yield SraRecord(rec)
