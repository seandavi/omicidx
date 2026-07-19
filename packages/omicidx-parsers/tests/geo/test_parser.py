import io

import pydantic
import pytest
from omicidx.parsers.geo import parser

TEST_GSE = "GSE10"

pytestmark = pytest.mark.network


def test_entrez_instance():
    entrez = parser.get_entrez_instance()
    # entrez is a module
    assert entrez.__name__ == "Bio.Entrez"


def test_get_geo_accession_xml():
    res = parser.get_geo_accession_xml(TEST_GSE)
    assert isinstance(res, io.BytesIO)
    firstline = next(res)
    assert isinstance(firstline, bytes)
    assert firstline.decode("UTF-8").startswith("<?xml")


def test_get_geo_accession_soft():
    res = parser.get_geo_accession_soft(TEST_GSE)
    assert isinstance(res, str)
    firstline = res.splitlines()[0]
    assert firstline.startswith("^SERIES = ")


def test_get_geo_entities():
    res = parser.get_geo_accession_soft(TEST_GSE)
    txt = res.splitlines()
    entities = parser.get_geo_entities(txt)
    assert isinstance(entities, dict)
    assert len(entities.keys()) == 6


def test_geo_entity_iterator():
    """Smoke-test the fetch adapter end-to-end over the network.

    Parse detail (counts, entity types, fields) is covered offline against a
    captured fixture in test_geo_soft_parsers.py; here we only prove the
    fetch adapter reaches NCBI and yields parseable models.
    """
    entities = list(parser.geo_entity_iterator(TEST_GSE, targ="all"))
    assert len(entities) >= 1
    assert all(isinstance(e, pydantic.BaseModel) for e in entities)
