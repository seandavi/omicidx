# these are just to test types
import Bio
import http
import pydantic

from omicidx.geo import parser

TEST_GSE = 'GSE10'

def test_entrez_instance():
    entrez = parser.get_entrez_instance()
    # entrez is a module
    assert entrez.__name__ == 'Bio.Entrez'

def test_get_geo_accession_xml():
    res = parser.get_geo_accession_xml(TEST_GSE)
    assert isinstance(res, http.client.HTTPResponse)
    firstline = next(res)
    assert isinstance(firstline, bytes)
    assert firstline.decode('UTF-8').startswith('<?xml')

def test_get_geo_accession_soft():
    res = parser.get_geo_accession_soft(TEST_GSE)
    # '^SERIES = GSE10\r\n'
    firstline = next(res)
    assert isinstance(firstline, str)
    assert firstline.startswith('^SERIES = ')

def test_get_geo_entities():
    res = parser.get_geo_accession_soft(TEST_GSE)
    txt = res.readlines()
    entities = parser.get_geo_entities(txt)
    assert isinstance(entities, dict)
    assert len(entities.keys()) == 5


def test_geo_soft_entity_iterator():
    res = parser.get_geo_accession_soft(TEST_GSE)
    i = parser.geo_soft_entity_iterator(res)
    n = 0
    for j in i:
        n +=1
        isinstance(j, pydantic.BaseModel)
    assert n == 5 # should be five entities in TEST_GSE
    