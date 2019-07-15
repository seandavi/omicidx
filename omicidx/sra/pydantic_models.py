from datetime import datetime
from typing import List, Dict
from pydantic import BaseModel
from pydantic.types import UrlStr

class Attribute(BaseModel):
    tag: str
    value: str


class Xref(BaseModel):
    namespace: str
    id: str

class Identifier(BaseModel):
    namespace: str
    id: str


class FileAlternative(BaseModel):
    url: str
    free_egress: str
    access_type: str
    org: str
    

class FileSet(BaseModel):
    cluster: str = "public"
    filename: str
    size: int = 0
    date: datetime = None
    md5: str
    sratoolkit: str = '1'
    alternatives: List[FileAlternative]


class BaseQualities(Dict[int, int]):
    pass
    

class TaxCountEntry(BaseModel):
    name: str
    parent: str = None
    total_count: int = 0
    self_count: int = 0
    tax_id: int

class TaxCountAnalysis(BaseModel):
    nspots_analyze: int
    total_spots: int
    mapped_spots: int
    tax_counts: Dict[str, List[TaxCountEntry]]

class RunRead(BaseModel):
    index: int
    count: int
    mean_length: float = 0.0
    sd_length: float = 0.0


class BaseCounts(Dict[str, int]):
    pass


class SraRun(BaseModel):
    alias: str = None
    run_date: datetime = None
    run_center: str = None
    center_name: str = None
    accession: str
    total_spots: int = 0
    total_bases: int = 0
    size: int = 0
    load_done: bool = True
    published: datetime = None
    is_public: bool = True
    cluster_name: str = "public"
    static_data_available: str = "1"
    avg_length: float = 0.0
    experiment_accession: str
    attributes: List[Attribute]
    files: List[FileSet] = None
    qualities: BaseQualities = None
    base_counts: BaseCounts = None
    reads: List[RunRead] = None
    tax_analysis: TaxCountLevels = None


class SraStudy(BaseModel):
    abstract: str = None
    BioProject: str = None
    Geo: str = None
    accession: str
    alias: str = None
    center_name: str = None
    broker_name: str = None
    description: str = None
    study_type: str
    title: str = None
    identifiers: List[Identifier] = None
    attributes: List[Attribute] = None
    pubmed_ids: List[int]


class SraExperiment(BaseModel):
    accession: str
    attributes: List[Attribute] = None
    alias: str = None
    center_name: str = None
    design: str = None
    description: str = None
    identifiers: List[Identifier] = None
    instrument_model: str = None
    library_name: str = None
    library_construction_protocol: str = None
    library_layout_orientation: str = None
    library_layout_length: float = None
    library_layout_sdev: float = None
    library_strategy: str = None
    library_source: str = None
    library_selection: str = None
    library_layout: str = None
    xrefs: List[Xref] = None
    platform: str = None
    sample_accession: str
    study_accession: str
    title: str = None


class SraSample(BaseModel):
    accession: str
    geo: str = None
    BioSample: str = None
    title: str = None
    alias: str = None
    organism: str = None
    taxon_id: int = None
    description: str = None
    identifiers: List[Identifier] = None
    attributes: List[Attribute] = None
    xrefs: List[Xref] = None
    
class FullSraRun(SraRun):
    experiment: SraExperiment = None
    sample: SraSample = None
    study: SraStudy = None
    
