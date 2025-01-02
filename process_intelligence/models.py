import dataclasses
from enum import Enum
from typing import Dict
from pydantic import BaseModel

from alignments import InfixType


class VariantFragment(BaseModel):
    fragment: dict
    infixType: str


class ClusteringAlgorithm(str, Enum):
    AGGLOMERATIVE_EDIT_DISTANCE_CLUSTERING = "AGGLOMERATIVE_EDIT_DISTANCE_CLUSTERING"
    LABEL_VECTOR_CLUSTERING = "LABEL_VECTOR_CLUSTERING"


class ClusteringParameters(BaseModel):
    algorithm: ClusteringAlgorithm
    params: Dict


@dataclasses.dataclass
class VariantInformation:
    infix_type: InfixType
    is_user_defined: bool
