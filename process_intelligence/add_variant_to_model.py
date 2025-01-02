import multithreading
from typing import List, Any

import pm4py
from cortado_core.utils.alignment_utils import typed_trace_fits_process_tree
from pm4py import PetriNet

from multithreading.pool_factory import PoolFactory
from process_tree_conversion import dict_to_process_tree, process_tree_to_dict
from utils import get_traces_from_variants
from cortado_core.utils.trace import TypedTrace

from cortado_core.freezing.apply import add_trace_to_pt_language_with_freezing
from cortado_core.lca_approach import add_trace_to_pt_language
from pm4py.objects.process_tree.obj import ProcessTree
from tqdm import tqdm

class InputAddVariantsToProcessModel(BaseModel):
    fitting_variants: List[Any]
    variants_to_add: List[Any]
    pt: dict


@router.post("/addConcurrencyVariantsToProcessModel")
async def add_cvariants_to_process_model(d: InputAddVariantsToProcessModel):
    fitting_variants = get_traces_from_variants(d.fitting_variants)
    to_add = get_traces_from_variants(d.variants_to_add)
    return add_variants_to_process_model(
        d.pt, fitting_variants, to_add, PoolFactory.instance().get_pool()
    )


class BaseModel:
    pass


class InputAddVariantsToProcessModelUnknownConformance(BaseModel):
    selected_variants: List[Any]
    pt: dict
    petrinet : PetriNet


def add_cvariants_to_process_model_unknown_conformance(
    d: InputAddVariantsToProcessModelUnknownConformance,
):
    selected_variants = get_traces_from_variants(d.selected_variants)

    fitting_traces = set()
    traces_to_add = set()
    process_tree = pm4py.convert_to_process_tree(d.petrinet)
    for selected_variant in selected_variants:
        if typed_trace_fits_process_tree(selected_variant, process_tree):
            fitting_traces.add(selected_variant)
        else:
            traces_to_add.add(selected_variant)

    return add_variants_to_process_model(
        d.pt,
        list(fitting_traces),
        list(traces_to_add),
        PoolFactory.instance().get_pool(),
    )



def add_variants_to_process_model(
    pt_dict: dict,
    fitting_traces: List[TypedTrace],
    traces_to_be_added: List[TypedTrace],
    pool: multithreading.pool.Pool,
):
    pt: ProcessTree
    frozen_subtrees: List[ProcessTree]
    pt, frozen_subtrees = dict_to_process_tree(pt_dict)

    frozen_subtrees_are_present = len(frozen_subtrees) > 0

    description = "adding variants to process tree without frozen subtrees"
    if frozen_subtrees_are_present:
        description = "adding variants to process tree including frozen subtrees"

    for t in tqdm(traces_to_be_added, desc=description):
        if not frozen_subtrees_are_present:
            pt = add_trace_to_pt_language(
                pt, fitting_traces, t, try_pulling_lca_down=True, pool=pool
            )
        else:
            pt, frozen_subtrees = add_trace_to_pt_language_with_freezing(
                pt,
                frozen_subtrees,
                fitting_traces,
                t,
                try_pulling_lca_down=True,
                pool=pool,
            )
        fitting_traces.append(t)
    res = process_tree_to_dict(pt, frozen_subtrees)
    return res
