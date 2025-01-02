
from cortado_core.models.infix_type import InfixType
from cortado_core.utils.sequentializations import generate_sequentializations
from cortado_core.utils.split_graph import Group
from cortado_core.utils.trace import TypedTrace
from pm4py.util.variants_util import variant_to_trace


def get_traces_from_variants(variants):
    n_sequentializations = -1
    traces = []

    for cvariant, infix_type in variants:
        sequentializations = generate_sequentializations(
            Group.deserialize(cvariant), n_sequentializations=n_sequentializations
        )
        traces += [
            TypedTrace(variant_to_trace(seq), InfixType(infix_type))
            for seq in sequentializations
        ]

    return traces
