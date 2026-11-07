"""A kit a partner can run against itself before we plug together (spec 10.4).

Nothing here is specific to our mocks: the checks take a Twin-shaped object and a
Guardian-shaped object and say whether what they produce is something the Brain can accept.
The Brain runs the kit against its own mocks in `tests/conformance/`, and against the live
partners when their endpoints are reachable.
"""

from conformance.test_partner_conformance import (
    check_guardian,
    check_twin,
    conformance_report,
)

__all__ = ["check_guardian", "check_twin", "conformance_report"]
