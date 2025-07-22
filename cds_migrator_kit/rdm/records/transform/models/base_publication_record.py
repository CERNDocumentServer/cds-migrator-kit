from cds_migrator_kit.rdm.records.transform.models.base_record import (
    rdm_base_record_model,
)
from cds_migrator_kit.transform.overdo import CdsOverdo


class CDSRDMPublication(CdsOverdo):
    """Base XML transform model."""


rdm_base_publication_model = CDSRDMPublication(
    bases=(rdm_base_record_model,),
    entry_point_group="cds_migrator_kit.migrator.rdm.rules.publication",
)
