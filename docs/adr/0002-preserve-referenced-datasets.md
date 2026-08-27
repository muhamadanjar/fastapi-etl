# Preserve referenced datasets

Data Sources are deactivated rather than cascade-deleted, and any Dataset referenced by a Transformation Run cannot be deleted. An unreferenced Dataset may be deleted manually, but automatic retention is excluded from the MVP; this deliberately favors reproducibility and lineage over automatic storage reclamation.
