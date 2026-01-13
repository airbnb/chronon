import ai.chronon.api.ttypes as api
from ai.chronon.api.ttypes import JoinPart


class ExternalJoinPart(JoinPart):
    def __init__(self, join_part: JoinPart, full_prefix: str):
        # Copy all attributes from the source join_part to this instance
        super().__init__()
        for key, value in join_part.__dict__.items():
            setattr(self, key, value)
        self.external_join_full_prefix = full_prefix
        self._source_join_part = join_part

    @classmethod
    def from_external_part(cls, external_part):
        """
        Factory method to create an ExternalJoinPart from an external part with offlineGroupBy.
        Creates the synthetic JoinPart and computes the full_prefix internally.

        :param external_part: The external part containing offlineGroupBy
        :return: ExternalJoinPart instance
        """
        # Import here to avoid circular dependency
        from ai.chronon.utils import sanitize

        synthetic_jp = api.JoinPart(
            groupBy=external_part.source.offlineGroupBy,
            keyMapping=external_part.keyMapping if external_part.keyMapping else None,
            prefix=external_part.prefix if external_part.prefix else None,
        )
        full_prefix = "_".join(
            [
                component
                for component in ['ext', external_part.prefix, sanitize(external_part.source.metadata.name)]
                if component is not None
            ]
        )
        return cls(synthetic_jp, full_prefix=full_prefix)
