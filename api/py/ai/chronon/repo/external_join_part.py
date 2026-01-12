from ai.chronon.api.ttypes import JoinPart


class ExternalJoinPart(JoinPart):
    def __init__(self, join_part: JoinPart, full_prefix: str):
        # Copy all attributes from the source join_part to this instance
        super().__init__()
        for key, value in join_part.__dict__.items():
            setattr(self, key, value)
        self.external_join_full_prefix = full_prefix
        self._source_join_part = join_part
