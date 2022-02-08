from ai.zipline.api import ttypes


class Source(ttypes.Source):
    """
    Subclass that takes additional arguments and stores them.
    """

    def __init__(self, events=None, entities=None, **kwargs):
        super().__init__(events=events, entities=entities)
        self.kwargs = kwargs

    @property
    def table(self):
        return self.events.table if self.events else self.entities.snapshotTable

    @property
    def extra_args(self):
        """
        Extra arguments to be passed to GroupBy or Join metadata.
        """
        return self.kwargs
