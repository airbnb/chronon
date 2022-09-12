"""
Statistics to be collected for different input types
"""
@dataclass
class Statistic:
    """Class for the basic statistics definitions"""
    name: str
    expression: str
    prefix: str

    def expression(self, base):
        return f"{self.expression.format(base)} as {self.output(base)}"

    def output(self, base):
        return f"{self.prefix}_{base}"


def percentile(value, accuracy=0.01):
    """ Value between 0-100, Accuracy (0, 1) Lower accuracy better."""
    return Statistic(name=f"p{value}", expression=f"APPROX_PERCENTILE({{}}, {value}, {accuracy})", prefix=f"p{value}")


# Generic Data Quality Statistics
NullCnt = Statistic(name='null_count', expression="IF({} IS NULL, 1, NULL)", prefix='null_cnt')
NonNullCnt = Statistic(name='non_null_count', expression="IF({} IS NOT NULL, 1, NULL)", prefix='non_null_cnt')
NullRate = Statistic(name="null_rate", expression="IF({} IS NULL, 1, NULL) / COUNT(1)", prefix='null_rate')

# Numeric Data Quality Statistics
Min = Statistic(name='min_value', expression="MIN({})", prefix='min')
Max = Statistic(name='max_value', expression="MAX({})", prefix='max')
P99 = percentile("99")
P95 = percentile("95")
P50 = percentile("50")

SparkHistogram = Histogram
# List of statistics per column types.
AnyStats = [NullCnt, NonNullCnt, NullRate]
NumericStats = AnyStats + [Min, Max, P99, P95, P50]
