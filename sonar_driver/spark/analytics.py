from pyspark.sql.functions import lead, lag
from pyspark.sql.window import Window
from pyspark.sql.functions import udf, col, explode, lit, split
from pyspark.sql.types import DoubleType, StringType, TimestampType


def split_dataframes(sparkdf, column):
    """
    Split a dataframe into multiple dataframes by distinct values along a column.
    :param sparkdf: Input Spark dataframe.
    :param column: Column name to split over.
    :return: A list of Spark dataframes, where each has a cardinality of 1 in the column.
    """

    # Get distinct values
    distinct_values = [
        row[column] for row in
        sparkdf.select(column).distinct().collect()
    ]

    # Filter by each distinct value
    return [
        sparkdf.filter(col(column) == value)
        for value in distinct_values
    ]


def finite_difference(sparkdf, xaxis, yaxes, window_size, monotonically_increasing=False):
    """
    Calculate the finite difference dY/dX for 1 or more Y=f(X) axes with respect to a single X axis
    :param sparkdf: Input Spark dataframe.
    :param xaxis: Column name for X axis.
    :param yaxes: List of column names for Y axes.
    :param window_size: Width of window over which to calculate finite difference (in number of points).
    :param monotonically_increasing: Whether Y axes should be monotonically increasing (e.g. counters). If set,
           negative finite differences in Y will be set to zero.
    :return: A Spark dataframe with one new column per Y axis calculated as dY/dX.
    """

    original_columns = sparkdf.schema.fieldNames()

    # Get the first value, so we can use it to define the initial "previous value"
    first_row = sparkdf.first()
    first_x = first_row[xaxis]

    # Slide over this window
    window = Window.orderBy(xaxis)

    # Create function to calculate difference between two columns
    delta_fn = udf(lambda col1, col2: col1 - col2, DoubleType())
    div_fn = udf(lambda col1, col2: col1 / col2 if col2 > 0 else float('nan'), DoubleType())

    # Get delta X
    xaxis_lag = xaxis + '_lag_' + str(window_size)
    xaxis_delta = xaxis + '_delta_' + str(window_size)
    df = (
        sparkdf
            .withColumn(xaxis, sparkdf[xaxis].cast(DoubleType()))
            .withColumn(xaxis_lag, lag(xaxis, window_size, first_x).over(window))
            .withColumn(xaxis_delta, delta_fn(col(xaxis), col(xaxis_lag)))
            .drop(xaxis_lag)
    )

    # Get delta y and dY/dX for each y
    for yaxis in yaxes:
        yaxis_lag = yaxis + '_lag_' + str(window_size)
        yaxis_delta = yaxis + '_delta_' + str(window_size)
        rate = 'rate_' + yaxis + '_over_' + xaxis
        rate_lag = rate + '_lag'
        rate_lead = rate + '_lead'

        first_y = first_row[yaxis]

        df = (
            df
                .withColumn(yaxis, sparkdf[yaxis].cast(DoubleType()))
                .withColumn(yaxis_lag, lag(yaxis, window_size, first_y).over(window))
                .withColumn(yaxis_delta, delta_fn(col(yaxis), col(yaxis_lag)))
        )

        if monotonically_increasing:
            df[yaxis_delta] = df[yaxis_delta].map(lambda d: max(0, d))

        df = (
            df
                .withColumn(rate, div_fn(col(yaxis_delta), col(xaxis_delta)))
                .drop(yaxis_lag)
                .drop(yaxis_delta)

                # Determine when the delta changes (lead and lag by 1)
                .withColumn(rate_lag, lag(rate, 1, 1).over(window))
                .withColumn(rate_lead, lead(rate, 1, 1).over(window))

                # Only get values where rate is different from before or after
                .filter(rate + '!=' + rate_lag + ' OR ' + rate + '!=' + rate_lead)

                .drop(rate_lag)
                .drop(rate_lead)
        )

    return df.drop(xaxis_delta)

def query_time_range(sparkdf, from_time, to_time, column=None):
    """
    Query jobs within a time range.
    :param sparkdf: Input Spark dataframe.
    :param from_time: Start of time range.
    :param to_time: End of time range.
    :param column: If set, only start times (column='StartTime') or end times (column='EndTime') 
           will be within time range. 
    :return: A Spark dataframe with jobs whose start times or end times are within specified time range.
    """
    col1, col2 = 'StartTime', 'EndTime'
    if column:
        col1, col2 = column, column
        
    return (
        sparkdf
            .withColumn('FromTime', lit(from_time).cast(TimestampType()))
            .withColumn('ToTime', lit(to_time).cast(TimestampType()))
            .filter("{} > FromTime AND {} < ToTime".format(col1, col2))
            .drop('FromTime')
            .drop('ToTime')
    )

def discrete_derivatives(sparkdf, column, window_size, slide_length):
    """
    Calculate discrete derivatives (job initiation rate or job completion rate).
    :param sparkdf: Input Spark dataframe.
    :param column: 'StartTime' for initiation rate and 'EndTime' for completion rate.
    :param window_size: Time range (secs) over which to calculate derivatives.
    :param slide_length: Amount of time (secs) to slide window at each step.
    :return: A Spark dataframe with discrete derivative calculated at each timestep.
    """
    sparkdf = cast_double(sparkdf)
            
    def range_windows(column, window, slide):
        first = int(window + slide * ((column - window) // slide + 1))
        last = int(window + slide * (column // slide))
        return range_list(first, last, slide)
    range_windows = udf(range_windows, StringType())
    
    return (
        sparkdf
            .withColumn('Range', split(range_windows(col(column), lit(window_size), lit(slide_length)), ','))
            .select(explode(col('Range')).alias('Time'))
            .select(col('Time').cast(DoubleType()).cast(TimestampType())).groupBy('Time').count().sort('Time')
            .withColumn('count', udf(lambda x: x / window_size, DoubleType())(col('count')))
    )

def discrete_integrals(sparkdf, slide_length):
    """
    Calculate discrete integrals (number of active jobs vs. time).
    :param sparkdf: Input Spark dataframe.
    :param slide_length: Amount of time (secs) between each timestep.
    :return: A Spark dataframe with discrete integral calculated at each timestep.
    """
    sparkdf = cast_double(sparkdf)
            
    def range_times(col1, col2, slide):
        first = int((col1 // slide + 1) * slide)
        last = int((col2 // slide) * slide)
        if first > last:
            return ""
        return range_list(first, last, slide)
    range_times = udf(range_times, StringType())
    
    return (
        sparkdf
            .withColumn('Range', split(range_times(col('StartTime'), col('EndTime'), lit(slide_length)), ','))
            .select(explode(col('Range')).alias('Time'))
            .select(col('Time').cast(DoubleType()).cast(TimestampType())).groupBy('Time').count().sort('Time')
            .where(col('Time').isNotNull())
    )

def cast_double(sparkdf):
    """
    Helper function to cast columns of type 'timestamp' to type 'double.'
    """
    for dtype in sparkdf.dtypes:
        if dtype[1] == 'timestamp':
            sparkdf = sparkdf.withColumn(dtype[0], col(dtype[0]).cast(DoubleType()))
    return sparkdf

def range_list(first, last, slide):
    """
    Helper function for discrete_derivatives and discrete_integrals functions.
    """
    range_list = ""
    for x in range(0, (last - first) // slide + 1):
        range_list = range_list + str(first + slide * x) + ','
    return range_list[:-1]

    
    
