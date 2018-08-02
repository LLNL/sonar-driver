import re

from pyspark.sql.functions import lead, lag
from pyspark.sql.window import Window
from pyspark.sql.functions import udf, col, explode, lit, split
from pyspark.sql.types import BooleanType, DoubleType, StringType, TimestampType


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

def query_jobs(sparkdf, time_range=None, nodes=None, users=None):
    """
    Query jobs within a time range, on certain clusters, nodes, and run by certain users.
    :param sparkdf: Input Spark dataframe.
    :param time_range: List, array-like of time range start and end and optional third argument which, if set,
           only start times ('StartTime') or end times ('EndTime') will be within time range. 
    :param nodes: List, array-like of clusters and nodes to filter. Format should follow this example:
           ['rztrona', rzgenie8', 'rztopaz[10-12]', 'rzalastor[1-2,3-4]'].
    :param users: List, array-like of users to query.
    :return: A Spark dataframe with jobs whose start times or end times are within specified time range.
    """
    if time_range:
        start_range, end_range = time_range[0], time_range[1]
        
        column = None if len(time_range) == 2 else time_range[2]
        col1, col2 = column if column else 'StartTime', column if column else 'EndTime'
        
        sparkdf = (
            sparkdf
                .where(
                    (col(col1) > lit(start_range).cast(TimestampType())) & 
                    (col(col2) < lit(end_range).cast(TimestampType()))
                )
        )
        
    if nodes:
        def cluster_nodes(node):
            if '[' in node:
                splits = [i for i, v in enumerate(node) if 
                          v == '-' or v == ',' or v == '[' or v == ']']
                
                cluster_name = node[:splits[0]]
                node_lst = []
                for i in range(len(splits) - 1):
                    node_lst.append(int(node[splits[i] + 1: splits[i + 1]]))
                    
                return cluster_name, node_lst
            
            elif node.isalpha():
                return node, 0
            
            else:
                m = re.search("\d", node)
                i = m.start()
                cluster_name, node_num = node[:i], int(node[i:])
                return cluster_name, [node_num, node_num]
        
        input_nodes = {}
        for n in nodes:
            cluster_name, node_lst = cluster_nodes(n)
            input_nodes[cluster_name] = node_lst
                
        def isin_nodes(node):
            cluster_name, node_lst = cluster_nodes(node)
            if cluster_name in input_nodes:
                input_node_lst = input_nodes[cluster_name]
                
                if input_node_lst == 0:
                    return True
                
                for i in range(len(node_lst) // 2):
                    on_nodes = False
                    for j in range(len(input_node_lst) // 2):
                        within_nodes = (
                            node_lst[2 * i] >= input_node_lst[2 * j] and 
                            node_lst[2 * i + 1] <= input_node_lst[2 * j + 1]
                        )
                        if within_nodes:
                            on_nodes = True
                            break
                    
                    if not on_nodes:
                        return False
                    
                return True
            
            return False    
        
        isin_nodes = udf(isin_nodes, BooleanType())
        sparkdf = sparkdf.filter(isin_nodes(sparkdf.NodeList))

    if users:
        sparkdf = sparkdf.filter(sparkdf.User.isin(*users) == True)
        
    return sparkdf

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

def allocs_sorted(sparkdf):
    pandasdf = sparkdf.toPandas()
    
    starts_dict = dict(zip(pandasdf['alloc_time'], pandasdf['size']))
    ends_dict = dict(zip(pandasdf['free_time'], pandasdf['size']))
    starts_sorted = sorted(starts_dict)
    ends_sorted = sorted(ends_dict)
    
    return starts_dict, ends_dict, starts_sorted, ends_sorted

def pool_counts(sparkdf):
    """
    Calculate minimum pools for each unique allocation size.
    :param sparkdf: Input Spark dataframe.
    :return: A list of dicts with size and count of each pool.
    """
    starts_dict, ends_dict, starts_sorted, ends_sorted = allocs_sorted(sparkdf)

    size_groups = {s:{'current': 0, 'max': 0} for s in [r.size for r in sparkdf.select('size').distinct().collect()]}
    
    start_index, end_index = 0, 0
    while start_index < len(starts_sorted) or end_index < len(ends_sorted):
        start, end = None, ends_sorted[end_index]
        if start_index < len(starts_sorted):
            start = starts_sorted[start_index]

        if start is None or start > end:
            group = size_groups[ends_dict[end]]
            group['current'] -= 1
            end_index += 1
        else:
            group = size_groups[starts_dict[start]]
            group['current'] += 1
            if group['current'] > group['max']:
                group['max'] = group['current']
            start_index += 1
    
    return [{'size': int(s), 'count': int(size_groups[s]['max'])} for s in size_groups.keys()]
    
def max_memory_unpooled(sparkdf):
    """
    Calculate maximum active memory needed for an unpooled allocation configuration.
    :param sparkdf: Input Spark dataframe.
    :return: Float of maximum active memory for unpooled allocations.
    """
    starts_dict, ends_dict, starts_sorted, ends_sorted = allocs_sorted(sparkdf)
    
    active_memory = {'current': 0, 'max': 0}
    
    start_index, end_index = 0, 0
    while start_index < len(starts_sorted) or end_index < len(ends_sorted):
        start, end = None, ends_sorted[end_index]
        if start_index < len(starts_sorted):
            start = starts_sorted[start_index]

        if start is None or start > end:
            active_memory['current'] -= ends_dict[end]
            end_index += 1
        else:
            active_memory['current'] += starts_dict[start]
            if active_memory['current'] > active_memory['max']:
                active_memory['max'] = active_memory['current']
            start_index += 1

    return active_memory['max']

def max_memory_pooled(sparkdf):
    """
    Calculate maximum active memory needed for a pooled allocation configuration.
    :param sparkdf: Input Spark dataframe.
    :return: Float of maximum active memory for pooled allocations.
    """
    pools = pool_counts(sparkdf)
    return sum([p['size'] * p['count'] for p in pools])

def total_bytesecs_unpooled(sparkdf):
    """
    Calculate total bytesecs (1 byte of memory allocated for 1 sec) needed for an 
        unpooled allocation configuration.
    :param sparkdf: Input Spark dataframe.
    :return: Float of total bytesecs for unpooled allocations.
    """
    calc_bytesecs = udf(lambda size, alloc, free: size * (free - alloc), DoubleType())

    return (
        sparkdf
            .withColumn('bytesecs', calc_bytesecs(col('size'), col('alloc_time'), col('free_time')))
    ).rdd.map(lambda x: float(x["bytesecs"])).reduce(lambda x, y: x+y) / 1e9

def total_bytesecs_pooled(sparkdf):
    """
    Calculate total bytesecs (1 byte of memory allocated for 1 sec) needed for a
        pooled allocation configuration.
    :param sparkdf: Input Spark dataframe.
    :return: Float of total bytesecs for pooled allocations.
    """
    pools = pool_counts(sparkdf)
    
    min_time = sparkdf.agg({"alloc_time": "min"}).collect()[0][0] / 1e9
    max_time = sparkdf.agg({"free_time": "max"}).collect()[0][0] / 1e9
    range_time = max_time - min_time

    return sum([p['size'] * p['count'] * range_time for p in pools])

    
    
