import re

from pyspark.sql.functions import udf, col, explode, lit, split
from pyspark.sql.types import ArrayType, BooleanType, DoubleType, IntegerType, StringType, TimestampType

def query_time_range(sparkdf, start_column, end_column, time_range):
    start_range, end_range = time_range[0], time_range[1]
    filter_starts, filter_ends = time_range[2], time_range[3]

    sparkdf = (
        sparkdf
            .where(
                (
                    (col(start_column) > lit(start_range).cast(TimestampType())) & 
                    (col(start_column) < lit(end_range).cast(TimestampType()))
                ) 
                | 
                (
                    (col(end_column) > lit(start_range).cast(TimestampType())) & 
                    (col(end_column) < lit(end_range).cast(TimestampType()))
                )
            )
    )

    if filter_starts:
        sparkdf = (
            sparkdf
                .where(
                    (col(start_column) > lit(start_range).cast(TimestampType())) & 
                    (col(start_column) < lit(end_range).cast(TimestampType()))
                )
        )

    if filter_ends:
        sparkdf = (
            sparkdf
                .where(
                    (col(end_column) > lit(start_range).cast(TimestampType())) & 
                    (col(end_column) < lit(end_range).cast(TimestampType()))
                )
        )
        
    return sparkdf

def cluster_nodes_jobdata(node):
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
    
def cluster_nodes_dict():
    return {
        'jobdata': cluster_nodes_jobdata
    }


def query_nodes(sparkdf, table_name, nodes_column, nodes):
    cluster_nodes = cluster_nodes_dict()[table_name]
    
    input_nodes = {}
    for n in nodes:
        cluster_name, node_lst = cluster_nodes(n)
        input_nodes[cluster_name] = node_lst
        
    def isin_nodes(node):
        cluster_nodes = cluster_nodes_dict()[table_name]

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
        
    isin_nodes_udf = udf(isin_nodes, BooleanType())
    return sparkdf.filter(isin_nodes_udf(col(nodes_column)))

def query_users(sparkdf, users_column, users):
    return sparkdf.filter(col(users_column).isin(*users) == True)
    
    
    
    
    
    