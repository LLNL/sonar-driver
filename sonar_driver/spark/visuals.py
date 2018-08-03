import plotly
import plotly.graph_objs as go
import numpy as np
import pandas as pd

from pyspark.sql.functions import col, desc

from bokeh.layouts import column
from bokeh.plotting import figure, show, output_notebook
from bokeh.models import CustomJS, ColumnDataSource, HoverTool
output_notebook()

def plot_analytics(x_axis, y_axis, plot_title=None, x_title=None, y_title=None):
    """
    General plotting function for a plotly graph.
    :x_axis: List of x values.
    :y_axis: List of y values.
    :plot_title: Title of plot.
    :x_title: Title of x-axis.
    :y_title: Title of y-axis.   
    :return: None.
    """
    plotly.offline.init_notebook_mode(connected=True)
    
    plotly.offline.iplot({
        "data": [go.Scatter(x=x_axis, 
                            y=y_axis)],
        "layout": go.Layout(title=plot_title,
                            xaxis=dict(title=x_title),
                            yaxis=dict(title=y_title))
    })

def plot_derivatives(sparkdf, column, window_size, slide_length):
    """
    Plots job start/completion rate versus time for a specified window size and slide length. 
    :sparkdf: Spark DataFrame of timesteps and rate of jobs started/completed at each window.
    :column: Can either be 'StartTime' or 'EndTime' to specify job start or job completion rate, respectively.
    :window_size: Window size of discrete derivatives.
    :slide_length: Slide length of window measurements.
    :return: None.
    """
    df = sparkdf.toPandas()
    
    x_axis = df['Time']
    y_axis = df['count']
    
    verb = "Completed" if column == "EndTime" else "Started"
    plot_title = "Number of Jobs " + verb + " Per Sec, Window=" + str(window_size) + ", Slide=" + str(slide_length)
    x_title = "Time"
    y_title = "Jobs " + verb + "/Sec in Previous " + str(window_size) + " Sec"
    
    plot_analytics(x_axis, y_axis, plot_title=plot_title, x_title=x_title, y_title=y_title)

def plot_integrals(sparkdf, slide_length):
    """
    Plots number of jobs running concurrently versus time with a specified slide length to determine timesteps.
    :df: Spark DataFrame of timesteps and number of jobs running concurrently at each timestep.
    :slide_length: Slide length of timesteps.
    :return: None.
    """
    df = sparkdf.toPandas()
    
    x_axis = df['Time']
    y_axis = df['count']
    
    plot_title = "Number of Jobs Running vs. Time, Slide Length=" + str(slide_length)
    x_title = "Time"
    y_title = "Number of Jobs Running"
    
    plot_analytics(x_axis, y_axis, plot_title=plot_title, x_title=x_title, y_title=y_title)
    
def plot_allocs(sparkdf):
    """
    Plots a bar chart of total number of allocations for each allocation size and a linked gantt chart
    that groups allocations of the same size into non-overlapping pools.
    
    :sparkdf: Spark DataFrame of allocation data
    :return: None
    """
    df = sparkdf.toPandas()
    df['alloc_time'] = pd.to_datetime(df['alloc_time'], unit='ns')
    df['free_time'] = pd.to_datetime(df['free_time'], unit='ns')
    
    alloc_counts = sparkdf.select(col('size')).groupBy('size').count().sort(desc('count')).toPandas()
    sizes, counts = [str(s) for s in alloc_counts['size']], list(alloc_counts['count'])
    
    c0 = ColumnDataSource(data=df)
    c1 = ColumnDataSource(data=dict(Sizes=sizes, Counts=counts))
    c2 = ColumnDataSource(data=dict(Address=[], Size=[], Start=[], End=[], Bottom=[], Top=[], Pool=[]))
    
    width = 500 if len(sizes) < 5 else len(sizes) * 75
    f1 = figure(tools='box_select', x_range=sizes, title="Counts of Allocations of Different Sizes",
                width=width, height=500)
    f1.vbar(x='Sizes', top='Counts', source=c1, width=0.9)
    f1.xaxis.axis_label = 'Allocation Size'
    f1.yaxis.axis_label = 'Count'
    f1.xgrid.grid_line_color = None
    f1.y_range.start = 0

    f2 = figure(title='Gantt Chart of Allocations', width=1000, height=500, 
                x_axis_type='datetime', tools='box_zoom,reset')
    f2.quad(left='Start', right='End', bottom='Bottom', top='Top', source=c2)
    f2.xaxis.axis_label = 'Time'
    f2.yaxis.axis_label = 'Pool Number'

    hover=HoverTool(tooltips="Address: @Address<br>\
                              Size: @Size<br>\
                              Start: @Start<br>\
                              End: @End<br>\
                              Pool: @Pool<br>")
    f2.add_tools(hover)

    jscode = """
        var d0 = c0.data;
        var d1 = cb_obj.data;
        var d2 = c2.data;

        var inds = cb_obj.selected.indices;
        var sizes = [];
        for (var i = 0; i < inds.length; i++) {
            sizes.push(parseInt(d1['Sizes'][inds[i]]));
        }

        d2['Address'] = [];
        d2['Size'] = [];
        d2['Start'] = [];
        d2['End'] = [];
        d2['Bottom'] = [];
        d2['Top'] = [];
        d2['Pool'] = [];

        var active = [];
        for (var i = 0; i < d0['index'].length; i++) {
            if (sizes.includes(d0['size'][i])) {
                var start = d0['alloc_time'][i];
                var end = d0['free_time'][i];
                
                d2['Address'].push(d0['address'][i]);
                d2['Size'].push(d0['size'][i]);
                d2['Start'].push(start);
                d2['End'].push(end);
                
                var pool = -1;
                var min_end = Number.MAX_SAFE_INTEGER;
                for (var j = 0; j < active.length; j++) {
                    jth_end = active[j];
                    if (start > jth_end && jth_end < min_end) {
                        pool = j;
                        min_end = jth_end;
                    }
                }
                if (pool == -1) {
                    active.push(end);
                    pool = active.length - 1;
                } else {
                    active[pool] = end;
                }
                
                d2['Bottom'].push(pool - 0.25);
                d2['Top'].push(pool + 0.25);
                d2['Pool'].push(pool);
            }
        }
        
        f2.title.setv({"text": "Gantt Chart of Allocations of Size " + sizes.toString()});
        f2.x_range.setv({"start": Math.min(...d2['Start']), "end": Math.max(...d2['End'])});
        f2.y_range.setv({"start": -1, "end": active.length + 1});

        c2.change.emit();
    """
    c1.callback = CustomJS(args=dict(c0=c0, c2=c2, f2=f2), code=jscode)

    layout = column(f1, f2)
    show(layout)