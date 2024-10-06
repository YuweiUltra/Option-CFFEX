from bokeh.plotting import figure, show, output_file, output_notebook
from bokeh.layouts import column, row, gridplot
from bokeh.models import ColumnDataSource, Legend, LegendItem, Range1d, CrosshairTool, HoverTool, LinearAxis, Span, \
    NumeralTickFormatter
from bokeh.palettes import Category10
from bokeh.transform import dodge
from datetime import timedelta
import pandas as pd


def Candlestick(eod_df, name="", h=300, w=400):
    """
    eod_df: DataFrame with columns ['date', 'open', 'close']
    """

    # Prepare data for plotting
    source = ColumnDataSource(eod_df)

    # Define increasing and decreasing days
    inc = eod_df['open'] < eod_df['close']
    dec = eod_df['open'] > eod_df['close']

    # Create the plot
    p = figure(x_axis_type="datetime", title=f"Candlestick Chart: {name}",
               height=h, width=w)

    # Plot candlesticks
    width = 12 * 60 * 60 * 1000  # half day in ms

    # Rectangles for increasing prices
    p.rect(x='date', y='midpoint', width=width, height='height', fill_color="green", line_color="black",
           source=ColumnDataSource(eod_df[inc]))

    # Rectangles for decreasing prices
    p.rect(x='date', y='midpoint', width=width, height='height', fill_color="red", line_color="black",
           source=ColumnDataSource(eod_df[dec]))

    return p


def p_lines_multicol(df, name="", h=300, w=400):
    """
    Plots multiple columns as lines with the index as x-axis
    """
    # Create plot
    p = figure(x_axis_type="datetime", title=name, height=h, width=w)

    # Define colors for the lines
    colors = Category10[10]

    # Add lines for each column in the DataFrame
    legend_items = []
    for idx, column in enumerate(df.columns):
        source = ColumnDataSource(data={'date': df.index, 'value': df[column]})
        line = p.line(x='date', y='value', line_width=2, color=colors[idx % len(colors)], source=source)
        legend_item = LegendItem(label=column, renderers=[line])
        legend_items.append(legend_item)

    # Configure y-axis
    p.yaxis.formatter = NumeralTickFormatter(format="0.")

    # Add hover and crosshair tools
    p.add_tools(CrosshairTool())
    p.add_tools(HoverTool(tooltips=[("Date", "@date{%F}"), ("Value", "@value")], formatters={'@date': 'datetime'}))

    # Add legend
    legend = Legend(items=legend_items, location="right")
    p.add_layout(legend, 'right')

    return p


def p_lines_multicol_twoaxis(df, name="", h=300, w=400, left_y_axis=None, right_y_axis=None, add_dots=False):
    """
    Plots two y-axes for multiple lines
    """
    # Create plot
    p = figure(x_axis_type="datetime", title=name, height=h, width=w)

    # Define colors for the lines
    colors = Category10[10]

    # Plot for left y-axis
    if left_y_axis:
        for idx, column in enumerate(left_y_axis):
            source = ColumnDataSource(data={'date': df.index, 'value': df[column]})
            line = p.line(x='date', y='value', line_width=2, color=colors[idx % len(colors)], source=source)
            if add_dots:
                p.circle(x='date', y='value', size=5, color=colors[idx % len(colors)], source=source)

    # Plot for right y-axis
    if right_y_axis:
        p.extra_y_ranges = {"right_axis": Range1d(start=df[right_y_axis].min().min(), end=df[right_y_axis].max().max())}
        p.add_layout(LinearAxis(y_range_name="right_axis"), 'right')
        for idx, column in enumerate(right_y_axis):
            source = ColumnDataSource(data={'date': df.index, 'value': df[column]})
            p.line(x='date', y='value', line_width=2, color=colors[(idx + len(left_y_axis)) % len(colors)],
                   y_range_name="right_axis", source=source)

    return p


def p_bars_multicol(df, name="", h=300, w=400, bar_width=0.8):
    """
    Plots multiple columns as bars with the index as x-axis
    """
    # Ensure datetime index is converted to string
    if pd.api.types.is_datetime64_any_dtype(df.index):
        df.index = df.index.strftime('%Y-%m-%d')

    # Prepare the data
    data = {'x': df.index.tolist()}
    for col in df.columns:
        data[col] = df[col].fillna(0).tolist()

    # Create ColumnDataSource
    source = ColumnDataSource(data=data)

    # Create the figure
    p = figure(x_range=data['x'], title=name, height=h, width=w)

    # Plot each column as a bar
    colors = ["#800000", "#FFD700", "#e84d60", "#ddb7b1"]  # Add more colors if needed
    for i, col in enumerate(df.columns):
        p.vbar(x=dodge('x', i * bar_width / len(df.columns) - bar_width / 2, range=p.x_range),
               top=col, width=bar_width / len(df.columns), source=source, color=colors[i % len(colors)],
               legend_label=col)

    # Customize the plot
    p.xgrid.grid_line_color = None
    p.x_range.range_padding = 0.1
    p.legend.location = "top_left"
    p.legend.orientation = "horizontal"
    p.yaxis.axis_label = "Values"
    p.xaxis.major_label_orientation = 1.2

    return p


def p_bars_multicol_twoaxis(df, name="", left_y_axis=None, right_y_axis=None, h=300, w=800, bar_width=0.2):
    """
    Plots multiple columns as bars with two y-axes
    """
    # Convert datetime index to string if necessary
    if pd.api.types.is_datetime64_any_dtype(df.index):
        df.index = df.index.strftime('%Y-%m-%d')

    # Prepare the data
    data = {'x': df.index.tolist()}
    for col in df.columns:
        data[col] = df[col].fillna(0).tolist()

    # Create ColumnDataSource
    source = ColumnDataSource(data=data)

    # Create the figure
    p = figure(x_range=data['x'], title=name, height=h, width=w)
    p.title.align = "center"

    # Plot for left y-axis
    colors_left = ["#FF6347", "#32CD32"]
    for i, col in enumerate(left_y_axis):
        p.vbar(x=dodge('x', i * bar_width, range=p.x_range), top=col, width=bar_width, source=source,
               color=colors_left[i % len(colors_left)], legend_label=col)

    # Plot for right y-axis
    colors_right = ["#1E90FF", "#FFA500"]
    p.extra_y_ranges = {"right_axis": Range1d(start=df[right_y_axis].min().min(), end=df[right_y_axis].max().max())}
    p.add_layout(LinearAxis(y_range_name="right_axis", axis_label="Right Y-Axis Values"), 'right')
    for i, col in enumerate(right_y_axis):
        p.vbar(x=dodge('x', (i + len(left_y_axis)) * bar_width, range=p.x_range), top=col, width=bar_width,
               source=source, color=colors_right[i % len(colors_right)], y_range_name="right_axis", legend_label=col)

    return p


def add_moving_average_line(p, df, window=30):
    """
    Adds a moving average line to the given Bokeh plot
    """
    df['moving_avg'] = df[df.columns[0]].rolling(window=window).mean()
    source = ColumnDataSource(data={'date': df.index, 'moving_avg': df['moving_avg']})

    p.line(x='date', y='moving_avg', source=source, line_width=2, color="orange", legend_label=f"{window}-Day MA")
    p.scatter(x='date', y='moving_avg', source=source, size=6, color="orange", fill_alpha=0.6,
              legend_label=f"{window}-Day MA")

    return p
