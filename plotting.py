import pandas as pd
import os
from datetime import datetime
from bokeh_plot import *
from bokeh.plotting import output_file, save
from bokeh.models import NumeralTickFormatter


def plotting(results_df, initial_cash):
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
    results_dir = os.path.join('./plots/temp', f'{timestamp}')

    # Create results directory
    try:
        os.makedirs(results_dir, exist_ok=True)
    except OSError as e:
        print(f"Error creating directory {results_dir}: {e}")
        return

    # Convert positions to a DataFrame where each column is an asset and values are numeric quantities held
    positions_list = results_df['positions'].tolist()
    positions_df = pd.DataFrame([{k: v['shares'] for k, v in day.items()} for day in positions_list],
                                index=results_df.index)

    # Add the 'cash' column to positions_df
    positions_df['cash'] = results_df['cash']

    # Create transactions DataFrame
    transactions = [tx for day in results_df['transactions'] for tx in day]
    transactions_df = pd.DataFrame(transactions)

    # Handle empty transactions case
    if not transactions_df.empty:
        transactions_df['dt'] = pd.to_datetime(transactions_df['date'])
        transactions_df.set_index('dt', inplace=True)
        transactions_df.index = transactions_df.index.tz_localize('UTC')
    else:
        transactions_df = pd.DataFrame(columns=['option_id', 'action', 'quantity', 'price', 'date'])

    # Ensure transactions_df index is sorted by datetime
    transactions_df.sort_index(inplace=True)

    # Calculate daily returns based on portfolio value
    results_df['returns'] = results_df['portfolio_value'].pct_change().fillna(0)
    results_df = results_df.rename(columns={'portfolio_value': '资产总值',
                                            'cash': '现金'})

    # Cash flow calculation
    cash_flow = results_df['现金'].diff()
    cash_flow.iloc[0] = results_df['现金'].iloc[0] - initial_cash
    results_df['现金流'] = cash_flow

    # Plotting the results
    p_top_left = p_bars_multicol(results_df[['现金流', '现金']], name='现金与现金流', w=800, h=400)
    p_top_left.yaxis.formatter = NumeralTickFormatter(format="0.")
    p_top_left.title.align = "center"

    # Plot positions, excluding cash
    p_top_middle = p_lines_multicol(positions_df.drop(columns=['cash']), h=400, w=800)

    # Daily return line plot
    p_bottom_left = p_lines_multicol(results_df[['returns']], h=300, w=500)
    p_bottom_left.yaxis.formatter = NumeralTickFormatter(format="0.00%")
    p_bottom_left.yaxis.axis_label = "Daily Returns"

    # Final layout and plotting
    layout = column(
        row(p_top_left, p_top_middle, spacing=40),
        row(p_bottom_left, spacing=70)
    )

    show(layout)

    # Save results as CSV
    results_df[['returns']].to_csv(os.path.join(results_dir, 'returns.csv'))

    # Save layout to file
    output_file(os.path.join(results_dir, 'plots.html'))
    save(layout)


