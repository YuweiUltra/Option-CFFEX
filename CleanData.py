import os
import pandas as pd
import warnings
from enums import AssetTypes
import numpy as np

warnings.filterwarnings("ignore")


def read_csv_from_directory(directory_path):
    all_data = []

    # Iterate through all files in the directory
    for file_name in os.listdir(directory_path):
        if file_name.endswith('.csv'):
            file_path = os.path.join(directory_path, file_name)

            # Read each CSV file
            try:
                df = pd.read_csv(file_path, encoding='gb18030')

                # Filter out rows with '小记' or '合计' in the first column
                df_filtered = df[~df.iloc[:, 0].str.contains('小计|合计', na=False)]

                # Optionally, keep track of the source file (removes the extension from the filename)
                df_filtered['date'] = file_name.replace('.csv', '')
                all_data.append(df_filtered)

            except Exception as e:
                print(f"Failed to read {file_name}: {e}")

    # Concatenate all dataframes into a single dataframe (optional)
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        return combined_df
    else:
        return pd.DataFrame()  # Return an empty dataframe if no data is found


def add_listed_and_delisted_dates(df):
    # Sort the dataframe by '合约代码' and 'date'
    df['date'] = pd.to_datetime(df['date'])  # Convert the date column to datetime for easy comparison
    df = df.sort_values(by=['合约代码', 'date'])

    # Get the listed and delisted dates
    listed_dates = df.groupby('合约代码')['date'].first().reset_index()
    listed_dates.columns = ['合约代码', 'listed_date']

    delisted_dates = df.groupby('合约代码')['date'].last().reset_index()
    delisted_dates.columns = ['合约代码', 'de_listed_date']

    # Merge the listed and delisted dates back to the original dataframe
    df = pd.merge(df, listed_dates, on='合约代码', how='left')
    df = pd.merge(df, delisted_dates, on='合约代码', how='left')

    return df


# Example usage:
directory = 'downloads'
combined_data = read_csv_from_directory(directory)

# Strip any extra spaces from '合约代码' to clean the data
combined_data['合约代码'] = combined_data['合约代码'].apply(lambda x: x.strip())

# Add the listed and delisted dates
combined_data = add_listed_and_delisted_dates(combined_data)

# Read the LastDay_Info.xlsx
info = pd.read_excel('LastDay_Info.xlsx')

# Strip extra spaces from '合约代码' in info for consistency
info['合约代码'] = info['合约代码'].apply(lambda x: x.strip())

# Merge '上市日' and '最后交易日' into combined_data
info_subset = info[['合约代码', '上市日', '最后交易日']]
info_subset['上市日'] = pd.to_datetime(info_subset['上市日'].astype(str))
info_subset['最后交易日'] = pd.to_datetime(info_subset['最后交易日'].astype(str))

# Update the listed_date and delisted_date for 合约代码 that are in both dataframes
combined_data = pd.merge(
    combined_data,
    info_subset,
    on='合约代码',
    how='left',
    suffixes=('', '_info')
)

# Replace the listed_date and delisted_date with 上市日 and 最后交易日 from info, if available
combined_data['listed_date'] = combined_data['上市日'].combine_first(combined_data['listed_date'])
combined_data['de_listed_date'] = combined_data['最后交易日'].combine_first(combined_data['de_listed_date'])

# Drop the columns from info that were used for merging
combined_data = combined_data.drop(columns=['上市日', '最后交易日'])

column_mapping = {
    '合约代码': 'uni_id',
    '成交量': 'volume',
    '今收盘': 'close',
    '今结算': 'close_adj',
    '今开盘': 'open',
    '最高价': 'high',
    '最低价': 'low',
}

combined_data = combined_data.rename(columns=column_mapping)
combined_data['exchange'] = 'ZJS'
combined_data['type'] = np.where(
    combined_data['uni_id'].str.contains('-C-|-P-'),  # Check if 'uni_id' contains '-C-' or '-P-'
    AssetTypes.Option.value,  # Assign 'Option' if true
    AssetTypes.Future.value  # Assign 'Future' otherwise
)
combined_data = combined_data[
    ['uni_id', 'date', 'exchange', 'type', 'open', 'high', 'low', 'close', 'close_adj', 'volume', 'listed_date',
     'de_listed_date']]
combined_data = combined_data.dropna(axis=0)

# combined_data = combined_data.set_index(['date', '合约代码'])
# combined_data = combined_data.sort_index(level=1).sort_index(level=0)

# Pass the final dataframe
combined_data_futures = combined_data[combined_data['type'] == AssetTypes.Future.value]
combined_data_options = combined_data[combined_data['type'] == AssetTypes.Option.value]
combined_data_options['strike_price'] = combined_data_options['uni_id'].apply(lambda x: x.split('-')[-1]).astype(int)
combined_data_options['option_type'] = combined_data_options['uni_id'].apply(lambda x: x.split('-')[1]).astype(str)
combined_data_options['underlying_id'] = combined_data_options['uni_id'].apply(lambda x: x.split('-')[0]).astype(str)

combined_data_futures.to_csv('CleanedData_futures.csv')
combined_data_options.to_csv('CleanedData_options.csv')
