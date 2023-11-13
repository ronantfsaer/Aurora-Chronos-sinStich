import pandas as pd

#inputs so can turn script into function later:

#store of sim titles:



'''first_sim = 'Contego_1 - Low'
second_sim = 'Contego_2 - Low'
combined_sim_name = 'Contego - low'''

#speicfify simuaion names to stich:
first_sim = 'Contego_1 - Central'
second_sim = 'Contego_2 - Central'
combined_sim_name = 'Contego - Central'

stictch_date = '2024-04-01'


# Load the data
file_path = '13112023_Final_Chronos_DB_DL.xlsx'

monthly_df = pd.read_excel(file_path, sheet_name='Monthly outputs')
yearly_df = pd.read_excel(file_path, sheet_name='Yearly outputs')
quarterly_df = pd.read_excel(file_path, sheet_name='Quarterly outputs')

# Convert and format the Timestamp column and Define the cutoff timestamp
monthly_df['Timestamp'] = pd.to_datetime(monthly_df['Timestamp'])
timestamp = pd.Timestamp(stictch_date)

# Combine the simulations
combined_monthly_df = pd.concat([
    monthly_df[(monthly_df['Simulation Title'] == first_sim) & (monthly_df['Timestamp'] < timestamp)],
    monthly_df[(monthly_df['Simulation Title'] == second_sim) & (monthly_df['Timestamp'] >= timestamp)]
])
combined_monthly_df['Simulation Title'] = combined_sim_name
combined_monthly_df = combined_monthly_df.sort_values(by='Timestamp')


# Add a year column for grouping & add quarter column
combined_monthly_df['year'] = combined_monthly_df['Timestamp'].dt.year
combined_monthly_df['quarter'] = combined_monthly_df['Timestamp'].dt.quarter
combined_monthly_df['month'] = combined_monthly_df['Timestamp'].dt.month

#load in agg mapping from excel file:
agg_file_path = 'Monthly_yearly_agg_fact_mapping.xlsx'
factAggMapping_df = pd.read_excel(agg_file_path, sheet_name='factStandardAggMapping')
weightedAvMapping_df = pd.read_excel(agg_file_path, sheet_name='weightedAvMapping')

# Standard aggregations from mapping, filtered for 'sum' and 'mean'
aggregations = {fact: method for fact, method in zip(factAggMapping_df['Fact'], factAggMapping_df['Aggragation']) if method in ['sum', 'mean']}

# Identify columns in monthly_df not in factAggMapping_df
exclude_columns = set(['Timestamp'])
non_aggregated_columns = set(monthly_df.columns) - set(factAggMapping_df['Fact']) #- exclude_columns

# Update the aggregations dictionary to include these columns; 'first' as these values constant
for col in non_aggregated_columns:
    aggregations[col] = 'first'

# Function to compute weighted average
def weighted_average(data, avg_name, weight_name):
    '''Data: a pandas DF.Groupby object with the data of interest to be averaged
    avg_name: fact to average over year/quarter (from weightedAvMapping_df)
    weight_name: weighting fact (from weightedAvMapping_df)

    (thing_to_average * weight).sum() / total_weight returns a single number
    '''
    data_now = data
    data_length = len(data_now)
    thing_to_average = data[avg_name]
    weight = data[weight_name]
    total_weight = weight.sum()
    w_av = (thing_to_average * weight).sum() / total_weight

    # Check if the weights sum to zero or NaN
    if total_weight == 0 or pd.isna(total_weight):
        return 0  # Or np.nan, depending on how you want to handle this case

    # Now calculate the weighted average
    return w_av

def calculate_weighted_average_for_fact(grouped_df, fact, weight):
    '''The apply method takes the weighted_average function and calls it once for each group within the grouped_df.
    For each group, the apply method internally passes the group (which is a DataFrame itself)
    '''
    # This function applies the weighted_average function to the grouped data; 2nd and 3rd args are passed to weighted_average
    #todo: THIS WEIGHTED AVERAGE IS NOT GIVEING THE SAME VALUE AS CASPARS SCRIPT ! SEE VALUES FOR DAY-AHEAD-IMPORT
    return grouped_df.apply(weighted_average, avg_name=fact, weight_name=weight)

#yearly weighted averages
# Compute and store weighted averages separately
yearly_weighted_avg_results = {}
quarterly_weighted_avg_results = {}

#grouped dfs:
grouped_df_yearly = combined_monthly_df.groupby('year')
grouped_df_quarterly = combined_monthly_df.groupby(['year', 'quarter'])

for index, row in weightedAvMapping_df.iterrows():
    fact = row['factToAverage']
    weight = row['weightingFact']

    # Compute the weighted average for each year and store it in a dictionary
    # for given row in weightedAvMapping_df, compute weighted avg for each year and store in dict
    yearly_weighted_avg_results[fact + '_weighted_avg'] = calculate_weighted_average_for_fact(grouped_df_yearly, fact, weight)
    quarterly_weighted_avg_results[fact + '_weighted_avg'] = calculate_weighted_average_for_fact(grouped_df_quarterly, fact, weight)


script_outputs = (
    yearly_weighted_avg_results,
    quarterly_weighted_avg_results,
    yearly_df,
    quarterly_df,
    combined_monthly_df,
    factAggMapping_df,
    weightedAvMapping_df,
    aggregations,
    non_aggregated_columns,
    weighted_average,
    calculate_weighted_average_for_fact,
    monthly_df,
    yearly_df,
    quarterly_df,
    combined_sim_name
)


