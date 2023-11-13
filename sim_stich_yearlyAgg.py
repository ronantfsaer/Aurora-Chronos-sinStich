
'''IN ORDER FOR THIS SCRIPT TO RUN, YOU MUST FIRST SET THE SIMULATION NAMES IN THE SIM_STICH_NEW_BASE.PY FILE'''
import pandas as pd

from sim_stich_base import script_outputs

#IMPORT VARIABLES
yearly_weighted_avg_results = script_outputs[0]
quarterly_weighted_avg_results = script_outputs[1]
yearly_df = script_outputs[2]
quarterly_df = script_outputs[3]
combined_monthly_df = script_outputs[4]
factAggMapping_df = script_outputs[5]
weightedAvMapping_df = script_outputs[6]
aggregations = script_outputs[7]
non_aggregated_columns = script_outputs[8]
weighted_average = script_outputs[9]
calculate_weighted_average_for_fact = script_outputs[10]
monthly_df = script_outputs[11]
yearly_df = script_outputs[12]
quarterly_df = script_outputs[13]
combined_sim_name = script_outputs[14]


for fact, series in yearly_weighted_avg_results.items():
    # Convert the series to DataFrame
    fact = fact
    trim_name = fact.replace('_weighted_avg', '')
    df_temp = series.reset_index(name=trim_name)

    # If the fact column already exists in combined_monthly_df, replace with the new weighted avg values
    if trim_name in combined_monthly_df.columns:
        # Replace the existing column in combined_monthly_df
        # Match the 'year' in combined_monthly_df with the index 'year' in df_temp
        combined_monthly_df = combined_monthly_df.drop(columns=trim_name)  # Drop the old column
        combined_monthly_df = pd.merge(combined_monthly_df, df_temp, on='year', how='left')  # Add the new weighted avg values

# Ensure the aggregated DataFrame includes these weighted averages
for fact in weightedAvMapping_df['factToAverage']:
    aggregations[fact] = 'first'

# Perform standard aggregations:
yearly_aggregated_df = combined_monthly_df.groupby('year').agg(aggregations).reset_index()


#reorder columns to match monthly_df
yearly_aggregated_df = yearly_aggregated_df.reindex(columns=monthly_df.columns)
combined_monthly_df = combined_monthly_df.reindex(columns=monthly_df.columns)

#final formatting
yearly_aggregated_df['Timestamp'] = yearly_aggregated_df['Timestamp'].dt.strftime('%Y-%m-%d')
combined_monthly_df['Timestamp'] = combined_monthly_df['Timestamp'].dt.strftime('%Y-%m-%d')


#export excel file
with pd.ExcelWriter(f'yearlyAggregatedOutputs_{combined_sim_name}.xlsx') as writer:
    yearly_aggregated_df.to_excel(writer, sheet_name='Yearly Aggregated Data', index=False)
    yearly_df.to_excel(writer, sheet_name='Original Yearly Data', index=False)
    combined_monthly_df.to_excel(writer, sheet_name='Stitched Monthly Data', index=False)
