# -*- coding: utf-8 -*-
"""
Created on Fri Oct  4 18:10:48 2024

@author: MatladiT
"""

import os
import pandas as pd
from scipy.stats import norm

# Function to calculate water balance for Penman-Monteith (PM ET0)
def calculate_water_balance_pm(df):
    # Check if required columns exist
    required_columns = ['Rain', 'PM ET0']
    if not all(col in df.columns for col in required_columns):
        raise KeyError(f"Missing required columns: {required_columns}")
    
    # Convert Rain and PM ET0 to numeric, forcing errors to NaN
    df['Rain'] = pd.to_numeric(df['Rain'], errors='coerce')
    df['PM ET0'] = pd.to_numeric(df['PM ET0'], errors='coerce')

    # Calculate Water Balance (Penman-Monteith method)
    df['Water_Balance_PM'] = df['Rain'] - df['PM ET0']
    
    return df

# Function to calculate cumulative monthly water balance
def calculate_cumulative_water_balance(df):
    # Check if 'Year', 'Month', and 'Day' columns exist or if there is a 'Date' column
    if all(col in df.columns for col in ['Year', 'Month', 'Day']):
        # Combine Year, Month, and Day into a single Date column
        df['Date'] = pd.to_datetime(df[['Year', 'Month', 'Day']], errors='coerce')
    elif 'Date' in df.columns:
        # If a single 'Date' column exists, use that
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    else:
        raise KeyError("No suitable date columns (Year, Month, Day or Date) found in the data.")
    
    # Ensure there are no missing or invalid dates
    if df['Date'].isnull().any():
        raise ValueError("Invalid or missing dates in the dataset.")
    
    df.set_index('Date', inplace=True)

    # Resample by month and calculate cumulative water balance for Penman-Monteith
    df_monthly = df.resample('ME').agg({
        'Water_Balance_PM': 'sum'
    }).reset_index()

    # Remove the time part from the Date column
    df_monthly['Date'] = df_monthly['Date'].dt.date

    return df_monthly

# Function to calculate SPEI
def calculate_spei(df, column_name):
    # Rolling cumulative sum of water balance (12-month rolling window)
    df[f'{column_name}_rolling'] = df[column_name].rolling(window=12, min_periods=1).sum()

    # Standardize the cumulative water balance to calculate SPEI (Z-score)
    df[f'SPEI_{column_name}'] = (df[f'{column_name}_rolling'] - df[f'{column_name}_rolling'].mean()) / df[f'{column_name}_rolling'].std()

    # Categorize the SPEI values
    df[f'SPEI_Category_{column_name}'] = df[f'SPEI_{column_name}'].apply(categorize_spei)

    return df

# Function to categorize SPEI values into drought/wetness categories
def categorize_spei(spei_value):
    if spei_value > 2:
        return "Extremely Wet"
    elif 1.5 < spei_value <= 2:
        return "Severely Wet"
    elif 1 < spei_value <= 1.5:
        return "Moderately Wet"
    elif -1 < spei_value <= 1:
        return "Normal"
    elif -1.5 < spei_value <= -1:
        return "Moderately Dry"
    elif -2 < spei_value <= -1.5:
        return "Severely Dry"
    else:
        return "Extremely Dry"

# Function to process each station file
def process_station(file_path):
    # Load data from file (Excel format in this case; adjust for CSV if needed)
    df = pd.read_excel(file_path)

    # Step 1: Calculate daily water balance for Penman-Monteith
    try:
        df = calculate_water_balance_pm(df)
    except KeyError as e:
        print(f"Error in {file_path}: {e}")
        return None

    # Step 2: Calculate monthly cumulative water balance
    try:
        df_monthly = calculate_cumulative_water_balance(df)
    except (KeyError, ValueError) as e:
        print(f"Error in {file_path}: {e}")
        return None

    # Step 3: Calculate SPEI for Penman-Monteith
    df_monthly = calculate_spei(df_monthly, 'Water_Balance_PM')

    return df_monthly

# Main function to process all station files in a directory and save to one Excel file with multiple sheets
def process_all_stations(directory, output_file):
    # Create an ExcelWriter object to write multiple sheets to one Excel file
    with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
        # Loop through all files in the directory
        for filename in os.listdir(directory):
            # Skip temporary Excel lock files, SPEI result files, and previously generated result files
            if filename.startswith('~$') or filename.endswith('_spei_results.xlsx') or filename.startswith('combined_spei_results'):
                continue
            if filename.endswith(".xlsx") or filename.endswith(".xls"):
                file_path = os.path.join(directory, filename)
                print(f"Processing station file: {filename}")
                try:
                    # Process the station and get the monthly SPEI results
                    station_result = process_station(file_path)

                    if station_result is not None:
                        # Add station name to the result
                        station_name = os.path.splitext(filename)[0]

                        # Remove periods ('.') from the station name
                        station_name_cleaned = station_name.replace('.', '')

                        # Save each station's result to a different sheet in the same Excel file
                        station_result.to_excel(writer, sheet_name=station_name_cleaned[:31], index=False)  # Sheet names have a max length of 31 characters
                        print(f"Saved SPEI results for {station_name_cleaned} to sheet in {output_file}")

                except KeyError as e:
                    print(f"Error processing {filename}: {e}")
                except Exception as e:
                    print(f"Unexpected error processing {filename}: {e}")

# Set the directory path (adjust this for your local environment)
directory_path = r'C:\Users\matladit\SPEI READY'

# Output file where all the station results will be saved, each on a separate sheet
output_file = os.path.join(directory_path, 'combined_spei_results.xlsx')

# Process all stations in the directory and save results to one Excel file with multiple sheets
process_all_stations(directory_path, output_file)
