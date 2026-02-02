#!/usr/bin/env python
# coding: utf-8

# # ETL-NPL-Pipeline
# Developed a Python-based ETL pipeline to collect, clean, transform, and validate household NPL data across multiple segments, including housing, automobile, and credit card loans. The dataset also integrates macroeconomic indicators such as inflation rate, GDP growth, interest rate, and macro shock index, along with NPL flowrates from Stage 2 to Stage 3. The pipeline is designed with a manual-trigger execution model, allowing future extension to automated scheduling and orchestration.
# 
# **Pipeline Design Note:**
# Each ETL stage is modularized to allow future extension to automated scheduling and source replacement without impacting downstream datasets.

# ## Initail Configuaration
# Remark : Includes required library imports and custom utility functions to support indicator-specific ETL logic. 

# ### Import Library

# In[99]:


import os
import pandas as pd
import requests
import json
from dateutil.relativedelta import relativedelta as reldel
from datetime import datetime, timedelta
from functools import reduce


# ### Global Function

# #### Get csv.

# In[100]:


def get_csv(path):

    raw_path = path
    csv = os.path.normpath(raw_path)
    df = pd.read_csv(csv)

    return df


# #### Get Excel

# In[101]:


def get_excel(path,sheet_name):

    raw_path = path
    csv = os.path.normpath(raw_path)
    df = pd.read_excel(csv, sheet_name = sheet_name)

    return df


# #### Format Quater "Qx/YYYY" -> "YYYY-Qx"

# In[102]:


# Format Quater "Qx/YYYY" -> "YYYY-Qx"
def reformat_quarter(val):
    if isinstance(val, str) and '/' in val:
        parts = val.split('/') 
        return f"{parts[1]}-{parts[0]}"
    return val


# #### Specific Table (BOT) Extraction 

# In[103]:


# Get data from specific  BOT's table 
# Observation BOT

def observations_bot(start_dt, end_dt, key, series_code):
    """
    Fetches data from the Bank of Thailand (BOT) API.
    """

    # Gateway URL
    url = "https://gateway.api.bot.or.th/observations"

    # Headers with authentication
    headers = {
        'Authorization': f'Bearer {key}',
        'accept': "application/json"
    }

    # Define parameters for the request
    params = {
        'series_code': series_code,  
        'start_period': start_dt,    
        'end_period': end_dt       
    }

    # Get Response from API
    response = requests.get(url, headers=headers, params=params)

    # Check if the response is successful (HTTP 200)
    if response.status_code == 200:           
        data_dict = json.loads(response.text)
        # Extract observations data from the JSON response
        data = data_dict['result']['series'][0]['observations']

    # Handle error cases
    else:                   
        print(f"Error {response.status_code}: {response.text}")

    return data


# #### NPL data trasformation

# In[104]:


# NPL data trasformation
def npl_transform(df):

    # --- 1. Data Extraction & Initial Setup ---
    # Extract specific rows relevant to the analysis based on their indices
    selected_indices = [4, 5, 16, 17, 18]
    df_s1 = df.iloc[selected_indices , 1:].copy()
    df_s1.reset_index(drop=True, inplace=True) 

    # --- 2. Data Cleaning: Primary Header (Row 0) ---
    # Forward fill missing values in the first header row (handling merged cells)
    df_s1.iloc[0, 1:] = df_s1.iloc[0, 1:].ffill()

    # Clean string format: Remove trailing characters (e.g., 'p', 'r' for preliminary/revised) and whitespace
    df_s1.iloc[0] = df_s1.iloc[0].astype(str).str.replace(r'\s*[pr]$', '', regex=True).str.strip() 

    # Standardize date format: Convert "Qx/YYYY" to "YYYY-Qx"
    df_s1.iloc[0] = df_s1.iloc[0].apply(reformat_quarter)

    # --- 3. Data Cleaning: Secondary Header (Row 1) ---
    # Strip whitespace from the secondary header row
    df_s1.iloc[1] =  df_s1.iloc[1].astype(str).str.strip()

    # Filter columns: Exclude columns labeled "% to NPLs"
    mask = df_s1.loc[1] != "% to NPLs"
    df_s1 = df_s1.loc[:, mask]

    # --- 4. Structural Transformation (Reshaping) ---
    # Create a MultiIndex header from the cleaned first and second rows
    headers = pd.MultiIndex.from_arrays(df_s1.iloc[0:2, 1:].values, names=['Quarter', 'Type'])

    # Create a new DataFrame containing only the data body (excluding header rows)
    df_s2 = df_s1.iloc[2:, 1:].copy()
    df_s2.columns = headers

    # Set the 'Category' index using the first column of the data
    df_s2.index = df_s1.iloc[2:, 0]
    df_s2.index.name = 'Category'

    # Reshape from Wide format to Long format using stack
    df_s2 = df_s2.stack(level=[0, 1], future_stack=True).reset_index(name='Value')

    # Pivot specific metrics ('Type') into columns to structure the data for analysis
    df_s2 = df_s2.pivot(index=['Category', 'Quarter'], columns='Type', values='Value')
    df_s2.reset_index(inplace=True)
    df_s2.columns.name = None

    # --- 5. Data Type Formatting & Calculation ---
    # Convert percentage string to float ratio (e.g., 2.34 -> 0.0234)
    df_s2['% to Total Loans'] = df_s2['% to Total Loans'].astype(float) / 100

    # Rename column and convert numeric string (with commas) to integer
    df_s2.rename(columns={'NPL Outstanding': 'Gross NPL'}, inplace=True)
    df_s2['Gross NPL'] = df_s2['Gross NPL'].astype(str).str.replace(',', '').astype(int)

    # Calculate 'Total Loan' based on Gross NPL and Percentage
    df_s2['Total Loan'] = df_s2['Gross NPL'] / df_s2['% to Total Loans']

    # --- 6. Data Segregation ---
    # Define target categories
    categories = ("Housing", "Automobile", "Credit Card")

    dfs_npl = {} # Initialize dictionary to store separated DataFrames

    for cate in categories:
        # Filter data by category name and store in the dictionary
        df_s3 = df_s2[df_s2['Category'].str.contains(cate, na=False)]

        # Remove the 'Category' column and reset index for the final output
        dfs_npl[cate] = df_s3.drop(columns=['Category']).reset_index(drop=True)

    return dfs_npl


# #### Flowrate Extraction 

# In[105]:


# Flowrate Extraction 
def flowrate_extract(start_dt , end_dt , key):

    # Define the set of series to fetch with their API codes
    series_set = {
        "Gross New NPL": 'FINPQ00177',  # Gross flow of new NPLs
        "Gross NPL": 'FINPQ00108',      # Outstanding Gross NPLs (Personal consumption)
        "%NPL": 'FINPQ00110'           # Percentage of NPLs to Total Loans
    } 

    dfs_flow_rate = {} # Initialize dictionary to store separated DataFrames
    BOT_key = key

    # Loop through each series to fetch and process data
    for name, code in series_set.items():
        # Fetch data using the helper function
        data = observations_bot(start_dt, end_dt, BOT_key ,code)

        # Convert JSON data to DataFrame
        df = pd.json_normalize(data)

        # Rename the generic 'value' column to the specific series name
        df.rename(columns={'value': name}, inplace=True) 

        # Convert the numeric column to float type
        df[name] = df[name].astype(float)

        # Store the DataFrame in the dictionary
        dfs_flow_rate[name] = df

    return dfs_flow_rate


# #### Flowrate Transformation

# In[106]:


# Flowrate Transformation
def flowrate_transform(dfs_flow_rate):

    # Merge all DataFrames (Gross New NPL, Gross NPL, %NPLs) into a single DataFrame based on 'period_start'
    df_s1 = dfs_flow_rate["Gross New NPL"].merge(dfs_flow_rate["Gross NPL"], how='inner', on='period_start') \
        .merge(dfs_flow_rate["%NPL"], how='inner', on='period_start')

    # Calculate 'Total Loan' (Back-calculation: Gross NPL / (%NPLs / 100))
    df_s1['Total Loan'] = df_s1['Gross NPL'] / (df_s1['%NPL'] / 100)

    # Calculate 'Flowrate Ratio' (Gross New NPL / Total Loan)
    df_s1['Flowrate Ratio'] = df_s1['Gross New NPL'] / df_s1['Total Loan'] 

    # Select only relevant columns for the final output
    df_s2 = df_s1[['period_start', 'Flowrate Ratio']].copy()

    # Rename 'period_start' to 'Quarter'
    df_s2.rename(columns={'period_start': 'Quarter'}, inplace=True)

    # Clean string format in 'Quarter' column (strip whitespace)
    df_s2['Quarter'] = df_s2['Quarter'].astype(str).str.strip() 

    df_flowrate = df_s2

    return df_flowrate


# #### Interest Rate (BOT) Extraction

# In[107]:


# Interest Rate

def interest_bot(start_dt_str, end_dt_str,key):
    """
    Fetches average loan rate data from the Bank of Thailand (BOT) API,
    looping month-by-month to handle data retrieval limits.
    """

    # Gateway URL
    url = "https://gateway.api.bot.or.th/LoanRate/v2/avg_loan_rate/"

    # Headers for authentication
    headers = {
        'Authorization': f'Bearer {key}',
        'accept': "application/json"
    }

    # Change format from str > datetime to allow calculation
    start_dt = datetime.strptime(start_dt_str, '%Y-%m-%d')
    end_dt = datetime.strptime(end_dt_str, '%Y-%m-%d')

    # Initial loop setting
    cur_start = start_dt
    all_data = []

    # Loop through the date range until the end date is reached
    while cur_start < end_dt:

        # Data extract for 1 month to prevent error (API limitation handling)
        cur_end = cur_start + reldel(months = 1) - timedelta(days=1)

        # Define parameter for requests
        params = {
            'start_period': cur_start.strftime('%Y-%m-%d'), # Change back to str for params input
            'end_period'  : cur_end.strftime('%Y-%m-%d')    # Change back to str for params input
        }

        # Get Response from API
        response = requests.get(url, headers=headers, params=params)

        # If respond can get (HTTP 200 OK)
        if response.status_code == 200:
            data = json.loads(response.text)
            # Extract specific data details from the JSON structure
            interests_data = data['result']['data']['data_detail']
            # Accumulate data into the main list
            all_data = all_data + interests_data

        # If error occurs
        else:
            print(f"Error {response.status_code}: {response.text}")
            break

        # Advance the start date to the next day after the current batch
        cur_start = cur_end + timedelta(days=1)

    # Convert the list of dictionaries to a DataFrame
    df_interest_rate = pd.json_normalize(all_data)
    return df_interest_rate


# #### Inflation Transformation

# In[108]:


# Inflation Transformation
def inflation_transformation(df_inflation):
    # Create a copy of the DataFrame to avoid modifying the original input
    df_s1 = df_inflation.copy()

    # Rename the first column (index 0) to 'Quarter'
    df_s1.rename(columns={df_s1.columns[0]: 'Quarter'}, inplace=True)

    # Convert the 'Quarter' column to datetime objects using the format dd/mm/yy
    df_s1['Quarter'] = pd.to_datetime(df_s1['Quarter'], format='%d/%m/%y')

    # Convert 'Inflation Rate' to float and divide by 100 (convert percentage to decimal)
    df_s1['Inflation Rate'] = df_s1['Inflation Rate'].astype(float) / 100

    # Convert dates to quarterly period strings and format them as 'YYYY-QX' (e.g., 2016-Q1)
    df_s1['Quarter'] = df_s1['Quarter'].dt.to_period('Q').astype(str).str.replace('Q', '-Q')

    # Group data by 'Quarter' and calculate the mean (average) for aggregation
    df_s1 = df_s1.groupby('Quarter').mean().reset_index(drop=False)

    return df_s1


# #### GDP Transformation

# In[109]:


# GDP Transformation
def gdp_transform(df_gdp):
    df_s1 = df_gdp.iloc[:,[0,11]].copy()

    # 1. Set the 4th row (Index 3) as the header
    df_s1.columns = df_s1.iloc[3]

    # 2. Drop the first few rows (0, 1, 2) and start data from the 5th row onwards
    df_s1 = df_s1.iloc[4:].reset_index(drop=True)

    # Rename the first column (Index 0) to "Quarter"
    df_s1.rename(columns={df_s1.columns[0]: 'Quarter'}, inplace=True)
    df_s1.rename(columns={df_s1.columns[1]: 'GDP Growth'}, inplace=True)

    df_s2 = df_s1.iloc[10:,:].copy()

    df_s2['Quarter'] = df_s2['Quarter'].astype(str).str.strip()
    df_s2['GDP Growth'] = df_s2['GDP Growth'].astype(float)/100

    # 2. Remove suffix characters (r, p, p1, r1) using Regex
    # \s* matches any whitespace characters before the suffix (if any)
    # $ ensures the match is at the very end of the string
    df_s2['Quarter'] = df_s2['Quarter'].str.replace(r'\s*(r1|p1|r|p)$', '', regex=True)
    df_s2 = df_s2.reset_index(drop=True)

    current_year = ""

    # Loop through the DataFrame index
    for i in df_s2.index:
        val = df_s2.at[i, 'Quarter'] # Extract the value

        # 1. If the string has 4 characters, treat it as the "Year"
        if len(val) == 4:
            current_year = val

        # 2. If the string has 2 characters, prefix it with the current "Year"
        elif len(val) == 2:
            # New data format = "Year-Current data"
            new_data = f"{current_year}-{val}"

            # Update the data back into the table
            df_s2.at[i, 'Quarter'] = new_data

    # Remove rows where the 'Quarter' column still has a length of 4 (the Year headers)
    df_s2 = df_s2[df_s2['Quarter'].astype(str).str.len() != 4]
    df_s2

    return df_s2


# #### MRR Transformation

# In[110]:


# MRR Transformation

def mrr_transformation(df_mrr):

    # Create a copy of the DataFrame to avoid modifying the original input
    df_s1 = df_mrr.copy()

    # Filter out rows where 'name_eng' is 'Average of Foreign Bank Branches' and reset the index
    df_s2 = df_s1.query("name_eng != 'Average of Foreign Bank Branches'").reset_index(drop=True)

    # Select only specific columns ('period' and 'mrr') for further processing
    df_s3 = df_s2[['period', 'mrr']].copy()

    # Convert 'period' column to datetime objects
    df_s3['period'] = pd.to_datetime(df_s3['period'])

    # Convert 'mrr' to float and divide by 100 (transform percentage to decimal)
    df_s3['mrr'] = df_s3['mrr'].astype(float) / 100

    # Convert dates to quarterly period strings and format as 'YYYY-QX' (e.g., 2016Q1 -> 2016-Q1)
    df_s3['period'] = df_s3['period'].dt.to_period('Q').astype(str).str.replace('Q', '-Q')

    # Group by 'period', calculate the mean, and reset index to make 'period' a column again
    df_s4 = df_s3.groupby('period').mean().reset_index(drop=False)

    # Rename columns to standard names
    df_s4.rename(columns={df_s4.columns[0]: 'Quarter'}, inplace=True)
    df_s4.rename(columns={df_s4.columns[1]: 'MRR'}, inplace=True)

    return df_s4


# ## Data Extraction

# ### 1. Gross NPL , %NPL , Total Loan
# Gross NPL and %NPL for each personal consumption segment are provided in csv. format from the Bank of Thailand (BOT) website.
# 
# Data Link : https://app.bot.or.th/BTWS_STAT/statistics/ReportPage.aspx?reportID=794
# 
# Remarks:
# - `Data is categorized by personal consumption segments.`
# - `Total Loan is calculated during the transformation stage.`

# In[111]:


raw_npls = get_csv(r'[Input your csv. file path]')
raw_npls


# ### 2. Flowrate (S2 -> S3)
# Flowrate data is provided in csv. format and accessed via the Bank of Thailand (BOT) API service.
# 
# Remarks:
# - `Data is aggregated across all personal consumption segments`
# - `Flowrate is calculated during the transformation stage`

# In[112]:


# Define time range for the data
start_dt = "2013-01-01"
end_dt = "2025-12-30"
statistic_catalogue_key = '[Input your Statistic Catalogue Key (BOT)]'

raw_flowrate = flowrate_extract(start_dt , end_dt , statistic_catalogue_key)
raw_flowrate


# In[113]:


raw_flowrate['Gross New NPL']


# In[114]:


raw_flowrate['Gross NPL']


# In[115]:


raw_flowrate['%NPL']


# ### 3. GDP Growth (%YoY)
# GDP year-over-year (YoY) quarterly data is sourced from the Gross Domestic Product, Chain Volume Measures report (Q3/2025), published by the Office of the National Economic and Social Development Council (NESDC).
# 
# Data Link : https://www.nesdc.go.th/?p=54903&ddl=85893

# In[116]:


raw_gdp= get_excel(r'[Input your excel file path]',"Table 2.2")
raw_gdp


# ### 4. Inflation Rate (%YoY)
# Remark : Bureau of Trade and Economic Indices, Ministry of Commerce, Thailand

# In[117]:


raw_inflation = get_csv(r'[Input your csv. file path]')
raw_inflation


# ### 5. Interest Rate (MRR)
# Interest rate data, including historical Minimum Retail Rate (MRR), is accessed via the Bank of Thailand (BOT) API service.

# In[118]:


# Input parameter
start_dt_interest = "2013-01-01"
end_dt_interest = "2026-01-01"
interesterate_catalogue_key = '[Input your Interest Rate Catalogue Key (BOT)]'

# Extract Interest Rate 
raw_mrr = interest_bot(start_dt_interest,end_dt_interest,interesterate_catalogue_key)
raw_mrr


# ### 6. Interest Rate (Minimum Payment of Credit Card)
# The minimum payment requirement for credit cards in Thailand has historically been 10% of the total outstanding balance, but this rate was significantly altered starting in 2020 due to the COVID-19 pandemic to alleviate the burden on consumers. 
# 
# Here is a summary of the credit card minimum payment rates in Thailand since 2013:
# - `2013–2019 : The standard minimum payment was 10% of the total outstanding balance.`
# - `2020–2023 : Due to the COVID-19 pandemic, the Bank of Thailand (BOT) reduced the minimum payment to 5%.`
# - `2024-2026 : The rate is set to remain at 8% until the end of 2027`
# 
# This dataset is manually constructed based on publicly available announcements and news releases, as the data is not directly accessible via an API.
# 
# Reference Link
# - `Somwang: Credit Card Minimum Payments :` https://www.somwang.co.th/article/credit-card-minimum-payments
# - `Bank of Thailand Official Announcement (Dec 2024) :` https://www.bot.or.th/th/news-and-media/news/news-20251204-2.html

# In[119]:


raw_minpay = get_csv(r'[Input your csv. file path]')
raw_minpay


# ### 7. Macro Shock Index
# **Definition:**
# The Macro Shock Index is a binary dummy variable (0/1) introduced to capture structural breaks and market distortions caused by the COVID-19 pandemic and its economic aftermath.
# 
# **Timeframe:**
# - `Value = 1 (Shock Period): Q3/2020 – 2023`
# - `Value = 0 (Normal Period): All other periods`
# 
# **Rationale & Justification:** During the shock period (Q3/2020 – 2023), the typical relationships between macroeconomic indicators (e.g., GDP growth, inflation) and Non-Performing Loans (NPLs) were significantly disrupted due to two key factors:
# 
# **Exogenous Shock (Noise):**
# Economic contraction was primarily driven by external health restrictions (lockdowns), rather than normal business cycle dynamics, introducing statistical noise that does not reflect standard borrower behavior.
# 
# **Policy Intervention (Distortion):**
# Extensive financial relief measures, such as debt moratoriums and regulatory forbearance, temporarily suppressed NPL figures despite weak economic fundamentals. This created a decoupling effect, where NPL trends did not follow traditional macroeconomic relationships.
# 
# **Analytics Impact:**
# Incorporating the Macro Shock Index allows the model to isolate anomaly periods from normal economic cycles. This prevents the model from learning misleading patterns (e.g., low GDP growth coinciding with stable NPLs) and improves predictive robustness for future, non-crisis conditions.

# In[120]:


raw_shock = get_csv(r'[Input your csv. file path]')
raw_shock


# ## Data Transforming

# ### 1. Gross NPL , %NPL , Total Loan
# After transforming **Gross NPL** and **%NPL**, **Total Loan** is calculated using the following relationship:
# > **%NPL = Gross NPL / Total Loan**
# 
# 
# The transformed data is then stored by personal consumption segment using a dictionary-based structure, enabling modular access for downstream processing. Each segment can be accessed via a key, for example:
# 
# - `transformed_npl['Housing']`
# - `transformed_npl['Automobile']`
# - `transformed_npl['Credit Card']`

# In[121]:


# Get NPL Data
transformed_npl = npl_transform(raw_npls)
transformed_npl


# In[122]:


transformed_npl['Housing']


# In[123]:


transformed_npl['Automobile']


# In[124]:


transformed_npl['Credit Card']


# ### 2. Flowrate (S2 -> S3)
# The Flowrate is calculated using the following relationship:
# > **Flowrate = Gross New NPL/Total Loans**

# In[125]:


# Get Flowrate Data
transformed_flowrate = flowrate_transform(raw_flowrate)
transformed_flowrate


# ### 3. GDP Growth (%YoY)

# In[126]:


transformed_gdp = gdp_transform(raw_gdp)
transformed_gdp


# ### 4. Inflation Rate (%YoY)

# In[127]:


transformed_inflation = inflation_transformation(raw_inflation)
transformed_inflation


# ### 5. Interest Rate (MRR)

# In[128]:


transformed_mrr = mrr_transformation(raw_mrr)
transformed_mrr


# ### 6. Interest Rate (Minimum Payment of Credit Card)

# In[129]:


transformed_minpay = raw_minpay
transformed_minpay


# ### 7. Macro Shock Index

# In[130]:


transformed_shock = raw_shock
transformed_shock


# ## Data Loading

# #### Operation
# Macroeconomic datasets, including **GDP Growth**, **Inflation Rate**, and **Macro Shock Index**, are joined with the NPL datasets for each household consumption segment.
# 
# Additional segment-specific features are included as follows:
# - **Housing** joined with: `GDP Growth, Inflation Rate, Macro Shock Index` , MRR
# 
# - **Automobile** joined with: `GDP Growth, Inflation Rate, Macro Shock Index`
# 
# - **Credit Card** joined with: `GDP Growth, Inflation Rate, Macro Shock Index` , Minimum Payment of Credit Card

# In[131]:


# ==========================================
# 1. Create Common DataFrame
# Method 1: Chaining merge (readable for 4 tables)
# Merging Flowrate, GDP, Inflation, and Shock data on 'Quarter'
# ==========================================
common_df = transformed_flowrate.merge(transformed_gdp, on='Quarter', how='outer') \
    .merge(transformed_inflation, on='Quarter', how='outer') \
    .merge(transformed_shock, on='Quarter', how='outer')

# ==========================================
# 2. Create Housing Data
# Join: NPL(Housing) + Common Data + MRR (Interest Rate)
# ==========================================
housing_data = transformed_npl['Housing'].merge(common_df, on='Quarter', how='outer') \
                                         .merge(transformed_mrr, on='Quarter', how='outer')

# ==========================================
# 3. Create Automobile Data
# Join: NPL(Automobile) + Common Data
# ==========================================
automobile_data = transformed_npl['Automobile'].merge(common_df, on='Quarter', how='outer')

# ==========================================
# 4. Create Credit Card Data
# Join: NPL(Credit Card) + Common Data + MinPay (Minimum Payment)
# ==========================================
credit_card_data = transformed_npl['Credit Card'].merge(common_df, on='Quarter', how='outer') \
                                                 .merge(transformed_minpay, on='Quarter', how='outer')


# #### Joined Dataframe
# - **Housing joined DataFrame**  
#   `NPL['Housing'] + GDP + Inflation Rate + Macro Shock Index + MRR`
# 
# - **Automobile joined DataFrame**  
#   `NPL['Automobile'] + GDP + Inflation Rate + Macro Shock Index`
# 
# - **Credit Card joined DataFrame**  
#   `NPL['Credit Card'] + GDP + Inflation Rate + Macro Shock Index + Minimum Payment of Credit Card`

# In[132]:


# Display the resulting Housing DataFrame
housing_data


# In[133]:


# Display the resulting Automobile DataFrame
automobile_data


# In[134]:


# Display the resulting Credit Card DataFrame
credit_card_data


# #### Save Data Frames : Processed Data
# The transformed datasets are saved in a structured format to serve as the analytics-ready database. 

# In[135]:


# Generate timestamp for the filename (e.g., 20231027_1030)
timestamp = datetime.now().strftime('%Y%m%d_%H%M')

# 1. Correct Dictionary Syntax: Use curly braces {} and correct quoting
category = {
    "housing": housing_data,
    "automobile": automobile_data,
    "credit_card": credit_card_data
}

# 2. Iterate through dictionary using .items()
for cate, data in category.items():

    # 3. Add .csv extension to the filename
    filename = f"{cate}_processed_{timestamp}.csv"

    # Combine Path
    folder_path = r'[Input your folder destination]'
    path_csv = os.path.join(folder_path, filename)

    # 4. Save to CSV
    # index=False: Do not save the row index numbers (0, 1, 2...)
    # header=True: Keep the column names (Default behavior)
    data.to_csv(path_csv, mode='w', index=False, header=True)

    print(f"Saved: {path_csv}")




