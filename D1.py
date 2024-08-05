import pandas as pd
import pyxlsb
import math
import numpy as np
import datetime
import os
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
from datetime import datetime, timedelta
#IMPORTING ALL THE REQUIRED LIBS
#########################################################

##############################################################
#CREATING ALL THE FUNCTIONS USED IN THE CODE

def days_remaining_in_month(year, month, day):
    # Create a date object for the given date
    input_date = datetime(year, month, day)
    
    # Find the first day of the next month
    if month == 12:  # If the month is December
        next_month = datetime(year + 1, 1, 1)
    else:
        next_month = datetime(year, month + 1, 1)
    
    # Calculate the last day of the current month
    last_day_of_month = next_month - timedelta(days=1)
    
    # Calculate the number of days remaining in the month
    days_remaining = (last_day_of_month - input_date).days
    
    return days_remaining


def import_range_google_sheet(google_sheet, sheet_name):
    json_key = {
        "type": "service_account",
        "project_id": "gstdata",
        "private_key_id": "0f1e4bb8a609a73c201726dc3198090d30ac2160",
        "private_key": "-----BEGIN PRIVATE KEY-----\nAdd your private key\n-----END PRIVATE KEY-----\n",
        "client_email": "gst-31@gstdata.iam.gserviceaccount.com",
        "client_id": "111375276834661809080",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/gst-31%40gstdata.iam.gserviceaccount.com",
    }

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    credentials = Credentials.from_service_account_info(json_key, scopes=scopes)
    gc = gspread.authorize(credentials)
    sh = gc.open(google_sheet)
    sheet = sh.worksheet(sheet_name)
    data = sheet.get_all_values()
    return pd.DataFrame(data[1:], columns=data[0])


def read_file(file_path):
    # Determine the file extension
    file_extension = file_path.split('.')[-1]
    
    # Read the file based on its extension
    if file_extension == 'csv':
        df = pd.read_csv(file_path)
    elif file_extension == 'xlsx':
        df = pd.read_excel(file_path, engine='openpyxl', sheet_name='DATA')
    elif file_extension == 'xlsb':
        df = pd.read_excel(file_path, engine='pyxlsb', sheet_name='DATA')
    else:
        raise ValueError("Unsupported file extension: {}".format(file_extension))
    
    return df


def categorize_bool(value):
    try:
        value = int(float(value))
    except ValueError:
        return np.nan
    if pd.isna(value):
        return np.nan
    if value > 180:
        return 'G=180+'
    for category, num_range in coll_cat_ranges.items():
        if value in num_range:
            return category
    return np.nan


def cleaning_columns(df):
    df.columns = df.columns.str.lower().str.strip().str.replace(" ", "_")
    return df

def convert_to_date(value):
    if isinstance(value, (int, float)):  # Check if value is numeric
        return (datetime(1899, 12, 30) + timedelta(days=value)).date()
    try:
        return pd.to_datetime(value).date()  # Convert string date to datetime.date
    except ValueError:
        return value  # Return the value unchanged if it's not numeric or a valid date string


def calculate_difference(row):
    try:
        bounced_date = pd.to_datetime(row['bounced date'])
        reciept_date = pd.to_datetime(row['reciept date'])
        return (bounced_date - reciept_date).days
    except:
        return row['bounced date']
    
    
def calculate_differencehy(row):
    try:
        bounced_date = pd.to_datetime(row['bounced date'])
        event_date = pd.to_datetime(row['event_date'])
        return (bounced_date - event_date).days
    except:
        return row['bounced date']
    
    

def replace_reso_status(row):
    if row['cih_reso'] == 'CIH':
        return 'UNRESO'
    else:
        return row['cih_reso']
    
#######################################################################

#######################################################################
#READING ALL THE NECESSARY FILES AND VARIABLES USED IN THE CODE

date = '2024-06-24'

input123 = import_range_google_sheet("drr_input", "input")

input123Filtered = input123.loc[input123['drr_for_date'] == date]


#FILE PATH OF THE PREVIOUS DAY DRR
file_path = input123Filtered['input_drr'].iloc[0]


#FILE PATH OF THE PAR REPORT
file_path2 = input123Filtered['par_report'].iloc[0]

#CIH FILE PATH
cih_path = input123Filtered['cih'].iloc[0]
cih_date = date

#BOUNCE FILE PATH
bounce_addition_path = input123Filtered['bounce_file'].iloc[0]

#TRANSACTION ALLOCATION FILE PATH 
Normal = input123Filtered['payment'].iloc[0]


OTS = input123Filtered['ots'].iloc[0]
Hybrid = input123Filtered['hybd'].iloc[0]
excel_file = r'C:\Users\rishit.lunia\Downloads\rishit24DRR1.xlsx'
sheet_name = 'Contracts24'

# creating 
########################################################################
df = read_file(file_path)

drr = df.copy()

dfPAR = pd.read_excel(file_path2,engine='pyxlsb')

dfpar = dfPAR.copy()

norm1 = pd.read_excel(Normal,sheet_name = "TransactionAllocation2024-06-22")

ots1 = pd.read_excel(OTS, sheet_name='old payment-Normal pool')

ots1HY = pd.read_excel(OTS, sheet_name='old payment-HYBD')

hybrid1 = pd.read_csv(Hybrid)


# drr and dfpar are the two dfs
drr.columns = drr.columns.str.lower()
drr = drr[['cl contract id','reso status','reso rate','opn dpd','current dpd','current pos','opn pos','emi','installment date','coll cat','colend ratio','risk cat','cih (y/n)','cih date','cihamt','cih reso']]

drr = drr[(drr['colend ratio'] == 'LK') | (drr['colend ratio'] == '80:20\xa0')]


dfpar.columns = dfpar.columns.str.lower()

dfpar = dfpar[['cl contract id','total late days','installment amount','instalment due date','ageing of days','ageing for par']]

dfpar_sorted = dfpar.sort_values(by='total late days', ascending=False)

dfpar_filtered = dfpar_sorted[dfpar_sorted['ageing for par'] != ' ']

dfpar_unique = dfpar_filtered.drop_duplicates(subset='cl contract id', keep='first')


#Converting the date format and filtering the dates
#############################################################################
# Finding the total late days
# Taking user input
year, month, day = map(int, date.split('-'))
new_date = datetime(year, month, day) - timedelta(days=1)
new_year, new_month, new_day = new_date.year, new_date.month, new_date.day

days_remaining = days_remaining_in_month(new_year, new_month, new_day)

################################################################################
# Merging the PAR report and the DRR file
merged_df = drr.merge(dfpar_unique, on='cl contract id', how='left')

merged_df['total late days'] = merged_df['total late days'].fillna(0)

merged_df['current dpd'] = merged_df['total late days']

merged_df['opn dpd'] = pd.to_numeric(merged_df['opn dpd'], errors='coerce')
merged_df['total late days'] = pd.to_numeric(merged_df['total late days'], errors='coerce')
merged_df['bool'] = merged_df['total late days'] + days_remaining


# Get today's current date

    
merged_df['bool'] = merged_df['bool'].astype(str)

merged_df['opn dpd'] = merged_df['opn dpd'].astype(str)

# Define the categories and their corresponding ranges
coll_cat_ranges = {
    '0=CURRENT': range(0, 1),
    'A=1-30': range(1, 31),
    'B=31-60': range(31, 61),
    'C=61-90': range(61, 91),
    'D=91-120': range(91, 121),
    'E=121-150': range(121, 151),
    'F=151-180': range(151, 181),
    'G=180+': range(180, 10000)  # Assuming an upper bound for "180+"
}


# Function to categorize the bool values


# List of months with 31 days as integers
months_with_31_days = [1, 3, 5, 7, 8, 10, 12]

# Define the dpd values to check
dpd_values_to_check = [30, 60, 90, 120, 150, 180]
merged_df['bool'] = merged_df['bool'].astype(float)
merged_df['bool'] = merged_df['bool'].astype(int)

merged_df['opn dpd'] = merged_df['opn dpd'].astype(float)
merged_df['opn dpd'] = merged_df['opn dpd'].astype(int)

# Apply the logic
if month in months_with_31_days:
    print("yes")
    merged_df['bool'] = merged_df.apply(
        lambda row: row['bool'] - 1 if row['opn dpd'] in dpd_values_to_check else row['bool'],
        axis=1
    )
    
merged_df['bool'] = merged_df['bool'].astype(str)
merged_df['opn dpd'] = merged_df['opn dpd'].astype(str)




# Apply the function to the 'bool' column
merged_df['coll cat for bool'] = merged_df['bool'].apply(categorize_bool)

merged_df['coll cat for opn dpd'] = merged_df['opn dpd'].apply(categorize_bool)

categories = ['0=CURRENT', 'A=1-30', 'B=31-60', 'C=61-90', 'D=91-120', 'E=121-150', 'F=151-180', 'G=180+']

# Create a mapping from category to an index
category_index = {category: index for index, category in enumerate(categories)}

# Map the categories to their respective indices
merged_df['bool_cat_index'] = merged_df['coll cat for bool'].map(category_index)
merged_df['opn_dpd_cat_index'] = merged_df['coll cat for opn dpd'].map(category_index)

merged_df['total late days'] = merged_df['total late days'].astype(int)

merged_df['cih reso'] = np.where(
    merged_df['bool_cat_index'] == merged_df['opn_dpd_cat_index'], 'RESO',
    np.where(
        merged_df['bool_cat_index'] > merged_df['opn_dpd_cat_index'], 'UNRESO',
        'RESO'
    )
)

merged_df['cih reso'] = np.where(
    (merged_df['coll cat'] == '0=CURRENT') & (merged_df['total late days'] == 0), 'RESO',
    np.where(
        (merged_df['coll cat'] == '0=CURRENT') & (merged_df['total late days'] != 0), 'UNRESO',
        merged_df['cih reso']  # Preserve existing values for other cases
    )
)


merged_df['cih reso'] = np.where(
    (merged_df['coll cat'] == 'A=1-30'), 
    merged_df['cih reso'],  # Preserve existing values for 'A=1-30'
    np.where(
        (merged_df['risk cat'] == 'A=1-7') & (merged_df['total late days'] == 0), 'RESO',
        np.where(
            (merged_df['risk cat'] == 'A=1-7') & (merged_df['total late days'] != 0), 'UNRESO',
            merged_df['cih reso']  # Preserve existing values for other cases
        )
    )
)

mask = merged_df['coll cat'].isin(['G=180+', 'H=OTR'])

merged_df['bool'] = merged_df['bool'].astype(int)

merged_df['opn dpd'] = merged_df['opn dpd'].astype(int)
# Assign 'RESO' to 'cih reso' where 'col cat' is '180+' or 'OTR' and 'bool' is less than 90
merged_df.loc[mask & (merged_df['bool'] < 90) & (merged_df['opn dpd'] > 180), 'cih reso'] = 'RESO'

# Assign 'UNRESO' to 'cih reso' where 'col cat' is '180+' or 'OTR' and 'bool' is greater than or equal to 90
merged_df.loc[mask & (merged_df['bool'] >= 90) & (merged_df['opn dpd'] > 180), 'cih reso'] = 'UNRESO'




# Claculating the cih reso
#############################################################################
# Calculating the reso rate
merged_df['reso rate'] = np.where(
    merged_df['coll cat for bool'] == merged_df['coll cat for opn dpd'], 'STAB',
    np.where(
    merged_df['bool_cat_index'] > merged_df['opn_dpd_cat_index'], 'FLOW',
    'ROLLBACK'
    )
)

merged_df['bool'] = pd.to_numeric(merged_df['bool'], errors='coerce')
merged_df['opn dpd'] = merged_df['opn dpd'].astype(float)
merged_df['opn dpd'] = merged_df['opn dpd'].astype(int)



mask = merged_df['coll cat'].isin(['G=180+', 'H=OTR'])

# Assign 'RESO' to 'cih reso' where 'col cat' is '180+' or 'OTR' and 'bool' is less than 90
merged_df.loc[mask & (merged_df['bool'] < 90) & (merged_df['opn dpd'] > 180), 'cih reso'] = 'RESO'

# Assign 'UNRESO' to 'cih reso' where 'col cat' is '180+' or 'OTR' and 'bool' is greater than or equal to 90
merged_df.loc[mask & (merged_df['bool'] >= 90) & (merged_df['opn dpd'] > 180), 'cih reso'] = 'UNRESO'


merged_df.loc[merged_df['total late days'] == 0, 'cih reso'] = 'RESO'

merged_df.loc[merged_df['total late days'] == 0, 'reso rate'] = 'NORM'

merged_df.loc[merged_df['cih reso'] == 'UNRESO', 'reso rate'] = 'FLOW'

merged_df['total late days'] = merged_df['total late days'].astype(float)
merged_df['total late days'] = merged_df['total late days'].astype(int)
merged_df['total late days'] = merged_df['total late days'].astype(str)
merged_df['opn dpd'] = merged_df['opn dpd'].astype(str)

#####################################################################################

#CIH TAGGING FROM THE CIH FILE LOGIC

cih = (
       pd.read_excel(cih_path , sheet_name='CIH').pipe(cleaning_columns)
       .assign(
           totl_cih_collx_month=lambda x: pd.to_numeric(
               x["totl_cih/collx_month"].astype("str").replace(",", ""),
               errors="coerce",
           ),
           cl_contract_id=lambda x: x["cl_contract_id"].astype("str").str.upper(),
       )
       .rename(
           columns={
               "cl_contract_id": "c_lender_id",
               "totl_cih/collx_month": "c_cih_amt",
               "cih_tag": "c_cih_tag",
           }
       )
       .assign(c_cih_date=cih_date)
   )

cih["c_lender_id"] = cih["c_lender_id"].astype("str")
merged_df["cl contract id"] = merged_df["cl contract id"].astype("str")

merged_df = merged_df.rename(columns={
    'cih date': 'cih_date',
    'cih reso': 'cih_reso',
    'cih (y/n)':'cih_(y/n)'
})

# Perform merge ###################################################################
merged_df = merged_df.merge(cih, left_on="cl contract id", right_on="c_lender_id", how="left")

# Update columns based on conditions
merged_df['cih_(y/n)'] = np.where(
    merged_df["c_lender_id"].notna(), merged_df["c_cih_tag"], merged_df["cih_(y/n)"]
)

merged_df['cih_date'] = np.where(
    merged_df["c_lender_id"].notna(), merged_df["c_cih_date"], merged_df["cih_date"]
)

merged_df['cihamt'] = np.where(
    merged_df["c_lender_id"].notna(), merged_df["c_cih_amt"], merged_df["cihamt"]
)

# Update 'cih_reso' based on conditions
merged_df['cih_reso'] = np.where(
    ~(merged_df["cih_reso"].astype("str").str.lower().eq("reso"))
    & (merged_df["cih_(y/n)"].astype("str").str.lower().eq("yes")),
    "CIH",
    merged_df["cih_reso"],
)

# Drop unnecessary columns
merged_df = merged_df.drop(columns=["c_lender_id", "c_cih_tag", "c_cih_date", "c_cih_amt"])



###########################################################################

#######################################################################
#manual changes
date = pd.to_datetime(date)
start_date = date.replace(day=1)
end_date = date + pd.offsets.MonthEnd(1)

manual_reso = import_range_google_sheet("drr_input", "manual_reso")
manual_unreso = import_range_google_sheet("drr_input", "manual_unreso")
cih_removal = import_range_google_sheet("drr_input", "cih_removal").add_prefix("c_")

for i in manual_reso["date"], manual_unreso["date"], cih_removal["c_cih_date"]:
    i = pd.to_datetime(i)

for i in (
    manual_reso["lender_id"],
    manual_unreso["lender_id"],
    cih_removal["c_lender_id"],
):
    i = i.astype("str").str.strip().str.upper()

manual_reso = manual_reso[
    (pd.to_datetime(manual_reso["date"]) >= start_date)
    & (pd.to_datetime(manual_reso["date"]) <= end_date)
]
manual_unreso = manual_unreso[
    (pd.to_datetime(manual_unreso["date"]) >= start_date)
    & (pd.to_datetime(manual_unreso["date"]) <= end_date)
]
cih_removal = cih_removal[
    (pd.to_datetime(cih_removal["c_cih_date"]) >= start_date)
    & (pd.to_datetime(cih_removal["c_cih_date"]) <= end_date)
]

merged_df = merged_df.merge(
    cih_removal, how="left", right_on="c_lender_id", left_on="cl contract id"
)

merged_df["cih_(y/n)"] = np.where(
    merged_df["c_lender_id"].notna(), merged_df["c_cih_tag"], merged_df["cih_(y/n)"]
)
merged_df["cihamt"] = np.where(
    merged_df["c_lender_id"].notna(), merged_df["c_cih_amount"], merged_df["cihamt"]
)

merged_df["cih_date"] = np.where(
    merged_df["c_lender_id"].notna(), merged_df["c_cih_date"], merged_df["cih_date"]
)

merged_df = merged_df.drop(
    columns=[
        "c_lender_id",
        "c_date_of_request",
        "c_cih_tag",
        "c_cih_amount",
        "c_cih_date",
    ]
)

if "cih_reso_reasons" not in merged_df.columns:
    merged_df["cih_reso_reasons"] = np.nan

# manual reso
merged_df["cih_reso"] = np.where(
    merged_df["cl contract id"].isin(manual_reso["lender_id"].to_list()),
    "reso",
    merged_df["cih_reso"],
)

merged_df["cih_reso_reasons"] = np.where(
    merged_df["cl contract id"].isin(manual_reso["lender_id"].to_list()),
    "manual reso",
    merged_df["cih_reso_reasons"],
)

# manual unreso
merged_df["cih_reso"] = np.where(
    merged_df["cl contract id"].isin(manual_unreso["lender_id"].to_list()),
    "unreso",
    merged_df["cih_reso"],
)

merged_df["cih_reso_reasons"] = np.where(
    merged_df["cl contract id"].isin(manual_unreso["lender_id"].to_list()),
    "manual unreso",
    merged_df["cih_reso_reasons"],
)
##################################################################################################3

bounce_df1 = pd.read_excel(
    bounce_addition_path,
    engine = 'pyxlsb',
    sheet_name="Sheet1",
)

bounce_df = bounce_df1

bounce_df.columns = bounce_df.columns.str.lower()
bounce_df = bounce_df[bounce_df["drr status"].astype("str").str.lower().eq("add in drr")]

bounce_df = bounce_df.rename(columns={"loan_id": "cl contract id"})

ids = bounce_df[~bounce_df["cl contract id"].isin(merged_df["cl contract id"])][["cl contract id"]]
merged_df = merged_df.merge(ids, on="cl contract id", how="outer")


##############################################################################


# drr123path = r"C:\Users\rishit.lunia\Downloads\DRR_JUN-24_19.06.24_Sent.xlsb"
# drr123 = pd.read_excel(drr123path , engine = 'pyxlsb',sheet_name = 'DATA')


drr123 = df.copy()


drr123.columns = drr123.columns.str.lower()


norm = norm1
ots = ots1

norm .columns = norm.columns.str.lower()
ots.columns = ots.columns.str.lower()
# norm['payment date'] = pd.to_numeric(norm['payment date'], errors='coerce')
# norm["payment date"] = pd.to_datetime(
#     norm["payment date"], unit="d", origin="1899-12-30"
#     ).dt.date
norm['payment date'] = pd.to_datetime(norm['payment date'])

ots_contract_ids = set(ots1['transaction id'])
norm['tags'] = norm['transaction id'].isin(ots_contract_ids).astype(int)
tagged_df = norm[norm['tags'] == 1]


# Take a date as input
input_month = month
input_year = year

# Filter the DataFrame
norm = norm[(norm['payment date'].dt.year > input_year) | 
                 ((norm['payment date'].dt.year == input_year) & (norm['payment date'].dt.month >= input_month))]

norm_filtered = norm[norm['payment mode'] != 'BT TRANSFER CASE']
norm_filtered = norm_filtered[~norm_filtered['narration'].str.contains('Ex-gratia Interest Relief Fund', case=False, na=False)]


norm_merged = pd.merge(norm_filtered, drr123[['cl contract id', 'coll cat','bounced date','cih date']], left_on='contract id', right_on='cl contract id', how='left')


norm_merged['cih date'] = norm_merged['cih date'].fillna(0)
norm_merged['bounced date'] = norm_merged['bounced date'].fillna(0)


norm_merged['payment mode'] = norm_merged['payment mode'].str.lower()


norm_merged['consider'] = np.where(
    (norm_merged['coll cat'].isin(['G=180+', 'H=OTR'])) &
    (norm_merged['cih date'] == 0) &
    ((norm_merged['payment mode'] == 'pdc') | (norm_merged['payment mode'].str.contains('nach', na=False))) &
    (~norm_merged['transaction id'].isin(ots_contract_ids)),
    'N', 'Y'
)



# Apply the conversion function to both 'bounced date' and 'receipt date'
norm_merged['bounced date'] = norm_merged['bounced date'].apply(convert_to_date)
norm_merged['reciept date'] = norm_merged['reciept date'].apply(convert_to_date)



# Apply the function to each row
norm_merged['difference'] = norm_merged.apply(calculate_difference, axis=1)


norm_merged['LPP'] = norm_merged['charges paid']
norm_merged['EMI'] = norm_merged['principal paid'] + norm_merged['interest paid'] + norm_merged['excess']


norm_merged['new'] = np.where(
    (norm_merged['difference'].apply(lambda x: isinstance(x, int) and x > 0)) &
    (norm_merged['coll cat'] == '0=CURRENT'),
    0,  # Value to assign if condition is true
    1   # Value to assign if condition is false
)

norm_merged.loc[norm_merged['new'] == 0, 'consider'] = 'N'

norm_merged.loc[norm_merged['tags'] == 1, 'consider'] = 'Y'

filtered_norm_merged = norm_merged[(norm_merged['consider'] == 'Y') | (norm_merged['tags'] == 1)]

pivot_tablenorm = filtered_norm_merged.pivot_table(
    index='contract id', 
    values=['LPP', 'EMI'], 
    aggfunc='sum'  # You can use other aggregation functions like 'mean', 'max', 'min' if needed
)

norm_merged = norm_merged.drop(columns=['new'])

pivot_tablenorm = pivot_tablenorm.rename_axis('cl contract id').reset_index()

# merged_df = merged_df.merge(pivot_tablenorm, on='cl contract id', how='left', suffixes=('', '_pivot'))

# merged_df['EMI'] = merged_df['EMI'].fillna(0)
# merged_df['LPP'] = merged_df['LPP'].fillna(0)
#############################################################################################

###########################################################################################
drr123 = df.copy()

drr123.columns = drr123.columns.str.lower()


hybrid = hybrid1
ots = ots1HY

hybrid .columns = hybrid.columns.str.lower()
ots.columns = ots.columns.str.lower()
# hybrid['payment date'] = pd.to_numeric(hybrid['payment date'], errors='coerce')
# hybrid["payment date"] = pd.to_datetime(
#     hybrid["payment date"], unit="d", origin="1899-12-30"
#     ).dt.date

ots_contract_ids = set(ots1HY['narration'])
hybrid['tags'] = hybrid['narration'].isin(ots_contract_ids).astype(int)
tagged_df = hybrid[hybrid['tags'] == 1]


hybrid_filtered = hybrid[hybrid['event_type'].str.lower() == 'transaction']

hybrid_merged = pd.merge(hybrid_filtered, drr123[['cl contract id', 'coll cat','bounced date','cih date']], left_on='contract_id', right_on='cl contract id', how='left')

hybrid_merged['EMI'] = hybrid_merged['amount']

hybrid_merged['bounced date'] = hybrid_merged['bounced date'].fillna(0)
hybrid_merged['bounced date'] = hybrid_merged['bounced date'].apply(convert_to_date)

hybrid_merged['event_date'] = hybrid_merged['event_date'].apply(convert_to_date)


# Apply the function to each row
hybrid_merged['difference'] = hybrid_merged.apply(calculate_differencehy, axis=1)

hybrid_merged['cih date'] = hybrid_merged['cih date'].fillna(0)

hybrid_merged['payment_mode'] = hybrid_merged['payment_mode'].str.lower()

hybrid_merged['consider'] = np.where(
    (hybrid_merged['coll cat'].isin(['G=180+', 'H=OTR'])) &
    (hybrid_merged['cih date'] == 0) &
    ((hybrid_merged['payment_mode'] == 'pdc') | (hybrid_merged['payment_mode'].str.contains('nach', na=False))) &
    (~hybrid_merged['narration'].isin(ots_contract_ids)),
    'N', 'Y'
)


hybrid_merged['new'] = np.where(
    (hybrid_merged['difference'].apply(lambda x: isinstance(x, int) and x > 0)) &
    (hybrid_merged['coll cat'] == '0=CURRENT'),
    0,  # Value to assign if condition is true
    1   # Value to assign if condition is false
)

hybrid_merged.loc[hybrid_merged['new'] == 0, 'consider'] = 'N'

hybrid_merged.loc[hybrid_merged['tags'] == 1, 'consider'] = 'Y'

filtered_hybrid_merged = hybrid_merged[(hybrid_merged['consider'] == 'Y') | (hybrid_merged['tags'] == 1)]


pivot_tableHY = filtered_hybrid_merged.pivot_table(
    index='contract_id', 
    values='EMI', 
    aggfunc='sum'  # You can use other aggregation functions like 'mean', 'max', 'min' if needed
)

# Drop the 'new' column from hybrid_merged
hybrid_merged = hybrid_merged.drop(columns=['new'])

# Rename the index of the pivot table and reset the index to make 'contract_id' a column again
pivot_tableHY = pivot_tableHY.rename_axis('cl contract id').reset_index()

# Merge the pivot table with merged_df on 'cl contract id'
# merged_df = merged_df.merge(pivot_tableHY, on='cl contract id', how='left', suffixes=('', '_pivot'))

combined_pivot = pd.concat([pivot_tableHY, pivot_tablenorm]).groupby('cl contract id').sum().reset_index()

merged_df = merged_df.merge(combined_pivot, on='cl contract id', how='left', suffixes=('', '_pivot'))

merged_df['EMI'] = merged_df['EMI'].fillna(0)
merged_df['LPP'] = merged_df['LPP'].fillna(0)


merged_df = merged_df.drop(columns=['total late days','installment amount','instalment due date','ageing of days','ageing for par','coll cat for bool','coll cat for opn dpd','bool_cat_index','opn_dpd_cat_index',
                                    'lk_loan_account_id','cih_amount',
                                    'bucket','payment_mode','transaction_id',
                                    'transaction_date','pool','mail_from',
                                    'lot','date','count','totl_cih_collx_month',
                                    'c_not_to_take','c_manual_cih_reso',
                                    'cih_reso_reasons'])

merged_df = merged_df.rename(columns={'bool':'New Opening'})

new_column_order = [
    'cl contract id', 'coll cat', 'risk cat',  # Place coll cat and risk cat after cl contract id
    'reso status', 'reso rate', 'opn dpd', 'current dpd', 'current pos', 'opn pos', 
    'emi', 'installment date', 'colend ratio', 'cih_(y/n)', 'cih_date', 
    'cihamt', 'cih_reso', 'New Opening', 'EMI', 'LPP'
]

merged_df = merged_df[new_column_order]

merged_df['reso status'] = merged_df.apply(replace_reso_status, axis=1)


#########################################################################################


# Check if file exists
file_exists = os.path.exists(excel_file)

# Write to Excel file, create if not exists
try:
    with pd.ExcelWriter(excel_file, engine='openpyxl', mode='a' if file_exists else 'w') as writer:
        # If file exists, remove the existing sheet with the same name to avoid appending
        if file_exists:
            writer.book.remove(writer.book[sheet_name]) if sheet_name in writer.book.sheetnames else None
        merged_df.to_excel(writer, index=False, sheet_name=sheet_name)
    print(f"Data saved to '{excel_file}' in the '{sheet_name}' sheet.")
except Exception as e:
    print(f"An error occurred: {e}")

