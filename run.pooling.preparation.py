#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# USAGE: python run.pooling.preparation.py

"""
This script consolidates the pooling workflow, replacing the two-step process
that depended on Clarity LIMS files. It performs the following actions:

1.  Reads the local SQLite database and the user-defined Excel pooling sheet.
2.  Calculates library pooling assignments, masses, and volumes.
3.  Automatically generates unique, sequential Pool IDs and LIMS IDs.
4.  Generates all necessary worklist files for downstream lab processes
    (Hamilton, Pippin, Fragment Analyzer).
5.  Creates the `pool_summary.csv` required for the QC and rework steps.
6.  Archives the old database and saves an updated version.
"""

import pandas as pd
import numpy as np
import sys
import os
import shutil
from datetime import datetime
from pathlib import Path
import openpyxl
import string
import random
import math
from sqlalchemy import create_engine
##########################
# Function Definitions
##########################

def readSQLdb(db_path):
    """Reads the SQLite database into a pandas DataFrame."""
    engine = create_engine(f'sqlite:///{db_path}')
    query = "SELECT * FROM lib_info_submitted_to_clarity"
    sql_df = pd.read_sql(query, engine)
    sql_df['Sample Barcode'] = sql_df['Sample Barcode'].astype('str')
    sql_df['Fraction #'] = sql_df['Fraction #'].astype('int')
    engine.dispose()
    return sql_df

def getConstantsFromXlsx(file_path):
    """Reads pooling constants from the assign_pool_number_sheet.xlsx file."""
    wb = openpyxl.load_workbook(filename=file_path, data_only=True)
    sheet = wb['Pooling_tool']

    min_tran_vol = sheet['Q2'].value
    max_conc_vol = sheet['Q4'].value
    max_dilut_vol = sheet['Q6'].value
    target_pool_mass = sheet['Q8'].value
    concentrate_switch = sheet['Q14'].value

    const_list = [target_pool_mass, max_conc_vol, max_dilut_vol, min_tran_vol, concentrate_switch]
    if not all(const_list):
        print("\n\nPooling tool .xlsx file is missing a constant. Aborting process.")
        sys.exit()
    
    return target_pool_mass, max_conc_vol, max_dilut_vol, min_tran_vol, concentrate_switch

def assignPool(my_lib_df, my_pool_df):
    """Assigns libraries to pools and validates the assignments."""
    my_pool_df.dropna(axis=0, how='all', inplace=True)
    if my_pool_df.isnull().values.any():
        print("\nPooling Tool .xlsx file is missing a plate id, pool#, or illumina index. Aborting method\n\n")
        sys.exit()

    plates_per_pool = my_pool_df['Assigned_Pool'].value_counts()
    if plates_per_pool.iloc[0] > 5:
        val = input("\n\nThe number of plates in at least one pool is >5. There may not be enough deck space. Continue? (y/n): ")
        if val.lower() != 'y':
            print('Ok, aborting script. Adjust pool assignments in the .xlsx file.\n\n')
            sys.exit()

    pool_dict = dict(zip(my_pool_df.Plate, my_pool_df.Assigned_Pool))
    my_lib_df['Pool_number'] = my_lib_df['Pool_source_plate'].map(pool_dict)

    # Check for index collisions
    x_df = my_lib_df.groupby(['Pool_number', 'Pool_Illumina_index_set', 'Pool_source_plate'])['Pool_Illumina_index'].agg('count').reset_index()
    x_df.drop(columns=['Pool_Illumina_index'], inplace=True)
    dup_df = x_df[x_df.duplicated(['Pool_number', 'Pool_Illumina_index_set'], keep=False)]
    if not dup_df.empty:
        print('\n\nAt least one pool has plates with the same illumina index set. Aborting.')
        print(dup_df)
        sys.exit()

    return my_lib_df

def getLibMass(my_passed_df, target_pool_mass):
    """Calculates the target mass required from each library."""
    pool_size_df = my_passed_df.groupby(['Pool_number'])['Pool_source_well'].agg('count').reset_index()
    pool_size_df = pool_size_df.rename(columns={'Pool_source_well': 'Pool_size'})
    pool_size_df['Target_Mass_per_library'] = (target_pool_mass / pool_size_df['Pool_size'])
    
    conc_dict = dict(zip(pool_size_df.Pool_number, pool_size_df.Target_Mass_per_library))
    my_passed_df['Pool_target_lib_mass_(pmol)'] = my_passed_df['Pool_number'].map(conc_dict)
    
    return my_passed_df

def getLibVolumes(my_passed_df, min_tran_vol, max_conc_vol, max_dilut_vol, concentrate_switch):
    """Calculates transfer volumes and determines source plate type."""
    my_passed_df['Pool_volume_concentrated_(uL)'] = (my_passed_df['Pool_target_lib_mass_(pmol)'] / (my_passed_df['Pool_nmole/L'] / 1000))
    my_passed_df['Pool_volume_diluted_(uL)'] = (my_passed_df['Pool_volume_concentrated_(uL)'] * my_passed_df['Pool_dilution_factor'])

    my_passed_df['Pool_use_conc_or_dilut'] = np.where(my_passed_df['Pool_volume_concentrated_(uL)'] < min_tran_vol, 'dilute', 'concentrate')
    my_passed_df['Pool_use_conc_or_dilut'] = np.where(((my_passed_df['Pool_volume_concentrated_(uL)'] < min_tran_vol) & (my_passed_df['Pool_volume_diluted_(uL)'] > (max_dilut_vol * concentrate_switch))), 'concentrate', my_passed_df['Pool_use_conc_or_dilut'])
    
    my_passed_df['Pool_transfer_plate'] = np.where(my_passed_df['Pool_use_conc_or_dilut'] == 'dilute', my_passed_df['Pool_source_plate'] + 'D', 'h' + my_passed_df['Pool_source_plate'])
    my_passed_df['Pool_transfer_volume_(uL)'] = np.where(my_passed_df['Pool_use_conc_or_dilut'] == 'dilute', my_passed_df['Pool_volume_diluted_(uL)'], my_passed_df['Pool_volume_concentrated_(uL)'])

    # Apply pipetting limits
    my_passed_df['Pool_ACTUAL_transfer_volume_(uL)'] = my_passed_df['Pool_transfer_volume_(uL)']
    my_passed_df['Pool_ACTUAL_transfer_volume_(uL)'] = np.where(((my_passed_df['Pool_use_conc_or_dilut'] == 'dilute') & (my_passed_df['Pool_transfer_volume_(uL)'] > max_dilut_vol)), max_dilut_vol, my_passed_df['Pool_ACTUAL_transfer_volume_(uL)'])
    my_passed_df['Pool_ACTUAL_transfer_volume_(uL)'] = np.where(((my_passed_df['Pool_use_conc_or_dilut'] == 'concentrate') & (my_passed_df['Pool_transfer_volume_(uL)'] > max_conc_vol)), max_conc_vol, my_passed_df['Pool_ACTUAL_transfer_volume_(uL)'])
    my_passed_df['Pool_ACTUAL_transfer_volume_(uL)'] = np.where(my_passed_df['Pool_ACTUAL_transfer_volume_(uL)'] < min_tran_vol, min_tran_vol, my_passed_df['Pool_ACTUAL_transfer_volume_(uL)'])

    # Round final values
    my_passed_df['Pool_target_lib_mass_(pmol)'] = my_passed_df['Pool_target_lib_mass_(pmol)'].round(4)
    my_passed_df['Pool_volume_concentrated_(uL)'] = my_passed_df['Pool_volume_concentrated_(uL)'].round(1)
    my_passed_df['Pool_volume_diluted_(uL)'] = my_passed_df['Pool_volume_diluted_(uL)'].round(1)
    my_passed_df['Pool_ACTUAL_transfer_volume_(uL)'] = my_passed_df['Pool_ACTUAL_transfer_volume_(uL)'].round(1)
    my_passed_df['Pool_transfer_volume_(uL)'] = my_passed_df['Pool_transfer_volume_(uL)'].round(1)

    return my_passed_df

def createSQLdb(updated_df, db_path):
    """Archives the old DB and CSV, then saves the updated DataFrame to both formats."""
    date = datetime.now().strftime("%Y_%m_%d-Time%H-%M-%S")
    csv_path = db_path.with_suffix('.csv')

    # Archive existing database and csv
    if db_path.exists():
        archive_db_path = ARCHIV_DIR / f"archive_lib_info_submitted_to_clarity_{date}.db"
        db_path.rename(archive_db_path)
        print(f"Archived existing database to {archive_db_path}")
    
    if csv_path.exists():
        archive_csv_path = ARCHIV_DIR / f"archive_lib_info_submitted_to_clarity_{date}.csv"
        csv_path.rename(archive_csv_path)
        print(f"Archived existing CSV to {archive_csv_path}")

    # Create new database
    engine = create_engine(f'sqlite:///{db_path}')
    table_name = 'lib_info_submitted_to_clarity'
    updated_df.to_sql(table_name, engine, if_exists='replace', index=False)
    print(f"Successfully created updated database at {db_path}")
    engine.dispose()

    # Create new CSV
    updated_df.to_csv(csv_path, index=False, header=True)
    print(f"Successfully created updated CSV at {csv_path}")


# ########################
# MAIN PROGRAM
# ########################

def autoGeneratePoolIDs(my_lib_df):
    """Generates unique, sequential Pool IDs and LIMS IDs."""
    pool_numbers = sorted(my_lib_df['Pool_number'].unique())
    
    # Generate seed for Pool_id (5 random uppercase letters)
    base_id = ''.join(random.choices(string.ascii_uppercase, k=5))
    
    # Generate seed for Pool_LIMS (random 6-digit number)
    base_lims_num = random.randint(100000, 999999)

    pool_id_map = {}
    pool_lims_map = {}

    for i, pool_num in enumerate(pool_numbers):
        # Assign LIMS ID
        current_lims_id = f"27-{base_lims_num + i}"
        pool_lims_map[pool_num] = current_lims_id

        # Assign Pool ID
        if i == 0:
            current_pool_id = base_id
        else:
            # Increment the previous ID
            temp_id = list(pool_id_map[pool_numbers[i-1]])
            for j in range(len(temp_id) - 1, -1, -1):
                if temp_id[j] == 'Z':
                    temp_id[j] = 'A'
                    continue
                else:
                    temp_id[j] = chr(ord(temp_id[j]) + 1)
                    break
            current_pool_id = "".join(temp_id)
        
        pool_id_map[pool_num] = current_pool_id

    my_lib_df['Pool_id'] = my_lib_df['Pool_number'].map(pool_id_map)
    my_lib_df['Pool_LIMS'] = my_lib_df['Pool_number'].map(pool_lims_map)

    # Update destination tube info, mimicking the original script's logic
    my_lib_df['Destination_Tube_Name'] = my_lib_df['Pool_number'].astype(str) + "a_" + my_lib_df['Pool_id']
    my_lib_df['Destination_Tube_Barcode'] = my_lib_df['Pool_LIMS']
    
    return my_lib_df

def addPippinInfo(my_lib_df):
    """Assigns Pippin Prep cassette and lane information."""
    pool_list = sorted(my_lib_df['Pool_number'].unique())

    destbc = random.choice(string.ascii_uppercase) + "".join(random.choices(string.digits + string.ascii_uppercase, k=5))

    lane_dict = {}
    cassette_dict = {}
    lane_pairs = ((1, 3), (5, 7), (9, 11), (2, 4), (6, 8))

    for i, p in enumerate(pool_list):
        pair_position = i % 5
        lane_dict[p] = lane_pairs[pair_position]
        cassette_dict[p] = f"{destbc}-{(math.floor(i / 5)) + 1}"

    my_lib_df['Pippin_Lane'] = my_lib_df['Pool_number'].map(lane_dict)
    my_lib_df[['1st_Pippin_lane', '2nd_Pippin_lane']] = pd.DataFrame(my_lib_df['Pippin_Lane'].tolist(), index=my_lib_df.index)
    my_lib_df.drop(['Pippin_Lane'], axis=1, inplace=True)
    my_lib_df['Pippin_Cassette'] = my_lib_df['Pool_number'].map(cassette_dict)

    return my_lib_df

def makePoolTransferFiles(my_lib_df):
    """Generates Hamilton transfer files for creating the pools."""
    pool_list = my_lib_df['Pool_number'].unique()
    for p in pool_list:
        tmp_df = my_lib_df.loc[my_lib_df['Pool_number'] == p].copy()
        tmp_df = tmp_df[['Pool_transfer_plate', 'Pool_source_well', 'Pool_ACTUAL_transfer_volume_(uL)', 'Destination_Tube_Name']]
        tmp_df['Destination_Tube_Barcode'] = tmp_df['Destination_Tube_Name']
        tmp_df['Source_Barcode'] = tmp_df['Pool_transfer_plate']
        tmp_df = tmp_df.rename(columns={'Pool_transfer_plate': 'Source_Name', 'Pool_source_well': 'Source_Well', 'Pool_ACTUAL_transfer_volume_(uL)': 'Transfer_Volume'})
        
        final_col_list = ['Source_Name', 'Source_Barcode', 'Source_Well', 'Transfer_Volume', 'Destination_Tube_Name', 'Destination_Tube_Barcode']
        tmp_df = tmp_df.reindex(columns=final_col_list)
        
        tmp_df.to_csv(ATTEMPT_DIR / f'Pool_{p}_transfer_file.csv', index=False, header=True)

def makeTubeBarcodeFiles(my_lib_df):
    """Generates barcode label files for tubes."""
    pippin_df = my_lib_df[['Destination_Tube_Name', 'Destination_Tube_Barcode', 'Pippin_Cassette', '1st_Pippin_lane', '2nd_Pippin_lane']].copy()
    pippin_df = pippin_df.drop_duplicates().sort_values(by=['Destination_Tube_Name'], ascending=True).reset_index(drop=True)
    pippin_df['Dest_Tube_Size_Selected'] = pippin_df['Destination_Tube_Name'].str.replace('a_', '-1_')

    pool_dict = dict(zip(pippin_df.Destination_Tube_Name, pippin_df.Dest_Tube_Size_Selected))
    my_dest_list = sorted(my_lib_df['Destination_Tube_Name'].unique())

    header = '%BTW% /AF="\\\\bartender\shared\\templates\JGI_Label_BCode5_Rex.btw" /D="%Trigger File Name%" /PRN="bcode5" /R=3 /P /DD\r\n%END%\r\n\r\n\r\n'
    
    with open(ATTEMPT_DIR / "pooling_TUBES_barcode_labels.txt", "w") as bc_file:
        bc_file.writelines(header)
        for p in my_dest_list:
            tube_name = p.split('_')
            for _ in range(3):
                bc_file.writelines(f'{p},{tube_name[0]},{tube_name[1]}\r\n')
            
            size_tubename = pool_dict[p].split('_')
            bc_file.writelines(f'{pool_dict[p]},{size_tubename[0]},{size_tubename[1]}\r\n')
            bc_file.writelines(',,\r\n')
            
    return pippin_df

def FAplatePoolQC(my_pippin_list):
    """Helper function to assign FA well positions."""
    row_list = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H']
    well_dict = {}
    for i, p in enumerate(my_pippin_list):
        r = math.floor(i / 10)
        c = (i % 10) + 1
        well_dict[p] = f"{row_list[r]}{c}"
    return well_dict

def makePippinTransferFiles(my_pippin_df):
    """Generates worklists for the Pippin Prep instrument."""
    my_pippin_df.rename(columns={'Destination_Tube_Name': 'Pool_Name', 'Destination_Tube_Barcode': 'Pool_Barcode'}, inplace=True)
    my_pippin_df['Sample_volume_(uL)'] = 44
    my_pippin_df['Marker_volume_(uL)'] = 11
    my_pippin_df['Load_volume_(uL)'] = 25
    my_pippin_df['Recover_volume_(uL)'] = 30

    pippin_list = my_pippin_df['Pool_Name'].unique().tolist()
    fa_well_dict = FAplatePoolQC(pippin_list)

    fabc = random.choice(string.ascii_uppercase) + "".join(random.choices(string.digits + string.ascii_uppercase, k=4)) + '-FA1'
    
    my_pippin_df['FA_plate_barcode'] = fabc
    my_pippin_df['FA_well'] = my_pippin_df['Pool_Name'].map(fa_well_dict)
    my_pippin_df['FA_transfer_vol_(uL)'] = 2.4

    my_pippin_df.to_csv(ATTEMPT_DIR / 'PIPPIN_load_unload_transfer_file.csv', index=False, header=True)
    
    dest_list = sorted(my_pippin_df['FA_plate_barcode'].unique())
    return my_pippin_df, dest_list

def makePippinBarcodeFile(my_pippin_df):
    # get list of all pool numbers
    pippin_list = my_pippin_df['Pippin_Cassette'].unique().tolist()

    fa_list = my_pippin_df['FA_plate_barcode'].unique().tolist()

    # add info to start of barcode print file indicating the template and printer to use
    x = '%BTW% /AF="\\\BARTENDER\shared\\templates\ECHO_BCode8.btw" /D="%Trigger File Name%" /PRN="bcode8" /R=3 /P /DD\r\n\r\n%END%\r\n\r\n\r\n'
    
    bc_file = open(ATTEMPT_DIR /"PIPPIN_CASSETTE_FA_PLATE_barcode_labels.txt", "w")
    
    bc_file.writelines(x)

    for p in pippin_list:
        bc_file.writelines(f'{p},"pip.casset {p}"\r\n')

    for fa in fa_list:
        bc_file.writelines(f'{fa},"FA.plate {fa}"\r\n')
    bc_file.close()

def getWellList(my_FA_df):
    """Helper function to generate a list of wells for the FA plate."""
    row_list = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H']
    well_list = []
    num_fa_wells = len(my_FA_df['Dest_Tube_Size_Selected'].unique())
    num_rows = math.ceil(num_fa_wells / 10) # 10 samples per row, not 12
    
    for r_idx in range(num_rows):
        for c_idx in range(1, 13):
            well_list.append(f"{row_list[r_idx]}{c_idx}")
            
    return well_list, num_rows

def makeFAinputFiles(my_pippin_df, my_dest_list):
    """Generates input CSV files for the Fragment Analyzer."""
    row_list = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H']
    FA_df = my_pippin_df[['Dest_Tube_Size_Selected', 'FA_plate_barcode', 'FA_well']].copy()
    FA_df['name'] = FA_df[['FA_plate_barcode', 'Dest_Tube_Size_Selected', 'FA_well']].astype(str).agg('.'.join, axis=1)

    my_well_list, my_num_rows = getWellList(FA_df)
    
    tmp_fa_df = pd.DataFrame(my_well_list, columns=["Well"])
    tmp_fa_df = tmp_fa_df.merge(FA_df, how='left', left_on=['Well'], right_on=['FA_well'])
    
    fabc = tmp_fa_df['FA_plate_barcode'].dropna().iloc[0]
    tmp_fa_df.drop(['FA_plate_barcode', 'FA_well', 'Dest_Tube_Size_Selected'], inplace=True, axis=1)
    tmp_fa_df['name'] = tmp_fa_df['name'].fillna('empty_well')

    for i in range(my_num_rows):
        start = i * 12
        stop = start + 12
        print_df = tmp_fa_df.iloc[start:stop].copy()
        print_df.index = range(1, 13)
        
        print_df.loc[11, 'name'] = 'LibStd'
        print_df.loc[12, 'name'] = 'ladder'
        
        print_df.to_csv(ATTEMPT_DIR / f'FA_upload_{fabc}_row{row_list[i]}.csv', index=True, header=False)

if __name__ == "__main__":
    # Define directories
    PROJECT_DIR = Path.cwd()
    ARCHIV_DIR = PROJECT_DIR / "archived_files"
    ARCHIV_DIR.mkdir(parents=True, exist_ok=True)
    POOL_DIR = PROJECT_DIR / "5_pooling"
    ASSIGN_DIR = POOL_DIR / "C_assign_libs_to_pools"
    REWORK_DIR = POOL_DIR / "E_pooling_and_rework"
    
    # This script will always generate the first attempt.
    # The rework script will handle subsequent attempts.
    ATTEMPT_DIR = REWORK_DIR / "Attempt_1"
    if ATTEMPT_DIR.exists():
        shutil.rmtree(ATTEMPT_DIR)
    ATTEMPT_DIR.mkdir(parents=True, exist_ok=True)

    print("Starting pooling preparation workflow...")

    # 1. Read Inputs
    print("--> Reading input files...")
    db_path = PROJECT_DIR / 'lib_info_submitted_to_clarity.db'
    if not db_path.exists():
        print(f"ERROR: Database file not found at {db_path}")
        sys.exit()
    lib_df = readSQLdb(db_path)
    
    assign_sheet_path = ASSIGN_DIR / 'assign_pool_number_sheet.xlsx'
    if not assign_sheet_path.exists():
        print(f"ERROR: Pool assignment sheet not found at {assign_sheet_path}")
        sys.exit()
    pool_df = pd.read_excel(assign_sheet_path, header=1, engine="openpyxl", usecols=['Plate', 'Assigned_Pool', 'Index'], converters={'Plate barcode': str, 'Assigned_Pool': int})
    target_pool_mass, max_conc_vol, max_dilut_vol, min_tran_vol, concentrate_switch = getConstantsFromXlsx(assign_sheet_path)

    # 2. Core Calculations
    print("--> Performing core calculations...")
    lib_df = assignPool(lib_df, pool_df)
    lib_df = getLibMass(lib_df, target_pool_mass)
    lib_df = getLibVolumes(lib_df, min_tran_vol, max_conc_vol, max_dilut_vol, concentrate_switch)

    # 3. Auto-Generate Pool IDs
    print("--> Generating Pool IDs...")
    lib_df = autoGeneratePoolIDs(lib_df)

    # 4. Generate Downstream Files
    print("--> Generating downstream files...")
    lib_df = addPippinInfo(lib_df)
    makePoolTransferFiles(lib_df)
    pippin_df = makeTubeBarcodeFiles(lib_df)
    pippin_df, dest_list = makePippinTransferFiles(pippin_df)
    makePippinBarcodeFile(pippin_df)
    makeFAinputFiles(pippin_df, dest_list)

    # 5. Create Pool Summary
    print("--> Creating pool summary file...")
    summary_df = pippin_df[['Pool_Name', 'Pool_Barcode', 'Pippin_Cassette', '1st_Pippin_lane','2nd_Pippin_lane','Dest_Tube_Size_Selected','FA_plate_barcode','FA_well']]
    summary_df.to_csv(POOL_DIR / 'pool_summary.csv', index=False, header=True)

    # 6. Finalize and Archive
    print("--> Archiving and updating database...")
    createSQLdb(lib_df, db_path)

    print("\nPooling preparation workflow completed successfully.")

    # Create success marker file
    os.makedirs('.workflow_status', exist_ok=True)
    with open('.workflow_status/run.pooling.preparation.success', 'w') as f:
        f.write('Script completed successfully')
