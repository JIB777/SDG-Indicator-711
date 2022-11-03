#James Gibson
#3/21/22

#SDG Indicator 7.1.1
#Step 4: Results

#All complete country fcs should have a field that holds the national pop percent error

import arcpy, sys, os
from collections import defaultdict
from arcpy import env
from arcpy.sa import *
import time
import datetime
import pandas as pd
import numpy as np
import multiprocessing
arcpy.env.overwriteOutput = True



## Global ##
print('National Results')
print('------------------------')

#global feature class
globalfc = r'G:\HumanPlanet\SDG711\Version2\2021\Results\Results.gdb\SDG711_2021_081622_National_Results'

#UNWPP
WPP = r'G:\HumanPlanet\WPP\WPP2019.csv'
df = pd.read_csv(WPP)

#Set workspace
workspace = r'G:\HumanPlanet\SDG711\Version2\2021\Countries'
arcpy.env.workspace = workspace
arcpy.env.overwriteOutput = True

gdbs = arcpy.ListWorkspaces("*","FileGDB")

isos =[]
total_pops = []
total_elecs = []
npas = []
npes = []
#We need to create a dictionary of isos and national percent access values
#We need national percent access, national pop percent error
for gdb in gdbs:
    try:
        iso = gdb[-7:-4]
        print('Starting %s...' % iso)
        arcpy.env.workspace = gdb
        arcpy.env.overwriteOutput = True
        complete = '%s_complete' % iso
        arcpy.AddField_management(complete,'NPA','DOUBLE')
        arcpy.AddField_management(complete,'NPE','DOUBLE')
        #Get total pop
        total_pop = 0
        with arcpy.da.SearchCursor(complete,['Total_Pop_Adm']) as cursor:
            for row in cursor:
                if row[0] == None:
                    pass
                else:
                    total_pop = total_pop + row[0]
        print('Total Pop: %s' % total_pop)
        #Get total pop with access to electricity
        tpwe = 0
        with arcpy.da.SearchCursor(complete,['Total_Pop_adm_lights']) as cursor:
            for row in cursor:
                if row[0] == None:
                    pass
                else:
                    tpwe = tpwe + row[0]
        print('Total Pop with Access: %s' % tpwe)
        #Calculate percent access
        access = (tpwe/total_pop) * 100
        arcpy.CalculateField_management(complete,'NPA',access,'PYTHON')
        #Calculate percent error
        query = df.loc[df['ISO'] == iso]
        WPP = query['WPP2020'].values[0]
        pct_error = abs((total_pop - WPP)/WPP) * 100
        arcpy.CalculateField_management(complete,'NPE',pct_error,'PYTHON')
        #append results to lists
        isos.append(iso)
        total_pops.append(total_pop)
        total_elecs.append(tpwe)
        npas.append(access)
        npes.append(pct_error)
        print('Done: %s' % iso)
        print('-------------------') 
    except Exception as e:
        print(gdb)
        print(e)
        print('-------------------')
    

print('------------------------------')
#create dataframe from lists
df_results = pd.DataFrame({'ISO':isos,
                           'Total Pop':total_pops,
                           'Total Elec':total_elecs,
                           'NPA':npas,
                           'NPE':npes})

arcpy.AddField_management(globalfc,'Total_Pop','DOUBLE')
arcpy.AddField_management(globalfc,'Total_Pop_w_Elec','DOUBLE')
arcpy.AddField_management(globalfc,'NPA','DOUBLE')
arcpy.AddField_management(globalfc,'NPE','DOUBLE')
#Fill out national results layer
with arcpy.da.UpdateCursor(globalfc,['GID_0','Total_Pop','Total_Pop_w_Elec','NPA','NPE']) as cursor:
    for row in cursor:
        try:
            query = df_results.loc[df_results['ISO'] == row[0]]
            row[1] = query['Total Pop'].values[0]
            row[2] = query['Total Elec'].values[0]
            row[3] = query['NPA'].values[0]
            row[4] = query['NPE'].values[0]
            cursor.updateRow(row)
            print('Done: %s' % row[0])
            print('-------------------')
        except Exception as e:
            print('error: %s' % row[0])
            print(e)
            print('-------------------')


## Subnational ##
print('Subnational Results')
print('------------------------')

arcpy.env.workspace = r'G:\HumanPlanet\SDG711\Version2\2021\Countries'
arcpy.env.overwriteOutput = True
gdbs = arcpy.ListWorkspaces("*","FileGDB")
for gdb in gdbs:
    try:
        arcpy.env.workspace = gdb
        iso = gdb[-7:-4]
        complete = '%s_complete' % iso
        out_copy = r'G:\HumanPlanet\SDG711\Version2\2021\Results\Mapping.gdb\%s_copy' % complete
        arcpy.CopyFeatures_management(complete,out_copy)
        print('Copied: %s' % iso)
    except:
        print('Error: %s' % gdb)

arcpy.env.workspace = r'G:\HumanPlanet\SDG711\Version2\2021\Results\Mapping.gdb'
arcpy.env.overwriteOutput = True
fcs = arcpy.ListFeatureClasses()
merge_list = []
for fc in fcs:
    if 'complete_copy' in fc:
        merge_list.append(fc)
    else:
        print('Error: %s' % fc)
        pass
    

out_merge = r'G:\HumanPlanet\SDG711\Version2\2021\Results\Results.gdb\SDG711_2021_081622_Subnational_Results'
arcpy.Merge_management(merge_list,out_merge)
print('Merged')


print('DONE')

        
    
