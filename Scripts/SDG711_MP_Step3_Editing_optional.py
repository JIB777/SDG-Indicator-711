#James Gibson
#3/21/22

#SDG 7.1.1
#Step 3: Editing (optional)

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

GADMGlobal = r'G:\HumanPlanet\GADM\GADM.gdb\GADMCopy3'
WPP = r'G:\HumanPlanet\WPP\WPP2019.csv'
df = pd.read_csv(WPP)

#Start Time
Start_Time = time.time()

def process(iso):
    message = None
    if message is None:
        try:
            gdb = r'G:\HumanPlanet\SDG711\Version2\2021\Countries\%s.gdb' % iso
            workspace = gdb
            arcpy.env.workspace = workspace
            #Data
            complete = '%s_complete' % iso
            #Add ISO Field
            arcpy.AddField_management(complete,'ISO','TEXT')
            expression = "\"%s\"" % iso
            arcpy.CalculateField_management(complete,'ISO',expression,'PYTHON')
##            #If check pop is greater than 1 we need to do proportional allocation
##            
##            arcpy.AddField_management(complete,'Total_Pop_adm_lights_adj','DOUBLE')
##            arcpy.AddField_management(complete,'Total_Pop_adm_no_lights_adj','DOUBLE')
##
##            
##            expression = "(!Total_Pop_adm_lights!/(!Total_Pop_adm_no_lights! + !Total_Pop_adm_lights!))"
##            arcpy.AddField_management(complete,'Light_Ratio','DOUBLE')
##            arcpy.CalculateField_management(complete,'Light_Ratio',expression,'PYTHON')
##
##            expression = "(!Total_Pop_adm_no_lights!/(!Total_Pop_adm_no_lights! + !Total_Pop_adm_lights!))"
##            arcpy.AddField_management(complete,'NoLight_Ratio','DOUBLE')
##            arcpy.CalculateField_management(complete,'NoLight_Ratio',expression,'PYTHON')
##            
##            #Update lights pop
##            with arcpy.da.UpdateCursor(complete,['ISO','Check_POP','Light_Ratio','Total_Pop_adm_lights','Total_Pop_Adm','Total_Pop_adm_lights_adj']) as cursor:
##                for row in cursor:
##                    if row[1] == None:
##                        pass
##                    elif row[1] >= 1:
##                        row[5] = row[2] * row[4]
##                        cursor.updateRow(row)
##                    else:
##                        row[5] = row[3]
##                        cursor.updateRow(row)
##                  
##            #Update no lights pop
##            with arcpy.da.UpdateCursor(complete,['ISO','Check_POP','NoLight_Ratio','Total_Pop_adm_no_lights','Total_Pop_Adm','Total_Pop_adm_no_lights_adj']) as cursor:
##                for row in cursor:
##                    if row[1] == None:
##                        pass
##                    elif row[1] >= 1:
##                        row[5] = row[2] * row[4]
##                        cursor.updateRow(row)
##                    else:
##                        row[5] = row[3]
##                        cursor.updateRow(row)
##                    
##            arcpy.AddField_management(complete,'Percent_Access_adj','DOUBLE')
##            expression = "(!Total_Pop_adm_lights_adj!/!Total_Pop_Adm!)*100"
##            arcpy.CalculateField_management(complete,'Percent_Access_adj',expression,'PYTHON')                  
            message = 'Success: ' + iso
        except Exception as e:
            message = 'Failed: ' + iso + ' ' + str(e)

    return message

def main():
    print('Starting Script...')
    #Start Time
    Start_Time = time.time()
    print('Start Time: %s' % str(Start_Time))
    mylist=[]
    with arcpy.da.SearchCursor(GADMGlobal,['GID_0']) as cursor:
        for row in cursor:
            if row[0] in mylist:
                pass
            else:
                mylist.append(row[0])
    print('Start Processing')
    length = len(mylist)
    pool = multiprocessing.Pool(processes=20, maxtasksperchild=1)
    results = pool.imap_unordered(process,mylist)
    counter = 0
    for result in results:
        print(result)
        counter = counter + 1
        print("{} countries processed out of {}".format(counter,length))
        print('---------------------------------------------------------')
    pool.close()
    pool.join()
    End_Time = time.time()
    Total_Time = End_Time - Start_Time
    print('Total Time: %s' % str(Total_Time))
    print('Script Complete')
    
    


if __name__ == '__main__':
    main()


