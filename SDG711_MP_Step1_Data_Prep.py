#3/21/22

#SDG 7.1.1
#Step 1: Data Prep

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


#Global Variables
wpp = r'G:\HumanPlanet\WPP\WPP2015.csv'
GADM0 = r'G:\HumanPlanet\GADM\GADM.gdb\GADM_admin0'
GADM2 = r'G:\HumanPlanet\GADM\GADM.gdb\GADM_admin2'
GADMGlobal = r'G:\HumanPlanet\GADM\GADM.gdb\GADMCopy3'
lights = r'G:\HumanPlanet\NASA DATA\Lights\Annual_VNL_V2\2021\Median_masked\VNL_v2_npp_2021_global_vcmslcfg_c202203152300.median_masked.tif'

#Start Time
Start_Time = time.time()



def process(iso):
    message = None
    if message is None:
        try:
            gdb = r'G:\HumanPlanet\SDG711\Version2\2021\Countries\%s.gdb' % iso
            arcpy.CreateFileGDB_management(r'G:\HumanPlanet\SDG711\Version2\2021\Countries','%s.gdb' % iso)
         
            
            #Set Workspace
            workspace = r'G:\HumanPlanet\SDG711\Version2\2021\Countries\%s.gdb' % iso
            arcpy.env.workspace = workspace
            arcpy.env.overwriteOutput = True

            #Get GADM Layer
            where_clause = '"GID_0" = \'%s\'' % iso
            out_gadm = '%s_gadm' % iso
            arcpy.Select_analysis(GADMGlobal,out_gadm,where_clause)

            gadm_copy = '%s_gadm_copy' % iso
            arcpy.CopyFeatures_management(out_gadm,gadm_copy)
            print('copied')

            #Dissolve to admin0 level
            boundary = '%s_admin0' % iso
            dissolveFields = ['GID_0','NAME_0']
            arcpy.Dissolve_management(gadm_copy, boundary, dissolveFields, "", 
                                    "MULTI_PART", "DISSOLVE_LINES")
 
            #COD IDs need to be adjusted
            if iso == 'COD':
                with arcpy.da.UpdateCursor(gadm_copy,'ID_0') as cursor:
                    for row in cursor:
                        row[0] = 1
                        cursor.updateRow(row)
                
                name_1 = []
                counter1 = 0
                with arcpy.da.UpdateCursor(gadm_copy,['ID_1','NAME_1']) as cursor:
                    for row in cursor:
                        if row[1] in name_1:
                            pass
                        else:
                            row[0] = counter1
                            cursor.updateRow(row)
                            counter1 = counter1 + 1
                name_2 = []
                counter2 = 0
                with arcpy.da.UpdateCursor(gadm_copy,['ID_2','NAME_2']) as cursor:
                    for row in cursor:
                        if row[1] in name_2:
                            pass
                        else:
                            row[0] = counter2
                            cursor.updateRow(row)
                            counter2 = counter2 + 1
            else:
                pass
                    

            #Assign UniqueIDs to GADM 2
            #Populate UniqueIDs field
            arcpy.AddField_management(gadm_copy,'ADMID','TEXT')
            expression = '"{}_{}_{}".format(!ID_0!, !ID_1!, !ID_2!)'
            arcpy.CalculateField_management(gadm_copy,'ADMID',expression,"PYTHON")

            #Dissolve GADM 2
            dissolveFields = ['GID_0','ID_0','NAME_0','ID_1','NAME_1','ID_2','NAME_2','ADMID']
            adm2 = '%s_admin2' % iso
            arcpy.Dissolve_management(gadm_copy,adm2,dissolveFields,"","MULTI_PART", "DISSOLVE_LINES")

            #Clipping WP data
            #WP = r'G:\HumanPlanet\WorldPopData\unconstrained\2020\%s\%s_ppp_2020_UNadj.tif' %(iso,iso.lower())
            #WP_clip = '%s_WP_2020' % iso

            #arcpy.env.snapRaster = WP
            #arcpy.env.cellSize = WP

            #arcpy.Clip_management(WP,"#",WP_clip,boundary,"#","#","#")

            #Clipping lights data
            lights_clip = '%s_lights' % iso

            arcpy.env.snapRaster = lights
            arcpy.env.cellSize = lights

            arcpy.Clip_management(lights,"#",lights_clip,boundary,"#","#","#")

           
            message = 'Succeeded: ' + iso

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
    mylist = ['NGA']
    length = len(mylist)
    pool = multiprocessing.Pool(processes=5, maxtasksperchild=1)
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


