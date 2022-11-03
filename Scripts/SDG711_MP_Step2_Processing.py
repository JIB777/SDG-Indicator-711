#James Gibson
#3/21/22

#SDG Indicator 7.1.1
#Step 2: Processing

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

#Global
GADMGlobal = r'G:\HumanPlanet\GADM\GADM.gdb\GADMCopy3'

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
            WP_clip = r'G:\HumanPlanet\WorldPopData\unconstrained\2020\%s\%s_ppp_2020_UNadj.tif' %(iso,iso.lower())
            lights_clip = '%s_lights' % iso
            adm2 = '%s_admin2' % iso
            boundary = '%s_admin0' % iso

            #Create complete fc
            complete = '%s_complete' % iso
            arcpy.CopyFeatures_management(adm2,complete)

            #Set environment rules which will be applied throughout processing
            arcpy.env.cellSize = WP_clip
            arcpy.env.snapRaster = WP_clip

            #Get Max and Min Values of Lights Raster
            rast = arcpy.Raster(lights_clip)
            max_value = rast.maximum
            min_value = rast.minimum

            #Reclass Lights Raster based on max and min
            out_raster = arcpy.sa.Reclassify(lights_clip, "VALUE", "%s 0 1;0 %s 2" % (min_value,max_value), "DATA")
            Outreclass = '%s_lights_reclass' % iso
            out_raster.save(Outreclass)

            #Convert Reclassed Raster to Polygons
            viirs_polygon = '%s_viirs_polygons' % iso
            arcpy.RasterToPolygon_conversion(Outreclass,viirs_polygon,"NO_SIMPLIFY","VALUE")

            #Intersect Lights Reclass Polygons with GADM 2 
            viirs_adm2_polygons = '%s_viirs_adm2_polygons' % iso
            arcpy.Intersect_analysis([viirs_polygon,adm2],viirs_adm2_polygons)

            #Dissolve
            viirs_adm2_polygons_dissolved = '%s_viirs_adm2_polygons_dissolved' % iso
            dissolvefields = ['ID_0','NAME_0','ID_1','NAME_1','ID_2','NAME_2','ADMID','gridcode']
            arcpy.Dissolve_management(viirs_adm2_polygons,viirs_adm2_polygons_dissolved,dissolvefields,"","MULTI_PART", "DISSOLVE_LINES")

            #Split by Attribute using select layer by attributes
            result = arcpy.SelectLayerByAttribute_management(viirs_adm2_polygons_dissolved,"NEW_SELECTION","gridcode = 1")
            no_lights = '%s_adm_no_lights_poly' % iso
            arcpy.CopyFeatures_management(result,no_lights)

            result = arcpy.SelectLayerByAttribute_management(viirs_adm2_polygons_dissolved,"NEW_SELECTION","gridcode = 2")
            lights = '%s_adm_lights_poly' % iso
            arcpy.CopyFeatures_management(result,lights)

            #Rasterize light and no light feature classes
            lights_raster = '%s_adm_lights' % iso
            arcpy.PolygonToRaster_conversion(lights,"ADMID",lights_raster,"CELL_CENTER")
            
            no_lights_raster = '%s_adm_no_lights' % iso
            arcpy.PolygonToRaster_conversion(no_lights,"ADMID",no_lights_raster,"CELL_CENTER")

            

            #Create Adm0 raster
            adm0_raster = '%s_adm0_raster' % iso
            arcpy.PolygonToRaster_conversion(boundary,"NAME_0",adm0_raster,"CELL_CENTER")

            #Create Adm2 raster
            adm2_raster = '%s_adm2_raster' % iso
            arcpy.PolygonToRaster_conversion(adm2,"ADMID",adm2_raster,"CELL_CENTER")

            #Zonal Stats
            #Lights raster
            arcpy.env.cellSize = WP_clip
            arcpy.env.snapRaster = WP_clip
            outtablename = '%s_total_pop_viirs_lights' % iso
            ZonalStatisticsAsTable(lights_raster,"ADMID",WP_clip,outtablename,"DATA","SUM")
            arcpy.JoinField_management(complete,"ADMID",outtablename,"ADMID",["SUM"])
            arcpy.AlterField_management(complete,"SUM",'Total_Pop_adm_lights','Total_Pop_adm_lights')

            #No Lights raster
            arcpy.env.cellSize = WP_clip
            arcpy.env.snapRaster = WP_clip
            outtablename = '%s_total_pop_viirs_no_lights' % iso
            ZonalStatisticsAsTable(no_lights_raster,"ADMID",WP_clip,outtablename,"DATA","SUM")
            arcpy.JoinField_management(complete,"ADMID",outtablename,"ADMID",["SUM"])
            arcpy.AlterField_management(complete,"SUM",'Total_Pop_adm_no_lights','Total_Pop_adm_no_lights')
            


            #Total Pop per admin unit
            arcpy.env.cellSize = WP_clip
            arcpy.env.snapRaster = WP_clip
            outtablename = '%s_total_pop_adm2' % iso
            ZonalStatisticsAsTable(adm2_raster,"ADMID",WP_clip,outtablename,"DATA","SUM")
            arcpy.JoinField_management(complete,"ADMID",outtablename,"ADMID",["SUM"])
            arcpy.AlterField_management(complete,"SUM",'Total_Pop_Adm','Total_Pop_Adm')
    

            #Total Pop
            arcpy.env.cellSize = WP_clip
            arcpy.env.snapRaster = WP_clip
            outtablename = '%s_total_pop_country' % iso
            ZonalStatisticsAsTable(adm0_raster,"NAME_0",WP_clip,outtablename,"DATA","SUM")
            arcpy.JoinField_management(complete,"NAME_0",outtablename,"NAME_0",["SUM"])
            arcpy.AlterField_management(complete,"SUM",'Total_Pop_Country','Total_Pop_Country')

            #Editing
            with arcpy.da.UpdateCursor(complete,['Total_Pop_adm_lights', 'Total_Pop_adm_no_lights', 'Total_Pop_Adm']) as cursor:
                for row in cursor:
                    if (row[0] is None) and (row[1] is not None):
                        row[0] = row[2] - row[1]
                        cursor.updateRow(row)
                    elif (row[0] is not None) and (row[1] is None):
                        row[1] = row[2] - row[0]
                        cursor.updateRow(row)
                    else:
                        pass


            #Get Access per Adm2 Unit
            arcpy.AddField_management(complete,'Percent_Access','DOUBLE')
            expression = "(!Total_Pop_adm_lights!/!Total_Pop_Adm!)*100" 
            arcpy.CalculateField_management(complete,'Percent_Access',expression,'PYTHON')

            arcpy.AddField_management(complete,'Check_POP','DOUBLE')
            expression = "(!Total_Pop_adm_lights! + !Total_Pop_adm_no_lights!)-!Total_Pop_Adm!" 
            arcpy.CalculateField_management(complete,'Check_POP',expression,'PYTHON')
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
    mylist = ['NGA']
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


