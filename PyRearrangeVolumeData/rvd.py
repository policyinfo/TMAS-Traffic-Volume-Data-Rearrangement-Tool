import pandas as pd
import tempfile
import os
import zipfile
# this file is to rearrange TMAS Traffic Volume Data downloaded from FHWA website
class Rvd:
  def __init__(self, inputzipfile, unpivot):
    self.inputzipfile = inputzipfile
    self.unpivot = unpivot
  def startup(self):
    print('Original zip file:'+ self.inputzipfile)
    # create a temp folder for unzip
    self.unzipFolder = tempfile.mkdtemp()
    mainUnzipFolder = self.unzipFolder
    print('Temporary unzip folder:' + self.unzipFolder)
    #unzip file
    zf = zipfile.ZipFile(self.inputzipfile, 'r')
    zf.extractall(self.unzipFolder)
    zf.close()
    self.DetectSubfolder()
    files = os.listdir(self.unzipFolder)

    # create a temp folder for saving unpivot files
    self.upFolder = tempfile.mkdtemp()
    for f in files:
        self.UnPivotAFile(f,self.unpivot )
        os.remove(os.path.join(self.unzipFolder,f))
    os.removedirs(self.unzipFolder)  # remove with the sub folder, will automatically remove the sub and then its parent if it is empty.
    # for example if unzipFolder is tmpxyz\january, it will first remove tmpxyz\january, then tmpxyz.
    # this is needed as unzipFolder may contain a sub zip folder under mainUnzipFolder
    print(mainUnzipFolder + ' removed.  Zipping started...')

    index = self.inputzipfile.find('.zip')
    
    mainfile = self.inputzipfile[0:index] + "_converted.zip"
    zf = zipfile.ZipFile(mainfile, 'w',compression=zipfile.ZIP_DEFLATED)
    for f in os.listdir(self.upFolder):
        zf.write(os.path.join(self.upFolder,f),f)  # this will not save the absoulte path,but only the file name in the zipped content
        os.remove(os.path.join(self.upFolder,f))
    zf.close()
    os.removedirs(self.upFolder)
    print(self.upFolder + ' removed.')
    print("Done. All converted files are zipped in " + mainfile)

  def DetectSubfolder(self): # unzip may contain a subfolder
      folder = self.unzipFolder
      for f in os.listdir(folder):
          if os.path.isfile(os.path.join(folder,f)):
              return # file is not zipped in a folder
          else : 
              self.unzipFolder = os.path.join(folder,f) # update the unzipped folder
              return 

  def DetectHeaderAndPipe(self,filename):
      # some files have the header some not, some are ',' separated some are fixed width
      # it needs to detect this first before feeding to pandas
      # return hasHeader,hasPipe
      f = open(self.unzipFolder + '/' + filename,'r')
      firstline = f.readline()
      f.close()
      hasPipe = False
      hasHeader = False
      if firstline.find('|') > 0 :
          hasPipe = True
      if firstline.upper().find('STATION_ID') > 0:
          hasHeader = True
      return hasHeader,hasPipe

  def UnPivotAFile(self,filename,unpivot):
      r = self.DetectHeaderAndPipe(filename)
      
      columnNames = ['Record_Type','State_Code','F_System','Station_Id','Travel_Dir','Travel_Lane','Year_Record','Month_Record','Day_Record','Day_of_Week',
                     'Hour_00','Hour_01','Hour_02','Hour_03','Hour_04','Hour_05','Hour_06','Hour_07','Hour_08','Hour_09',
                     'Hour_10','Hour_11','Hour_12','Hour_13','Hour_14','Hour_15','Hour_16','Hour_17','Hour_18','Hour_19',
                     'Hour_20','Hour_21','Hour_22','Hour_23','Restrictions']
      columnTypes = {'Record_Type':'str','State_Code':'str','F_System':'str','Station_Id':'str','Travel_Dir':'str','Travel_Lane':'str',
               'Year_Record':'str','Month_Record':'str','Day_Record':'str','Day_of_Week':'str',
               'Hour_00':'str','Hour_01':'str','Hour_02':'str','Hour_03':'str',
               'Hour_04':'str','Hour_05':'str','Hour_06':'str','Hour_07':'str',
               'Hour_08':'str','Hour_09':'str','Hour_10':'str','Hour_11':'str',
               'Hour_12':'str','Hour_13':'str','Hour_14':'str','Hour_15':'str',
               'Hour_16':'str','Hour_17':'str','Hour_18':'str','Hour_19':'str',
               'Hour_20':'str','Hour_21':'str','Hour_22':'str','Hour_23':'str','Restrictions':'str'}
      # cols for |
#3|02|1R|000101|1|1|21|12|01|4|00007|00005|00008|00002|00003|00010|00004|00005|00009|00007|00014|00014|00015|00021|00017|00015|00011|00014|00005|00004|00010|00004|00004|00009|0
      colspecs = [(0,1),(2,4),(5,7),(8,14),(15,16),(17,18),(19,21),(22,24),(25,27),(28,29),
                  (30,35),(36,41),(42,47),(48,53),(54,59),(60,65),(66,71),(72,77),(78,83),(84,89),(90,95),(96,101),
                  (102,107),(108,113),(114,119),(120,125),(126,131),(132,137),(138,143),(144,149),(150,155),(156,161),(162,167),(168,173),(174,175)
                  ]
      # cols for fixed width
#3021R0001011119010720000400007000030000000001000040000700006000070000400020000190001800021000160002600015000160000600009000150001200004000070
      if r[1] == False:
        colspecs = [(0,1),(1,3),(3,5),(5,11),(11,12),(12,13),(13,15),(15,17),(17,19),(19,20),
                    (20,25),(25,30),(30,35),(35,40),(40,45),(45,50),(50,55),(55,60),(60,65),(65,70),(70,75),(75,80),
                    (80,85),(85,90),(90,95),(95,100),(100,105),(105,110),(110,115),(115,120),(120,125),(125,130),(130,135),(135,140),(140,141)
                    ]
      if r[0] :
        df = pd.read_fwf(self.unzipFolder + '/' + filename,names=columnNames,dtype=columnTypes,colspecs = colspecs, header = 0)
      else:
        df = pd.read_fwf(self.unzipFolder + '/' + filename,names=columnNames,dtype=columnTypes,colspecs = colspecs)
      total = len(df)
      dfu3 = df
      if unpivot == 'Y':
        dfu = pd.melt(df,id_vars=['Record_Type','State_Code','F_System','Station_Id','Travel_Dir','Travel_Lane','Year_Record','Month_Record','Day_Record','Day_of_Week','Restrictions'],
                       value_vars=['Hour_00','Hour_01','Hour_02','Hour_03','Hour_04','Hour_05','Hour_06','Hour_07','Hour_08','Hour_09',
                     'Hour_10','Hour_11','Hour_12','Hour_13','Hour_14','Hour_15','Hour_16','Hour_17','Hour_18','Hour_19',
                     'Hour_20','Hour_21','Hour_22','Hour_23'],
                       var_name = 'Hour_Record1',value_name='Hour_Volume',ignore_index=False) # keep original index
        dfu['Hour_Record'] = dfu['Hour_Record1'].str.slice(5, 7)

        dfu1 = dfu.reset_index()  # this will put original index to column as index
        dfu2 = dfu1.sort_values(by=['index','Hour_Record'])  # this technique will make the primary order the same as input order, and the second order is by hour
        dfu3 = dfu2[['Record_Type','State_Code','F_System','Station_Id','Travel_Dir','Travel_Lane','Year_Record','Month_Record','Day_Record','Day_of_Week','Hour_Record','Hour_Volume','Restrictions']]
        
      total1 = len(dfu3)
      dfu3.to_csv(self.upFolder + '/' + filename,sep=',',index=False)
      print('Converted:'+ filename + ' Header=' + str(r[0]) + ', Pipe=' + str(r[1]) + ', Total Records = ' + str(total) + ", Total Records after conversion = " + str(total1))

inputZipfile = input("Please enter the original zipped data file:")
unpivot = input("Do you want to convert a day record into 24 hour records? Enter Y for Yes, any other key for No:")
unpivot = unpivot.upper()
up = Rvd(inputZipfile,unpivot)
up.startup()