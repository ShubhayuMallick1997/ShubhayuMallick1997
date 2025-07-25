
import os
import urllib.request
import ssl


data_dir = "data"
os.makedirs(data_dir, exist_ok=True)

data_dir1 = "hadoop/bin"
os.makedirs(data_dir1, exist_ok=True)

urls_and_paths = {
    "https://raw.githubusercontent.com/saiadityaus1/SparkCore1/master/test.txt": os.path.join(data_dir, "test.txt"),
    "https://github.com/saiadityaus1/SparkCore1/raw/master/winutils.exe": os.path.join(data_dir1, "winutils.exe"),
    "https://github.com/saiadityaus1/SparkCore1/raw/master/hadoop.dll": os.path.join(data_dir1, "hadoop.dll")
}

# Create an unverified SSL context
ssl_context = ssl._create_unverified_context()

for url, path in urls_and_paths.items():
    # Use the unverified context with urlopen
    with urllib.request.urlopen(url, context=ssl_context) as response, open(path, 'wb') as out_file:
        data = response.read()
        out_file.write(data)
import os, urllib.request, ssl; ssl_context = ssl._create_unverified_context(); [open(path, 'wb').write(urllib.request.urlopen(url, context=ssl_context).read()) for url, path in { "https://github.com/saiadityaus1/test1/raw/main/df.csv": "df.csv", "https://github.com/saiadityaus1/test1/raw/main/df1.csv": "df1.csv", "https://github.com/saiadityaus1/test1/raw/main/dt.txt": "dt.txt", "https://github.com/saiadityaus1/test1/raw/main/file1.txt": "file1.txt", "https://github.com/saiadityaus1/test1/raw/main/file2.txt": "file2.txt", "https://github.com/saiadityaus1/test1/raw/main/file3.txt": "file3.txt", "https://github.com/saiadityaus1/test1/raw/main/file4.json": "file4.json", "https://github.com/saiadityaus1/test1/raw/main/file5.parquet": "file5.parquet", "https://github.com/saiadityaus1/test1/raw/main/file6": "file6", "https://github.com/saiadityaus1/test1/raw/main/prod.csv": "prod.csv", "https://github.com/saiadityaus1/test1/raw/main/state.txt": "state.txt", "https://github.com/saiadityaus1/test1/raw/main/usdata.csv": "usdata.csv"}.items()]

# ======================================================================================

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import sys

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path

os.environ['JAVA_HOME'] = r''

conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.default.parallelism", "1")
sc = SparkContext(conf=conf)

spark = SparkSession.builder.getOrCreate()

spark.read.format("csv").load("data/test.txt").toDF("Success").show(20, False)


##################🔴🔴🔴🔴🔴🔴 -> DONT TOUCH ABOVE CODE -- TYPE BELOW ####################################

print()


#############################################################################################################################



#############################################################################################################################

################################### LIST of NUMBERS
lst1 = [ 1,2,3,4,5,6,7,8,9]

rdd = sc.parallelize(lst1)

print ('================RAW DATA============')
print()
print(lst1)
print()

addlst1 = rdd.map( lambda x : x + 100)

print ('================ADD DATA============')
print()
print(addlst1.collect())
print()

sublst1 = rdd.map( lambda x : x - 100)

print ('================SUB DATA============')
print()
print(sublst1.collect())
print()

mullst1 = rdd.map( lambda x : x * 100)

print ('================MUL DATA============')
print()
print(mullst1.collect())
print()

divlst1 = rdd.map( lambda x : x / 100 )
print ('================DIV DATA============')
print()
print(divlst1.collect())
print()

filt1 = rdd.filter( lambda x : x > 3 )
print ('================FIL1 DATA ================')
print()
print ( filt1.collect())
print()

filt2 = rdd.filter( lambda x : x < 7 )
print( ' =================== FIL 2 DATA =================')
print()
print(filt2.collect())
print()

filt3 = rdd.filter( lambda x : 3 < x < 7 )

print ('===================FIL 3 DATA =====================')
print()
print(filt3.collect())
print()

filt4 = rdd.filter( lambda x : x > 3 and x < 7 )

print( ' ===================FIL 4 DATA ===================')
print()
print(filt4.collect())
print()

filt5 = rdd.filter( lambda x : x > 3 or x < 7 )

print(' ==================FIL 5 DATA ======================')
print()
print(filt5.collect())
print()

filt6 = rdd.filter ( lambda x : 3 <= x <= 7 )

print ( ' ========================FIL 6 DAT v===============')
print()
print(filt6.collect())
print()

filt7 = rdd.filter( lambda x : x >= 3 and x <= 7 )

print( '============= FIL 7 DATA ==========================')
print()
print(filt7.collect())
print()

########################################### LIST of STRINGS

lstr1 = [ "Shubhayu" , "Raja" , "Arsenal FC", "Barcelona FC" , "East Bengal FC "]

rdd = sc.parallelize(lstr1)
print ()
print('==================RAW DATA ==================')
print()
print(lstr1)
print()
con1 = rdd.map( lambda x : x + " JBL " )

print()
print('==================CON 1 DATA =====================')
print()
print(con1.collect())
print()

con2 = rdd.map( lambda x : x + " 2 " )

print ()
print ( '=======================CON 2 DATA ==================')
print()
print(con2.collect())
print()

"""
con3 = rdd.filter( lambda x : "FC" in x )

print()
print('=========================CON 3 DATA =====================')
print()
print(con3.collect())
print()


rep1 = rdd.map( lambda x : x.replace("FC", "Football Club"))

print()
print('========================REP 1 DATA =====================')
print()
print(rep1.collect())
print()

"""

########################################################################

lst2 = [100, 40, 200, 1500, 800, 90, 60 ]


print()
print('==============RAW DATA =============')
print()
print(lst2)
print()

rdd = sc.parallelize(lst2)


addlst2 = rdd.map( lambda x : x + 100)

print()
print('==============ADD DATA =============')
print()
print(addlst2.collect())
print()

sublst2 = rdd.map( lambda x : x - 100)

print()
print('==============SUB DATA =============')
print()
print(sublst2.collect())
print()


mullst2 = rdd.map( lambda x : x * 100)

print()
print('==============MUL DATA =============')
print()
print(mullst2.collect())
print()


divlst2 = rdd.map( lambda x : x / 100)

print()
print('==============DIV DATA =============')
print()
print(divlst2.collect())
print()


