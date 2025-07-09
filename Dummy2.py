
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


########################################################################

lst2 = [100, 40, 200, 1500, 800, 90, 60 ,450 , 570, 1900, 430, 580, 790, 680, 1100]


print()
print('==============RAW DATA =============')
print()
print(lst2)
print()

rdd = sc.parallelize(lst2)


addlst2 = rdd.map( lambda x : x + 10000)

print()
print('==============ADD DATA =============')
print()
print(addlst2.collect())
print()

sublst2 = rdd.map( lambda x : x - 10000)

print()
print('==============SUB DATA =============')
print()
print(sublst2.collect())
print()


mullst2 = rdd.map( lambda x : x * 10000)

print()
print('==============MUL DATA =============')
print()
print(mullst2.collect())
print()


divlst2 = rdd.map( lambda x : x / 10000)

print()
print('==============DIV DATA =============')
print()
print(divlst2.collect())
print()



##############################################

lstr2 = ["Odegaard", "Saliba", "Vini Jr", "Bukayo Saka", "Neymar Jr"]

print()
print('==============RAW DATA ===============')
print(lstr2)
print()

rdd = sc.parallelize(lstr2)

con4 = rdd.map( lambda x : x + " Ballon Dor")

print()
print('==============CON 4 DATA ===============')
print(con4.collect())
print()

con5 = rdd.map( lambda x : "Jr" in x)

print()
print('==============CON 5 DATA ===============')
print(con5.collect())
print()


con6 = rdd.map( lambda x : x.replace("Jr","Senior"))

print()
print('==============CON 4 DATA ===============')
print(con6.collect())
print()

lst3 = [100, 40, 200, 1500, 800, 90, 60 ,450 , 570, 1900, 430, 580, 790, 680, 1100]

rdd = sc.parallelize(lst3)

filt8 = rdd.filter( lambda x : x > 600)

print()
print('==================== FILT 8 DATA======================')
print()
print(filt8.collect())
print()

filt9 = rdd.filter( lambda x : x < 600)

print()
print('==================== FILT 9 DATA======================')
print()
print(filt9.collect())
print()

filt10 = rdd.filter( lambda x : 400 < x < 800)

print()
print('==================== FILT 10 DATA======================')
print()
print(filt10.collect())
print()

filt11 = rdd.filter( lambda x : 400 <= x <= 800)

print()
print('==================== FILT 11 DATA======================')
print()
print(filt11.collect())
print()


filt12 = rdd.filter( lambda x : x >= 400 and x>= 800)

print()
print('==================== FILT 12 DATA======================')
print()
print(filt12.collect())
print()


filt13 = rdd.filter( lambda x : x <= 400 or x>= 800)

print()
print('==================== FILT 13 DATA======================')
print()
print(filt13.collect())
print()



