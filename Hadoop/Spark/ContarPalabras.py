import sys
try:
    from pyspark import SparkConf, SparkContext

    conf = SparkConf()
    sc = SparkContext(conf=conf)
    inputPath = sys.argv[1]
    outputPath = sys.argv[2]


    Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
    FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
    Configuration = sc._gateway.jvm.org.apache.hadoop.conf.Configuration
    fs = FileSystem.get(Configuration())


    if(fs.exists(Path(inputPath)) == False):
        print("El fichero de entrada no existe")

    else:
        if(fs.exists(Path(outputPath))):
            fs.delete(Path(outputPath), True)

        sc.textFile(inputPath).flatMap(lambda l: l.split(" ")).map(lambda w: (w, 1)).reduceByKey(lambda t, e: t + e).saveAsTextFile(outputPath)

    print ("Se han importado los modulos de Spark")

except ImportError as e:
    print ("No puedo importar los modulos de Spark", e)
    sys.exit(1)
