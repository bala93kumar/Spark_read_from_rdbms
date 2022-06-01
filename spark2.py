import os
import sys

# os.environ['PYSPARK_PYTHON']="C:\\Users\\user\\AppData\\Local\\Programs\Python\\Python310\\"

from pyspark import SparkConf , SparkContext

sc = SparkContext(master="local", appName="spark Demo")

print(sc.textFile("C:\\\BALAKUMAR\LocalFiles\SparkProjects\deckofcards.txt").first())
