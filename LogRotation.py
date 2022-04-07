#!/usr/bin/python3

# script.py logfile.txt 10 5

import os       #for get size
import sys      #for get argv
import shutil   #for copy

#Check correct count of argv
if len(sys.argv) < 4:
    print("Wrong count of argv")
    exit(1)

filename = sys.argv[1]
logsize = int(sys.argv[2])
lognumbers = int(sys.argv[3])


if os.path.isfile(filename):                    #check for exist
    file_size = os.stat(filename).st_size
    file_size = file_size / 1024                #size to kb
    if file_size >= logsize:                    #check correct size
        if lognumbers > 0:                      #check number of log files
            for i in range(lognumbers, 1, -1):
                src = filename + "_" + str(i - 1)
                dst = filename + "_" + str(i)
                if os.path.isfile(src):
                    shutil.copyfile(src, dst)
            shutil.copyfile(filename, filename + "_1")
        s = open(filename, 'w')
        s.close()

