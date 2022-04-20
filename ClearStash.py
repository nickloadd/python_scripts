import time
import os

COUNT_DIRECTORIES = 0
COUNT_FILES = 0

days = 1 #Older than days
old = time.time() - (days * 24 * 60 * 60) #Older in seconds

#List of directories to script
FOLDERS = [
        "C:\Trash"
          ]


for DIR in FOLDERS:
    for root, dirs, files in os.walk(DIR): #os.walk for root_directory, directory(cwd), files
        if os.path.isdir(root):
            if len(os.listdir(root)) == 0: #if directory is empty
                print(root)
                COUNT_DIRECTORIES += 1
                os.rmdir(root) #delete empty directories
        for file in files:
            path = root + "\\" + file
            if old >= os.path.getctime(path): #if file older than days
                print(path)
                COUNT_FILES +=1
                os.remove(path) #delete old files

#Statisctic
print("Empty directories deleted: " + str(COUNT_DIRECTORIES) + "\n" + "Files older than " + str(days) + " days deleted: " + str(COUNT_FILES))