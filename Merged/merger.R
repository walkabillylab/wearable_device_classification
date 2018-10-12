# Created by Javad Rahimipour Anaraki on 25/03/18 updated 21/06/18
# Ph.D. Candidate
# Department of Computer Science
# Memorial University of Newfoundland
# jra066 [AT] mun [DOT] ca | www.cs.mun.ca/~jra066

#   input: All collected data
#  output: A single files containing all collected data

rm(list = ls())
#========================Libraries=========================
list.of.packages <-
  c("lubridate",
    "stringr",
    "data.table",
    "dplyr",
    "car",
    "bit64")
new.packages <-
  list.of.packages[!(list.of.packages %in% installed.packages()[, "Package"])]
if (length(new.packages))
  install.packages(new.packages)

library(lubridate)
library(stringr)
library(data.table)
library(dplyr)
library(car)
library(bit64)


#=========================Functions========================
rep.row <- function(x, n) {
  unlistDataframe(data.frame(matrix(rep(x, each = n), nrow = n)))
}


unlistDataframe <- function(data) {
  temp1 <- sapply(data, is.list)
  temp2 <- do.call(
    cbind, lapply(data[temp1], function(x) 
      data.frame(do.call(rbind, x), check.names=FALSE)))
  cbind(data[!temp1], temp2)
}

#=========================Variables========================
OS <- Sys.info()
if (OS["sysname"] == "Windows") {
  geneActivePath <-
    "Z:/HeroX/data/GENEActiv/"
  healthDataPath <-
    "Z:/HeroX/data/HealthData/"
  senseDocPath <-
    "Z:/HeroX/data/SenseDoc/"
  path <-
    "Z:/HeroX/data/Merged/"
  intrPath <-
    "Z:/HeroX/data/"
} else {
  geneActivePath <-
    "/HeroX/data/GENEActiv/"
  healthDataPath <-
    "/HeroX/data/HealthData/"
  senseDocPath <-
    "/HeroX/data/SenseDoc/"
  path <-
    "/HeroX/data/Merged/"
  intrPath <-
    "/HeroX/data/"
}

setwd(path)
flag = FALSE

#A list of all data folders
listofDataFiles <-
  c("geneActiveFilenames",
    "healthDataFilenames",
    "senseDocFilenames")

#Timezone
timeZone <- "America/St_Johns"
Sys.setenv(TZ = timeZone)

#Required user id to be processed
participants <-
  list.dirs(path = path,
            full.names = FALSE,
            recursive = FALSE)

#====================Read in data files====================
for (k in 1:length(participants)) {
  uid <- participants[k]
  print(uid)
  
  geneActiveFilenames <-
    list.files(paste(geneActivePath, uid, sep = ""),
               full.names = TRUE,
               pattern = "*_labeled.csv")
  
  if (file.exists(paste0(path, uid, "/", uid, "_finalData.csv"))) {
    print(paste0("Final data file for ", uid, " exists"))
    next
  }
  
  healthDataFilenames <-
    list.files(paste(healthDataPath, uid, sep = ""),
               full.names = TRUE,
               pattern = "*_data.csv")
  
  senseDocFilenames <-
    list.files(paste(senseDocPath, uid, sep = ""),
               full.names = TRUE,
               pattern = "*_labeled.csv")
  
  intervals <-
    fread(paste(intrPath, "intervals.csv", sep = ""),
          sep = ",",
          data.table = FALSE)
  
  for (i in 1:length(listofDataFiles)) {
    for (j in 1:length(eval(parse(text = listofDataFiles[i])))) {
      if (length(eval(parse(text = listofDataFiles[i]))[j]) == 0) {
        print(paste0("Missing ", listofDataFiles[i], " for ", uid))
        flag = TRUE
      }
    }
  }
  
  if (flag) {
    flag = FALSE
    next
  }
  
  #=====================Preprations==========================
  #Final output
  finalData <- NULL
  
  #Filtering data out based on uid and start and end date
  usrInfo <- intervals[intervals[, "userid"] == uid,]
  startDate <- as.POSIXct(usrInfo[, "start"])
  endDate <- as.POSIXct(usrInfo[, "end"])
  
  #Create a sequence as a reference to merge data
  interval.seconds <-
    as.integer(difftime(endDate, startDate, units = "secs"))
  finalData <-
    as.data.frame(substr(seq(startDate, endDate, by = "sec"), 1, 19))
  colnames(finalData) <- "record_time"
  
  #==================Loop over data files====================
  for (i in 1:length(listofDataFiles)) {
    for (j in 1:length(eval(parse(text = listofDataFiles[i])))) {
      #Read in data file
      inData <-
        fread(eval(parse(text = listofDataFiles[i]))[j],
              sep = ",",
              data.table = FALSE)
      
      #Extract folder name to use as a prefix for columns' name
      if (listofDataFiles[i] == "healthDataFilenames") {
        #Folder name
        temp <-
          strsplit(eval(parse(text = listofDataFiles[i])), "/")
        prefix <- temp[[1]][length(temp[[1]]) - 2]
        device_name <- strsplit(temp[[j]][length(temp[[1]])], "_")[[1]][1]
        prefix <- paste(prefix, device_name, sep = "_")
        
      } else {
        #Folder name
        temp <-
          strsplit(eval(parse(text = listofDataFiles[i])), "/")
        prefix <- temp[[1]][length(temp[[1]]) - 2]
      }
      
      #Change column name of input data
      colnames(inData) <- paste(prefix, colnames(inData), sep = "_")
      
      #Find the time column
      timeIdx <- grep("time|Time|utcdate", colnames(inData))
      if (length(timeIdx) == 1) {
        colnames(inData)[timeIdx] <- "record_time"
      } else {
        print("Could not find time column | more than one column found")
      }
      
      #Modify the time values for GENEActiv
      if (listofDataFiles[i] == "geneActiveFilenames") {
        inData[, "record_time"] <- substr(inData[, "record_time"], 1, 19)
      }
      
      #Merging inpput data with final output based on recorded time
      switch (
        listofDataFiles[i],
        "geneActiveFilenames" = finalData <-
          merge(finalData, inData, by = "record_time", all.x = TRUE),
        "healthDataFilenames" = finalData <-
          merge(finalData, inData, by = "record_time", all.x = TRUE),
        "senseDocFilenames" = finalData <-
          merge(finalData, inData, by = "record_time", all.x = TRUE)
      )
    }
  }
  
  #===================Add demographic data===================
  demograph <- usrInfo[c("age", "gender", "weight", "height")]
  finalData <- cbind(finalData, rep.row(demograph, nrow(finalData)))
  colnames(finalData)[ncol(finalData)-3:0] <- colnames(demograph)
  
  #=================Export the final result==================
  fileName <- paste0(path, uid, "/", uid, "_finalData.csv")
  write.csv(finalData, fileName, row.names = FALSE)
}
