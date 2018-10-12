# Created by Javad Rahimipour Anaraki on 22/02/18 updated 21/06/18
# Ph.D. Candidate
# Department of Computer Science
# Memorial University of Newfoundland
# jra066 [AT] mun [DOT] ca | www.cs.mun.ca/~jra066

#   input: ACC data from Ethica, labels from GENEActiv, Wear Location and Intervals
#  output: Aggregated data with labels

rm(list = ls())
#========================Libraries=========================
list.of.packages <-
  c("lubridate",
    "stringr",
    "data.table",
    "dplyr",
    "car",
    "kimisc")
new.packages <-
  list.of.packages[!(list.of.packages %in% installed.packages()[, "Package"])]
if (length(new.packages))
  install.packages(new.packages)

library(lubridate)
library(stringr)
library(data.table)
library(dplyr)
library(car)
library(kimisc)

#=========================Variables========================
OS <- Sys.info()
if (OS["sysname"] == "Windows") {
  path <-
    "Z:/HeroX/data/Ethica/"
  labelPath <-
    "Z:/HeroX/data/GENEActiv/"
  intrPath <-
    "Z:/HeroX/data/"
} else {
  path <-
    "/HeroX/data/Ethica/"
  labelPath <-
    "/HeroX/data/GENEActiv/"
  intrPath <-
    "/HeroX/data/"
}
setwd(path)

#Timezone
timeZone <- "America/St_Johns"
Sys.setenv(TZ = timeZone)

#Required user id to be processed
participants <-
  list.dirs(path = path,
            full.names = FALSE,
            recursive = FALSE)

#====================Read in data files====================
for (i in 1:length(participants)) {
  uid <- participants[i]
  
  filenames <-
    list.files(paste(path, uid, sep = ""), full.names = TRUE)
  
  wearLocation <-
    fread(paste(path, "WearLocation.csv", sep = ""),
          sep = ",",
          data.table = FALSE)
  
  intervals <-
    fread(paste(intrPath, "intervals.csv", sep = ""),
          sep = ",",
          data.table = FALSE)
  
  #Read labels, change column name and trim record time values
  labelfilename <-
    list.files(paste(labelPath, uid, sep = ""),
               pattern = "*_labeled.csv",
               full.names = TRUE)
  
  label <-
    fread(labelfilename[1],
          sep = ",",
          data.table = FALSE)
  colnames(label)[5] <- "record_time"
  label[, "record_time"] <- substr(label[, "record_time"], 0, 19)
  res <- label[, c("record_time", "activity_final")]
  
  #Filtering data out based on uid and start and end date
  usrInfo <- intervals[intervals[, "userid"] == uid, ]
  startDate <- usrInfo[, "start"]
  endDate <- usrInfo[, "end"]
  
  
  #================Loop over wear locations==================
  for (i in 1:nrow(wearLocation)) {
    wear_loc <- wearLocation[i, "Location"]
    devID <- wearLocation[i, "ID"]
    
    #Reading accelerometer data
    acc <-
      read.csv(
        file = paste(path, uid, "/", devID, ".csv", sep = ""),
        header = TRUE,
        sep = ","
      )
    
    #Selecting required columns
    acc <- acc[, c("record_time", "x_axis", "y_axis", "z_axis")]
    
    #Rounding up the time to minutes
    acc[, "record_time"] <- substr(acc[, "record_time"], 0, 19)
    
    #Cut data based on start and end dates
    acc <-
      acc[(format.Date(acc[, "record_time"]) >= format.Date(startDate)), ]
    acc <-
      acc[(format.Date(acc[, "record_time"]) <= format.Date(endDate)), ]
    
    #Aggregating mData by record time and averaging of all values
    acc <-
      aggregate(
        acc[, c("x_axis", "y_axis", "z_axis")],
        by = list(record_time = acc$record_time),
        FUN = mean,
        na.rm = FALSE
      )
    
    #Adding corresponding labels to mData
    #To include all responses to the final aggregation dataset, change this to all=TRUE
    lData <- merge(acc, res, by = "record_time", all = FALSE)
    
    #Save the results as a CSV file
    fileName <- paste(devID, "_labeled.csv", sep = "")
    write.csv(lData, paste(path, uid, "/", fileName, sep = ""), row.names = FALSE)
  }
}