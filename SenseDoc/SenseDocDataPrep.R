# Created by Javad Rahimipour Anaraki on 02/04/18 updated 21/06/18
# Ph.D. Candidate
# Department of Computer Science
# Memorial University of Newfoundland
# jra066 [AT] mun [DOT] ca | www.cs.mun.ca/~jra066

#   input: ACC data from SenseDoc and Intervals
#  output: Clean data

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
    "Z:/HeroX/data/SenseDoc/"
  labelPath <-
    "Z:/HeroX/data/GENEActiv/"
  intrPath <-
    "Z:/HeroX/data/"
} else {
  path <-
    "/HeroX/data/SenseDoc/"
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
for (j in 1:length(participants)) {
  uid <- participants[j]
  print(uid)
  
  filenames <-
    list.files(paste0(path, uid),
               pattern = "[0-9]{3}-accel_utcdate-1.csv",
               full.names = TRUE)
  
  intervals <-
    fread(paste0(intrPath, "intervals.csv"),
          sep = ",",
          data.table = FALSE)
  
  #Read labels, change column name and trim record time values
  labelfilename <-
    list.files(paste0(labelPath, uid),
               pattern = "*_labeled.csv",
               full.names = TRUE)
  
  if (length(labelfilename) == 0) {
    print(paste0("GENEActiv labeled file for ", uid, " is missing!"))
    next
  }
  
  if (file.size(labelfilename) == 0) {
    print(paste0("No GENEActiv label record for ", uid))
    next
  }

  label <-
    fread(labelfilename[1],
          sep = ",",
          data.table = FALSE)
  colnames(label)[5] <- "record_time"
  label[, "record_time"] <- substr(label[, "record_time"], 0, 19)
  res <- label[, c("record_time", "activity_final")]
  
  #=====================Data prepration======================
  nFiles <- length(filenames)
  
  if (nFiles > 0) {
    for (i in 1:nFiles) {
      if (file.size(filenames[i]) == 0 |
          file.exists(paste0(path,
                             uid,
                             "/",
                             unlist(strsplit(
                               basename(filenames[i]), "[.]"
                             ))[1],
                             "_labeled.csv"))) {
        print(paste0("SenseDoc labeled file for ", uid, " exists"))
        break
      }
      
      accel_data <-
        fread(
          filenames[i],
          header = TRUE,
          sep = ",",
          data.table = FALSE
        )
      
      #Remove unneccesary columns
      accel_data$rowid <- NULL
      accel_data$ts <- NULL
      
      #Change column name
      colnames(accel_data)[1] <- "record_time"
      
      #Filtering data out based on uid and start and end date
      usrInfo <- intervals[intervals[, "userid"] == uid, ]
      startDate <- usrInfo[, "start"]
      endDate <- usrInfo[, "end"]
      
      #Convert UTC to locan timezone + Cut data based on start and end dates
      local.date <- as.POSIXct(accel_data$record_time, tz = "UTC")
      accel_data$record_time <- format(local.date, tz = timeZone)
      
      accel_data <-
        accel_data[(format.Date(accel_data[, "record_time"]) >= format.Date(startDate)), ]
      accel_data <-
        accel_data[(format.Date(accel_data[, "record_time"]) <= format.Date(endDate)), ]
      
      #Collapse data to one second
      accel_data_sec <- accel_data %>%
        group_by(record_time) %>%
        summarise(
          m_x_counts = mean(x),
          m_y_counts = mean(y),
          m_z_counts = mean(z)
        )
      
      #Adding corresponding labels to mData
      #To include all responses to the final aggregation dataset, change this to all=TRUE
      lData <-
        merge(accel_data_sec, res, by = "record_time", all.x = TRUE)
      
      #=========================Exporting========================
      #Save the results as a CSV file
      fileName <-
        paste(unlist(strsplit(basename(filenames), "[.]"))[1], "_labeled.csv", sep = "")
      write.csv(lData,
                paste(path, uid, "/", fileName, sep = ""),
                row.names = FALSE)
    }
  }
}
