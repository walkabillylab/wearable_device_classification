# Programmed by Javad Rahimipour Anaraki on 05/03/18 updated 09/11/18
# Ph.D. Candidate
# Department of Computer Science
# Memorial University of Newfoundland
# jra066 [AT] mun [DOT] ca | www.cs.mun.ca/~jra066

#This script preprocesses the health data before merging with GeneActive data
# Given users detail information, a subset of data is selected
# On which then missing value imputation and interpolation is conducted

rm(list = ls())
#========================Libraries=========================
list.of.packages <-
  c("imputeTS",
    "data.table")
new.packages <-
  list.of.packages[!(list.of.packages %in% installed.packages()[, "Package"])]
if (length(new.packages))
  install.packages(new.packages)

library(data.table)
library(imputeTS)

#========================Functions=========================
detect_nas <- function(data,
                       column = "heart",
                       interval = 10.0) {
  start = 1
  end = 1
  
  for (j in 1:nrow(data)) {
    row <- data[j,]
    
    if (row[column][[1]] == 0) {
      end <- j
      
      if ((end - start >= interval &&
           sum(data[start:end, column]) == 0) |
          (end == nrow(data)) && sum(data[start:end, column]) == 0) {
        data[start:end, column] <- NaN
        start = end + 1
      }
      
    } else {
      
      end <- j - 1
      
      if ((end - start >= interval &&
           sum(data[start:end, column]) == 0) |
          (end == nrow(data)) && sum(data[start:end, column]) == 0) {
        data[start:end, column] <- NaN
      }
      
      start = end + 2
    }
    
  }
  return(data)
}



#=========================Variables========================
OS <- Sys.info()
if (OS["sysname"] == "Windows") {
  path <-
    "Z:/Research/dfuller/Walkabilly/studies/HeroX/data/HealthData/"
  intrPath <-
    "Z:/Research/dfuller/Walkabilly/studies/HeroX/data/"
} else {
  path <-
    "/Volumes/hkr-storage/Research/dfuller/Walkabilly/studies/HeroX/data/HealthData/"
  intrPath <-
    "/Volumes/hkr-storage/Research/dfuller/Walkabilly/studies/HeroX/data/"
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
  print(uid)
  
  appleData <-
    dir(paste0(path, uid),
        pattern = "apple_[0-9]",
        full.names = TRUE)
  
  fitbitData <-
    dir(paste0(path, uid),
        pattern = "fitbit_[0-9]",
        full.names = TRUE)
  
  
  #==========Create apple watch and fitbit subsets=========
  if (length(appleData) > 0)
    if (file.exists(appleData)) {
      data_subset_apple <-
        fread(
          appleData,
          data.table = FALSE,
          nThread = 4,
          header = TRUE
        )
      
      if (nrow(data_subset_apple) > 0) {
        data_subset_apple <-
          data_subset_apple[with(data_subset_apple, order(user_name, date_time)), ]
        data_subset_apple <-
          data_subset_apple[, 2:ncol(data_subset_apple)]
        
        #=============Detect NAs (interval of 10 min)============
        apple_process <- data_subset_apple
        
        apple_process <-
          detect_nas(data = apple_process,
                     column = "heart",
                     interval = 10)
        
        #============Remove NAs (interval of 10 min)============
        apple_process$interpolated <- NA
        apple_cleaned <- na.omit(apple_process)
        
        #==================Interpolate data=====================
        #Replace 0s with NAs in Apple Watch data to prepare for interpolation
        if (sum(apple_cleaned$heart == 0) < nrow(apple_cleaned)) {
          apple_cleaned[apple_cleaned$heart == 0, "heart"] <- NA
          apple_cleaned$interpolated <- 0
          apple_cleaned[is.na(apple_cleaned$heart), "interpolated"] <-
            1
          try(apple_cleaned$heart <-
                na.interpolation(apple_cleaned$heart, option = "linear"))
        }
      
        write.csv(
          apple_cleaned,
          paste0(path, uid, "/applewatch_data.csv"),
          row.names = F,
          quote = F
        )
        
      } else {
        
        if (!any(str_detect(colnames(data_subset_apple), "interpolated"))) {
          data_subset_apple[1, ] <- NA
          data_subset_apple$interpolated <- NA
          data_subset_apple <- na.omit(data_subset_apple)
        }
        
          write.csv(
          data_subset_apple,
          paste0(path, uid, "/applewatch_data.csv"),
          row.names = F,
          quote = F
        )
        
      }
    }
  
  if (length(fitbitData) > 0)
    if (file.exists(fitbitData)) {
      data_subset_fitbit <-
        fread(
          fitbitData,
          data.table = FALSE,
          nThread = 4,
          header = TRUE
        )
      
      if (nrow(data_subset_fitbit) > 0) {
        data_subset_fitbit <-
          data_subset_fitbit[with(data_subset_fitbit, order(user_name, date_time)), ]
        data_subset_fitbit <-
          data_subset_fitbit[, 2:ncol(data_subset_fitbit)]
        
        #=============Detect NAs (interval of 10 min)============
        fitbit_process <- data_subset_fitbit
        
        fitbit_process <-
          detect_nas(data = fitbit_process,
                     column = "heart",
                     interval = 10)
        
        #============Remove NAs (interval of 10 min)============
        fitbit_process$interpolated <- NA
        fitbit_cleaned <- na.omit(fitbit_process)
        
        #==================Interpolate data=====================
        #Replace 0s with NAs in Fitbit data to prepare for interpolation
        if (sum(fitbit_cleaned$heart == 0) < nrow(fitbit_cleaned)) {
          fitbit_cleaned[fitbit_cleaned$heart == 0, "heart"] <- NA
          fitbit_cleaned$interpolated <- 0
          fitbit_cleaned[is.na(fitbit_cleaned$heart), "interpolated"] <-
            1
          try(fitbit_cleaned$heart <-
                na.interpolation(fitbit_cleaned$heart, option = "linear"))
        }
        
        write.csv(
          fitbit_cleaned,
          paste(path, uid, "/fitbit_data.csv", sep = ""),
          row.names = F,
          quote = F
        )
      } else {
        
        if (!any(str_detect(colnames(data_subset_fitbit), "interpolated"))) {
          data_subset_fitbit[1, ] <- NA
          data_subset_fitbit$interpolated <- NA
          data_subset_fitbit <- na.omit(data_subset_fitbit)
        }
        
        write.csv(
          data_subset_fitbit,
          paste(path, uid, "/fitbit_data.csv", sep = ""),
          row.names = F,
          quote = F
        )
      }
    }
}
