# Created by Faramarz Dorani on 05/03/18
# Department of Computer Science
# Memorial University of Newfoundland
# fd6713 [AT] mun [DOT] ca

# Modified by Javad Rahimipour Anaraki on 05/03/18 updated 10/09/18
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
  # A function to detect zeroes and NAs in the dataset for a column
  first <- NaN
  last <- NaN
  user <-  data[1, "user_name"]
  for (i in 1:nrow(data)) {
    row <- data[i,]
    if (row[column][[1]] == 0 && is.nan(first)) {
      first <- row$date_time
      user <- row$user_name
    }
    
    if (!is.nan(first) &&
        row$date_time > first && row[column][[1]] == 0) {
      last <- row$date_time
    }
    
    # User is changed but still there are zero
    if (row[column][[1]] == 0 &&
        !is.nan(first) && !is.nan(last) && user != row$user_name) {
      first_t <- as.POSIXct(first, format = "%Y-%m-%d %H:%M:%S")
      last_t <- as.POSIXct(last, format = "%Y-%m-%d %H:%M:%S")
      lap <- difftime(last_t, first_t, units = "min")
      if (lap >= interval) {
        data[which(
          data$user_name == user &
            data[, column] == 0 &
            data$date_time >= first &
            data$date_time <= last
        ), column] <- NaN
      }
      first <- NaN
      last <- NaN
    }
    
    # Streak of zeroes ends
    if (row[column][[1]] != 0 &&
        !is.nan(first) && !is.nan(last) && user == row$user_name) {
      first_t <- as.POSIXct(first, format = "%Y-%m-%d %H:%M:%S")
      last_t <- as.POSIXct(last, format = "%Y-%m-%d %H:%M:%S")
      lap <- difftime(last_t, first_t, units = "min")
      if (lap >= interval) {
        data[which(data$user_name == user &
                     data[, column] == 0 &
                     data$DateTime >= first &
                     data$DateTime <= last), column] <- NaN
      }
    }
    
    # Reinitialize the variables
    if (row[column][[1]] != 0) {
      first <- NaN
      last <- NaN
    }
  }
  return(data)
}


#=========================Variables========================
OS <- Sys.info()
if (OS["sysname"] == "Windows") {
  path <-
    "Z:/HeroX/data/HealthData/"
  intrPath <-
    "Z:/HeroX/data/"
} else {
  path <-
    "/HeroX/data/HealthData/"
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
        write.csv(
          data_subset_fitbit,
          paste(path, uid, "/fitbit_data.csv", sep = ""),
          row.names = F,
          quote = F
        )
      }
    }
}
