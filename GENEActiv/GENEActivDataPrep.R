# Programmed by Daniel Fuller
# Canada Research Chair in Population Physical Activity
# School of Human Kinetics and Recreation
# Memorial University of Newfoundland
# dfuller [AT] mun [DOT] ca | www.walkabilly.ca/home/

# Modified by Javad Rahimipour Anaraki on 28/11/17 updated 21/06/18
# Ph.D. Candidate
# Department of Computer Science
# Memorial University of Newfoundland
# jra066 [AT] mun [DOT] ca | www.cs.mun.ca/~jra066

# Modified by Hui (Henry) Luan
# Postdoc Fellow
# School of Human Kinetics and Recreation
# Memorial University of Newfoundland
# hluan [AT] mun [DOT] ca

#   input: accel_sec_geneav raw CSV data files
#  output: Clean and collapsed to second level CSV data files

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
    "Z:/HeroX/data/GENEActiv/"
  intrPath <-
    "Z:/HeroX/data/"
} else {
  path <-
    "/HeroX/data/GENEActiv/"
  intrPath <-
    "/HeroX/data/"
}
setwd(path)

wear_loc <- "wrist"

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
    dir(paste0(path, uid),
        pattern = "([0-9].csv)",
        full.names = TRUE)
  
  intervals <-
    read.csv(paste(intrPath, "intervals.csv", sep = ""), sep = ",")
  
  #=====================Data prepration======================
  nFiles <- length(filenames)
  
  if (nFiles > 0) {
    for (i in 1:nFiles) {
      if (file.size(filenames[i]) == 0 |
          file.exists(paste0(path, uid, "/", unlist(strsplit(
            basename(filenames[i]), "[.]"
          ))[1], "_labeled.csv"))) {
        print(paste0("GENEActiv file for ", uid, " exists"))
        next
      }
      
      accel_data <-
        fread(
          filenames[i],
          skip = 99,
          header = FALSE,
          col.names = c(
            "time",
            "x_axis",
            "y_axis",
            "z_axis",
            "lux",
            "button",
            "temp"
          ),
          select = 1:7,
          sep = ",",
          data.table = FALSE,
          nThread = 4
        )
      
      #Filtering data out based on uid and start and end date
      usrInfo <- intervals[intervals[, "userid"] == uid,]
      startDate <- as.character(usrInfo[, "start"])
      endDate <- as.character(usrInfo[, "end"])

      #Cut data based on start and end dates
      accel_data <-
        accel_data[(substring(format.Date(accel_data[, "time"]), 1, 10) >= format.Date(startDate)),]
      accel_data <-
        accel_data[(substring(format.Date(accel_data[, "time"]), 1, 10) <= format.Date(endDate)),]
      
      fileName <-
        paste0(path, uid, "/", unlist(strsplit(basename(filenames), "[.]"))[1], "_labeled.csv")
      
      if (nrow(accel_data) == 0) {
        print(paste0("No data from ", startDate, " to ", endDate))
        file.create(fileName)
        next
      }
      
      #Correct time
      accel_data$time1 <-
        str_sub(accel_data$time, 1, str_length(accel_data$time) - 4)  ## Remove the milliseconds
      accel_data$time2 <-
        ymd_hms(accel_data$time1)  ## Create a POSIXct formated variable
      accel_data$day <-
        day(accel_data$time2) ## Create a day variable
      accel_data$hour <-
        hour(accel_data$time2)  ## Create an hour variable
      accel_data$minute <-
        minute(accel_data$time2)  ## Create a minute variable
      accel_data$second <-
        second(accel_data$time2)  ## Create a second variable
      
      #Calculate vector magnitude
      accel_data$vec_mag = sqrt(accel_data$x_axis ^ 2 + accel_data$y_axis ^
                                  2 + accel_data$z_axis ^ 2)
      accel_data$vec_mag_g = accel_data$vec_mag - 1
      
      #Collapse data to one second
      accel_sec_genea <- accel_data %>%
        group_by(day, hour, minute, second) %>%
        summarise(
          time = first(time),
          m_x_axis = mean(x_axis),
          m_y_axis = mean(y_axis),
          m_z_axis = mean(z_axis),
          vec_mag = mean(vec_mag),
          vec_mag_g = mean(abs(vec_mag_g)),
          vec_mag_median = median(vec_mag),
          vec_mag_g_median = median(abs(vec_mag_g)),
          sd_x_axis = sd(x_axis),
          sd_y_axis = sd(y_axis),
          sd_z_axis = sd(z_axis)
        )
      
      accel_sec_genea$activity_mean <-
        car::recode(
          accel_sec_genea$vec_mag_g,
          "lo:0.190='1.Sedentary';
          0.190:0.314='2.Light';
          0.314:0.9989='3.Moderate';
          0.9989:hi='4.Vigorous';",
          as.factor = TRUE
        )
      
      accel_sec_genea$activity_median <-
        car::recode(
          accel_sec_genea$vec_mag_g_median,
          "lo:0.190='1.Sedentary';
          0.190:0.314='2.Light';
          0.314:0.9989='3.Moderate';
          0.9989:hi='4.Vigorous';",
          as.factor = TRUE
        )
      
      
      #===========================Sleep==========================
      x <- accel_sec_genea$m_x_axis
      y <- accel_sec_genea$m_y_axis
      z <- accel_sec_genea$m_z_axis
      accel_sec_genea$angle <-
        atan(z / sqrt(x ^ 2 + y ^ 2)) * 180 / pi
      
      start_id <- seq.int(1, nrow(accel_sec_genea) - 1, by = 5)
      end_id <-
        c((start_id - 1)[2:length(start_id)], nrow(accel_sec_genea))
      mean_angle5s <-
        sapply(1:length(start_id), function(i) {
          mean(accel_sec_genea$angle[start_id[i]:end_id[i]])
        })
      
      flag_s <- seq(1, length(mean_angle5s))
      flag_e <- flag_s + 1
      flag_e <- flag_e[-length(flag_e)]
      flag <-
        sapply(1:length(flag_e), function(i) {
          ifelse(abs(mean_angle5s[flag_e[i]] - mean_angle5s[flag_s[i]]) <= 5, 1, 0)
        })
      flag_final <- c(1, flag)
      
      zero_IDs <- which(flag_final == 0)
      zero_s <- zero_IDs[-length(zero_IDs)]
      zero_e <- zero_IDs[-1]
      
      for (i in 1:length(zero_s)) {
        if ((zero_e[i] - zero_s[i]) < 121 && (zero_e[i] - zero_s[i]) > 1)
          flag_final[(zero_s[i] + 1):(zero_e[i] - 1)] = 0
      }
      
      # Reverse to second-level
      flag_second <- rep(flag_final, each = 5)
      sleep_id <- which(flag_second == 1)
      
      activity_final <- as.character(accel_sec_genea$activity_mean)
      activity_final[sleep_id] <- "0.Sleep"
      accel_sec_genea$activity_final <-
        activity_final[1:nrow(accel_sec_genea)]

      table(accel_sec_genea$activity_final)

      #=========================Exporting========================
      #Save the results as a CSV file
      write.csv(accel_sec_genea, fileName,
                row.names = FALSE)
    }
  }
}
