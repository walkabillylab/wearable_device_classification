# Created by Javad Rahimipour Anaraki on 13/09/18
# Ph.D. Candidate
# Department of Computer Science
# Memorial University of Newfoundland
# jra066 [AT] mun [DOT] ca | www.cs.mun.ca/~jra066

#   input: Collected data for each participant
#  output: A single files containing all collected data

rm(list = ls())
#========================Libraries=========================
list.of.packages <-
  c("data.table",
    "bit64")
new.packages <-
  list.of.packages[!(list.of.packages %in% installed.packages()[, "Package"])]
if (length(new.packages))
  install.packages(new.packages)

library(data.table)
library(bit64)

#=========================Variables========================
OS <- Sys.info()
if (OS["sysname"] == "Windows") {
  path <-
    "Z:/HeroX/data/Merged/"
} else {
  path <-
    "/HeroX/data/Merged/"
}

setwd(path)

finalData <- NULL

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
  
  filename <-
    list.files(paste(path, uid, sep = ""),
               full.names = TRUE,
               pattern = "*_finalData.csv")
  
  if (length(filename) > 0) {
    data <- fread(filename, header = TRUE, data.table = FALSE)
    
    if (k > 1) {
      if (ncol(data) != ncol(finalData)) {
        len <- length(strsplit(filename, "\\/")[[1]])
        print(
          paste0(
            "Number of columns in ",
            strsplit(filename, "\\/")[[1]][len],
            " for participant ",
            uid,
            " do not match!"
          )
        )
        next
      }
    }
    
    finalData <- rbind(finalData, data)
  }
}

classNAs <- which(is.na(finalData$GENEActiv_activity_final))
finaDataNoNAClass <- finalData[-classNAs, ]
finaDataNoNA <- na.omit(finalData)

#=================Export the final result==================
fileName <- paste0(path, "/", "mergedData.csv")
write.csv(finalData, fileName, row.names = FALSE)

fileName <- paste0(path, "/", "mergedDataNoNA.csv")
write.csv(finaDataNoNA, fileName, row.names = FALSE)

fileName <- paste0(path, "/", "mergedDataNoNAClass.csv")
write.csv(finaDataNoNAClass, fileName, row.names = FALSE)
