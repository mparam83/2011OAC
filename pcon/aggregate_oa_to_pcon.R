# Title: Aggregating the OAC 60 variable input data from OA level to PCON level
# Author: Meenakshi Parameshwaran
# Date: 10/02/16

# 1. Set up the work
rm(list = ls()) # clear the work space
setwd("~/GitHub/2011OAC/pcon")
library(pacman) # load pacman package manager
p_load(RODBC, dplyr, tidyr, ggplot2, stringr)

# 2. load in the OA - PCON lookup table from SQL
myconn <- odbcConnect(dsn = "Datalab", uid = "", pwd = "")
mytables <- sqlTables(channel = myconn)
oapconlookup <- sqlFetch(channel = myconn, sqtable = "SC.OAPCONlookup")
odbcClose(channel = myconn) # close the database connection
rm(mytables) # tidy up by removing the list of tables

# 3. read in the 60 variable census input data at OA level
oac_raw_kvars <- read.csv("T:/7 GIS files/2011 Output Area Classification/2011 OAC 60 Variables/2011_OAC_Raw_kVariables.csv")

# 4. join the relevant pcon code to the oac_raw_kvars dataframe
oac_raw_kvars_pcon <- left_join(x = oac_raw_kvars, y = oapconlookup, by = c("OA" = "OA11CD"))

# 5. count up the number of OAs in each PCON
pcon_oac_counts <- oac_raw_kvars_pcon %>% 
        group_by(PCON11CD, PCON11NM) %>% 
        summarise(num_oa = n())

# 6. aggregate all the census input data columns up to PCON level
pcon_agg <- oac_raw_kvars_pcon %>% 
        group_by(PCON11CD, PCON11NM) %>% 
        summarise_each(funs(sum), -OA)

# 7. Join the counts into the aggregated table
pcon_agg_counts <- left_join(x = pcon_agg, y = pcon_oac_counts, by = c("PCON11CD", "PCON11NM"))

# Don't add in the number of OAs column to the final table as then the original OAC won't run because of the extra column. Also drop the name of the constituency

# 8. Set up a df without the number of OAs column and the pcon name column
pcon_final <- select(.data = pcon_agg_counts, -PCON11NM, -num_oa)

# drop the final row which is all the OAs (489808) that weren't joined up to a PCON - I think these are outside of England and Wales    
pcon_final <- pcon_final[-574,]

# 8. write out the new table to a csv file
write.csv(x = pcon_final, file = "pcon_raw_60_kvariables.csv", row.names = FALSE)

# repeat to the server
write.csv(x = pcon_final, file = "T:/7 GIS files/2011 Output Area Classification/2011 OAC R Code/PCON Classification R Code/pcon_raw_60_kvariables.csv", row.names = FALSE)

## END ##
