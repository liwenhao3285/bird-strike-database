# Practicum II Part 2: Create Star/Snowflake Schema
# Wenhao Li
# Fall 2023

# Load require library
library(RMySQL)
library(dplyr)
library(RSQLite)

# This function get all information required for sales_facts table from database
# This function takes a database connection as input
# This function returns a dataframe as output
getSaleFact <- function(dbcon){
  sql <- "SELECT territory, strftime('%Y', s.saleDate) AS year, strftime('%m', s.saleDate) 
                  AS month, SUM(TOTAL) AS totalSales, SUM(qty) AS units
          FROM sales s INNER JOIN reps r ON s.rid = r.rid
          GROUP BY territory, Year, Month"
  
  df <- dbGetQuery(dbcon, sql)
  
  # add column "quarter" base on existing column "month"
  df$quarter <- mapply(determineQuarter, df$month)
  
  # group all data required into one dataframe
  df <- df %>% 
    group_by(territory, year, quarter) %>% 
    summarise(totalSales = sum(totalSales), units = sum(units))
  
  return(df)
}

# This function get all information required for reps_facts table from database
# This function takes a database connection as input
# This function returns a dataframe as output
getRepsFact <- function(dbcon){
  sql <- "SELECT r.firstName || ' ' || r.surName AS name, strftime('%Y', s.saleDate) AS year, strftime('%m', s.saleDate) 
                  AS month, p.name AS product, SUM(TOTAL) AS totalSales
          FROM sales s 
            INNER JOIN reps r ON s.rid = r.rid
            INNER JOIN products p ON s.pid = p.pid
          GROUP BY s.rid, Year, Month, product"
  
  df <- dbGetQuery(dbcon, sql)
  
  # add column "quarter" base on existing column "month"
  df$quarter <- mapply(determineQuarter, df$month)
  
  # group all data required into one dataframe
  df <- df %>% 
    group_by(name, product, year, quarter) %>% 
    summarise(totalSales = sum(totalSales))
  
  return(df)
}

# This function create all fact tables in mysql database
# This function takes a database connection as input
# This function returns no output
createTables <- function(dbcon){
  sql <- "CREATE TABLE IF NOT EXISTS sales_facts(
            territory TEXT,
            year TEXT,
            quarter TEXT,
            totalSales NUMERIC,
            units INTEGER
          )"
  
  dbExecute(dbcon, sql)
  
  sql <- "CREATE TABLE IF NOT EXISTS rep_facts(
            name TEXT,
            product TEXT,
            year TEXT,
            quarter TEXT,
            totalSales NUMERIC
          )"
  
  dbExecute(dbcon, sql)
}

# This function determine quarter value base on month value
# This function takes a string as input
# This function returns the correct quarter value (string) as output
determineQuarter <- function(month){
  if (month == "01" | month == "02" | month == "03"){
    return("first")
  }
  else if (month == "04" | month == "05" | month == "06") {
    return("second")
  }
  else if (month == "07" | month == "08" | month == "09") {
    return("third")
  }
  else{
    return("forth")
  }
}

main <- function(){
  fpath <- ""
  dbfile <- "pharmaSales.db"
  
  dbcon <- dbConnect(RSQLite::SQLite(), paste0(fpath,dbfile))
  
  df <- getSaleFact(dbcon)
  
  db_name_fh <- "sql3654492"
  db_user_fh <- "sql3654492"
  db_host_fh <- "sql3.freemysqlhosting.net"
  db_pwd_fh <- "LfRIZIav7w"
  db_port_fh <- 3306
  
  mydb.fh <-  dbConnect(RMySQL::MySQL(), user = db_user_fh, password = db_pwd_fh,
                        dbname = db_name_fh, host = db_host_fh, port = db_port_fh)
  
  dbcon2 <- mydb.fh
  
  createTables(dbcon2)
  
  # Load sales_facts table
  dbWriteTable(dbcon2, "sales_facts", df, append = T, row.names = F)
  
  df <- getRepsFact(dbcon)
  
  # Load rep_facts table
  dbWriteTable(dbcon2, "rep_facts", df, append = T, row.names = F)
  
  dbDisconnect(dbcon)
  dbDisconnect(dbcon2)
}

main()