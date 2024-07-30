# Practicum II Part 1: Load XML Data
# Wenhao Li
# Fall 2023

# All xml sales files
fileNameReps = "txn-xml/pharmaReps-F23.xml"
allfiles <- list.files(path = "txn-xml", pattern = "pharmaSalesTxn*")

# Load required library
library(RSQLite)
library(XML)
library(dplyr)

# This function create all the table required
# This function takes a connection to sqlite database
# This function return no output
createTables <- function(dbcon){
  sql <- "CREATE TABLE IF NOT EXISTS products(
            pid INTEGER PRIMARY KEY,
            name TEXT
          )"
  
  dbExecute(dbcon, sql)
  
  sql <- "CREATE TABLE IF NOT EXISTS reps(
            rid TEXT PRIMARY KEY,
            firstName TEXT,
            surName TEXT,
            territory TEXT,
            commission NUMERIC
          )"
  
  dbExecute(dbcon, sql)
  
  sql <- "CREATE TABLE IF NOT EXISTS customers(
            cid INTEGER PRIMARY KEY,
            name TEXT,
            country TEXT
          )"
  
  dbExecute(dbcon, sql)
  
  sql <- "CREATE TABLE IF NOT EXISTS sales(
            sid INTEGER PRIMARY KEY,
            saleDate DATE,
            qty INTEGER,
            total NUMERIC,
            currency TEXT,
            cid INTEGER,
            rid TEXT,
            pid INTEGER,
            
            CONSTRAINT sales_fk_c FOREIGN KEY(cid) REFERENCES customers(cid),
            CONSTRAINT sales_fk_r FOREIGN KEY(rid) REFERENCES reps(rid),
            CONSTRAINT sales_fk_p FOREIGN KEY(pid) REFERENCES products(pid)
          )"
  
  dbExecute(dbcon, sql)
}

# This function open xml for reps and load them into database
# This function takes xml file path and database connection as input
# This function return no output
loadXMLReps <- function(xmlFile, dbcon){
  xmlDoc <- xmlParse(xmlFile)
  
  # Pre-define data frame
  df <- data.frame(rid = character(),
                   firstName = character(),
                   surName = character(),
                   territory = character(),
                   commission = numeric()
                  )
  
  root <- xmlRoot(xmlDoc)
  n <- xmlSize(root)
  
  # Populate df with xml file
  for(i in 1:n){
    df[i, "rid"] = as.character(xmlAttrs(root[[i]]))
    df[i, "firstName"] = as.character(xmlValue(root[[i]][[1]][[1]]))
    df[i, "surName"] = as.character(xmlValue(root[[i]][[1]][[2]]))
    df[i, "territory"] = as.character(xmlValue(root[[i]][[2]]))
    df[i, "commission"] = as.numeric(xmlValue(root[[i]][[3]]))
  }
  
  # Load df to Table reps in dataframe
  dbWriteTable(dbcon, "reps", df, append = T, row.names = F)
}

# This function pre-define dataframe used for sales
# This function takes no input
# This function return an empty dataframe
buildDf <- function(){
  df <- data.frame(txnid = integer(),
                   rid = character(),
                   customer = character(),
                   country = character(),
                   saleDate = character(),
                   product = character(),
                   qty = integer(),
                   total = numeric(),
                   currency = character()
  )
  return(df)
}

# This function load xml for sales into one dataframe
# This function takes xml file path, database connection and a dataframe
# This function return a loaded df as output
loadXMLSales <- function(xmlFile, dbcon, df){
  xmlDoc <- xmlParse(xmlFile)
  
  root <- xmlRoot(xmlDoc)
  n <- xmlSize(root)
  length <- nrow(df)
  
  # Load xml data into dataframe
  # all xml data for sales in different files will result in one dataframe
  for(i in 1:n){
    df[i+length, "txnid"] = (as.integer(xmlAttrs(root[[i]])[[1]]))
    df[i+length, "rid"] = as.character(xmlAttrs(root[[i]])[[2]])
    df[i+length, "customer"] = as.character(xmlValue(root[[i]][[1]]))
    df[i+length, "country"] = as.character(xmlValue(root[[i]][[2]]))
    df[i+length, "saleDate"] = as.character(xmlValue(root[[i]][[3]][[1]]))
    df[i+length, "product"] = as.character(xmlValue(root[[i]][[3]][[2]]))
    df[i+length, "qty"] = as.integer(xmlValue(root[[i]][[3]][[3]]))
    df[i+length, "total"] = as.numeric(xmlValue(root[[i]][[3]][[4]]))
    df[i+length, "currency"] = as.character(xmlAttrs(root[[i]][[3]][[4]]))
  }
  
  return(df)
}

# This function bulk load all sales data into database
# This function takes dataframe and database connection as input
# This function returns no output
loadToTables <- function(df, dbcon){
  # Group all product information and add row number as primary key
  product_df <- df[] %>% group_by(product) %>%
    summarise(.groups = 'drop') %>%
    mutate(pid = row_number())
  
  # Load to Table products
  colnames(product_df)[1] <- "name"
  dbWriteTable(dbcon, "products", product_df, append = T, row.names = F)
  
  # Group all customer information and add row number as primary key
  customer_df <- df[] %>% group_by(customer, country) %>%
    summarise(.groups = 'drop') %>%
    mutate(cid = row_number())
  
  # Load to Table customer
  colnames(customer_df)[1] <- "name"
  dbWriteTable(dbcon, "customers", customer_df, append = T, row.names = F)
  
  # Group all customer information and add row number as primary key
  sales_df <- df[] %>% group_by(txnid, rid, saleDate, qty, total, currency, product, customer) %>%
    summarise(.groups = 'drop') %>%
    mutate(sid = row_number())
  
  # change rid to correct form
  sales_df$rid <- paste("r", sales_df$rid, sep = "")
  
  # find out foreign keys with inner join function from dplyr
  sales_df <- sales_df %>% inner_join(product_df, 
                                    by = c('product' = 'name'))
  sales_df <- sales_df %>% inner_join(customer_df, 
                                    by = c('customer' = 'name'))
  sales_df <- subset(sales_df, select = -c(product, customer, country, txnid))
  
  # modify dates to the correct format "YYYY-MM-DD"
  sales_df$saleDate <- as.Date(sales_df$saleDate, format = "%m/%d/%Y")
  sales_df$saleDate <- as.character(sales_df$saleDate)
  
  # Load all data to Table sales
  dbWriteTable(dbcon, "sales", sales_df, append = T, row.names = F)
}

main <- function(){
  fpath <- ""
  dbfile <- "pharmaSales.db"
  
  dbcon <- dbConnect(RSQLite::SQLite(), paste0(fpath,dbfile))
  
  createTables(dbcon)
  
  loadXMLReps(fileNameReps, dbcon)
  
  df <- buildDf()
  
  # Load all xml file about sales into one dataframe
  for (i in 1:length(allfiles)){
    df <- loadXMLSales(paste("txn-xml/", allfiles[[i]], sep = ""), dbcon, df)
  }
  
  loadToTables(df, dbcon)
  
  dbDisconnect(dbcon)
}

main()
