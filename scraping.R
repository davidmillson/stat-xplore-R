library(tidyverse)
library(lubridate)
library(rvest)
library(httr)
library(stringr)
library(lazyeval)
library(jsonlite)
source("scraping_templates.R")

### scrape_from_stat_xplore takes a list representing the query body for the api,
### and an integer representing the number of months to look backwards for data
### already stored in the cache. If lookback is set to 0 (or less than 0), the
### data will always be scraped from the api, if possible

scrape_from_stat_xplore = function(body, lookback) {
  ### check if there is an up-to-date cached version
  stat_xplore_cache_key = tryCatch(
    readRDS("cache/stat_xplore_cache_key.rds"),
    warning = function(warning)
      tibble(filename = integer(0),
             date = numeric(0) %>% as.Date(origin = origin),
             query = character(0)
             )
    )
  ### find the latest date in the cache for the given body
  cache_entry = stat_xplore_cache_key %>% filter(query == body)
  
  ### if there is no cache for the query body, set the date to be an arbitrary point
  ### far enough in the past that it won't be relevant
  if(cache_entry %>% NROW() == 0) {
    date_of_cache = ymd("1970-01-01")
    filename = tryCatch(
      max(stat_xplore_cache_key$filename) + 1L,
      warning = function(warning) 1L)
  }
  ### if there is cache data for that exact query body, get the date of origin of
  ### the data and the filename it is stored under
  else {
    date_of_cache = cache_entry$date
    filename = cache_entry$filename
  }
  
  ### if there is an up-to-date cached version of the data, load it up and return
  ### it
  if(date_of_cache %m+% months(lookback) > now()) {
    print("loading from cache")
    readRDS(str_c("cache/cache files/",
               stat_xplore_cache_key$filename[stat_xplore_cache_key$query == body],
               ".rds"))
    ### This is the end of the function, if you make it here at all
  }
  ### otherwise, post the API call with the query body
  else {
    print("posting API call")
    response = execute_stat_xplore_POST_call(body)
    
    status = response %>% status_code()
    
    ### an http status code of 200 means a success. Everything else is a failure.
    ### In the case of a successful API call, we process the response and return a
    ### table
    if(status == 200) {
      data = response %>% content(type = "text") %>% fromJSON()
      ### the best way to understand this is just to generate a body, grab the
      ### response with execute_stat_xplore_POST_call, and then explore it. It's
      ### basically converting the multi-dimensional array returned by the api
      ### into a 2d long/tidy dataframe
      dimnames = data$fields$items %>%
        map(~.$labels %>% unlist)
      values = data$cubes[[1]]$values
      dimnames(values) = dimnames
      df = as.data.frame.table(values, stringsAsFactors = FALSE) %>%
        as_tibble() %>% 
        set_names(c(data$fields$label,"value"))
      
      ### this gets the most recent date in the retrieved dataset. This is what
      ### gets compared against the lookback variable if anyone runs the same
      ### query again in the future
      latest_date = df %>% interpret_date() %>% max()
      
      ### if there wasn't an existing cache entry, then a new row is added to the
      ### cache key
      if(cache_entry %>% NROW() == 0) {
        stat_xplore_cache_key = stat_xplore_cache_key %>% 
          bind_rows(tibble(filename = filename, query = body, date = latest_date))
      }
      ### otherwise, the date column in the cache key is updated
      else
        stat_xplore_cache_key$date[stat_xplore_cache_key$query == body] = latest_date
      
      ### the cache and cache key are saved, and the data is returned
      saveRDS(df, file = str_c("cache/cache files/", filename, ".rds"))
      saveRDS(stat_xplore_cache_key, file = "cache/stat_xplore_cache_key.rds")
      df
      ### This is the end of the function, if you make it here at all
    }
    ### if the API call failed for any reason, we either return the cached version
    ### if available (with a warning), or return an error if there is no cached
    ### version
    else {
      if(ymd(date_of_cache) > ymd("1970-01-01")) {
        warning("API call failed with status ", status,
                ". Loading cached data from ", date_of_cache)
        readRDS(str_c("cache/cache files/",
                   stat_xplore_cache_key$filename[stat_xplore_cache_key$query == body],
                   ".rds"))
        df
        ### This is the end of the function, if you make it here at all
      }
      else
        stop("API call failed with status ", status, ", with no available cached data")
    }
  }
}

### This function performs the scrape itself. Ordinarily, it would probably make
### sense to use rvest for this, which would automatically handling some of the
### complications, but I can't get it to work with the proxy settings, so instead
### it uses httr directly. The function takes a Stat-Xplore query in JSON format
### and returns the response.
execute_stat_xplore_POST_call = function(body, allowed_fails = 5) {
  tryCatch({
    response = POST("https://stat-xplore.dwp.gov.uk/webapi/rest/v1/table",
                    ### rather than storing the information in the code, you
                    ### can put sensitive/user dependendent information in you
                    ### .renviron file. The code then loads it up from there
                    ### when it needs it. There may well be a better way of
                    ### doing this, but sadly I haven't found it. Here, we've
                    ### got the stat-xplore api key, plus proxy settings for
                    ### accessing the internet through our corporate system
                    ### including username and password
                    add_headers(apiKey = Sys.getenv("stat_xplore_key"),
                    content_type_json(),
                    body = body,
                    use_proxy(Sys.getenv("proxy_address")),
                              as.numeric(Sys.getenv("proxy_port")),
                              username = Sys.getenv("proxy_username"),
                              password = Sys.getenv("proxy_password")))
    ### prints a message to the console to say how many calls are remaining, and
    ### how long until the limit resets
    meta = response %>% headers()
    str_c("Remaining calls: ", meta$`x-ratelimit-remaining`) %>% print()
    str_c("Reset at: ", meta$`x-ratelimit-reset` %>% as.numeric %>% `/`(1000) %>% as.POSIXlt(origin="1970-01-01")) %>% print()
    response
  },
  error = function(error) {
    ### I've noticed the API occasionally times out, so I've set the function to
    ### keep repeating until it succeeds or runs out of tries. By default, it will
    ### try five times before stopping and giving an error
    if(error$message == "Timeout was reached") {
      if(allowed_fails > 0)
        execute_stat_xplore_POST_call(body, allowed_fails = allowed_fails-1)
      else stop("Timeout was reached too many times")
    }
    else stop(error$message) 
  })
}

build_stat_xplore_body = function(database, measures, dimensions, recodes) {
  ### The stat-xplore api has a particular language that is almost but not quite consistent.
  ### Rather than try to write something that can take account of all exceptions, I
  ### am currently shifting the burden onto the user. There is a schema that should act as
  ### a map/dictionary of the language, which will hopefully suffice to give all the required
  ### api ids. You can also get some inspiration for what a query should be by building it
  ### in the browser version of stat-xplore and selecting the api option
  
  query_list = list(
    ### database is the particular dataset to draw from. It has to be unboxed, as it must
    ### appear in the JSON as a bare string, without square brackets. It takes an api id 
    ### of type "database"
    database = unbox(database),
    
    ### measures is the value to be returned, e.g. caseload or weekly award amount. I
    ### assume it can return multiple values for each query, but haven't tested it. It takes
    ### an api id of type "count" or "measure"
    measures = measures,
    
    ### dimensions describes how to slice the data on different fields. It takes a 1d matrix
    ### of api ids of type "field"
    dimensions = dimensions %>% matrix())
    
    ### recodes allows subsetting and aggregation. One particular use we can put this to is to
    ### filter the results to only those from Scotland. It takes a list where the element names 
    ### are api ids of type "field" and the elements are lists whose single element is a 1d 
    ### matrix of ap ids of type "value". Any fields mapped in this way have to appear in the
    ### dimensions
    if(!is_empty(recodes))
      query_list = query_list %>% c(list(recodes = recodes))
  
  query_list %>% toJSON()
}

### There seemingly isn't a way to recode to all values in a valueset, so this function takes
### a given valueset and uses a call to the api schema to produce a list of the required
### values
auto_recodes = function(valueset_id) {
  base_url = "https://stat-xplore.dwp.gov.uk/webapi/rest/v1/schema/"
  url = base_url %>% str_c(valueset_id %>% str_replace_all(":", "%3A"))
  valueset = get_schema(url)
  list(map = valueset$id %>% as.list())
}

### intended to be a universal translator for the different date formats from Stat-Xplore,
### of which there are two that I'm currently aware. Takes either a dataframe with a Month
### or Quarter column, or a vector of dates
interpret_date = function(...) {
  input = list(...)[[1]]
  
  if(class(input)[[1]] == "tbl_df") {
    dates = tryCatch({
      input$Quarter
      },
      warning = function(warning) {
        if(warning$message == "Unknown column 'Quarter'")
          input$Month
        })
  }
  else dates = input
  
  tryCatch({
    dates %>% 
      str_extract("^\\d*") %>% 
      str_c("01") %>% 
      ymd() %m+% months(1) - 1
  },
  warning = function(warning) {
    if(warning$message == "All formats failed to parse. No formats found.") {
      "01-" %>% 
        str_c(dates) %>% 
        dmy() %m+% months(1) - 1
    }
  })
}
