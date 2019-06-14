library(tidyverse)
library(httr)
library(jsonlite)
library(stringr)

### the schema is a tree map of all the data in the database, with information about
### the codes to use for each part

### build_schema is used to create the schema if you don't already have one, or if,
### for whatever reason, you don't like the one you have and wish to kill it and
### make another
build_schema = function(filename = "schema/schema.rds") {
  ### this is the root of the schema
  full_schema = tibble(id = "str:folder:root",
                       label = "Root",
                       location = "https://stat-xplore.dwp.gov.uk/webapi/rest/v1/schema",
                       type = "FOLDER",
                       parent = "NA")
  saveRDS(full_schema, file = filename)
  
  ### initialises still_to_map to the root of the schema, and then keeps looping, looking
  ### for children of all entries in still_to_map, adding them and removing those just
  ### mapped. This continues until no more children are found that we want to map.
  still_to_map = full_schema
  while(still_to_map %>% NROW() > 0) {
    new_schema = still_to_map %>% get_children()
    ### Only folders, databases, measures and fields are mapped in this way, as otherwise
    ### it takes forever. Things like valuesets may be mapped later on and added to the
    ### schema if they're needed for a recode
    still_to_map = new_schema %>% 
      filter(type %in% c("FOLDER","DATABASE","MEASURE","FIELD"))
    ### the new entries are added to the full_schema, which is then saved. Saving isn't
    ### particularly useful. It means if the process crashes, you haven't lost anything,
    ### but equally the code is not currently set up to allow you to repair or restart
    ### an aborted build
    full_schema = full_schema %>% bind_rows(new_schema)
    saveRDS(full_schema, file = filename)
  }
}

### get_children takes a schema and queries the api to get the children of each
### entry
get_children = function(parent_schema) {
  parent_schema$location %>% map_df(get_schema, 
                                    check_cache = FALSE, 
                                    filename = "schema/schema.rds")
}

### get_schema takes a schema location url and returns the schema children for that
### entry
get_schema = function(url, check_cache = TRUE, filename = "schema/schema.rds") {
  ### check_cache is set to TRUE, which is the default, the function looks for
  ### the url as a parent in the existing schema prior to interrogating the api.
  ### If it's set to FALSE, the function will skip straight to the api
  if(check_cache) {
    full_schema = readRDS(filename)
    parent_id = full_schema$id[full_schema$location == url]
    if(parent_id %>% length() == 0)
      parent_id = "empty"
    
    schema = full_schema %>% filter(parent == parent_id)
  }
  else {
    schema = NULL
    full_schema = NULL
  }
  
  ### if we don't have any entries, we need to get some, so it's time to try
  ### the api
  if(schema %>% NROW() == 0) {
    lexical_err = "lexical error: invalid char in json text.\n                                       <!DOCTYPE html PUBLIC \"-//W3C//\n                     (right here) ------^\n"
    empty_schema = tibble(id = character(0),
                          label = character(0),
                          location = character(0),
                          type = character(0),
                          parent = character(0))
    
    tryCatch({
      ### this sends the GET request to the api. See scraping.R for notes on
      ### the use of httr for GET and POST requests
      response = GET(url,
                     add_headers(apiKey = Sys.getenv("stat_xplore_key")),
                     use_proxy(Sys.getenv("proxy_address"),
                               as.numeric(Sys.getenv("proxy_port")),
                               username = Sys.getenv("proxy_username"),
                               password = Sys.getenv("proxy_password")))
      ### as in scraping.R, this reports on the api usage and the state of
      ### the user's account limits
      meta = response %>% headers()
      str_c("Remaining calls: ", meta$`x-ratelimit-remaining`) %>% print()
      str_c("Reset at: ", meta$`x-ratelimit-reset` %>% 
              as.numeric %>% 
              `/`(1000) %>% 
              as.POSIXlt(origin="1970-01-01")) %>% 
        print()
      response_JSON = response %>% 
        content(type = "text") %>% 
        fromJSON()
      ### this stores the children of the schema entry, tagging on the parent's
      ### id to each of the new entries
      schema = response_JSON$children %>% 
        as_tibble() %>% 
        mutate(parent = response_JSON$id %>% as.character())
    },
    ### there's a few entries that throw these lexical errors, which I find it
    ### convenient to brush under the carpet, albeit with a warning. I'm not sure
    ### this is still happening, actually, but it doesn't cost anything to keep it
    ### in just in case
    error = function(error) {
      cat("some kind of error")
      if(error$message == lexical_err) {
        print(str_c("skipping ", url))
        empty_schema
      }
    })
  }
  
  ### if there was no schema in prior existence, we save the newly created one
  if(full_schema %>% is_null) schema %>% distinct %>% saveRDS(file = filename)
  ### otherwise, we tack the new bit to the old bit and save the whole lot
  else full_schema %>% bind_rows(schema) %>% distinct %>% saveRDS(file = filename)
  ### finally, return the children of the original url
  schema
}
