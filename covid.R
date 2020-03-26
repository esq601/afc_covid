library(httr)
library(readr)
library(tidyverse)


load("covid.rda")
load("vantage_installations.rda")
load("token.rds")

### Manually adjust the date in the URL and run the entire script once a day.  Could do more programatically, but this works.
cases <- read_csv(content(GET("https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/03-25-2020.csv"))) %>%
  mutate_if(is.numeric,as.character)



#### The code segment below was used to pull the historical data.  The full_cases df is saved in the rda file.  This needs to be run initially.

# cases_22mar <- read_csv(content(GET(paste0("https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/03-22-2020.csv")))) %>%
#  mutate_all(as.character)
# cases_22mar$Last_Update <- as.POSIXct(cases_22mar$Last_Update, format = "%m/%d/%y %H:%M")
# 
# #colnames(cases_temp) <- colnames_cases
# 
# 
# full_cases <- cases
# 
# 
#   for(j in 23:23) {
# #    tryCatch({
#       print(paste0("https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/03-",j,"-2020.csv"))
#       
#       cases_temp <- mutate_all(cases_temp, as.character)
#       full_cases <- rbind(full_cases,cases_temp)
# #    }, error=function(e){})
# 
#   }
# 


#### This is for your Vantage token
#token <- "Bearer xxxxxxxxxxxxxxx"
#save(file = "token.rds",token)


full_cases <- bind_rows(full_cases,cases) %>%
  distinct()


installations <- data4 %>%
  ungroup() %>%
  select(instl_cd,instl_name) %>%
  distinct()


vantage_pull <- function(dataset) {
  
  cataloghost <- "https://vantage.army.mil/foundry-data-proxy/api/dataproxy/query"
  
  van_dataset <- dataset
  
  args <-  paste0('{"query":"SELECT * FROM `master`.`',van_dataset,
                       '` "}')
  
  qry <- POST(cataloghost,
                   add_headers(Authorization = token,"Content-Type"="application/json"),
                   body = args,verbose())
  
  content <- content(qry)
  
  schema <- data.frame(Reduce(rbind, content[[1]]$fieldSchemaList))
  
  df <- data.frame(Reduce(rbind, content[[2]])) %>%
    mutate_if(is.list, as.character)
  
  colnames(df) <- schema$name
  
  df
  
}





instaldata <- "ri.foundry.main.dataset.646dd517-d27c-4e6e-9d17-365512646b20"

inst_df <- vantage_pull(instaldata)

afc_inst_other <- read_csv("Army Futures Command Unit Location Listing Test.csv") %>%
  mutate(instl = case_when(
    Post == "Off Post" ~ City,
    is.na(Post) == T ~ City,
    TRUE ~ Post
  )) %>%
  select(instl_cd = NULL, instl_name.x = instl,instl_lat_crd = Latitude, instl_lng_crd = Longitude, on_inst_people_all_ranks = NULL) %>%
  distinct()


afc_inst <- installations %>%
  left_join(inst_df, by = "instl_cd") %>%
  select(instl_cd,instl_name.x,state,instl_lat_crd,instl_lng_crd,on_inst_people_all_ranks) %>%
  mutate_at(vars(instl_lat_crd,instl_lng_crd,on_inst_people_all_ranks),as.numeric) %>%
  bind_rows(afc_inst_other) %>%
  distinct() %>%
  mutate(instl_name.x = stringr::str_to_upper(instl_name.x)) %>%
  group_by(instl_name.x) %>%
  summarise(instl_lat_crd= mean(instl_lat_crd, na.rm = T),instl_lng_crd= mean(instl_lng_crd, na.rm = T),
            on_inst_people_all_ranks= mean(on_inst_people_all_ranks, na.rm = T)) %>%
  filter(is.na(instl_lat_crd) == F) %>%
  mutate(cases = 0,deaths=0,active=0,case_last = 0, deaths_last=0)

full_cases_mod <- full_cases %>%
  group_by(Combined_Key) %>%
  arrange(Last_Update) %>%
  mutate_at(vars(Confirmed, Deaths,Active, Lat, Long_),as.numeric) %>%
  mutate(case_last = lag(Confirmed),death_last = lag(Deaths))


full_cases_latest <- full_cases_mod %>%
  filter(Last_Update== max(Last_Update)) %>%
  mutate(afc_impact = NA) %>%
  filter(is.na(Lat) == F)


for(i in 1:nrow(afc_inst)) {
  print(i)
  
  for(j in 1:nrow(full_cases_latest)) {
    #print(j)
    if(0.621371 *sp::spDists(x = matrix(c(afc_inst[[i,3]],full_cases_latest[[j,7]], afc_inst[[i,2]],full_cases_latest[[j,6]]),nrow=2),longlat=T,segments=T) < 50) {
      afc_inst[i,5] <- afc_inst[i,5] + full_cases_latest[j,8]
      afc_inst[i,6] <- afc_inst[i,6] + full_cases_latest[j,9]
      afc_inst[i,7] <- afc_inst[i,7] + full_cases_latest[j,11]
      afc_inst[i,8] <- afc_inst[i,8] + full_cases_latest[j,13]
      afc_inst[i,9] <- afc_inst[i,9] + full_cases_latest[j,14]
      full_cases_latest[j,15] <- T
    }
  }
  
}



afc_inst <- afc_inst %>%
  mutate(scaled_cases = scales::rescale(log(cases,5),to=c(3,20))) %>%
  mutate(case_pct = (cases-case_last)/case_last, death_pct = (deaths - deaths_last)/deaths_last) %>%
  mutate(content = paste(sep = "<br/>",
                         paste0("<b>Intallation: </b>",instl_name.x),
                         paste0("<b>Inst. Employees: </b>",on_inst_people_all_ranks),
                         paste0("<b>Local Cases: </b>",cases),
                         paste0("<b>Local Deaths: </b>",deaths),
                         paste0("<b>Local Active: </b>",active))
  )


### personnel stuff ####
pers_mil <- "ri.foundry.main.dataset.4fd82ba1-9fb6-4cde-ab51-fe446f7ffd36"
pers_mil_df <- vantage_pull(pers_mil)

pers_loc <- pers_mil_df %>%
  mutate(geohash = paste0(round(as.numeric(latitude),0),round(as.numeric(longitude),0))) %>%
  group_by(geohash) %>%
  summarise(num = n())

pers_inst <- afc_inst %>%
  mutate(geohash = paste0(round(as.numeric(instl_lat_crd),0),round(as.numeric(instl_lng_crd),0))) %>%
  left_join(pers_loc, by= "geohash")


afc_table <- pers_inst %>%
  select(Installation = instl_name.x, `Military PAX` = num, `Local Cases` = cases, `Local Deaths` = deaths, `Case Change` = case_pct)

### time series ####

afc_locs_ts <- full_cases %>%
  left_join(afc_locs[,12:13], by="Combined_Key") %>%
  filter(afc_impact == T) %>%
  mutate(date = lubridate::floor_date(Last_Update, "day")) %>%
  group_by(date) %>%
  summarise(cases = sum(as.numeric(Confirmed), na.rm=T),deaths = sum(as.numeric(Deaths), na.rm=T)) 

full_cases_ts <- full_cases %>%
  mutate(date = lubridate::floor_date(Last_Update, "day")) %>%
  filter(`Country_Region` == "US") %>%
  group_by(date) %>%
  summarise(cases = sum(as.numeric(Confirmed), na.rm=T),deaths = sum(as.numeric(Deaths), na.rm=T)) %>%
  filter(date >= min(afc_locs_ts$date))


#### This plot is generated in the app.r now

# plot1 <- ggplot(afc_locs_ts)
# plot1 + geom_ribbon(aes(x=date, ymax = cases,ymin=deaths,fill="Cases"), alpha=.5) +
#   geom_ribbon(aes(x=date, ymax = deaths,ymin=0,fill="Deaths"), alpha=.5) +
#   geom_path(data = full_cases_ts, aes(x=date, y =cases), color = "darkblue") +
#   geom_path(data = full_cases_ts, aes(x=date, y =deaths), color = "darkred") +
#   scale_y_log10(labels=scales::comma_format()) +
#   scale_fill_manual(name="Designation",
#                       values=c(Cases="darkblue", Deaths="darkred")) +
#   #scale_y_continuous(labels=scales::comma_format()) +
#   theme_minimal()

save(file = "covid.rda", afc_inst, afc_table, afc_locs, full_cases, afc_locs_ts,full_cases_ts)
# app.r loads covid.rda with the necessary dfs.  