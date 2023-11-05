

library(tidyverse)
library(tidycensus)
library(patchwork)
library(sf)
library(DBI)
library(curl)
library(scales)
library(viridis)
library(janitor)
library(conflicted)
library(nngeo )
library(writexl)
library(ptaxsim)
conflict_prefer("select", "dplyr")
conflict_prefer("filter", "dplyr")
options(tigris_use_cache = TRUE)
readRenviron("~/.Renviron")
options(scipen = 99999)

#census_dict <- load_variables(year = 2020, dataset = "acs5", cache = TRUE)

# Set working directory ---------------------------------------------------

setwd(dirname(rstudioapi::getActiveDocumentContext()$path))
getwd()

# ERSB map ----------------------------------------------------------------

#May map: https://www.google.com/maps/d/viewer?mid=1Fn8x0LQOHPQP962ycjJTMBNNYGO98MA&ll=41.83399880095682%2C-87.731885&z=11
#October map: https://www.google.com/maps/d/viewer?mid=1Db4BN9WccvYBclkzZrCcI3yMaUP62UA&ll=41.83399880095682%2C-87.731885&z=11
ersb_leg_0417 <- st_read('ERSB-4.17.23.kml')
ersb_leg_1031 <- st_read('ERSB-10.31.23.kml')

ersb_leg_1031 <- ersb_leg_1031 %>%
  select_all(~gsub("\\s+|\\.|\\/|-|,|\\+", "_", .)) %>%
  rename_all(list(tolower)) %>%
  mutate(pop_bal = gsub('TARGET_DEV ','',description),
         pop_bal = gsub('<br> TOTAL ',';',pop_bal)) %>% 
  separate(pop_bal, c("target_dev", "total_pop"), ";") %>%
  mutate_at(vars(c("target_dev", "total_pop")), list(as.numeric)) %>%
  select(name, target_dev, total_pop) %>%
  st_zm(., drop = TRUE) %>%
  st_collection_extract(., "POLYGON") %>% 
  rename(stateleg_district_1031 = name) %>%
  select(stateleg_district_1031, geometry) %>%
  st_make_valid()

ersb_leg_0417 <- ersb_leg_0417 %>%
  select_all(~gsub("\\s+|\\.|\\/|-|,|\\+", "_", .)) %>%
  rename_all(list(tolower)) %>%
  mutate(pop_bal = gsub('TARGET_DEV ','',description),
         pop_bal = gsub('<br> TOTAL ',';',pop_bal)) %>% 
  separate(pop_bal, c("target_dev", "total_pop"), ";") %>%
  mutate_at(vars(c("target_dev", "total_pop")), list(as.numeric)) %>%
  select(name, target_dev, total_pop) %>%
  st_zm(., drop = TRUE) %>%
  st_collection_extract(., "POLYGON") %>%
  rename(stateleg_district_0417 = name) %>%
  select(stateleg_district_0417, geometry) %>%
  st_make_valid()


# Local boundaries --------------------------------------------------------

community_areas <- st_read('https://data.cityofchicago.org/api/geospatial/cauq-8yn6?method=export&format=GeoJSON') %>%
  select(community) %>%
  st_transform(4326) %>%
  st_make_valid()

chi_outline <- st_union(community_areas)%>% st_as_sf() %>%
  mutate(chicago_block = 'Chicago') %>%
  st_make_valid()

blocks_geometry <- get_decennial(geography = "block",
                                 variables = c('P2_001N'), 
                                 year = 2020, state = '17', county = '031', geometry = TRUE, cache = TRUE) 

bofficial <- read_delim('https://www2.census.gov/geo/maps/DC2020/DC20BLK/st17_il/place/p1714000_chicago/DC20BLK_P1714000_BLK2MS.txt', delim=';') %>%
  mutate_at(vars('FULLCODE'), list(as.character)) %>%
  select(FULLCODE) %>%
  mutate(in_chi = 1)

blocks_geometry <- blocks_geometry  %>% 
  st_transform(4326) %>% 
  st_make_valid() %>% 
  inner_join(., bofficial, by = c('GEOID'='FULLCODE')) %>% 
  rename_all(list(tolower)) %>%
  select(geoid) %>%
  rename(block_fips = geoid)

bgroup_geometry <- get_acs(year = 2021, geography = "block group", 
                           survey = 'acs5', variables = c('B01003_001'),
                           state = '17', county = '031', geometry = TRUE) %>%
  st_transform(4326) %>%
  rename_all(list(tolower)) %>%
  select(geoid, geometry)

# Property tax data -------------------------------------------------------

# Set DB connection
# Download here: https://github.com/ccao-data/ptaxsim#ptaxsim
ptaxsim_db_conn <- DBI::dbConnect(RSQLite::SQLite(), "/ptaxsim-2021.0.4.db")

# PIN geometries
ptax_data_geo <- dbGetQuery(ptaxsim_db_conn, "SELECT pin10, longitude, latitude FROM pin_geometry_raw")

ptax_data_geo <- ptax_data_geo %>%
  st_as_sf(coords = c("longitude", "latitude"), crs = 4326, agr = "constant") %>%
  group_by(pin10) %>%
  mutate(dup = row_number()) %>%
  ungroup() %>%
  filter(dup == 1) %>%
  st_join(., chi_outline, join = st_intersects) %>%
  filter(chicago_block == "Chicago") %>%
  st_join(., ersb_leg_0417, join = st_intersects) %>%
  st_join(., ersb_leg_1031, join = st_intersects) %>%
  st_join(., blocks_geometry, join = st_intersects) %>%
  st_drop_geometry() 

ptax_data_geo <- ptax_data_geo %>%
  group_by(pin10) %>%
  mutate(dup = row_number()) %>%
  ungroup() %>%
  filter(dup == 1)

# Property tax bills 
# ptax_data <- dbGetQuery(ptaxsim_db_conn, "SELECT year, pin, tax_bill_total, av_mailed, av_certified, av_board, av_clerk FROM pin where year >= 2019") %>% as_tibble()

# PIN universe for Chicago -----------------------------------------------------------------

ptax_data_2021 <- dbGetQuery(ptaxsim_db_conn, "SELECT * FROM pin where year = 2021") %>% as_tibble()

class_data <- ccao::class_dict

pin_data <- ptax_data_2021 %>%
  mutate(pin10 = str_sub(pin, start = 1L, end = 10L),
         township_code = str_sub(tax_code_num, start = 1L, end = 2L)) %>%
  left_join(., ptax_data_geo , by = c("pin10" = "pin10")) %>% 
  left_join(., class_data, by = c('class'='class_code')) %>%
  collect() 

pin_data <- pin_data %>%
  filter(chicago_block == 'Chicago')
rm(ptax_data_2021, ptax_data_geo)

# PTaxSim tax bills -----------------------------------------------------------------

base_year = 2021
pin_universe <- unique(pin_data$pin)

# Grab unaltered components to feed to tax_bill()
base_pins_dt <- lookup_pin(base_year, pin_universe)
base_tax_codes <- lookup_tax_code(base_year, pin_universe)
base_levies <- lookup_agency(base_year, base_tax_codes)
base_tifs <- lookup_tif(base_year, base_tax_codes)

# Get unaltered, unprojected tax bills
base_bills <- tax_bill(
  year_vec = base_year,
  pin_vec = pin_universe,
  tax_code_vec = base_tax_codes,
  pin_dt = base_pins_dt,
  agency_dt = base_levies,
  tif_dt = base_tifs
)

base_bills <- base_bills %>%
  filter(agency_name == 'BOARD OF EDUCATION')

base_bills <- base_bills %>%
  inner_join(., pin_data,
            by = c('pin'='pin'))  

base_bills_sum <- base_bills %>%
  filter(major_class_type == 'Residential') %>%
  group_by(chicago_block, stateleg_district_0417, stateleg_district_1031, block_fips, reporting_group, major_class_type) %>%
  summarize_at(vars(c("av", "eav", "final_tax", "tax_bill_total", "av_mailed", "av_certified", "av_board", "av_clerk")), list(sum), na.rm = TRUE) %>%
  ungroup()

base_bills_sum <- base_bills_sum %>%
  mutate(block_group_fips = str_sub(block_fips, 1, 12))

# Census P2 data -------------------------------------------------------------

# P2 Block Population
b_p2_2020 <- get_decennial(geography = "block",
                           variables = c('P2_002N', 'P2_005N', 'P2_006N', 'P2_007N', 'P2_008N', 'P2_009N', 'P2_010N', 'P2_011N'),
                           summary_var = c('P2_001N'), year = 2020, state = '17', county = '031', geometry = FALSE, cache = TRUE) 

b_p2_2020 <- b_p2_2020 %>%
  rename_all(list(tolower)) %>%
  rename(block_fips = geoid,
         pop_2020 = value,
         census_pop_2020 = summary_value) %>%
  mutate(race_ethnicity = case_when(variable == 'P2_002N' ~ 'Latino',
                                    variable == 'P2_005N' ~ 'White',
                                    variable == 'P2_006N' ~ 'Black',
                                    variable == 'P2_007N' ~ 'Other', #'Native'
                                    variable == 'P2_008N' ~ 'Asian',
                                    variable == 'P2_009N' ~ 'Other', #'Pacific Islander'
                                    variable == 'P2_010N' ~ 'Other', #'Other race'
                                    variable == 'P2_011N' ~ 'Other', #'Two or more races'
                                    TRUE ~ as.character(variable)))

b_p2_2020 <- b_p2_2020 %>%
  group_by(block_fips, race_ethnicity, census_pop_2020) %>%
  summarize_at(vars(pop_2020), list(sum)) %>%
  ungroup() %>%
  group_by(block_fips) %>%
  mutate(pop_2020_sum = sum(pop_2020)) %>%
  ungroup() %>%
  mutate(pop_2020_share = pop_2020 / pop_2020_sum) %>%
  pivot_wider(id_cols = c(block_fips, census_pop_2020),
              names_from = c(race_ethnicity), 
              values_from = c(pop_2020, pop_2020_share)) %>%
  select_all(~gsub("\\s+|\\.|\\/", "_", .)) %>%
  rename_all(list(tolower))

b_p2_2020 <- b_p2_2020 %>%
  filter(block_fips %in% unique(blocks_geometry$block_fips))


# P2 Block Voting age Population
b_p4_2020 <- get_decennial(geography = "block",
                           variables = c('P4_002N', 'P4_005N', 'P4_006N', 'P4_007N', 'P4_008N', 'P4_009N', 'P4_010N', 'P4_011N'),
                           summary_var = c('P4_001N'), year = 2020, state = '17', county = '031', geometry = FALSE, cache = TRUE) 

b_p4_2020 <- b_p4_2020 %>%
  rename_all(list(tolower)) %>%
  rename(block_fips = geoid,
         pop_2020 = value,
         census_pop_2020 = summary_value) %>%
  mutate(race_ethnicity = case_when(variable == 'P4_002N' ~ 'voting_age_latino',
                                    variable == 'P4_005N' ~ 'voting_age_white',
                                    variable == 'P4_006N' ~ 'voting_age_black',
                                    variable == 'P4_007N' ~ 'voting_age_other', #'Native'
                                    variable == 'P4_008N' ~ 'voting_age_asian',
                                    variable == 'P4_009N' ~ 'voting_age_other', #'Pacific Islander'
                                    variable == 'P4_010N' ~ 'voting_age_other', #'Other race'
                                    variable == 'P4_011N' ~ 'voting_age_other', #'Two or more races'
                                    TRUE ~ as.character(variable)))

b_p4_2020 <- b_p4_2020 %>%
  group_by(block_fips, race_ethnicity, census_pop_2020) %>%
  summarize_at(vars(pop_2020), list(sum)) %>%
  ungroup() %>%
  group_by(block_fips) %>%
  mutate(pop_2020_sum = sum(pop_2020)) %>%
  ungroup() %>%
  mutate(pop_2020_share = pop_2020 / pop_2020_sum) %>%
  pivot_wider(id_cols = c(block_fips, census_pop_2020),
              names_from = c(race_ethnicity), 
              values_from = c(pop_2020, pop_2020_share)) %>%
  select_all(~gsub("\\s+|\\.|\\/", "_", .)) %>%
  rename_all(list(tolower)) %>%
  rename(census_voting_age_pop_2020 = census_pop_2020)

b_p4_2020 <- b_p4_2020 %>%
  filter(block_fips %in% unique(blocks_geometry$block_fips))

blocks_data <- blocks_geometry %>%
  left_join(., b_p2_2020, by = c('block_fips' = 'block_fips')) %>%
  left_join(., b_p4_2020, by, by = c('block_fips' = 'block_fips')) %>%
  mutate_all(~replace_na(.x, 0)) 

gc()

# Census student population -----------------------------------------------

bgroup_kids <- get_acs(year = 2021, geography = "block group", 
                               survey = 'acs5', variables = c( 'B16004_002',
                                                               'B14002_029', 'B14002_005', 
                                                               'B14002_032', 'B14002_008', 
                                                               'B14002_035', 'B14002_011', 
                                                               'B14002_038', 'B14002_014', 
                                                               'B14002_041', 'B14002_017'),
                               cache_table = TRUE,
                               state = '17', county = '031', geometry = FALSE)  %>% 
  rename_all(list(tolower)) %>%
  mutate(variable_label = case_when(variable == 'B16004_002' ~ 'Children ages 5 to 17',
                                    variable %in% c('B14002_029', 'B14002_005') ~ 'Enrolled in nursery school, preschool',
                                    variable %in% c('B14002_032', 'B14002_008') ~ 'Enrolled in kindergarten',
                                    variable %in% c('B14002_035', 'B14002_011') ~ 'Enrolled in grade 1 to grade 4',
                                    variable %in% c('B14002_038', 'B14002_014') ~ 'Enrolled in grade 5 to grade 8',
                                    variable %in% c('B14002_041', 'B14002_017') ~ 'Enrolled in grade 9 to grade 12')) %>%
  group_by(geoid, variable_label) %>% 
  summarize_at(vars(estimate), list(sum)) %>%
  ungroup() %>%
  pivot_wider(id_cols = c(geoid),
              names_from = c(variable_label), 
              values_from = c(estimate)) %>%
  select_all(~gsub(",|\\s+|\\.|\\/", "_", .)) %>%
  select_all(~gsub("__", "_", .)) %>%
  rename_all(list(tolower)) %>%
  mutate(enrolled_in_prek_to_grade_12 = enrolled_in_nursery_school_preschool + enrolled_in_kindergarten + enrolled_in_grade_1_to_grade_4 + enrolled_in_grade_5_to_grade_8 + enrolled_in_grade_9_to_grade_12) %>%
  select(geoid, children_ages_5_to_17, enrolled_in_prek_to_grade_12, enrolled_in_nursery_school_preschool, enrolled_in_kindergarten, enrolled_in_grade_1_to_grade_4, enrolled_in_grade_5_to_grade_8, enrolled_in_grade_9_to_grade_12)

bgroup_data <- bgroup_geometry %>%
  #left_join(., bgroup_race, by = c('geoid'='geoid')) %>%
  left_join(., bgroup_kids, by = c('geoid'='geoid')) %>%
  mutate_at(vars(c("total_population", 
                   "share_asian", "share_black", "share_latino", "share_other", "share_white", 
                   "estimate_asian", "estimate_black", "estimate_latino", "estimate_other", "estimate_white", 
                   "total_kids")),
            replace_na, 0) 
  # mutate_at(vars(c("rent_share_of_income", "household_income")),
  #           ~replace_na(., mean(., na.rm=TRUE)))


# -------------------------------------------------------------------------

# Join block groups to leg districts
blocks_geometry <- blocks_geometry %>% st_make_valid()  %>%
  st_join(., ersb_leg_1031, largest = TRUE) %>%
  st_join(x = ., y = ersb_leg_1031, join = st_nn)

blocks_geometry <- blocks_geometry %>%
  mutate(stateleg_district_1031 = coalesce(stateleg_district_1031.x, stateleg_district_1031.y)) %>%
  select(-one_of(c('stateleg_district_1031.x', 'stateleg_district_1031.y')))

# Join blocks to leg districts
bgroup_geometry <- bgroup_geometry %>% st_make_valid() %>%
  st_join(., chi_outline, left = FALSE) %>%
  st_join(., ersb_leg_1031, largest = TRUE) %>%
  st_join(x = ., y = ersb_leg_1031, join = st_nn) 

bgroup_geometry <- bgroup_geometry%>%
  mutate(stateleg_district_1031 = coalesce(stateleg_district_1031.x, stateleg_district_1031.y)) %>%
  select(-one_of(c('stateleg_district_1031.x', 'stateleg_district_1031.y')))

# CPS school data ----------------------------------------------------------------

# https://data.cityofchicago.org/Education/Chicago-Public-Schools-School-Profile-Information-/9a5f-2r4p
schooldata <- st_read('https://data.cityofchicago.org/api/geospatial/9a5f-2r4p?accessType=DOWNLOAD&method=export&format=GeoJSON')

schoolwp <- schooldata  %>%
  select( network, is_high_school, is_pre_school, is_middle_school, is_elementary_school, student_count_total, student_count_asian, student_count_black, student_count_white, student_count_native_american, student_count_other_ethnicity, student_count_special_ed, student_count_low_income, student_count_ethnicity_not, student_count_english_learners, student_count_hawaiian_pacific, student_count_asian_pacific, student_count_hispanic, rating_status, multisensory, refugee_services, visual_impairments, bilingual_services) %>%
  mutate(school_type = case_when(
    network %in% c('Charter',  "Contract", 'AUSL') ~ 'charter_contract_ausl_schools',
    network %in% c("ISP") ~ 'independent_schools',
    network %in% c( "Options") ~ 'options_schools',
    TRUE ~ as.character('public_schools')),
    count_high_school = case_when(is_high_school == 'true' ~ 1, TRUE ~ 0),
    count_pre_school = case_when(is_pre_school == 'true' ~ 1, TRUE ~ 0),
    count_middle_school = case_when(is_middle_school == 'true' ~ 1, TRUE ~ 0),
    count_elementary_school = case_when(is_elementary_school == 'true' ~ 1, TRUE ~ 0),
    count_refugee_services = case_when(refugee_services  == 'true' ~ 1, TRUE ~ 0),
    count_bilingual_services = case_when(bilingual_services  == 'true' ~ 1, TRUE ~ 0)) %>%
  mutate(value = 1) %>%
  spread(school_type, value, fill = 0) %>%
  st_make_valid() %>%
  st_join(., ersb_leg_1031 %>% st_make_valid()) %>%
  st_drop_geometry() 

schoolwp <- schoolwp  %>%
  mutate_at(vars(c("student_count_total", "student_count_asian", "student_count_black", "student_count_white", "student_count_native_american", "student_count_other_ethnicity", "student_count_special_ed", "student_count_low_income", "student_count_ethnicity_not", "student_count_english_learners", "student_count_hawaiian_pacific", "student_count_asian_pacific", "student_count_hispanic")), list(as.integer) ) %>%
  group_by(stateleg_district_1031) %>%
  summarize_at(vars(c("student_count_total", "student_count_asian", "student_count_black", "student_count_white", "student_count_native_american", "student_count_other_ethnicity", "student_count_special_ed", "student_count_low_income", "student_count_ethnicity_not", "student_count_english_learners", "student_count_hawaiian_pacific", "student_count_asian_pacific", "student_count_hispanic", "count_high_school", "count_pre_school", "count_middle_school", "count_elementary_school", "count_refugee_services", "count_bilingual_services", "charter_contract_ausl_schools", "independent_schools", "options_schools", "public_schools")), list(sum)) %>%
  ungroup()

ersb_leg_1031 <- ersb_leg_1031 %>% st_make_valid()  %>%
  #select()
  st_join(., community_areas, largest = TRUE) 

# Analysis --------------------------------------------------------

state_leg_property_taxes <-  base_bills_sum %>% 
  group_by(stateleg_district_1031) %>%
  summarize_at(vars(av, final_tax, tax_bill_total), list(sum), na.rm = TRUE) %>%
  ungroup()

# Students by district
student_df <- bgroup_geometry %>% st_drop_geometry() %>% 
  select(geoid, stateleg_district_1031) %>% 
  rename(block_group_fips = geoid) %>%
  left_join(., bgroup_kids, by = c('block_group_fips'='geoid')) %>%
  group_by(stateleg_district_1031) %>%
  summarize_at(vars(children_ages_5_to_17, enrolled_in_prek_to_grade_12), list(sum), na.rm = TRUE) %>%
  ungroup() %>%
  left_join(., state_leg_property_taxes %>% select(stateleg_district_1031, av, final_tax, tax_bill_total), by =c('stateleg_district_1031' = 'stateleg_district_1031')) %>%
  left_join(., schoolwp %>% select(stateleg_district_1031, student_count_total, public_schools) %>% rename(cps_enrollment_in_district = student_count_total), by = c('stateleg_district_1031'='stateleg_district_1031')) %>%
  #filter(!is.na( stateleg_district_1031)) %>%
  mutate(taxes_per_cps_student = final_tax/enrolled_in_prek_to_grade_12,
         public_enrollment_share_census = enrolled_in_prek_to_grade_12/sum(enrolled_in_prek_to_grade_12),
         public_enrollment_share_cpsschool = cps_enrollment_in_district /sum(cps_enrollment_in_district ),
         av_share = av/sum(av),
         final_tax_share = final_tax/sum(final_tax),
         tax_bill_total_share = tax_bill_total/sum(tax_bill_total)) %>%
  left_join(., ersb_leg_1031 %>% select(stateleg_district_1031, community)  %>% st_drop_geometry(), by = c('stateleg_district_1031'='stateleg_district_1031')) %>%
  relocate(stateleg_district_1031, community, 
           av, av_share, final_tax, final_tax_share, tax_bill_total, tax_bill_total_share, 
           enrolled_in_prek_to_grade_12, public_enrollment_share_census, cps_enrollment_in_district , public_enrollment_share_cpsschool, taxes_per_cps_student)

# Demographics by district
demo_df <- blocks_data %>% st_drop_geometry() %>%
  left_join(., blocks_geometry %>% st_drop_geometry() %>% select(block_fips, stateleg_district_1031), by = c('block_fips'='block_fips')) %>%
  group_by(stateleg_district_1031) %>%
  summarize_at(vars(census_pop_2020, pop_2020_asian, pop_2020_black, pop_2020_latino, pop_2020_other, pop_2020_white, 
                    census_voting_age_pop_2020, pop_2020_voting_age_asian, pop_2020_voting_age_black, pop_2020_voting_age_latino, pop_2020_voting_age_other, pop_2020_voting_age_white), list(sum), na.rm = TRUE) %>%
  ungroup() %>%
  left_join(., state_leg_property_taxes %>% st_drop_geometry(), by = c('stateleg_district_1031' = 'stateleg_district_1031')) %>%
  #filter(!is.na( stateleg_district_1031)) %>%
  mutate(across(all_of(c('pop_2020_asian', 'pop_2020_black', 'pop_2020_latino', 'pop_2020_other', 'pop_2020_white')), .fns = ~./census_pop_2020, .names = "share_{.col}")) %>%
  mutate(across(all_of(c('pop_2020_voting_age_asian', 'pop_2020_voting_age_black', 'pop_2020_voting_age_latino', 'pop_2020_voting_age_other', 'pop_2020_voting_age_white')), .fns = ~./census_voting_age_pop_2020, .names = "share_{.col}")) %>%
  left_join(., ersb_leg_1031 %>% select(stateleg_district_1031, community) %>% st_drop_geometry() , by = c('stateleg_district_1031'='stateleg_district_1031')) %>%
  mutate(av_share = av/sum(av),
         final_tax_share = final_tax/sum(final_tax),
         tax_bill_total_share = tax_bill_total/sum(tax_bill_total)) %>%
  relocate(stateleg_district_1031, community, 
           av, av_share, final_tax, final_tax_share, tax_bill_total, tax_bill_total_share)


block_property_taxes <-  base_bills_sum %>% 
  group_by(block_fips) %>%
  summarize_at(vars(av, final_tax, tax_bill_total), list(sum), na.rm = TRUE) %>%
  ungroup()

# Demographics citywide
demo_df_citywide <- blocks_data %>% st_drop_geometry() %>%
  mutate(predominant_race = case_when(pop_2020_share_asian > .5 ~ 'Majority Asian',
                          pop_2020_share_black > .5 ~ 'Majority Black',
                          pop_2020_share_latino > .5 ~ 'Majority Latino/a',
                          pop_2020_share_other > .5 ~ 'Majority Other',
                          pop_2020_share_white > .5 ~ 'Majority White',
                          pop_2020_share_asian > pmax(pop_2020_share_black, pop_2020_share_latino, pop_2020_share_other, pop_2020_share_white) ~ 'Plurality Asian',
                          pop_2020_share_black  > pmax(pop_2020_share_asian, pop_2020_share_latino, pop_2020_share_other, pop_2020_share_white) ~ 'Plurality Black',
                          pop_2020_share_latino > pmax(pop_2020_share_asian, pop_2020_share_black, pop_2020_share_other, pop_2020_share_white) ~ 'Plurality Latino/a',
                          pop_2020_share_other > pmax(pop_2020_share_asian, pop_2020_share_black, pop_2020_share_latino, pop_2020_share_white) ~ 'Plurality Other',
                          pop_2020_share_white > pmax(pop_2020_share_asian, pop_2020_share_black, pop_2020_share_latino, pop_2020_share_other) ~ 'Plurality White',
                          TRUE ~ 'Mixed')) %>%
  left_join(., blocks_geometry %>% st_drop_geometry() %>% select(block_fips, stateleg_district_1031), by = c('block_fips'='block_fips')) %>%
  left_join(., block_property_taxes, by = c('block_fips' = 'block_fips')) %>%
  mutate_all(~replace(., is.na(.), 0)) %>%
  group_by(predominant_race) %>%
  summarize_at(vars(av, final_tax, tax_bill_total,
                    census_pop_2020, pop_2020_asian, pop_2020_black, pop_2020_latino, pop_2020_other, pop_2020_white, 
                    census_voting_age_pop_2020, pop_2020_voting_age_asian, pop_2020_voting_age_black, pop_2020_voting_age_latino, pop_2020_voting_age_other, pop_2020_voting_age_white), list(sum), na.rm = TRUE) %>%
  ungroup() %>%
  mutate(across(all_of(c('pop_2020_asian', 'pop_2020_black', 'pop_2020_latino', 'pop_2020_other', 'pop_2020_white')), .fns = ~./census_pop_2020, .names = "share_{.col}")) %>%
  mutate(across(all_of(c('pop_2020_voting_age_asian', 'pop_2020_voting_age_black', 'pop_2020_voting_age_latino', 'pop_2020_voting_age_other', 'pop_2020_voting_age_white')), .fns = ~./census_voting_age_pop_2020, .names = "share_{.col}"))

demo_df_citywide <- demo_df_citywide %>%
  mutate(share_av = av/sum(av),
         share_final_tax = final_tax/sum(final_tax),
         share_tax_bill_total = tax_bill_total/sum(tax_bill_total),
         share_city_census_pop_2020 = census_pop_2020/sum(census_pop_2020),
         share_city_pop_2020_asian = pop_2020_asian/sum(pop_2020_asian), 
         share_city_pop_2020_black = pop_2020_black/sum(pop_2020_black), 
         share_city_pop_2020_latino = pop_2020_latino/sum(pop_2020_latino), 
         share_city_pop_2020_other = pop_2020_other/sum(pop_2020_other), 
         share_city_pop_2020_white = pop_2020_white/sum(pop_2020_white)) %>%
  relocate(any_of(c("predominant_race", "av", "share_av", "final_tax", "share_final_tax", "tax_bill_total", "share_tax_bill_total", "census_pop_2020", "share_city_census_pop_2020", "pop_2020_white", "share_city_pop_2020_white", "pop_2020_asian", "share_city_pop_2020_asian", "pop_2020_black", "share_city_pop_2020_black", "pop_2020_latino", "share_city_pop_2020_latino", "pop_2020_other", "share_city_pop_2020_other")))

# Extra ---------------------------------------------------------------

blockgroup_property_taxes <-  base_bills_sum %>% 
  group_by(block_group_fips) %>%
  summarize_at(vars(av, final_tax, tax_bill_total), list(sum), na.rm = TRUE) %>%
  ungroup()

scatter_data <- bgroup_geometry %>% st_drop_geometry() %>% 
  select(geoid, stateleg_district_1031) %>% 
  rename(block_group_fips = geoid) %>%
  left_join(., bgroup_kids, by = c('block_group_fips'='geoid')) %>%
  left_join(., blockgroup_property_taxes, by = c('block_group_fips'='block_group_fips')) %>%
  left_join(., ersb_leg_1031 %>% select(stateleg_district_1031, community), by = c('stateleg_district_1031'='stateleg_district_1031')) 
  
names(scatter_data)
  
ggplot(scatter_data, aes(x = log10(enrolled_in_prek_to_grade_12), y = log10(final_tax), color = community, fill = community)) +
  geom_point(alpha = .6)
  
ggplot(scatter_data, aes(x = (enrolled_in_prek_to_grade_12), y = final_tax, color = community, fill = community)) +
  geom_point(alpha = .6, size =.7) + 
  theme_bw()

ggplot() +
  geom_sf(data = scatter_data %>% mutate(tax_per_student = final_tax/enrolled_in_prek_to_grade_12 ),
          aes(fill = tax_per_student) ) +
  theme_bw()


scatter_data <- scatter_data %>% select(block_group_fips, stateleg_district_1031, community, children_ages_5_to_17, enrolled_in_prek_to_grade_12, av, final_tax, tax_bill_total)
write_csv(scatter_data, 'block_group_students_taxes.csv')  

# -------------------------------------------------------------------------
write_csv(student_df , 'student_districts.csv')
write_csv(demo_df, 'demo_df.csv')
write_csv(demo_df_citywide, 'demo_df_citywide.csv')

# Data in this GSheet: https://docs.google.com/spreadsheets/d/1-Dl9byWcIMZBjW2rqOWEsTYmEM_vQCyqCetYdusXVKM/
