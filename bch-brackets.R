
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
library(writexl)
library(ptaxsim)
conflict_prefer("select", "dplyr")
conflict_prefer("filter", "dplyr")
options(tigris_use_cache = TRUE)
readRenviron("~/.Renviron")
options(scipen = 99999)

# Set working directory ---------------------------------------------------

setwd(dirname(rstudioapi::getActiveDocumentContext()$path))
getwd()

# PTaxSim -----------------------------------------------------------------

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

# BCH ---------------------------------------------------------------------
#https://chicago.suntimes.com/city-hall/2023/10/31/23940970/real-estate-transfer-tax-home-sales-referendum-bring-chicago-home-affordable-housing-city-council
#The new tax rate on sales under $1 million would be 0.60%, down from the current rate of 0.75%. 
# Sales of $1 million or over, but under $1.5 million, will pay a 2% tax, more than 2.5 times what they pay now. 
# And sales of $1.5 million and higher will pay 3%, four times the current rate.

bch_df <- pin_data %>%
  mutate(pin_count = 1) %>%
  mutate(market_value = case_when(assessment_level == '10%' ~ 10*av_clerk,
                                  assessment_level == '20%' ~ 5*av_clerk,
                                  assessment_level == '25%' ~ 4*av_clerk,
                                  TRUE ~ 0),
         class_4way = case_when(major_class_type %in% c("Vacant", "Not For Profit") ~ 'Vacant lots & non-profits',
                                major_class_type %in% c("Residential") ~ 'Residential',
                                major_class_type %in% c("Multi-Family") ~ "Multi-Family",
                                major_class_type %in% c("Commercial","Industrial") ~ "Commercial & Industrial"),
         transfer_bracket = case_when(market_value >= 1500000 ~ 'Bracket 3 - $1.5M+',
                                  market_value < 1500000 & market_value >= 1000000 ~ 'Bracket 2 - $1M-1.5M',
                                  market_value < 1000000 ~ 'Bracket 1 - $1M',
                                  TRUE ~ ''),
         transfer_tax = case_when(market_value >= 1500000 ~ .03,
                                          market_value < 1500000 & market_value >= 1000000 ~ .02,
                                          market_value < 1000000 ~ .06,
                                          TRUE ~ 0)) %>%
  mutate(transfer_tax_estimate = market_value * transfer_tax) %>%
  filter(!(major_class_type %in% c('Industrial Incentive', 'Industrial Brownfield', 'Commercial Incentive', 'Commercial Incentive', 'Commercial/Industrial Incentive', 'Multi-Family Incentive'))) %>%
  filter(!is.na(major_class_type))
  # filter(!is.na(major_class_code)) %>%
  # filter(!is.na(assessment_level)) %>%

bracket_x_majorclass <- bch_df %>%
  group_by(year, transfer_bracket, major_class_code, major_class_type, assessment_level) %>%
  summarize_at(vars(market_value, transfer_tax_estimate, pin_count, tax_bill_total, av_mailed, av_certified, av_board, av_clerk), list(sum)) %>%
  ungroup() %>%
  mutate(market_value_share = market_value/sum(market_value),
         average_market_value = market_value/pin_count) %>%
  relocate(year, transfer_bracket, major_class_type, average_market_value, market_value, market_value_share, pin_count)

bracket_x_rescom <- bch_df %>%
  group_by(year, transfer_bracket, class_4way, assessment_level) %>%
  summarize_at(vars(market_value, transfer_tax_estimate, pin_count, tax_bill_total, av_mailed, av_certified, av_board, av_clerk), list(sum)) %>%
  ungroup() %>%
  mutate(market_value_share = market_value/sum(market_value),
         average_market_value = market_value/pin_count) %>%
  relocate(year, transfer_bracket, class_4way, average_market_value, market_value, market_value_share, pin_count)
         
bracket <- bch_df %>%
  group_by(year, transfer_bracket) %>%
  summarize_at(vars(market_value, transfer_tax_estimate, pin_count, tax_bill_total, av_mailed, av_certified, av_board, av_clerk), list(sum)) %>%
  ungroup() %>%
  mutate(market_value_share = market_value/sum(market_value),
         average_market_value = market_value/pin_count) %>%
  relocate(year, transfer_bracket, average_market_value, market_value, market_value_share, pin_count)

write_csv(bracket_x_majorclass, 'bch_major.csv')
write_csv(bracket_x_rescom, 'bch_4way.csv')
write_csv(bracket, 'bch_1way.csv')
