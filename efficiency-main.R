#####################################################
# Dynamic Market Efficiency in Peer-to-Peer Betting #
# Author: Nigel Dowler                              #
# August 2019                                       #
# STAT40710 Dissertation                            #
# MA Statistics                                     #
# School of Mathematics and Statistics              #
# University College Dublin                         #
#####################################################


###-------------------------- Main script---------------------------###

# Set the working directory
# setwd("C:\\ExchangeEfficiency\\R")

# Fix the RNG seed
set.seed(42)


###---------- Load packages (and install if necessary) -------------###

# This code makes heavy use of dplyr and ggplot
if (!require("tidyverse")) {
  install.packages("tidyverse")
  library(tidyverse)
}

# Not essential. Only needed for connecting to the database initially.
if (!require("RMariaDB")) {
  install.packages("RMariaDB")
  library(RMariaDB)
}

# Used for printing nice tables
if (!require("kableExtra")) {
  install.packages("kableExtra")
  library(kableExtra)
}

# Used for modelling a variable to impute missing values
if (!require("randomForest")) {
  install.packages("randomForest")
  library(randomForest)
}

# Used for performing generalised additive models (GAMs)
if (!require("mgcv")) {
  install.packages("mgcv")
  library(mgcv)
}


###-------------------------- Functions ----------------------------###

# Source the functions file, which contains functions I wrote for:
# reading, processing and saving data;
# calculating R-squared values;
# performing Mincer-Zarnowitz regression and Wald tests;
# plotting graphs;

source("efficiency-functions.R")



###-----------------------------------------------------------------###
###                               DATA
###-----------------------------------------------------------------###

# In my database, data exists at a second-by-second level but that is
# overkill for this project. To try to read in all that data would
# crash R/RStudio on any normal computer. Therefore, I define specific
# time points of the data to use; 115 time points to be precise. The
# explanation for these chosen time points is in the thesis manuscript.

# Define a vector of the time points to use. These values represent
# the number of seconds before the market close.
secs_seq <- c(seq(0, 3600, 60),
              seq(3900, 10800, 300),
              seq(11700, 21600, 900),
              seq(25200, 86400, 3600))
secs_seq
length(secs_seq)

#######################################################################
#
# Note:
# To skip all the data reading and analysis, you can jump straight
# from here to the modelling section (line 1274) and read in the
# reduced data set for modelling directly from a CSV.
#
#######################################################################


#######################################################################
#
# Important!
#
# This section is only used for fetching the raw data from a MySQL
# database. It is a one-time job and takes a while to complete.
# Thankfully, it is already done! After reading in the data, we
# saved it into CSV files. So, we can skip this and move on to the
# next section which reads the data from those CSVs instead.
#
# for (secs in secs_seq) {
#   
#   # Run query
#   message("Querying raw data for time point: ", secs, " seconds")
#   rawdata <- get.raw.data(secs)
#   message("..got raw data")
#   
#   # Save to csv
#   save.data.to.csv(rawdata, secs)
#   message("..saved to csv")
#   rm(rawdata)
# }
#
#######################################################################


###------------------ READ DATA FROM CSV FILES ---------------------###

# Important: CSV files must be in "data" subdirectory of the working dir
#
# They have the filename format: rawdata_N.csv,
# where N = a number of seconds, defining the time point.
# Can be downloaded online from the link in Appendix B of the thesis.

# Read in data from CSV files and store in two tibbles:
# (i) horsedata - horse and race-level data
# (ii) timedata - time-level price data at various time points
#
# This saves valuable memory.
# Warning: a lot of memory is still used up!
# Use the gc() command liberally.

horsedata <- tibble()
timedata <- tibble()

# Define list of columns to go into each tibble
horsedata_cols <- c("market_id",
                    "race_time",
                    "off_time",
                    "off_delay",
                    "day_of_week",
                    "country_code",
                    "race_class",
                    "track",
                    "race_number",
                    "runners",
                    "selection_id",
                    "selection_name",
                    "bsp",
                    "fav_rank",
                    "position",
                    "inplay_min",
                    "inplay_max",
                    "horse_age",
                    "horse_sex",
                    "weight_carried",
                    "handicap_mark",
                    "draw",
                    "jockey",
                    "trainer",
                    "owner",
                    "surface",
                    "race_type",
                    "going",
                    "distance",
                    "race_code",
                    "handicap",
                    "age",
                    "sex_limit",
                    "prize_fund",
                    "tv")

timedata_cols <- c("market_id",
                   "seconds_before_off",
                   "race_time",
                   "country_code",
                   "race_class",
                   "runners",
                   "selection_id",
                   "prob",
                   "last_traded_price",                   
                   "bsp",
                   "fav_rank",
                   "position",
                   "back",
                   "back_vol",
                   "lay",
                   "lay_vol",
                   "midpoint",
                   "cumul_traded_vol_selection",
                   "cumul_traded_vol_market",
                   "reduction_factor_to_apply",
                   "or_bsp")

# Read in the data, apply cleaning function, then split it
# into horse-level and time-level data.
# Files need to be in "data" subdirectory of the working dir.
# It takes about 10 minutes to run this loop.
for (secs in secs_seq) {
  
  # Get the raw data from the CSV file for one time point
  message("Reading raw data from csv file for time point: ", secs, " seconds")
  rawdata <- get.data.from.csv("rawdata", secs)
  message("..got raw data")
  
  # Clean the data
  cleandata <- clean.raw.data(rawdata)
  message("..cleaned data")
  rm(rawdata)
  
  # Extract the horse-level data once only
  if (secs == 0) {
    horsedata <- cleandata[horsedata_cols]
    message("..extracted horse-level data")
  }
  
  # Extract the time-level data
  timedata <- bind_rows(timedata, cleandata[timedata_cols])
  message("..extracted time-level data")  
  rm(cleandata)
}

# Check memory size of data
format(object.size(horsedata), units = "Mb")
format(object.size(timedata), units = "Mb")

# Run garbage collection to free memory
gc()


###---------------------------------------------------------------###
###                          DATA CHECK
###---------------------------------------------------------------###

# How many selections at market close?
nrow(horsedata)
timedata %>%
  filter(seconds_before_off == 0) %>%
  count()

# How many unique selections altogether?
timedata %>%
  group_by(market_id, selection_id) %>%
  n_groups()

# 293,197 vs 293,259

# Find and remove all instances of the extra 62 selections
starters <-
  timedata %>%
  filter(seconds_before_off == 0) %>%
  select(market_id, selection_id) %>%
  distinct()

useless_data <- data.frame(market_id = integer(),
                           selection_id = integer())

for (secs in secs_seq[secs_seq > 0]) {
  message("Checking time point ", secs)
  useless_data <- bind_rows(useless_data,
    timedata %>%
      filter(seconds_before_off == secs) %>%
      select(market_id, selection_id) %>%
      distinct() %>%
      anti_join(starters, by = c("market_id", "selection_id"))
  )
}

useless_data <- useless_data %>% distinct()

# Remove them
timedata <-
  timedata %>% anti_join(useless_data)

# Check equal to 293,197
timedata %>%
  group_by(market_id, selection_id) %>%
  n_groups()

rm(starters)
rm(useless_data)
gc()



###---------------------------------------------------------------###
###                    EXPLORATORY ANALYSIS
###---------------------------------------------------------------###

# Examine some of the variables individually

###-------------------- Last Traded Price ------------------------###

# What is the distribution of the final last traded price?
timedata %>%
  filter(seconds_before_off == 0) %>%
  select(last_traded_price) %>%
  ggplot() +
  geom_density(aes(x = last_traded_price), color = "darkgrey", fill = "lightyellow") +
  scale_x_continuous(limits = c(1, 1000),
                     breaks = c(1, 250, 500, 750, 1000)) +
  xlab("Last Traded Price") +
  ylab("Density") +
  theme_bw()
# Note the spike at 1,000

# What is the distribution of the final probabilities?
final_probs <-
  timedata %>%
    filter(seconds_before_off == 0) %>%
    transmute(p = 1 / last_traded_price) %>%
    arrange(p)

# Plot the final probs
ggplot(final_probs) +
  geom_density(aes(x = p), color = "darkgrey", fill = "lightblue") +
  xlab("Final probabilities") +
  ylab("Density") +
  theme_bw()

# How many selections ever traded at 1.01 and 1000?
timedata %>%
  group_by(market_id, selection_id) %>%
  filter(any(last_traded_price %in% c(1.01, 1000))) %>%
  summarise(price = if_else(min(last_traded_price) == 1.01, 1.01, 1000)) %>%
  ungroup() %>%
  group_by(price) %>%
  summarise(n = n()) %>%
  ungroup()
#    39 horses at 1.01
# 6,989 horses at 1000

# Remove them all from every time point
timedata <-
  timedata %>%
  group_by(market_id, selection_id) %>%
  filter(!any(last_traded_price %in% c(1.01, 1000))) %>%  
  ungroup()

# Redo the final probabilities
final_probs <-
  timedata %>%
    filter(seconds_before_off == 0) %>%
    transmute(p = 1 / last_traded_price) %>%
    arrange(p)

# Plot it again
ggplot(final_probs) +
  geom_density(aes(x = p), color = "darkgrey", fill = "lightblue") +
  xlab("Final probabilities") +
  ylab("Density") +
  theme_bw()

# Apply a log transform to the probabilities
final_probs_log <-
  final_probs %>%
  transmute(logp = log(p)) %>%
  arrange(logp)

# Plot the logit-transformed distribution
ggplot(final_probs_log) +
  geom_density(aes(x = logp), color = "darkgrey", fill = "pink") +  
  xlab("Log transform of final probabilities") +
  ylab("Density") +
  theme_bw()

# QQ plot (takes a minute to generate)
ggplot(final_probs_log, aes(sample = logp)) +
  geom_qq() +
  geom_qq_line() +
  theme_bw()

# Not very good results

# So instead, apply a logit transform to the probabilities
final_probs_logit <-
  final_probs %>%
    transmute(logit = log(p / (1 - p))) %>%
    arrange(logit)

# Plot the logit-transformed distribution
ggplot(final_probs_logit) +
  geom_density(aes(x = logit), color = "darkgrey", fill = "pink") +  
  xlab("Logit transform of final probabilities") +
  ylab("Density") +
  theme_bw()

# QQ plot (takes a minute to generate)
ggplot(final_probs_logit, aes(sample = logit)) +
  geom_qq() +
  geom_qq_line() +
  theme_bw()

# Logit transformation looks a bit better than log

# Shapiro-Wilk normality test
shapiro.test(sample(final_probs_logit$logit, 5000))

# Reject null hypothesis of normality


###------------------------- Country -----------------------------###

# How many selections in each country?
timedata %>%
  filter(seconds_before_off == 0) %>%
  select(Country = country_code) %>%
  group_by(Country) %>%
  summarise(Count = n()) %>%
  ungroup() %>%
  mutate(Prop = format(Count / sum(Count), digits = 3),
         Count = format(Count, big.mark = ",")) %>%
  kable(align = c("l", "r", "l")) %>%
  kable_styling(full_width = FALSE)


###----------------------- Race Class ----------------------------###

# First tabulate the numbers
timedata %>%
  filter(seconds_before_off == 0) %>%
  group_by(race_class, country_code) %>%
  summarise(count = n()) %>%
  ungroup() %>%
  spread(country_code, count, fill = 0) %>%
  rename(`Race class` = race_class) %>%
  mutate(GB = format(GB, big.mark = ","),
         IE = format(IE, big.mark = ",")) %>%
  kable(align = c("c", "r", "r")) %>%
  kable_styling(full_width = FALSE)

# Create new variable: Race class band
timedata <- timedata %>%
  mutate(race_class_band = case_when(race_class <= 2 ~ "high",
                                     race_class > 2 ~ "low",
                                     TRUE ~ NA_character_))
gc()

# Tabulate it
timedata %>%
  filter(seconds_before_off == 0) %>%
  group_by(race_class_band, country_code) %>%
  summarise(count = n()) %>%
  ungroup() %>%
  spread(country_code, count, fill = 0) %>%
  rename(`Race class band` = race_class_band) %>%
  mutate(GB = format(GB, big.mark = ","),
         IE = format(IE, big.mark = ",")) %>%
  kable(align = c("c", "r", "r")) %>%
  kable_styling(full_width = FALSE)

###----------- Impute missing values for race class -----------###

# First create a data set: one row for each race,
# along with relevant variables converted to factors
classes_data <-
  horsedata %>%
  select(market_id, race_class, country_code, surface,
         race_type, handicap, age, prize_fund, tv) %>%
  distinct() %>%
  mutate(race_class   = factor(race_class),
         country_code = factor(country_code),
         surface      = factor(surface),
         race_type    = factor(race_type),
         handicap     = factor(handicap),
         age          = factor(age),
         tv           = factor(tv))

# Split into data with and without race_class
classes <-
  classes_data %>%
  filter(!is.na(race_class))

classes_missing <-
  classes_data %>%
  filter(is.na(race_class))

gc()

# Summary of the data
table(classes$race_class)
table(classes$country_code, useNA = "ifany")
table(classes$surface, useNA = "ifany")
table(classes$race_type, useNA = "ifany")
table(classes$handicap, useNA = "ifany")
table(classes$age, useNA = "ifany")
table(classes$tv, useNA = "ifany")

# Split into training and test set
train_idx <- sample(1:nrow(classes), floor(nrow(classes) * 0.75))
classes.train <- classes[train_idx,]
classes.test  <- classes[-train_idx,]  

# Perform random forest classification
rf1 <- randomForest(race_class ~ . -market_id,
                    data = classes.train,
                    na.action = na.exclude)
print(rf1)
varImpPlot(rf1, main = "")

# Test set predictions
classes.pred.rf1 <- predict(rf1, newdata = classes.test)

# Check test accuracy
pred.table.rf1 <- table(classes.test$race_class, classes.pred.rf1)
pred.table.rf1
sum(diag(pred.table.rf1)) / sum(pred.table.rf1)
# 79.95% accuracy

# Check test accuracy on race class bands
test.bands <- if_else(classes.test$race_class %in% c(1, 2), "high", "low")
pred.bands <- if_else(classes.pred.rf1 %in% c(1, 2), "high", "low")
table.bands <- table(test.bands, pred.bands)
table.bands
sum(diag(table.bands)) / sum(table.bands)
# 98.4% accuracy

# Let's use this model to impute missing values for race class
# and then convert to the race class bands as before.
classes_missing$race_class_pred <- predict(rf1, newdata = classes_missing)

# Convert back from a factor to an integer
classes_missing$race_class_pred <- as.integer(as.character(classes_missing$race_class_pred))

# Join to the horsedata table and update the NA values
horsedata <- horsedata %>% left_join(classes_missing[,c("market_id", "race_class_pred")])
horsedata$race_class[is.na(horsedata$race_class)] <-
  horsedata$race_class_pred[is.na(horsedata$race_class)]
horsedata <- horsedata %>% select(-race_class_pred)

# Now join to the timedata table and update the NA values
timedata <- timedata %>% left_join(classes_missing[,c("market_id", "race_class_pred")])
timedata$race_class[is.na(timedata$race_class)] <-
  timedata$race_class_pred[is.na(timedata$race_class)]
timedata <- timedata %>% select(-race_class_pred)

# Update the race_class_band variable
timedata <- timedata %>%
  mutate(race_class_band = case_when(race_class <= 2 ~ "high",
                                     race_class > 2 ~ "low",
                                     TRUE ~ NA_character_))
gc()

# Finally, tabulate it again
timedata %>%
  filter(seconds_before_off == 0) %>%
  group_by(race_class_band, country_code) %>%
  summarise(count = n()) %>%
  ungroup() %>%
  spread(country_code, count, fill = 0) %>%
  rename(`Race class band` = race_class_band) %>%
  mutate(GB = format(GB, big.mark = ","),
         IE = format(IE, big.mark = ",")) %>%
  kable(align = c("c", "r", "r")) %>%
  kable_styling(full_width = FALSE)


###----------------------- Field size ----------------------------###

# Number of selections per race
# small = 2-7
# mid   = 8-11
# large = 12+
# (This is slow - takes about 7 minutes)
timedata <-
  timedata %>%
  group_by(seconds_before_off, market_id) %>%
  mutate(field_size = factor(case_when(runners <= 7 ~ "small",
                                       runners <= 11 ~ "mid",
                                       TRUE ~ "large"),
                             levels = c("small", "mid", "large"))) %>%
  ungroup()

gc()

# Tabulate it
timedata %>%
  filter(seconds_before_off == 0) %>%
  group_by(field_size) %>%
  summarise(numRaces = n_distinct(market_id),
            numHorses = n()) %>%
  ungroup() %>%
  rename(`Field size` = field_size) %>%
  mutate(Range = c("2 - 7", "8 - 11", "12+"),
         numRaces = format(numRaces, big.mark = ","),
         numHorses = format(numHorses, big.mark = ",")) %>%
  select(`Field size`, Range, numRaces, numHorses) %>%
  kable(align = c("c", "c", "r", "r")) %>%
  kable_styling(full_width = FALSE)


###------------------------- All run -----------------------------###

# Did all horses run in the race or any non-runners?
timedata <-
  timedata %>%
  group_by(market_id) %>%
  mutate(all_run = if_else(all(is.na(reduction_factor_to_apply)), TRUE, FALSE)) %>%
  ungroup()

gc()

# Tabulate by number of races
table(timedata %>%
        filter(seconds_before_off == 0) %>%
        group_by(market_id) %>%
        summarise(all_run = first(all_run)) %>%
        ungroup() %>%
        pull(all_run))


###------------------------ Short fav ----------------------------###

# Does the race have a short-priced favourite or not?
# i.e. last_traded_price <= 2.5
timedata <-
  timedata %>%
  group_by(market_id) %>%
  mutate(short_fav = if_else(any(seconds_before_off == 0 &
                                 last_traded_price <= 2.5),
                             TRUE, FALSE)) %>%  
  ungroup()

gc()

# Tabulate by number of races
table(timedata %>%
        filter(seconds_before_off == 0) %>%
        group_by(market_id) %>%
        summarise(short_fav = first(short_fav)) %>%
        ungroup() %>%
        pull(short_fav))


###--------------------- Probability band -------------------------###

# Create a new categorical variable for probability band
timedata <-
  timedata %>%
  mutate(prob_band = factor(case_when(eff_prob >= 0.4   ~ "prob_high",
                                      eff_prob >= 0.2   ~ "prob_mid",
                                      eff_prob >= 0.067 ~ "prob_low",
                                      TRUE              ~ "prob_vlow"),
                            levels = c("prob_high", "prob_mid", "prob_low", "prob_vlow")))

gc()

# Tabulate the prob_band
timedata %>%
  filter(seconds_before_off == 0) %>%
  group_by(prob_band) %>%
  summarise(numHorses = n()) %>%
  ungroup() %>%
  mutate(prob_band = c("high", "mid", "low", "vlow"),
         Range = c("[0.4 - 1)", "[0.2 - 0.4)", "[0.067 - 0.2)", "(0 - 0.067)"),
         numHorses = format(numHorses, big.mark = ",")) %>%
  rename(`Probability band` = prob_band) %>%
  select(`Probability band`, Range, numHorses) %>%
  kable(align = c("c", "c", "r")) %>%
  kable_styling(full_width = FALSE)


###-------------------
# Entropy of a race
timedata %>%
  group_by(seconds_before_off, market_id) %>%
  mutate(entropy = sum(log(last_traded_price)/last_traded_price)) %>%
  ungroup() %>%
  select(race_time, selection_id, seconds_before_off, last_traded_price, prob, entropy)
gc()



###---------------------------------------------------------------###
###                          ANALYSIS
###---------------------------------------------------------------###

# Look at the data at market close only first
closingdata <- timedata %>% filter(seconds_before_off == 0)

# Example of plotting an imbalanced discretising of probabilities.
# Round each probability to the nearest whole percentage point,
# then compute actual win-rate and plot it.
closingdata %>%
  mutate(prob = round(prob, 2)) %>%
  group_by(prob) %>%
  summarise(count = n(),
            actual = sum(position == 1)/ count) %>%
  mutate(binomial_sd = sqrt(prob * (1 - prob) / count)) %>%
  plotActualExpected.imbalanced(title = "Actual vs Expected at the market close")


# Better approach...
# Split the probabilities into 100 quantiles
closingdata$prob_bucket <- ntile(closingdata$prob, 100)
table(closingdata$prob_bucket)

# Calculate the mean probability in each quantile and the actual win-rate
closingdata_summary <-
  closingdata %>%
  group_by(prob_bucket) %>%
  summarise(count = n(),
            prob = mean(prob),
            actual = sum(position == 1) / count) %>%
  mutate(binomial_sd = sqrt(prob * (1 - prob) / count)) %>%
  ungroup()

View(closingdata_summary)

# Nice table
closingdata_summary %>%
  kable() %>%
  kable_styling(bootstrap_options = c("striped", "condensed"))

# Plot it
# (This function is written by me in efficiency-functions.R)
plotActualExpected(closingdata_summary, "Actual vs Expected at the market close")

# Calculate the R-squared value (function in efficiency-functions.R)
r.squared(closingdata_summary)

# Mincer-Zarnowitz regression and Wald test (function in efficiency-functions.R)
mzw <- mincer.zarnowitz.wald(closingdata_summary)
mzw

# Plot linear regression line
ggplot() +
  geom_point(data = closingdata_summary,
             aes(x = prob, y = actual), size = 1, color = "blue", alpha = 0.8) +
  geom_abline(data = mzw,
              aes(intercept = b0, slope = b1), size = 0.7, colour = "red", alpha = 0.8) +
  scale_x_continuous(name = "Implied Probability from the Last Traded Price",
                     limits = c(0, 1),
                     breaks = seq(0, 1, 0.1),
                     labels = scales::percent) +
  scale_y_continuous(name = "Actual Win Rate",
                     limits = c(0, 1),
                     breaks = seq(0, 1, 0.1),
                     labels = scales::percent) +
  ggtitle("Linear regression of Actual ~ Probability") +
  theme_bw() +
  theme(legend.position = "none") +
  annotate("label", x = 0.7, y = 0.15, hjust = 0, fill = "lightyellow",
           label = paste0("intercept = ", round(mzw$b0, 6), "\n",
                          "slope = ", round(mzw$b1, 6), "\n",
                          "Wald test p-value = ", round(mzw$wald.p, 3)))


# For each time point, apply the same summary and stats as above
# First define empty data frames to hold our results
timedata_summary <- tibble(seconds_before_off = integer(),
                           prob_bucket = integer(),
                           prob = numeric(),
                           count = integer(),
                           actual = numeric(),
                           binomial_sd = numeric())

timedata_summary_stats <- tibble(seconds_before_off = integer(),
                                 r2 = numeric(),
                                 wald.p = numeric())

# Loop through each time point
for (secs in secs_seq) {
  
  # Calculate the values for actual and expected
  summary <- timedata %>%
    filter(seconds_before_off == secs) %>%
    mutate(prob_bucket = ntile(prob, 100)) %>%
    group_by(prob_bucket) %>%
    summarise(count = n(),
              prob = mean(prob),
              actual = sum(position == 1) / count) %>%
    mutate(seconds_before_off = secs,
           binomial_sd = sqrt(prob * (1 - prob) / count))

  # Add to summary results
  timedata_summary <- bind_rows(timedata_summary, summary)

  # Now calculate the R-squared value and Mincer-Zarnowitz results
  stats <- c(seconds_before_off = secs,
             r2 = r.squared(summary),
             wald.p = mincer.zarnowitz.wald(summary)$wald.p)

  # Add to summary stats
  timedata_summary_stats <- bind_rows(timedata_summary_stats, stats)  

  gc()

  message("Summarised data for ", secs)
}

timedata_summary
timedata_summary_stats
gc()


# Plot actual vs expected at distinct time points
# 1 hour out
plotActualExpected(timedata_summary %>% filter(seconds_before_off == 3600),
                   title = "Actual vs Expected 1 hour prior to market close")
timedata_summary_stats %>% filter(seconds_before_off == 3600) %>% pull(r2)
timedata_summary_stats %>% filter(seconds_before_off == 3600) %>% pull(wald.p)


# 3 hours out
plotActualExpected(timedata_summary %>% filter(seconds_before_off == 10800),
                   title = "Actual vs Expected 3 hours prior to market close")
timedata_summary_stats %>% filter(seconds_before_off == 10800) %>% pull(r2)
timedata_summary_stats %>% filter(seconds_before_off == 10800) %>% pull(wald.p)


# 9 hours out
plotActualExpected(timedata_summary %>% filter(seconds_before_off == 32400),
                   title = "Actual vs Expected 9 hours prior to market close")
timedata_summary_stats %>% filter(seconds_before_off == 32400) %>% pull(r2)
timedata_summary_stats %>% filter(seconds_before_off == 32400) %>% pull(wald.p)


# 18 hours out
plotActualExpected(timedata_summary %>% filter(seconds_before_off == 64800),
                   title = "Actual vs Expected 18 hours prior to market close")
timedata_summary_stats %>% filter(seconds_before_off == 64800) %>% pull(r2)
timedata_summary_stats %>% filter(seconds_before_off == 64800) %>% pull(wald.p)


# Plot all R-squared values over 24 hours
r2_plot_24hrs <-
  ggplot(timedata_summary_stats,
         aes(x = seconds_before_off / 3600, y = r2)) +
  geom_point(colour = "blue", alpha = 0.4) +
  scale_x_reverse(name = "Hours before market close",
                  limits = c(24, 0),
                  breaks = seq(24, 0, -2)) +
  scale_y_continuous(name = "R-squared",
                     limits = c(0, 1),
                     breaks = seq(0, 1, 0.1)) +
  ggtitle("R-squared of Actual vs Expected from 24 hours before market close") +
  theme_bw() +
  theme(legend.position = "none")
r2_plot_24hrs

# R-squared: final hour only
r2_plot_1hr <-
  ggplot(timedata_summary_stats %>% filter(seconds_before_off <= 3600),
         aes(x = -seconds_before_off / 60, y = r2)) +
  geom_point(colour = "blue", alpha = 0.4) +
  scale_x_continuous(name = "Minutes before the off",
                     limits = c(-60, 0),
                     breaks = seq(-60, 0, 5)) +
  scale_y_continuous(name = "R-squared",
                     limits = c(0.9, 1),
                     breaks = seq(0.9, 1, 0.01),
                     labels = scales::percent_format(accuracy = 1)) +
  ggtitle("R-squared of Actual vs Expected from 1 hour before market close") +
  theme_bw() +
  theme(legend.position = "none")
r2_plot_1hr


# Plot all Wald test p-values over 24 hours
wald_plot_24hrs <-
  ggplot(timedata_summary_stats,
         aes(x = seconds_before_off / 3600, y = wald.p)) +
  geom_point(colour = "green4", alpha = 0.4) +
  geom_line(colour = "green4", size = 0.2, alpha = 0.2) +
  scale_x_reverse(name = "Hours before market close",
                  limits = c(24, 0),
                  breaks = seq(24, 0, -2)) +
  scale_y_continuous(name = "Wald test p-values",
                     limits = c(0, 1),
                     breaks = seq(0, 1, 0.1)) +
  ggtitle("Wald test p-values from Mincer-Zarnowitz regression of Actual vs Expected") +
  theme_bw() +
  theme(legend.position = "none")
wald_plot_24hrs

# Wald test: final 3 hours only
wald_plot_3hrs <-
  ggplot(timedata_summary_stats %>% filter(seconds_before_off <= 10800),
         aes(x = seconds_before_off / 60, y = wald.p)) +
  geom_point(colour = "green4", alpha = 0.4) +
  geom_line(colour = "green4", size = 0.2, alpha = 0.2) +
  scale_x_reverse(name = "Minutes before the off",
                  limits = c(180, 0),
                  breaks = seq(180, 0, -15)) +
  scale_y_continuous(name = "Wald test p-values",
                     limits = c(0, 1),
                     breaks = seq(0, 1, 0.1)) +
  ggtitle("Wald test p-values from Mincer-Zarnowitz regression of Actual vs Expected") +
  theme_bw() +
  theme(legend.position = "none")
wald_plot_3hrs

# Wald test: final hour only
wald_plot_1hr <-
  ggplot(timedata_summary_stats %>% filter(seconds_before_off <= 3600),
         aes(x = seconds_before_off / 60, y = wald.p)) +
  geom_point(colour = "green4", alpha = 0.4) +
  geom_line(colour = "green4", size = 0.2, alpha = 0.2) +
  scale_x_reverse(name = "Minutes before the off",
                  limits = c(60, 0),
                  breaks = seq(60, 0, -5)) +
  scale_y_continuous(name = "Wald test p-values",
                     limits = c(0, 1),
                     breaks = seq(0, 1, 0.1)) +
  ggtitle("Wald test p-values from Mincer-Zarnowitz regression of Actual vs Expected") +
  theme_bw() +
  theme(legend.position = "none")
wald_plot_1hr

# Highest Wald test p-value
timedata_summary_stats %>% arrange(desc(wald.p))



###-------------------------------------------------------------###
###   New measure of efficiency: Distance to efficient price
###-------------------------------------------------------------###

# Extract the final probability of every horse at market close
efficient_probs <-
  timedata %>%
  filter(seconds_before_off == 0) %>%
  select(market_id,
         selection_id,
         eff_prob = prob)

# Join the efficient probability to all other time points
timedata <- left_join(timedata, efficient_probs, by = c("market_id", "selection_id"))

gc()

# Recall: probabilities are approximately log-normal
timedata %>%
  select(prob) %>%
  sample_n(10000) %>%
  ggplot(aes(x = prob)) +
  geom_density(fill = "khaki") +
  xlab("Probability") +
  ylab("Density") +
  ggtitle("Density of Probabilities") +
  theme_bw()

# Plot log transform of probabilities
timedata %>%
  select(prob) %>%
  sample_n(10000) %>%
  ggplot(aes(x = log(prob))) +
  geom_density(fill = "khaki2") +
  xlab("Log(Probability)") +
  ylab("Density") +
  ggtitle("Log transform") +
  theme_bw()

# Define our "error" as the absolute distance between log of probabilities
timedata <-
  timedata %>%
  mutate(error = abs(log(prob / eff_prob)))

gc()  

# Density plot of errors
timedata %>%
  filter(seconds_before_off == 3600) %>%
  select(error) %>%
  sample_n(10000) %>%
  ggplot(aes(x = error)) +
  geom_density(fill = "salmon") +
  xlab("Distance to efficiency") +
  ylab("Density") +
  ggtitle("No transform") +
  theme_bw()

# Boxplot of errors
timedata %>%
  select(error) %>%
  sample_n(10000) %>%
  ggplot(aes(y = error)) +
  geom_boxplot(colour = "salmon4", fill = "salmon") +
  xlab("") +
  ylab("Distance to efficiency") +
  ggtitle("") +
  theme_bw()

# Density plot of errors, log transformed
timedata %>%
  select(error) %>%
  filter(error > 0) %>%
  sample_n(10000) %>%
  ggplot(aes(x = log(error))) +
  geom_density(fill = "salmon2") +
  xlab("Distance to efficiency") +
  ylab("Density") +
  ggtitle("Log transform") +
  theme_bw()

# Boxplot of errors, log transformed
timedata %>%
  select(error) %>%
  filter(error > 0) %>%
  sample_n(10000) %>%
  ggplot(aes(y = log(error))) +
  geom_boxplot(colour = "salmon4", fill = "salmon2") +
  xlab("") +
  ylab("Distance to efficiency") +
  ggtitle("") +
  theme_bw()

# Density plot of errors, square root transformed
timedata %>%
  select(error) %>%
  filter(error > 0) %>%  
  sample_n(10000) %>%
  ggplot(aes(x = sqrt(error))) +
  geom_density(fill = "salmon3") +
  xlab("Distance to efficiency") +
  ylab("Density") +
  ggtitle("Square root transform") +
  theme_bw()

# Boxplot of errors, square root transformed
timedata %>%
  select(error) %>%
  filter(error > 0) %>%
  sample_n(10000) %>%
  ggplot(aes(y = sqrt(error))) +
  geom_boxplot(colour = "salmon4", fill = "salmon3") +
  xlab("") +
  ylab("Distance to efficiency") +
  ggtitle("") +
  theme_bw()


# We now define "distance to efficiency" as the mean error
# in any grouping of horses
dist.to.efficiency <- function(inputvector) {
  mean(inputvector, na.rm = TRUE)
}

# Prior to modelling, examine relationship between "distancy to efficiency"
# and various independent variables one by one

###-------------- Distance to efficiency at each time point --------------###
# Correlation
cor(timedata$error, timedata$seconds_before_off)

# Plot the mean distance to efficiency over time
timedata %>%
  group_by(seconds_before_off) %>%
  summarise(dist_to_eff = dist.to.efficiency(error)) %>%
  ungroup() %>%
  ggplot(aes(x = seconds_before_off / 3600, y = dist_to_eff)) +
  geom_point() +
  scale_x_reverse(name = "Hours before the off",
                  limits = c(24, 0),
                  breaks = seq(24, 0, -2)) +
  scale_y_continuous(name = "Distance to efficiency") +
  theme_bw()

# We could do a log transform of the time variable
timedata %>%
  group_by(seconds_before_off) %>%
  summarise(dist_to_eff = dist.to.efficiency(error)) %>%
  ungroup() %>%
  ggplot(aes(x = log(seconds_before_off), y = dist_to_eff)) +
  geom_point() +
  scale_x_reverse(name = "Log (Time before the off)") +
  scale_y_continuous(name = "Distance to efficiency") +
  theme_bw()


###--------------- Distance to efficiency per country_code ---------------###
timedata %>%
  group_by(country_code) %>%
  summarise(dist_to_eff = dist.to.efficiency(error)) %>%
  ungroup()

# Plot country over time
timedata %>%
  group_by(seconds_before_off, country_code) %>%
  summarise(dist_to_eff = dist.to.efficiency(error)) %>%
  ungroup() %>%
  ggplot(aes(x = seconds_before_off / 3600, y = dist_to_eff, colour = country_code)) +
  geom_point(alpha = 0.5) +
  scale_color_manual(name = "Country",
                     values = c("blue2", "green3")) +
  scale_x_reverse(name = "Hours before the off",
                  limits = c(24, 0),
                  breaks = seq(24, 0, -2)) +
  scale_y_continuous(name = "Distance to efficiency",
                     limits = c(0, 1),
                     breaks = seq(0, 1, 0.2)) +
  theme_bw()


###-------------- Distance to efficiency per race_class_band --------------###
timedata %>%
  group_by(race_class_band) %>%
  summarise(dist_to_eff = dist.to.efficiency(error)) %>%
  ungroup()

# Plot race_class_band over time
timedata %>%
  group_by(seconds_before_off, race_class_band) %>%
  summarise(dist_to_eff = dist.to.efficiency(error)) %>%
  ungroup() %>%
  ggplot(aes(x = seconds_before_off / 3600, y = dist_to_eff, colour = race_class_band)) +
  geom_point(alpha = 0.5) +
  scale_color_manual(name = "Race\nClass\nBand",
                     values = c("purple3", "brown2")) +
  scale_x_reverse(name = "Hours before the off",
                  limits = c(24, 0),
                  breaks = seq(24, 0, -2)) +
  scale_y_continuous(name = "Distance to efficiency",
                    limits = c(0, 1),
                    breaks = seq(0, 1, 0.2)) +
  theme_bw()


###------------------ Distance to efficiency per prob_band ------------------###

# Distance to efficiency per prob_band
timedata %>%
  group_by(prob_band) %>%
  summarise(dist_to_eff = dist.to.efficiency(error)) %>%
  ungroup()

# Plot prob_band over time
timedata %>%
  group_by(seconds_before_off, prob_band) %>%
  summarise(dist_to_eff = dist.to.efficiency(error)) %>%
  ungroup() %>%
  ggplot(aes(x = seconds_before_off / 3600, y = dist_to_eff, colour = prob_band)) +
  geom_point(alpha = 0.5) +
  scale_color_discrete(name = "Probability\nBand",
                     labels = c("high", "middle", "low", "very low")) +
  scale_x_reverse(name = "Hours before the off",
                  limits = c(24, 0),
                  breaks = seq(24, 0, -2)) +
  scale_y_continuous(name = "Distance to efficiency",
                     limits = c(0, 1),
                     breaks = seq(0, 1, 0.2)) +
  theme_bw()


###----------------- Distance to efficiency per field_size -----------------###
timedata %>%
  group_by(field_size) %>%
  summarise(dist_to_eff = dist.to.efficiency(error)) %>%
  ungroup()

# Plot field_size over time
timedata %>%
  group_by(seconds_before_off, field_size) %>%
  summarise(dist_to_eff = dist.to.efficiency(error)) %>%
  ungroup() %>%
  ggplot(aes(x = seconds_before_off / 3600, y = dist_to_eff, colour = field_size)) +
  geom_point(alpha = 0.5) +
  scale_color_manual(name = "Field\nSize",
                     values = c("darkorange2", "turquoise3", "deeppink4")) +
  scale_x_reverse(name = "Hours before the off",
                  limits = c(24, 0),
                  breaks = seq(24, 0, -2)) +
  scale_y_continuous(name = "Distance to efficiency",
                     limits = c(0, 1),
                     breaks = seq(0, 1, 0.2)) +
  theme_bw()


###------------------ Distance to efficiency per all_run ------------------###
timedata %>%
  group_by(all_run) %>%
  summarise(dist_to_eff = dist.to.efficiency(error)) %>%
  ungroup()

# Plot all_run over time
timedata %>%
  group_by(seconds_before_off, all_run) %>%
  summarise(dist_to_eff = dist.to.efficiency(error)) %>%
  ungroup() %>%
  ggplot(aes(x = seconds_before_off / 3600, y = dist_to_eff,
             colour = factor(all_run, levels = c("TRUE", "FALSE")))) +
  geom_point(alpha = 0.5) +
  scale_color_discrete(name = "All Run") +
  scale_x_reverse(name = "Hours before the off",
                  limits = c(24, 0),
                  breaks = seq(24, 0, -2)) +
  scale_y_continuous(name = "Distance to efficiency",
                     limits = c(0, 1),
                     breaks = seq(0, 1, 0.2)) +
  theme_bw()


###----------------- Distance to efficiency per short_fav ----------------###
timedata %>%
  group_by(short_fav) %>%
  summarise(dist_to_eff = dist.to.efficiency(error)) %>%
  ungroup()

# Plot short_fav over time
timedata %>%
  group_by(seconds_before_off, short_fav) %>%
  summarise(dist_to_eff = dist.to.efficiency(error)) %>%
  ungroup() %>%
  ggplot(aes(x = seconds_before_off / 3600, y = dist_to_eff,
             colour = factor(short_fav, levels = c("TRUE", "FALSE")))) +
  geom_point(alpha = 0.5) +
  scale_color_manual(name = "Short Fav",
                     values = c("darkslateblue", "lightsalmon2")) +
  scale_x_reverse(name = "Hours before the off",
                  limits = c(24, 0),
                  breaks = seq(24, 0, -2)) +
  scale_y_continuous(name = "Distance to efficiency",
                     limits = c(0, 1),
                     breaks = seq(0, 1, 0.2)) +
  theme_bw()


gc()


### How much data do we have at each time point?
timedata %>%
  group_by(seconds_before_off) %>%
  filter(seconds_before_off %% 300 == 0) %>%
  summarise(n = n()) %>%
  ungroup() %>%
  ggplot(aes(x = seconds_before_off / 3600, y = n)) +
  geom_bar(stat = "identity", width = 0.3, colour = "darkgreen", alpha = 0.5) +
  #scale_color_gradient() +
  scale_x_reverse(name = "Hours before the off",
                  limits = c(24, 0),
                  breaks = seq(24, 0, -2)) +
  scale_y_continuous(name = "Data observations",
                     limits = c(0, 300000),
                     breaks = seq(0, 300000, 50000),
                     labels = scales::comma) +
  theme_bw()


###----------------------------------------------------------###
###            Define our data set for modelling
###----------------------------------------------------------###

# Take data from 12 hours prior to the off.
# Less reliable data between 12 to 24 hours.
# Don't include the zero time point.
# Convert categorical variables to factors where needed.
modeldata <-
  timedata %>%
  filter(seconds_before_off > 0,
         seconds_before_off <= 43200) %>%
  group_by(seconds_before_off,
           country_code,
           race_class_band,
           field_size,
           all_run,
           short_fav,
           prob_band) %>%
  summarise(dist_to_eff = dist.to.efficiency(error)) %>%
  ungroup() %>%
  mutate(country_code    = factor(country_code),
         race_class_band = factor(race_class_band),
         field_size      = factor(field_size))

gc()

# Alternatively, read in the modeldata from a CSV file if it
# has already been generated.
#modeldata <- read_csv("data\\modeldata.csv")

# Save this data to a CSV file for quick recall
write_csv(modeldata, path = paste0("data\\modeldata.csv"))


# Check the data
dim(modeldata)
# 16,626 x 8

summary(modeldata)

# Correlation between log(time) and distance to efficiency
cor(log(modeldata$seconds_before_off), modeldata$dist_to_eff)

# Print a small sample of the data
modeldata %>%
  sample_n(6) %>%
  arrange(seconds_before_off) %>%
  rename("time to off" = seconds_before_off,
         "country"     = country_code,
         "race class band" = race_class_band,
         "field size" = field_size,
         "all run" = all_run,
         "short fav" = short_fav,
         "probability band" = prob_band,
         "distance to efficiency" = dist_to_eff) %>%
  select("time to off", "country", "race class band", "probability band",
         "field size", "all run", "short fav", "distance to efficiency") %>%
  kable(align = c("r", "c", "c", "l", "c", "c", "c", "c")) %>%
  kable_styling(full_width = FALSE,
                bootstrap_options = c("striped"))


###----------------------------------------------------------###
###     Split into a training, validation and test set
###----------------------------------------------------------###
n <- nrow(modeldata)
props <- c(train = 0.6, valid = 0.2, test = 0.2)
splits <- sample(cut(1:n, n * cumsum(c(0, props)), labels = names(props)))

modeldata.train <- modeldata[splits=="train", ]
modeldata.valid <- modeldata[splits=="valid", ]
modeldata.test  <- modeldata[splits=="test", ]

nrow(modeldata.train)  # 9,975
nrow(modeldata.valid)  # 3,325
nrow(modeldata.test)   # 3,326

# Now work on the training set alone,
# using the validation set only to measure model error.

# Look at distribution of response variable
plot(density(modeldata.train$dist_to_eff))
boxplot(modeldata.train$dist_to_eff)

plot(density(log(modeldata.train$dist_to_eff)))
boxplot(log(modeldata.train$dist_to_eff))

plot(density(sqrt(modeldata.train$dist_to_eff)))
boxplot(sqrt(modeldata.train$dist_to_eff))

# Sqrt transform looks closest to normal but not great.
# Possibly bimodal distribution?

# Density plot of errors, square root transformed
modeldata.train %>%
  select(dist_to_eff) %>%
  filter(dist_to_eff > 0) %>%  
  ggplot(aes(x = sqrt(dist_to_eff))) +
  geom_density(fill = "salmon3") +
  xlab("Distance to efficiency") +
  ylab("Density") +
  ggtitle("Square root transform") +
  theme_bw()

# Boxplot of errors, square root transformed
modeldata.train %>%
  select(dist_to_eff) %>%
  filter(dist_to_eff > 0) %>%
  ggplot(aes(y = sqrt(dist_to_eff))) +
  geom_boxplot(colour = "salmon4", fill = "salmon3") +
  xlab("") +
  ylab("Distance to efficiency") +
  ggtitle("") +
  theme_bw()


###---------------------------------------------------------------###
###                           Modelling
###---------------------------------------------------------------###

###--------------------------- MODEL 1 ---------------------------###

# Multiple linear regression model with everything
mod1 <- lm(dist_to_eff ~ ., data = modeldata.train)

# Diagnostics
summary(mod1)  # Adj R2 = 0.8265
AIC(mod1)  # -28944
plot(mod1)

# Make predictions on the validation set and compute the RMSE
preds <- predict(mod1, newdata = modeldata.valid)
rmse <- sqrt(mean((modeldata.valid$dist_to_eff - preds)^2))
rmse
# 0.05701


###--------------------------- MODEL 2 ---------------------------###

# Multiple linear regression model with log(time)
mod2 <- lm(dist_to_eff ~ . - seconds_before_off + log(seconds_before_off),
            data = modeldata.train)

# Diagnostics
summary(mod2)  # Adj R2 = 0.8744
AIC(mod2)  # -32170
plot(mod2)

# Make predictions on the validation set and compute the RMSE
preds <- predict(mod2, newdata = modeldata.valid)
rmse <- sqrt(mean((modeldata.valid$dist_to_eff - preds)^2))
rmse
# 0.04722


###--------------------------- MODEL 3 ---------------------------###

# Multiple linear regression model with log(time) and interaction
mod3 <- lm(dist_to_eff ~ . - seconds_before_off + log(seconds_before_off) +
             race_class_band*prob_band,
           data = modeldata.train)

# Diagnostics
summary(mod3)  # Adj R2 = 0.8888
AIC(mod3)  # -33377
plot(mod3)

# Make predictions on the validation set and compute the RMSE
preds <- predict(mod3, newdata = modeldata.valid)
rmse <- sqrt(mean((modeldata.valid$dist_to_eff - preds)^2))
rmse
# 0.04382


###--------------------------- MODEL 4 ---------------------------###

# Multiple linear regression model with sqrt response
mod4 <- lm(sqrt(dist_to_eff) ~ ., data = modeldata.train)

# Diagnostics
summary(mod4)  # Adj R2 = 0.8360
AIC(mod4)  # -30241
plot(mod4)

# Make predictions on the validation set and compute the RMSE
preds <- predict(mod4, newdata = modeldata.valid)
rmse <- sqrt(mean((modeldata.valid$dist_to_eff - (preds^2))^2)) # squared pred
rmse
# 0.05272


###--------------------------- MODEL 5 ---------------------------###

# Multiple linear regression model with sqrt response and log(time)
mod5 <- lm(sqrt(dist_to_eff) ~ . - seconds_before_off + log(seconds_before_off),
           data = modeldata.train)

# Diagnostics
summary(mod5)  # Adj R2 = 0.9032
AIC(mod5)  # -35496
plot(mod5)

# Make predictions on the validation set and compute the RMSE
preds <- predict(mod5, newdata = modeldata.valid)
rmse <- sqrt(mean((modeldata.valid$dist_to_eff - (preds^2))^2)) # squared pred
rmse
# 0.03969


###--------------------------- MODEL 6 ---------------------------###

# Multiple linear regression model with sqrt response and log(time) and interaction
mod6 <- lm(sqrt(dist_to_eff) ~ . - seconds_before_off + log(seconds_before_off) +
             race_class_band*prob_band,
           data = modeldata.train)

# Diagnostics
summary(mod6)  # Adj R2 = 0.9081
AIC(mod6)  # -36015
plot(mod6)
# obs 2560 outlier ?

# Make predictions on the validation set and compute the RMSE
preds <- predict(mod6, newdata = modeldata.valid)
rmse <- sqrt(mean((modeldata.valid$dist_to_eff - (preds^2))^2)) # squared pred
rmse
# 0.03758


###--------------------------- MODEL 7 ---------------------------###

# Generalised linear model with Gaussian log link
mod7 <- glm(dist_to_eff + .Machine$double.eps ~ .,
             data = modeldata.train,
             family = gaussian(link = "log"))

# Diagnostics
summary(mod7)  # AIC -32236
1 - (mod7$deviance / mod7$null.deviance)  # R2 0.8754
plot(mod7)

# Make predictions on the validation set and compute the RMSE
preds <- predict(mod7, newdata = modeldata.valid, type = "response")
rmse <- sqrt(mean((modeldata.valid$dist_to_eff - preds)^2))
rmse
# 0.04886


###--------------------------- MODEL 8 ---------------------------###

# Generalised linear model with Gaussian log link and log(time)
mod8 <- glm(dist_to_eff + .Machine$double.eps ~ . - seconds_before_off +
              log(seconds_before_off),
            data = modeldata.train,
            family = gaussian(link = "log"))

# Diagnostics
summary(mod8)  # AIC -39614
1 - (mod8$deviance / mod8$null.deviance)  # R2 0.9405
plot(mod8)

# Make predictions on the validation set and compute the RMSE
preds <- predict(mod8, newdata = modeldata.valid, type = "response")
rmse <- sqrt(mean((modeldata.valid$dist_to_eff - preds)^2))
rmse
# 0.03316


###--------------------------- MODEL 9 ---------------------------###

# Generalised linear model with Gaussian log link and log(time) and interaction
mod9 <- glm(dist_to_eff + .Machine$double.eps ~ . - seconds_before_off +
              log(seconds_before_off) + race_class_band*prob_band,
            data = modeldata.train,
            family = gaussian(link = "log"))

# Diagnostics
summary(mod9)  # AIC -40114
1 - (mod9$deviance / mod9$null.deviance)  # R2 0.9435
plot(mod14)

# Make predictions on the validation set and compute the RMSE
preds <- predict(mod9, newdata = modeldata.valid, type = "response")
rmse <- sqrt(mean((modeldata.valid$dist_to_eff - preds)^2))
rmse
# 0.03217


###--------------------------- MODEL 10 ---------------------------###

# Generalised additive model with smoothed time
mod10 <- gam(dist_to_eff ~ s(seconds_before_off) +
               race_class_band + prob_band + country_code +
               all_run + field_size + short_fav,
             data = modeldata.train)

# Diagnostics
summary(mod10)  # Adj R2 = 0.868
gam.check(mod10)
AIC(mod10)  # -31673
plot(mod10)
qq.gam(mod10)

# Make predictions on the validation set and compute the RMSE
preds <- predict(mod10, newdata = modeldata.valid)
rmse <- sqrt(mean((modeldata.valid$dist_to_eff - preds)^2))
rmse
# 0.04859


###--------------------------- MODEL 11 ---------------------------###

# Generalised additive model with smoothed log(time)
mod11 <- gam(dist_to_eff ~ s(log(seconds_before_off)) +
               race_class_band + prob_band + country_code +
               all_run + field_size + short_fav,
             data = modeldata.train)

# Diagnostics
summary(mod11)  # Adj R2 = 0.876
gam.check(mod11)
AIC(mod11)  # -32313
plot(mod11)
qq.gam(mod11)

# Make predictions on the validation set and compute the RMSE
preds <- predict(mod11, newdata = modeldata.valid)
rmse <- sqrt(mean((modeldata.valid$dist_to_eff - preds)^2))
rmse
# 0.04676


###--------------------------- MODEL 12 ---------------------------###

# Generalised additive model with smoothed log(time)
mod12 <- gam(dist_to_eff ~ s(log(seconds_before_off)) +
               race_class_band + prob_band + country_code +
               all_run + field_size + short_fav +
               race_class_band*prob_band,
             data = modeldata.train)

# Diagnostics
summary(mod12)  # Adj R2 = 0.891
gam.check(mod12)
AIC(mod12)  # -33541
plot(mod12)
qq.gam(mod12)

# Make predictions on the validation set and compute the RMSE
preds <- predict(mod12, newdata = modeldata.valid)
rmse <- sqrt(mean((modeldata.valid$dist_to_eff - preds)^2))
rmse
# 0.04331


###--------------------------- MODEL 13 ---------------------------###

# Generalised additive model with log link and smoothed time variable
mod13 <- gam(dist_to_eff ~ s(seconds_before_off) +
               race_class_band + prob_band + country_code +
               all_run + field_size + short_fav,
             data = modeldata.train,
             family = gaussian(link="log"))

# Diagnostics
summary(mod13)  # Adj R2 = 0.935
gam.check(mod13)
AIC(mod13)  # -38758
plot(mod13)
qq.gam(mod13)

# Make predictions on the validation set and compute the RMSE
preds <- predict(mod13, newdata = modeldata.valid, type = "response")
rmse <- sqrt(mean((modeldata.valid$dist_to_eff - preds)^2))
rmse
# 0.03439


###--------------------------- MODEL 14 ---------------------------###

# Generalised additive model with log link and smoothed time variable
mod14 <- gam(dist_to_eff ~ s(log(seconds_before_off)) +
               race_class_band + prob_band + country_code +
               all_run + field_size + short_fav,
             data = modeldata.train,
             family = gaussian(link="log"))

# Diagnostics
summary(mod14)  # Adj R2 = 0.949
gam.check(mod14)
AIC(mod14)  # -41143
plot(mod14)
qq.gam(mod14)

# Make predictions on the validation set and compute the RMSE
preds <- predict(mod14, newdata = modeldata.valid, type = "response")
rmse <- sqrt(mean((modeldata.valid$dist_to_eff - preds)^2))
rmse
# 0.03069


###--------------------------- MODEL 15 ---------------------------###

# Generalised additive model with log link and smoothed time variable and interaction
mod15 <- gam(dist_to_eff ~ s(log(seconds_before_off)) +
               race_class_band + prob_band + country_code +
               all_run + field_size + short_fav +
               race_class_band*prob_band,
             data = modeldata.train,
             family = gaussian(link="log"))

# Diagnostics
summary(mod15)  # Adj R2 = 0.952
gam.check(mod15)
AIC(mod15)  # -41737
plot(mod15)
qq.gam(mod15)

# Make predictions on the validation set and compute the RMSE
preds <- predict(mod15, newdata = modeldata.valid, type = "response")
rmse <- sqrt(mean((modeldata.valid$dist_to_eff - preds)^2))
rmse
# 0.02962


###------------------------- FINAL MODEL -------------------------###

# We choose model 14 with the insignificant variable removed
mod.final <- gam(dist_to_eff ~ s(log(seconds_before_off)) +
                   race_class_band + prob_band + country_code +
                   field_size + short_fav,
             data = modeldata.train,
             family = gaussian(link="log"))

# Diagnostics
summary(mod.final)  # Adj R2 = 0.949

gam.check(mod.final, col = "blue", cex = 0.9)
grid()

AIC(mod.final)  # -41143

# Plot of smooth
plot(mod.final,
     se = TRUE,
     col = "blue", 
     xlab = "log (time to off)",
     ylab = "smooth ( log (time to off) )")
grid()

# QQ plot
qq.gam(mod.final, main = "Q-Q Plot")
grid()

# Make predictions on the validation set and compute the RMSE
preds <- predict(mod.final, newdata = modeldata.valid, type = "response")
rmse <- sqrt(mean((modeldata.valid$dist_to_eff - preds)^2))
rmse
# 0.03068

# Make predictions on the test set and compute the RMSE
preds.test <- predict(mod.final, newdata = modeldata.test, type = "response")
rmse.test <- sqrt(mean((modeldata.test$dist_to_eff - preds.test)^2))
rmse.test
# 0.03131

gc()

# Create some more diagnostic plots
resids <- data.frame(fitted.values = mod.final$fitted.values,
                     y = mod.final$y)

# Fitted values vs observed responses
ggplot(resids, aes(x = fitted.values, y = y)) +
  geom_point(color = "blue", alpha = 0.5) +
  geom_abline(intercept = 0, slope = 1, lty = "dashed", color = "red") +
  scale_x_continuous(name = "Fitted values",
                     limits = c(0, 1),
                     breaks = seq(0, 1, 0.2)) +
  scale_y_continuous(name = "Response",
                     limits = c(0, 1),
                     breaks = seq(0, 1, 0.2)) +
  theme_bw() +
  theme(legend.position = "none")

# Fitted values vs residuals
ggplot(resids, aes(x = fitted.values, y = y - fitted.values)) +
  geom_point(color = "deepskyblue3", alpha = 0.5) +
  geom_hline(yintercept = 0, lty = "dashed", color = "red") +
  scale_x_continuous(name = "Fitted values",
                     limits = c(0, 1),
                     breaks = seq(0, 1, 0.2)) +
  scale_y_continuous(name = "Residuals",
                     limits = c(-0.22, 0.22),
                     breaks = seq(-0.2, 0.2, 0.1)) +
  theme_bw() +
  theme(legend.position = "none")

gc()


###----------------------------- END -----------------------------###
