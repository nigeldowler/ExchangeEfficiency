#####################################################
# Dynamic Market Efficiency in Peer-to-Peer Betting #
# Author: Nigel Dowler                              #
# August 2019                                       #
# STAT40710 Dissertation                            #
# MA Statistics                                     #
# School of Mathematics and Statistics              #
# University College Dublin                         #
#####################################################


###-------------------------- Functions ----------------------------###

###-----------------------------------------------------------------###
###                 Run a query on MySQL database
###-----------------------------------------------------------------###
mysql.query <- function(query, timer = FALSE) {
  
  # Load packages if needed
  require(RMariaDB)
  require(dplyr)
  
  # Set up connection settings here
  # Actual details removed for security
  db <- dbConnect(MariaDB(),
                  host='',
                  port='3306',
                  dbname='',
                  user='',
                  password='')
  
  # To record the time taken, get the current time
  start.time <- Sys.time()
  
  # Run query
  message("Running query...")
  q <- dbSendQuery(db, query)
  results <- dbFetch(q)
  
  # Time taken
  time.taken <- Sys.time() - start.time
  if (timer) {
    message(time.taken)
  }
  
  # Successful query
  if (dbHasCompleted(q))
  {
    message("..returned successfully")

    # Clean up
    dbClearResult(q)
    
    # Disconnect from MySQL database
    dbDisconnect(db)
    
    # Return results
    return(as_tibble(results))
  }
  else
  {
    # Something went wrong
    message("Error")
    return(NULL)
  }
}

###-----------------------------------------------------------------###
###       Run a query to get raw data at a specific timepoint
###-----------------------------------------------------------------###
get.raw.data <- function(seconds_before_off = 0) {
  
  # Build the query
  query.raw.data <- paste("
    select
      sels.market_id,
      exch.seconds_before_off,    
      sels.race_time,
      exch.market_off_datetime as off_time,
      time_to_sec(timediff(exch.market_off_datetime, sels.race_time)) as off_delay,
      sels.day_of_week,
      sels.country_code,
      sels.track,
      sels.race_number,
      sels.runners,
      sels.selection_id,
      sels.selection_name,
      sels.bsp,
      sels.sp,
      sels.demarg_sp_win_prob,
      sels.fav_rank,
      sels.`position`,
      exch.back,
      exch.back_vol,
      exch.lay,
      exch.lay_vol,
      exch.last_traded_price,
      exch.midpoint,
      exch.weighted_average,
      exch.cumul_traded_vol_selection,
      exch.cumul_traded_vol_market,
      sels.inplay_min,
      sels.inplay_max,
      exch.reduction_factor_to_apply,
      sels.horse_age,
      sels.horse_sex,
      sels.weight_carried,
      sels.handicap_mark,
      sels.draw,
      sels.jockey,
      sels.trainer,
      sels.`owner`,
      sels.surface,
      sels.race_type,
      sels.going,
      sels.distance,
      sels.race_class,
      sels.race_code,
      sels.handicap,
      sels.age,
      sels.sex_limit,
      sels.prize_fund,
      sels.tv
    from horseracing_uki_selections sels
    left join horseracing_uki_exchange_snapshots exch
      on sels.market_id = exch.market_id
      and sels.selection_id = exch.selection_id
    where
      position is not null
      and seconds_before_off = ",
        seconds_before_off,
    " order by sels.market_id, exch.seconds_before_off, sels.selection_id")

  # Run the query and return the results
  mysql.query(query.raw.data)
}


###-----------------------------------------------------------------###
###                   Save query results to CSV
###-----------------------------------------------------------------###
save.data.to.csv <- function(thedata, ref = "", folder = "data") {
  write.csv(thedata,
            file = paste0(folder,
                          "\\",
                          deparse(substitute(thedata)),
                          "_",
                          ref,
                          ".csv"),
            row.names = FALSE)
}


###-----------------------------------------------------------------###
###                Get saved raw data from csv file
###-----------------------------------------------------------------###
get.data.from.csv <- function(prefix, seconds, folder = "data") {
  
  # Define the column types we expect from the csv
  col_types <- cols(
    market_id                  = col_integer(),
    seconds_before_off         = col_integer(),
    race_time                  = col_datetime(),
    off_time                   = col_datetime(),
    off_delay                  = col_integer(),
    day_of_week                = col_integer(),
    country_code               = col_character(),
    track                      = col_character(),
    race_number                = col_integer(),
    runners                    = col_integer(),
    selection_id               = col_integer(),
    selection_name             = col_character(),
    bsp                        = col_double(),
    sp                         = col_double(),
    demarg_sp_win_prob         = col_double(),
    fav_rank                   = col_integer(),
    position                   = col_integer(),
    back                       = col_double(),
    back_vol                   = col_double(),
    lay                        = col_double(),
    lay_vol                    = col_double(),
    last_traded_price          = col_double(),
    midpoint                   = col_double(),
    weighted_average           = col_double(),
    cumul_traded_vol_selection = col_double(),
    cumul_traded_vol_market    = col_double(),
    inplay_min                 = col_double(),
    inplay_max                 = col_double(),
    reduction_factor_to_apply  = col_double(),
    horse_age                  = col_integer(),
    horse_sex                  = col_character(),
    weight_carried             = col_integer(),
    handicap_mark              = col_integer(),
    draw                       = col_integer(),
    jockey                     = col_character(),
    trainer                    = col_character(),
    owner                      = col_character(),
    surface                    = col_character(),
    race_type                  = col_character(),
    going                      = col_character(),
    distance                   = col_double(),
    race_class                 = col_integer(),
    race_code                  = col_character(),
    handicap                   = col_integer(),
    age                        = col_character(),
    sex_limit                  = col_character(),
    prize_fund                 = col_integer(),
    tv                         = col_character()
  )
  
  # Read the csv and convert to a tibble
  as_tibble(read_csv(paste0(folder, "\\", prefix, "_", seconds, ".csv"),
                     col_types = col_types,
                     progress = FALSE))
}


###-----------------------------------------------------------------###
###                   Clean and prep the raw data
###-----------------------------------------------------------------###
clean.raw.data <- function(inputdata) {
  
  inputdata %>%
    filter(!is.na(market_id), market_id > 0) %>%
    group_by(market_id) %>%
      filter(!(any(is.na(position)) |
                 any(is.na(last_traded_price)) |
                 any(last_traded_price <= 0.0)
      )) %>%
    mutate(last_traded_price = if_else(is.na(reduction_factor_to_apply),
                                       last_traded_price,
                                       pmax(1.01,
                                            last_traded_price *
                                              (1 - reduction_factor_to_apply))),
           prob = 1 / last_traded_price,
           or_bsp = sum(1/bsp)) %>%
    filter(or_bsp >= 0.97, or_bsp <= 1.03) %>%  
    ungroup()
}


###-----------------------------------------------------------------###
###                    Calculate R-squared value
###-----------------------------------------------------------------###
r.squared <- function(inputdata) {
  n <- nrow(inputdata)
  x <- inputdata$prob
  y <- inputdata$actual
  r2 <- ((n * sum(x * y) - sum(x) * sum(y)) ^ 2) /
    ((n * sum(x * x) - (sum(x)^2)) * (n * sum(y * y) - (sum(y)^2)))
  
  return(r2)
}


###-----------------------------------------------------------------###
###                 Calculate Weighted R-squared
###-----------------------------------------------------------------###
weighted.r.squared <- function(inputdata) {
  n <- nrow(inputdata)
  x <- inputdata$prob
  y <- inputdata$actual
  w <- inputdata$weight
  
  wgt_r2 <- ((n * sum(w * x * y) - sum(w * x) * sum(w * y)) ^ 2) /
    ((n * sum(w * x * x) - (sum(w * x)^2)) * (n * sum(w * y * y) - (sum(w * y)^2)))
  
  return(wgt_r2)
}


###-----------------------------------------------------------------###
###           Calculate Weighted Root-Mean-Square Error
###-----------------------------------------------------------------###
weighted.rmse <- function(inputdata) {
  with(inputdata, sqrt(sum(count * (prob-actual)^2) / sum(count)))
}


###-----------------------------------------------------------------###
###      Perform Mincer-Zarnowitz regression with a Wald test
###-----------------------------------------------------------------###
mincer.zarnowitz.wald <- function(inputdata) {
  fit <- lm(actual ~ prob, data = inputdata)
  coef = fit$coefficients
  cov = vcov(fit)
  s <- (coef - c(0, 1)) %*% solve(cov) %*% c(coef - c(0, 1))
  p <- pchisq(s, 2, lower.tail = FALSE)
  return(data.frame(b0 = coef[[1]], b1 = coef[[2]], wald.stat = s, wald.p = p))
}


###-----------------------------------------------------------------###
###             Plot actual vs expected with 95% CIs
###-----------------------------------------------------------------###
plotActualExpected <- function(inputdata, title = "") {

  ggplot(inputdata) +
    geom_ribbon(aes(x = prob,
                    ymin = pmax(0, prob - 1.96 * binomial_sd),
                    ymax = pmin(1, prob + 1.96 * binomial_sd)),
                fill = "pink",
                alpha = 0.8) +
    geom_point(aes(x = prob, y = actual), size = 1, color = "blue", alpha = 0.8) +
    geom_line(aes(x = prob, y = actual), color = "blue", alpha = 0.5) +
    geom_abline(slope = 1, size = 0.7, colour = "red", lty = "dashed", alpha = 0.8) +
    scale_x_continuous(name = "Implied Probability from the Last Traded Price",
                       limits = c(0, 1),
                       breaks = seq(0, 1, 0.1),
                       labels = scales::percent) +
    scale_y_continuous(name = "Actual Win Rate",
                       limits = c(0, 1),
                       breaks = seq(0, 1, 0.1),
                       labels = scales::percent) +
    ggtitle(title) +
    theme_bw() +
    theme(legend.position = "none")
}

# Similar to previous function but just used as a one-off. Plots the size
# of points in proportion to the number of horses in the data point.
plotActualExpected.imbalanced <- function(inputdata, title = "") {
  
  ggplot(inputdata) +
    geom_ribbon(aes(x = prob,
                    ymin = pmax(0, prob - 1.96 * binomial_sd),
                    ymax = pmin(1, prob + 1.96 * binomial_sd)),
                fill = "pink",
                alpha = 0.8) +
    geom_point(aes(x = prob, y = actual, size = count), color = "blue", alpha = 0.5) +
    geom_line(aes(x = prob, y = actual), color = "blue", alpha = 0.5) +
    geom_abline(slope = 1, size = 0.7, colour = "red", lty = "dashed", alpha = 0.8) +
    scale_x_continuous(name = "Implied Probability from the Last Traded Price",
                       limits = c(0, 1),
                       breaks = seq(0, 1, 0.1),
                       labels = scales::percent) +
    scale_y_continuous(name = "Actual Win Rate",
                       limits = c(0, 1),
                       breaks = seq(0, 1, 0.1),
                       labels = scales::percent) +
    ggtitle(title) +
    theme_bw() +
    theme(legend.position = "none")
}

