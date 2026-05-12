library(tidyverse)
library(duckdb)
library(dfms)
library(arrow)

con <- dbConnect(
  duckdb::duckdb(),
  "db/unified_trade_data.duckdb",
  read_only = TRUE
)

# dbDisconnect(con, shutdown = TRUE)
dbGetQuery(con, "SHOW TABLES")

# -----------------------------------------
# Forecast window setup:
# A) forecast start date
# B) forecast end date
# C) number of periods
# -----------------------------------------

dbGetQuery(con, "
  SELECT PERIOD, TNVED2, NAPR, STOIM, STRANA, TYPE
  FROM unified_trade_data"
) %>%
  filter(TYPE == "fact") %>%
  reframe(last_period = max(PERIOD), .by = c(STRANA)) %>%
  filter(last_period > max(last_period) %m-% months(11)) %>%
  ggplot(aes(x = last_period)) +
  geom_histogram()

country_last_periods <-
  dbGetQuery(con, "
  SELECT PERIOD, TNVED2, NAPR, STOIM, STRANA, TYPE
  FROM unified_trade_data"
  ) %>%
  filter(TYPE == "fact") %>%
  reframe(last_period = max(PERIOD), .by = c(STRANA))

# Critical fix:
# use max(last_period), not last(last_period), to avoid order-dependent behavior.
max_last_period <- country_last_periods %>% pull(last_period) %>% max()

fc_dates <-
  country_last_periods %>%
  filter(last_period > max_last_period %m-% months(11)) %>%
  arrange(last_period) %>%
  mutate(last_period_cdf = cume_dist(last_period)) %>%
  filter(last_period_cdf >= 0.5)

fc_from <-
  fc_dates %>%
  pull(last_period) %>%
  min()

fc_to <-
  fc_dates %>%
  pull(last_period) %>%
  max()

fc_periods <- interval(fc_from, fc_to) %/% months(1) + 1

# ----------------------------------------------
# Forecast on upper level (TNVED2 x NAPR)
# ----------------------------------------------

df_raw <- dbGetQuery(con, "
  SELECT PERIOD, TNVED2, NAPR, STOIM, TYPE
  FROM unified_trade_data"
) %>%
  filter(TYPE == "fact") %>%
  reframe(
    stoim = sum(STOIM, na.rm = TRUE),
    .by = c("PERIOD", "TNVED2", "NAPR")
  ) %>%
  arrange(TNVED2, NAPR, PERIOD) %>%
  mutate(gr = paste0(TNVED2, "_", NAPR))

df_var_1 <- df_raw %>%
  mutate(
    stoim = c(0, diff(log1p(stoim))) %>% as.numeric(),
    .by = "gr"
  ) %>%
  filter(PERIOD > as_date("2019-01-01")) %>%
  select(PERIOD, gr, stoim)

df_var_1 %>%
  ggplot(aes(x = PERIOD, y = stoim, color = gr)) +
  geom_line(show.legend = FALSE)

train_dates <- seq(
  from = df_var_1 %>% pull(PERIOD) %>% first(),
  to = fc_from %m-% months(1),
  by = "month"
)
test_dates <- seq(
  from = fc_from,
  to = fc_to,
  by = "month"
)

df_var_1_train <-
  df_var_1 %>%
  pivot_wider(names_from = gr, values_from = stoim) %>%
  filter(PERIOD %in% train_dates) %>%
  select(-PERIOD)

df_var_1_train %>%
  is.na() %>%
  colSums()

ic <- ICr(df_var_1_train)
plot(ic)
screeplot(ic)

n_var_lags <- vars::VARselect(ic$F_pca[, 1:ic$r.star[3]])

model <- DFM(
  df_var_1_train,
  r = ic$r.star[3],
  p = min(c(n_var_lags$selection %>% min(), 2))
)

plot(model, method = "all", type = "individual")

fitted(model, orig.format = TRUE) %>%
  mutate(PERIOD = train_dates) %>%
  pivot_longer(-PERIOD) %>%
  ggplot(aes(x = PERIOD, y = value, color = name)) +
  geom_line(show.legend = FALSE)

forecast_test <- predict(model, h = fc_periods)
forecast_test <- forecast_test$X_fcst %>%
  as_tibble(.name_repair = "minimal") %>%
  mutate(PERIOD = test_dates, type = "pred") %>%
  pivot_longer(
    -c(PERIOD, type),
    names_to = "gr",
    values_to = "stoim"
  )

df_var_1 %>%
  filter(PERIOD %in% test_dates) %>%
  mutate(type = "fact") %>%
  bind_rows(forecast_test) %>%
  filter(str_starts(gr, "1")) %>%
  ggplot(aes(x = PERIOD, y = stoim, color = type)) +
  geom_line() +
  facet_wrap(~gr)

forecast_level <-
  forecast_test %>%
  left_join(
    df_raw %>%
      filter(PERIOD == max(train_dates)) %>%
      select(gr, stoim_last = stoim),
    by = "gr"
  ) %>%
  arrange(gr, PERIOD) %>%
  mutate(
    stoim_level = expm1(log1p(stoim_last) + cumsum(stoim)),
    .by = gr
  ) %>%
  mutate(
    TNVED2 = substr(gr, start = 1, stop = 2),
    NAPR = substr(gr, start = 4, stop = 5)
  ) %>%
  select(PERIOD, TNVED2, NAPR, STOIM = stoim_level, type)

res_2 <-
  df_raw %>%
  select(PERIOD, TNVED2, NAPR, STOIM = stoim) %>%
  filter(PERIOD < fc_from) %>%
  mutate(type = "fact") %>%
  bind_rows(forecast_level)

res_2 %>%
  mutate(gr = paste0(TNVED2, "_", NAPR)) %>%
  filter(str_starts(TNVED2, "70")) %>%
  ggplot(aes(x = PERIOD, y = STOIM, color = type)) +
  geom_line() +
  facet_wrap(~gr, scales = "free")

# --------------------------------------------------------
# Decompose upper forecast to TNVED10 x STRANA weights
# --------------------------------------------------------

df_10 <-
  dbGetQuery(con, "
  SELECT PERIOD, STRANA, TNVED2, TNVED, NAPR, STOIM, NETTO, TYPE
  FROM unified_trade_data"
  ) %>%
  filter(TYPE == "fact") %>%
  filter(any(STOIM > 0), .by = c(STRANA, NAPR, TNVED)) %>%
  mutate(PERIOD = as_date(PERIOD)) %>%
  right_join(
    dbGetQuery(con, "
  SELECT STRANA, NAPR, TNVED, PERIOD, STOIM, NETTO, TYPE
  FROM unified_trade_data
               ") %>%
      filter(TYPE == "fact") %>%
      filter(any(STOIM > 0), .by = c(STRANA, NAPR, TNVED)) %>%
      distinct(STRANA, TNVED, NAPR) %>%
      cross_join(
        data.frame(
          PERIOD = seq.Date(
            min(df_raw$PERIOD),
            max(test_dates),
            by = "month"
          )
        )
      ),
    by = c("STRANA", "NAPR", "TNVED", "PERIOD")
  ) %>%
  mutate(
    STOIM = coalesce(STOIM, 0),
    NETTO = coalesce(NETTO, 0)
  ) %>%
  mutate(TNVED2 = substr(TNVED, start = 1, stop = 2)) %>%
  arrange(STRANA, TNVED, NAPR, PERIOD) %>%
  full_join(
    res_2 %>%
      select(PERIOD, TNVED2, NAPR, type, STOIM_ALL_2 = STOIM),
    by = c("PERIOD", "TNVED2", "NAPR")
  ) %>%
  # Critical fix:
  # protect against division by zero when upper-level value is 0.
  mutate(share = if_else(STOIM_ALL_2 > 0, STOIM / STOIM_ALL_2, 0)) %>%
  mutate(
    # Unit price only where volume is positive to avoid Inf.
    unit_price = if_else(NETTO > 0, STOIM / NETTO, NA_real_),
    share_mean = mean(share[PERIOD %in% tail(train_dates, 12)], na.rm = TRUE),
    price_mean = mean(unit_price[PERIOD %in% tail(train_dates, 12)], na.rm = TRUE),
    .by = c("TNVED", "STRANA", "NAPR")
  ) %>%
  mutate(
    # Stabilize missing or non-finite estimates.
    share_mean = if_else(is.finite(share_mean), share_mean, 0),
    price_mean = if_else(is.finite(price_mean) & price_mean > 0, price_mean, NA_real_)
  ) %>%
  mutate(
    share_mean = if_else(type == "pred", share_mean[type == "fact"][1], share_mean),
    price_mean = if_else(type == "pred", price_mean[type == "fact"][1], price_mean),
    .by = c("TNVED", "STRANA", "NAPR")
  ) %>%
  mutate(
    # Fallback keeps prediction finite even for sparse groups.
    price_mean = coalesce(price_mean, 1),
    stoim_fc = if_else(type == "pred", STOIM_ALL_2 * share_mean, STOIM_ALL_2 * share),
    netto_fc = if_else(type == "pred", stoim_fc / price_mean, STOIM_ALL_2 / price_mean)
  )

df_10_tidy <-
  df_10 %>%
  mutate(netto_fc = pmax(NETTO, netto_fc, na.rm = TRUE)) %>%
  select(
    STRANA,
    PERIOD,
    TNVED,
    NAPR,
    TYPE = type,
    STOIM = stoim_fc,
    NETTO = netto_fc
  ) %>%
  filter(!is.na(TYPE))

df_10_complementary <-
  dbGetQuery(con, "
  SELECT PERIOD, STRANA, TNVED, NAPR, STOIM, NETTO, TYPE
  FROM unified_trade_data"
  ) %>%
  filter(TYPE == "fact") %>%
  filter(PERIOD >= first(fc_from)) %>%
  arrange(STRANA, TNVED, NAPR, PERIOD) %>%
  mutate(TYPE = "fact")

write_parquet(
  df_10_tidy %>%
    anti_join(
      df_10_complementary,
      by = c("STRANA", "NAPR", "TNVED", "PERIOD")
    ) %>%
    bind_rows(df_10_complementary),
  "data_processed/nowcast/nowcast.parquet"
)
