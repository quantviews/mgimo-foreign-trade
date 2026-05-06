library(tidyverse)
library(duckdb)
library(dfms)

con <- dbConnect(
  duckdb::duckdb(),
  "db/unified_trade_data.duckdb",
  read_only = TRUE
)

dbGetQuery(con, "SHOW TABLES")

# Baseline idea:
# 1) forecast upper level (TNVED2 x NAPR),
# 2) then distribute forecast to lower levels separately.

df_raw <- dbGetQuery(con, "
  SELECT PERIOD, TNVED2, NAPR, STOIM
  FROM unified_trade_data"
) %>%
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

# Fixed holdout split.
train_dates <- seq(
  from = df_var_1 %>% pull(PERIOD) %>% first(),
  to = ymd("2024-12-01"),
  by = "month"
)
test_dates <- seq(
  from = ymd("2025-01-01"),
  to = df_var_1 %>% pull(PERIOD) %>% last(),
  by = "month"
)

df_var_1 %>%
  ggplot(aes(x = PERIOD, y = stoim, color = gr)) +
  geom_line(show.legend = FALSE)

df_var_1_train <-
  df_var_1 %>%
  pivot_wider(names_from = gr, values_from = stoim) %>%
  filter(PERIOD %in% train_dates) %>%
  select(-PERIOD) %>%
  # Sparse groups can produce NA after pivot; for growth rates, 0 is a safe neutral fallback.
  mutate(across(everything(), ~coalesce(.x, 0)))

ic <- ICr(df_var_1_train)
plot(ic)
screeplot(ic)
r_star <- max(1, as.integer(ic$r.star[3]))
n_var_lags <- vars::VARselect(ic$F_pca[, 1:r_star, drop = FALSE])
p_star <- max(1, as.integer(min(n_var_lags$selection, na.rm = TRUE)))

model_1 <- DFM(
  df_var_1_train,
  r = r_star,
  p = p_star
)

plot(model_1, method = "all", type = "individual")

fitted(model_1, orig.format = TRUE) %>%
  mutate(PERIOD = train_dates) %>%
  pivot_longer(-PERIOD) %>%
  ggplot(aes(x = PERIOD, y = value, color = name)) +
  geom_line(show.legend = FALSE)

forecast_test <- predict(model_1, h = length(test_dates))
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
  filter(str_starts(gr, "6")) %>%
  ggplot(aes(x = PERIOD, y = stoim, color = type)) +
  geom_line() +
  facet_wrap(~gr)

# Critical fix:
# cumsum(stoim) must be done per group, otherwise groups leak into each other.
forecast_level <-
  forecast_test %>%
  left_join(
    df_raw %>%
      filter(PERIOD == last(train_dates)) %>%
      select(gr, stoim_last = stoim),
    by = "gr"
  ) %>%
  mutate(stoim_last = coalesce(stoim_last, 0)) %>%
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
