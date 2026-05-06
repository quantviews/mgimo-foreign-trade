library(tidyverse)
library(duckdb)
library(arrow)

# ------------------------------------------------------------
# Compare nowcast predictions vs factual data for overlap period.
# Use case: 2025 and partial 2026 facts are already available for some countries.
# ------------------------------------------------------------

con <- dbConnect(
  duckdb::duckdb(),
  "db/unified_trade_data.duckdb",
  read_only = TRUE
)

# You can set an explicit path, otherwise the script will try common locations.
nowcast_path <- NULL
compare_from <- as_date("2025-10-01")
# Fixed window for validation against current nowcast horizon.
compare_to <- as_date("2025-12-01")
# Optional country filter, e.g. c("КИТАЙ", "ТУРЦИЯ")
countries_filter <- NULL

# Normalize join keys to avoid silent mismatches:
# - TNVED can lose leading zeros if treated as numeric;
# - NAPR/STRANA can differ by type/spaces/case.
normalize_keys <- function(df) {
  df %>%
    mutate(
      PERIOD = as_date(PERIOD),
      STRANA = STRANA %>% as.character() %>% stringr::str_squish() %>% stringr::str_to_upper(),
      TNVED = TNVED %>% as.character() %>% stringr::str_replace_all("\\.0$", "") %>% stringr::str_pad(width = 10, side = "left", pad = "0"),
      NAPR = NAPR %>% as.character() %>% stringr::str_squish()
    )
}

resolve_nowcast_path <- function(path_override = NULL) {
  candidates <- c(
    path_override,
    "data_processed/nowcast/nowcast.parquet",
    "data_processed/nowcast.parquet"
  ) %>%
    unique() %>%
    discard(is.null)

  existing <- candidates[file.exists(candidates)]
  if (length(existing) == 0) {
    stop("nowcast parquet not found. Checked: ", paste(candidates, collapse = "; "))
  }
  existing[[1]]
}

# ---------------------------
# Load predictions from nowcast
# ---------------------------
nowcast_file <- resolve_nowcast_path(nowcast_path)
message("Using nowcast file: ", nowcast_file)

nowcast_pred <- read_parquet(nowcast_file) %>%
  as_tibble() %>%
  normalize_keys() %>%
  filter(TYPE == "pred")

# ---------------------------
# Load factual data from source
# ---------------------------
fact_raw <- dbGetQuery(
  con,
  "
  SELECT PERIOD, STRANA, TNVED, NAPR, STOIM, NETTO
  FROM unified_trade_data
  "
) %>%
  as_tibble() %>%
  normalize_keys()

if (!is.null(countries_filter)) {
  nowcast_pred <- nowcast_pred %>% filter(STRANA %in% countries_filter)
  fact_raw <- fact_raw %>% filter(STRANA %in% countries_filter)
}

if (is.null(compare_to)) {
  compare_to <- fact_raw %>%
    filter(PERIOD >= compare_from) %>%
    summarise(max_period = max(PERIOD, na.rm = TRUE)) %>%
    pull(max_period)
}

if (!is.finite(compare_to)) {
  stop("No factual data found from compare_from onward. Check compare_from/countries_filter.")
}

# ---------------------------
# Align keys and period
# ---------------------------
pred_slice <- nowcast_pred %>%
  filter(PERIOD >= compare_from, PERIOD <= compare_to) %>%
  select(PERIOD, STRANA, TNVED, NAPR, STOIM_pred = STOIM, NETTO_pred = NETTO)

fact_slice <- fact_raw %>%
  filter(PERIOD >= compare_from, PERIOD <= compare_to) %>%
  group_by(PERIOD, STRANA, TNVED, NAPR) %>%
  summarise(
    STOIM_fact = sum(STOIM, na.rm = TRUE),
    NETTO_fact = sum(NETTO, na.rm = TRUE),
    .groups = "drop"
  )

# Fast diagnostics before join.
period_overlap_n <- length(intersect(unique(pred_slice$PERIOD), unique(fact_slice$PERIOD)))
message("Pred rows in window: ", nrow(pred_slice))
message("Fact rows in window: ", nrow(fact_slice))
message("Overlapping months: ", period_overlap_n)

cmp_detail <- pred_slice %>%
  inner_join(fact_slice, by = c("PERIOD", "STRANA", "TNVED", "NAPR")) %>%
  mutate(
    err_stoim = STOIM_pred - STOIM_fact,
    abs_err_stoim = abs(err_stoim),
    ape_stoim = if_else(STOIM_fact != 0, abs_err_stoim / abs(STOIM_fact), NA_real_),
    smape_stoim = if_else(
      (abs(STOIM_pred) + abs(STOIM_fact)) > 0,
      2 * abs_err_stoim / (abs(STOIM_pred) + abs(STOIM_fact)),
      NA_real_
    )
  )

if (nrow(cmp_detail) == 0) {
  # Explain why empty result happened.
  key_pred <- pred_slice %>% distinct(PERIOD, STRANA, TNVED, NAPR)
  key_fact <- fact_slice %>% distinct(PERIOD, STRANA, TNVED, NAPR)
  overlap_months <- intersect(unique(key_pred$PERIOD), unique(key_fact$PERIOD))
  month_only_overlap <- key_pred %>% semi_join(key_fact %>% distinct(PERIOD), by = "PERIOD")
  message("No overlap on full key PERIOD+STRANA+TNVED+NAPR.")
  message("Pred distinct keys: ", nrow(key_pred), "; Fact distinct keys: ", nrow(key_fact))
  message("Rows with month overlap only (pred side): ", nrow(month_only_overlap))
  message("Common months: ", paste(as.character(overlap_months), collapse = ", "))
  message("Hint: inspect TNVED formatting and country names in both sources.")
}

# ---------------------------
# Coverage diagnostics
# ---------------------------
coverage_by_country_month <- pred_slice %>%
  distinct(PERIOD, STRANA, TNVED, NAPR) %>%
  mutate(has_pred = TRUE) %>%
  full_join(
    fact_slice %>%
      distinct(PERIOD, STRANA, TNVED, NAPR) %>%
      mutate(has_fact = TRUE),
    by = c("PERIOD", "STRANA", "TNVED", "NAPR")
  ) %>%
  mutate(
    has_pred = coalesce(has_pred, FALSE),
    has_fact = coalesce(has_fact, FALSE),
    overlap = has_pred & has_fact
  ) %>%
  group_by(PERIOD, STRANA) %>%
  summarise(
    n_pred_series = sum(has_pred),
    n_fact_series = sum(has_fact),
    n_overlap_series = sum(overlap),
    overlap_ratio = if_else(n_pred_series > 0, n_overlap_series / n_pred_series, 0),
    .groups = "drop"
  ) %>%
  arrange(PERIOD, desc(overlap_ratio))

# ---------------------------
# Metrics by country and month
# ---------------------------
cmp_country_month <- cmp_detail %>%
  group_by(PERIOD, STRANA) %>%
  summarise(
    STOIM_pred = sum(STOIM_pred, na.rm = TRUE),
    STOIM_fact = sum(STOIM_fact, na.rm = TRUE),
    MAE = mean(abs_err_stoim, na.rm = TRUE),
    MAPE = mean(ape_stoim, na.rm = TRUE),
    sMAPE = mean(smape_stoim, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(
    bias = STOIM_pred - STOIM_fact,
    bias_pct = if_else(STOIM_fact != 0, bias / STOIM_fact, NA_real_)
  ) %>%
  arrange(PERIOD, STRANA)

cmp_country_total <- cmp_country_month %>%
  group_by(STRANA) %>%
  summarise(
    months_compared = n_distinct(PERIOD),
    STOIM_pred_total = sum(STOIM_pred, na.rm = TRUE),
    STOIM_fact_total = sum(STOIM_fact, na.rm = TRUE),
    MAE_avg = mean(MAE, na.rm = TRUE),
    MAPE_avg = mean(MAPE, na.rm = TRUE),
    sMAPE_avg = mean(sMAPE, na.rm = TRUE),
    bias_total = sum(bias, na.rm = TRUE),
    bias_pct_total = if_else(STOIM_fact_total != 0, bias_total / STOIM_fact_total, NA_real_),
    .groups = "drop"
  ) %>%
  arrange(desc(abs(bias_pct_total)))

# ---------------------------
# Aggregate scorecards
# ---------------------------
calc_scorecard <- function(df, segment_label) {
  df %>%
    summarise(
      segment = segment_label,
      n_rows = n(),
      n_countries = n_distinct(STRANA),
      n_months = n_distinct(PERIOD),
      STOIM_pred_total = sum(STOIM_pred, na.rm = TRUE),
      STOIM_fact_total = sum(STOIM_fact, na.rm = TRUE),
      abs_err_total = sum(abs_err_stoim, na.rm = TRUE),
      bias_total = sum(err_stoim, na.rm = TRUE),
      WAPE = if_else(STOIM_fact_total > 0, abs_err_total / STOIM_fact_total, NA_real_),
      bias_pct = if_else(STOIM_fact_total > 0, bias_total / STOIM_fact_total, NA_real_),
      MAPE_mean = mean(ape_stoim, na.rm = TRUE),
      sMAPE_mean = mean(smape_stoim, na.rm = TRUE)
    )
}

top20_countries <- cmp_detail %>%
  group_by(STRANA) %>%
  summarise(STOIM_fact_total = sum(STOIM_fact, na.rm = TRUE), .groups = "drop") %>%
  arrange(desc(STOIM_fact_total)) %>%
  slice_head(n = 20) %>%
  pull(STRANA)

overall_metrics <- calc_scorecard(cmp_detail, "overall")
top20_metrics <- calc_scorecard(cmp_detail %>% filter(STRANA %in% top20_countries), "top20_by_fact")
tail_metrics <- calc_scorecard(cmp_detail %>% filter(!STRANA %in% top20_countries), "tail_ex_top20")

scorecards <- bind_rows(overall_metrics, top20_metrics, tail_metrics)

# Largest contributors to absolute model error (where to debug first).
worst_country_code <- cmp_detail %>%
  group_by(STRANA, TNVED, NAPR) %>%
  summarise(
    STOIM_pred_total = sum(STOIM_pred, na.rm = TRUE),
    STOIM_fact_total = sum(STOIM_fact, na.rm = TRUE),
    abs_err_total = sum(abs_err_stoim, na.rm = TRUE),
    bias_total = sum(err_stoim, na.rm = TRUE),
    WAPE = if_else(STOIM_fact_total > 0, abs_err_total / STOIM_fact_total, NA_real_),
    bias_pct = if_else(STOIM_fact_total > 0, bias_total / STOIM_fact_total, NA_real_),
    .groups = "drop"
  ) %>%
  arrange(desc(abs_err_total))

worst_country <- cmp_detail %>%
  group_by(STRANA) %>%
  summarise(
    STOIM_pred_total = sum(STOIM_pred, na.rm = TRUE),
    STOIM_fact_total = sum(STOIM_fact, na.rm = TRUE),
    abs_err_total = sum(abs_err_stoim, na.rm = TRUE),
    bias_total = sum(err_stoim, na.rm = TRUE),
    WAPE = if_else(STOIM_fact_total > 0, abs_err_total / STOIM_fact_total, NA_real_),
    bias_pct = if_else(STOIM_fact_total > 0, bias_total / STOIM_fact_total, NA_real_),
    .groups = "drop"
  ) %>%
  arrange(desc(abs_err_total))

# ---------------------------
# Quick visual check (country-level sums)
# ---------------------------
country_order <- cmp_country_month %>%
  group_by(STRANA) %>%
  summarise(
    STOIM_fact_total = sum(STOIM_fact, na.rm = TRUE),
    months_with_fact = sum(!is.na(STOIM_fact) & STOIM_fact > 0),
    .groups = "drop"
  ) %>%
  filter(months_with_fact > 0) %>%
  arrange(desc(STOIM_fact_total)) %>%
  pull(STRANA)

plot_country_month <- cmp_country_month %>%
  filter(STRANA %in% country_order) %>%
  mutate(STRANA = factor(STRANA, levels = country_order)) %>%
  select(PERIOD, STRANA, STOIM_pred, STOIM_fact) %>%
  pivot_longer(cols = c(STOIM_pred, STOIM_fact), names_to = "series", values_to = "value") %>%
  ggplot(aes(x = PERIOD, y = value, color = series)) +
  geom_line() +
  facet_wrap(~STRANA, scales = "free_y") +
  labs(
    title = "Nowcast vs Fact (countries with factual data, sorted by trade volume)",
    x = NULL,
    y = "STOIM"
  )

print(plot_country_month)

# ---------------------------
# Save outputs for review
# ---------------------------
out_dir <- "data_processed/nowcast_validation"
dir.create(path.expand(out_dir), recursive = TRUE, showWarnings = FALSE)

write_parquet(cmp_detail, file.path(path.expand(out_dir), "cmp_detail.parquet"))
write_parquet(cmp_country_month, file.path(path.expand(out_dir), "cmp_country_month.parquet"))
write_parquet(cmp_country_total, file.path(path.expand(out_dir), "cmp_country_total.parquet"))
write_parquet(coverage_by_country_month, file.path(path.expand(out_dir), "coverage_by_country_month.parquet"))
write_parquet(scorecards, file.path(path.expand(out_dir), "scorecards.parquet"))
write_parquet(worst_country, file.path(path.expand(out_dir), "worst_country.parquet"))
write_parquet(worst_country_code, file.path(path.expand(out_dir), "worst_country_code.parquet"))

message("Comparison window: ", as.character(compare_from), " ... ", as.character(compare_to))
message("Compared rows (detail): ", nrow(cmp_detail))
message("Scorecards:")
print(scorecards)
message("Top-10 worst countries by abs error:")
print(worst_country %>% slice_head(n = 10))
message("Top-10 worst country-code pairs by abs error:")
print(worst_country_code %>% slice_head(n = 10))
message("Saved outputs to: ", path.expand(out_dir))

