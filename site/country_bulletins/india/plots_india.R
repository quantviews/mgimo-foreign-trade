library(tidyverse)
library(arrow)
library(ggbreak)
library(patchwork)
library(sysfonts)
library(showtext)
library(plotly)

mgimo_colors <- c(
  primary    = "#003d7a",    # МГИМО синий основной
  secondary  = "#e74c3c",    # Красный акцент
  accent1    = "#27ae60",    # Зелёный для позитива
  accent2    = "#3498db",    # Голубой
  accent3    = "#f39c12",    # Оранжевый
  text       = "#2c3e50",    # Тёмный серо-голубой
  light_bg   = "#f8f9fa"     # Светлый фон
)

font_add_google("Source Sans 3", "ssp")   # в showtextdb нет "Source Sans Pro"
showtext_auto()

clean_legend_plotly <- function(p){
  for (i in 1:length(p$x$data)) {
    if (!is.null(p$x$data[[i]]$name)) {
      p$x$data[[i]]$name <- str_extract(p$x$data[[i]]$name, "(?<=\\()[^,]*(?=,)")
    }
  }
  p
}

date_min <- read_parquet("site/country_bulletins/india/data/trade_balance_india.parquet") %>%
  pull(PERIOD) %>%
  min()

date_max <- read_parquet("site/country_bulletins/india/data/trade_balance_india.parquet") %>%
  pull(PERIOD) %>%
  max()

pl_tb <- 
  read_parquet("site/country_bulletins/india/data/trade_balance_india.parquet") %>%
  mutate(STOIM = STOIM / 10^9) %>%
  ggplot() +
  geom_col(
    data = ~ filter(.x, NAPR == "ТБ"),
    aes(PERIOD, STOIM, fill = NAPR)
  ) +
  geom_line(
    data = ~ filter(.x, NAPR != "ТБ"),
    aes(PERIOD, STOIM, color = NAPR),
    linewidth = 1
  ) +
  scale_color_manual(
    values = c(
      "ЭК" = mgimo_colors[["secondary"]],
      "ИМ"  = mgimo_colors[["primary"]]
    ),
    name = NULL
  ) +
  scale_fill_manual(
    values = c(
      "ТБ" = mgimo_colors[["accent1"]]
    ),
    name = NULL
  ) +
  scale_x_date(
    breaks = seq(date_min, date_max, by = 'year'),
    labels = scales::label_date("%Y")
    ) +
  theme_classic(base_size = 13, base_family = "ssp") +
  coord_cartesian(clip = 'off') +
  theme(
    legend.position = "bottom",
    legend.key.width = unit(1.1, "cm"),
    strip.background = element_blank(),
    axis.text.x = element_text(size = 10),
    panel.spacing.y = unit(3, "cm"),
    panel.spacing.x = unit(3, 'cm'),
    panel.background = element_rect(fill = 'transparent'),
    plot.background = element_rect(fill = 'transparent'),
    legend.background = element_rect(fill = 'transparent'),
  ) +
  labs(x = NULL,
       y = 'млрд $')

ggplotly(pl_tb) %>%
  clean_legend_plotly()

# Рисунки с категориями
# Нефть временной ряд (тыс т)
# Кроме нефти изменения импорт экспорт

# Импорт

pl_im_ind <-
read_parquet('site/country_bulletins/india/data/data_4_india.parquet') %>%
  filter(NAPR == "ИМ") %>%
  mutate(across(
    c(STOIM_last12, STOIM_year_before, STOIM_diff) ,
    ~ .x * 1000
    )
    ) %>%
  mutate(
    STOIM_diff_abs = abs(STOIM_diff),
    type = if_else(STOIM_diff > 0, "positive", "negative"),
    position_label = (STOIM_last12 + STOIM_year_before) / 2,
    label = if_else(STOIM_diff > 0, paste0("+", round(STOIM_diff, 1)), paste0(round(STOIM_diff, 1)))
  ) %>%
  arrange(-STOIM_diff_abs) %>%
  head(15) %>%
  mutate(TNVED4_string = fct_reorder(TNVED4_string, STOIM_last12)) %>%
  ggplot(aes(color = type)) +
  geom_segment(aes(x = STOIM_year_before, xend = STOIM_last12, y = TNVED4_string, yend = TNVED4_string)) +
  geom_point(aes(x = STOIM_last12, y = TNVED4_string),
             #fill = site_bg,
             shape = 21) +
  scale_color_manual(
    values = c(mgimo_colors[["accent1"]], mgimo_colors[["secondary"]]),
    breaks = c("positive", "negative")
  ) +
  geom_text(
    aes(x = position_label, y = TNVED4_string, label = label),
    nudge_y = 0.41,
    size = 5) +
  #scale_x_break(c(5, 40)) +
  coord_cartesian(clip = "off") +
  #geom_vline(xintercept = c(5, 40), color = "grey50", linetype = "solid", linewidth = 1.7) +
  #theme_groups() +
  labs(y = NULL, x = "млн долл.")

# Экспорт

pl_ex_ind <-
  read_parquet('site/country_bulletins/india/data/data_4_india.parquet') %>%
  filter(NAPR == "ЭК",
         TNVED4 != '2709') %>%
  mutate(across(
    c(STOIM_last12, STOIM_year_before, STOIM_diff) ,
    ~ .x * 1000
  )
  ) %>%
  mutate(
    STOIM_diff_abs = abs(STOIM_diff),
    type = if_else(STOIM_diff > 0, "positive", "negative"),
    position_label = (STOIM_last12 + STOIM_year_before) / 2,
    label = if_else(STOIM_diff > 0, paste0("+", round(STOIM_diff, 1)), paste0(round(STOIM_diff, 1)))
  ) %>%
  arrange(-STOIM_diff_abs) %>%
  head(15) %>%
  mutate(TNVED4_string = fct_reorder(TNVED4_string, STOIM_last12)) %>%
  ggplot(aes(color = type)) +
  geom_segment(aes(x = STOIM_year_before, xend = STOIM_last12, y = TNVED4_string, yend = TNVED4_string)) +
  geom_point(aes(x = STOIM_last12, y = TNVED4_string),
             #fill = site_bg,
             shape = 21) +
  scale_color_manual(
    values = c(mgimo_colors[["accent1"]], mgimo_colors[["secondary"]]),
    breaks = c("positive", "negative")
  ) +
  geom_text(
    aes(x = position_label, y = TNVED4_string, label = label),
    nudge_y = 0.41,
    size = 5) +
  coord_cartesian(clip = "off") +
  #theme_groups() +
  labs(y = NULL, x = "млн долл.", title = 'Экспорт')

# Нефть прочая ( + доля Индии в общем экспорте нефти?)

pl_neft_ind <- 
  read_parquet('site/country_bulletins/india/data/data_oil_export_india.parquet') %>%
  ggplot(
    aes(
      x = PERIOD,
      y = NETTO)
    ) +
  geom_line(color = mgimo_colors[['primary']]) +
  geom_point(shape = 21, fill = 'white',
             color = mgimo_colors[['primary']]) +
  scale_x_date(
    breaks = seq(date_min, date_max, by = 'year'),
    labels = scales::label_date("%Y")
  ) +
  theme_classic(base_size = 13, base_family = "Source Sans Pro") +
  #coord_cartesian(clip = 'off') +
  theme(
    strip.background = element_blank(),
    axis.text.x = element_text(size = 10),
    panel.background = element_rect(fill = 'transparent'),
    plot.background = element_rect(fill = 'transparent'),
    legend.background = element_rect(fill = 'transparent'),
  ) +
  labs(x = NULL,
       y = 'млн т'
       )

(pl_neft_ind) /
  (pl_ex_ind + pl_im_ind) +
  plot_layout(heights = c(2, 2),
              widths = c(2, 4))

pl_ex_ind / pl_im_ind

pl_ex_ind +
  theme(
    plot.title.position = "plot",
    plot.title = ggtext::element_textbox_simple(
      size = 18,
      face = "bold",
      family = 'ssp',
      color = mgimo_colors[["primary"]],
      halign = 0.05,
      padding = margin(4, 10, 4, 10),
      margin = margin(0, 0, 6, r = 0),
      fill = 'transparent',
      box.color = mgimo_colors[["primary"]],
      linewidth = 2,
      r = unit(6, "pt")
  )
  )

  