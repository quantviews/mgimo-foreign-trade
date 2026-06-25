# Рисунок выкладки по физобъёмам

pl_fizob <- 
  bulletin_fo %>%
  mutate(  STRANA = case_when(
           STRANA == 'CN' ~ 'Китай',
           STRANA == 'TR' ~ 'Турция',
           STRANA == 'IN' ~ 'Индия',
           STRANA == 'OTHER' ~ 'Прочие'
         ) %>% factor(
           levels = c(
             'Китай',
             'Индия',
             'Турция',
             'Прочие'
           )
         )
  ) %>%
  mutate(
    fizob = 100* round(fizob, 3)
  ) %>%
  rename(
    Дата = PERIOD,
    Направление = NAPR,
    `Изменение физобъёма, %` =  fizob
  ) %>%
  ggplot(aes(x = Дата,
             y = `Изменение физобъёма, %`,
             color = Направление)
  ) +
  geom_hline(yintercept = 0,
             linetype = 'dashed',
             color = 'grey50',
             linewidth = 0.3) +
  geom_line() +
  facet_wrap(~STRANA, scales = 'free') +
  scale_y_continuous(labels = scales::percent_format(accuracy = 1, scale = 1, style_positive = 'plus'),
                     expand = c(0, 0)) +
  scale_x_date(date_breaks = "1 years", date_labels = "%Y",
               expand = c(0.01, 0.01)) +
  scale_color_manual(
    values =  c(mgimo_colors[["primary"]], mgimo_colors[['secondary']]),
    breaks = c('ИМ', 'ЭК')
  ) +
  labs(
    x = NULL,
    y = NULL,
    color = NULL #'Направление'
  ) +
  theme_classic(#base_family = "Computer Modern", 
    base_size = 13) +
  coord_cartesian(clip = 'off') +
  theme(
    legend.position = "bottom",
    legend.key.width = unit(1.1, "cm"),
    strip.background = element_blank(),
    strip.text = element_text(face = "bold"),
    axis.text.x = element_text(size = 10),
    panel.spacing.y = unit(0.5, "cm")
  )

ggplotly(pl_fizob, dynamicTicks = F) %>%
  layout(
    legend = list(
      orientation = "h",
      x = 0.5,
      xanchor = "center",
      y = -0.06   # чем меньше значение, тем ниже легенда
    )
  )

#-----------------------
# Рисунок по нефтегазовому и ненефтегазовому экспорту
#-----------------------

pl_oil_nonoil <- 
  tab_stoim_oil %>%
  mutate(  STRANA = case_when(
           STRANA == 'CN' ~ 'Китай',
           STRANA == 'TR' ~ 'Турция',
           STRANA == 'IN' ~ 'Индия',
           STRANA == 'OTHER' ~ 'Прочие'
         ) %>% factor(
           levels = c(
             'Китай',
             'Индия',
             'Турция',
             'Прочие'
           )
         ),
         STOIM = round(STOIM, 3)
  ) %>%
  rename(
    Дата = PERIOD,
    Тип = type,
    `Экспорт, млрд. долл.` =  STOIM
  ) %>%
  ggplot(aes(x = Дата,
             y = `Экспорт, млрд. долл.`,
             color = Тип)
  ) +
  geom_hline(yintercept = 0,
             linetype = 'dashed',
             color = 'grey50',
             linewidth = 0.3) +
  geom_line() +
  facet_wrap(~STRANA, scales = 'free') +
  scale_y_continuous(expand = c(0, 0)) +
  scale_x_date(date_breaks = "1 years", date_labels = "%Y",
               expand = c(0.01, 0.01)) +
  scale_color_manual(
    values =  c(mgimo_colors[["accent1"]], mgimo_colors[['accent3']]),
    breaks = c('Кроме нефти и газа', 'Нефтегазовый')
  ) +
  labs(
    x = NULL,
    y = NULL,
    color = NULL #'Направление'
  ) +
  theme_classic(#base_family = "Computer Modern", 
    base_size = 13) +
  coord_cartesian(clip = 'off') +
  theme(
    legend.position = "bottom",
    legend.key.width = unit(1.1, "cm"),
    strip.background = element_blank(),
    strip.text = element_text(face = "bold"),
    axis.text.x = element_text(size = 10),
    panel.spacing.y = unit(0.5, "cm")
  )

ggplotly(pl_oil_nonoil, dynamicTicks = F) %>%
  layout(
    legend = list(
      orientation = "h",
      x = 0.5,
      xanchor = "center",
      y = -0.06   # чем меньше значение, тем ниже легенда
    )
  )

# График по товарным группам: экспорт

names_4 <- 
  read_parquet('data_comparison/bulletin_data_prep/metadata/hs4_labels.parquet') %>%
  select(TNVED4 = hs4,
         TNVED4_string = name_ru_short)

df_groups %>%
  left_join(names_4,
            by = 'TNVED4'
  ) %>%
  filter(NAPR == 'ЭК') %>%
  mutate(STOIM_diff_abs = abs(STOIM_diff),
         type = if_else(STOIM_diff > 0, 'positive', 'negative'),
         position_label = (STOIM_last12 + STOIM_year_before) / 2,
         label = if_else(STOIM_diff > 0,
                         paste0("+", round(STOIM_diff, 1)),
                         paste0(round(STOIM_diff, 1))
         )
  ) %>%
  arrange(-STOIM_diff_abs) %>%
  head(20) %>%
  mutate(TNVED4_string = fct_reorder(TNVED4_string, STOIM_last12)) %>%
  ggplot(aes(color = type)) +
  geom_segment(aes(x = STOIM_year_before, xend = STOIM_last12, y = TNVED4_string, yend = TNVED4_string)) +
  geom_point(aes(x = STOIM_last12, y = TNVED4_string), fill = 'white', shape = 21) +
  scale_color_manual(
    values =  c(mgimo_colors[["accent1"]], mgimo_colors[['secondary']]),
    breaks = c('positive', 'negative')
  ) +
  geom_text(aes(x = position_label, y = TNVED4_string, label = label),
            nudge_y = 0.42) +
  scale_x_break(c(38, 98)) +
  coord_cartesian(clip = 'off') +
  geom_vline(xintercept = c(39.5, 97.5),
             color = 'grey50',
             linetype = 'solid',
             linewidth = 1.7) +
  theme_minimal() +
  theme(panel.grid.minor = element_blank(),
        panel.grid.major.y = element_blank(),
        legend.position = 'none'
  ) +
  labs(y = NULL,
       x = 'Млрд. долл')

# График по товарным группам: импорт

df_groups %>%
  left_join(names_4,
            by = 'TNVED4'
  ) %>%
  filter(NAPR == 'ИМ') %>%
  mutate(STOIM_diff_abs = abs(STOIM_diff),
         type = if_else(STOIM_diff > 0, 'positive', 'negative'),
         position_label = (STOIM_last12 + STOIM_year_before) / 2,
         label = if_else(STOIM_diff > 0,
                         paste0("+", round(STOIM_diff, 1)),
                         paste0(round(STOIM_diff, 1))
         )
  ) %>%
  arrange(-STOIM_diff_abs) %>%
  head(20) %>%
  mutate(TNVED4_string = fct_reorder(TNVED4_string, STOIM_last12)) %>%
  ggplot(aes(color = type)) +
  geom_segment(aes(x = STOIM_year_before, xend = STOIM_last12, y = TNVED4_string, yend = TNVED4_string)) +
  geom_point(aes(x = STOIM_last12, y = TNVED4_string), fill = 'white', shape = 21) +
  scale_color_manual(
    values =  c(mgimo_colors[["accent1"]], mgimo_colors[['secondary']]),
    breaks = c('positive', 'negative')
  ) +
  geom_text(aes(x = position_label, y = TNVED4_string, label = label),
            nudge_y = 0.42) +
  #scale_x_break(c(38, 98)) +
  coord_cartesian(clip = 'off') +
  #geom_vline(xintercept = c(39.5, 97.5),
  #           color = 'grey50',
  #           linetype = 'solid',
  #           linewidth = 1.7) +
  theme_minimal() +
  theme(panel.grid.minor = element_blank(),
        panel.grid.major.y = element_blank(),
        legend.position = 'none'
  ) +
  labs(y = NULL,
       x = 'Млрд. долл')
