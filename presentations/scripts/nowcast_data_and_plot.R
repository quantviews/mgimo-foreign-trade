#-------------------------------------------------
# Подготовка данных для визуализации по наукасту -
#-------------------------------------------------

dbListFields(con, "unified_trade_data_enriched")
dbListTables(con)

dbGetQuery(con, '
           SELECT STRANA, NAPR, PERIOD, TNVED2, TYPE, STOIM, NETTO
           FROM unified_trade_data_enriched') %>%
  filter(TNVED2 == '22',
         STRANA == 'ES',
         NAPR == 'ИМ',
         PERIOD > ymd('2020-01-01')) %>%
  reframe(
    across(
      c(STOIM, NETTO),
      ~sum(.x, na.rm = T)
    ),
    .by = c(NAPR, PERIOD, TYPE)
  ) %>%
  bind_rows(
    tibble(NAPR ='ИМ',
           PERIOD = ymd('2026-02-01'),
           TYPE = 'pred',
           STOIM = 4216079.6,
           NETTO = 1233364.3
    )
  ) %>%
  mutate(NETTO = NETTO / 10^3,
         TYPE = if_else(TYPE == 'fact', 'Факт', 'Прогноз'))  %>%
  ggplot(
    aes(
      x = PERIOD, y = NETTO, color = TYPE
    )
  ) +
  geom_line() +
  geom_point(shape = 21,
             fill = 'white',
             show.legend = F) +
  scale_color_manual(
    values =  c(mgimo_colors[["primary"]], mgimo_colors[['secondary']]),
    breaks = c('Факт', 'Прогноз')
  ) +
  labs(
    x = NULL,
    y = NULL,
    color = NULL
  ) +
  theme_classic(base_size = 13, base_family = "Source Sans Pro") +
  coord_cartesian(clip = 'off') +
  theme(
    legend.position = "right",
    legend.key.width = unit(1.1, "cm"),
    strip.background = element_blank(),
    axis.text.x = element_text(size = 10),
    panel.spacing.y = unit(3, "cm"),
    panel.spacing.x = unit(3, 'cm'),
    panel.background = element_rect(fill = 'transparent'),
    plot.background = element_rect(fill = 'transparent'),
    #legend.background = element_rect(fill = 'transparent'),
    legend.background = element_rect(color = "black", fill = "transparent", linewidth = 0.5)
  )

ggsave('presentations/figures/nowcast.svg')