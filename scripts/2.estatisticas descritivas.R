rm(list = ls())

# __________________________ ----
# ESTATÍSTICAS DESCRITIVAS ----
# ¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨----

# 0. Carregar pacotes ----
library(arrow)
library(DataExplorer)
library(dplyr)
library(skimr)
library(tidyr)
library(writexl)
library(lubridate)
library(ggplot2)
library(forcats)
library(scales)
library(showtext)

# 1. Carregar bases ----
caminho_draft <- "../data/02_interim"
caminho_resultados <- "../data/03_processed"

#___1.1. ANS ----
nome_ans_completa <- "ans_15.23_completa.parquet"
caminho_ans_completa <- file.path(caminho_resultados, nome_ans_completa)
ans_15.23 <- read_parquet(caminho_ans_completa)

nome_ans_final <- "ans_cmed_15_23_intersecao_final.parquet"
caminho_ans_final <- file.path(caminho_resultados, nome_ans_final)
ans_cmed_15_23_intersecao <- read_parquet(caminho_ans_final)

names(ans_cmed_15_23_intersecao)
ans_cmed_15_23_intersecao = ans_cmed_15_23_intersecao |>
  rename(
    PRECO_UNITARIO = VALOR
  ) 

n_zeros <- sum(ans_cmed_15_23_intersecao$PRECO_UNITARIO == 0, na.rm = TRUE)
n_zeros
names(ans_cmed_15_23_intersecao)

num_registros_unicos_ans <- ans_cmed_15_23_intersecao %>%
  summarise(total_unicos = n_distinct(REGISTRO))
num_registros_unicos_ans

#___1.2. BPS ----
nome_bps_completa <- "bps_97.24_completa.parquet"
caminho_bps_completa <- file.path(caminho_resultados, nome_bps_completa)
bps_97.24 <- read_parquet(caminho_bps_completa)

nome_bps_final <- "bps.cmed_15.23_intersecao_final.parquet"
caminho_bps_final <- file.path(caminho_resultados, nome_bps_final)
bps.cmed_15.23_intersecao <- read_parquet(caminho_bps_final)

names(bps.cmed_15.23_intersecao)


#___1.3. CMED ----
nome_arquivo_cmed <- "cmed_10.24_completa.parquet"
dir.parquet_cmed_10.24 <- file.path(caminho_resultados, nome_arquivo_cmed)
cmed_10.24 <- read_parquet(dir.parquet_cmed_10.24)



# 2. Análise preliminar ----

# Função para gerar relatório descritivo para um data frame
gera_relatorio <- function(df, nome_dataset) {
  cat("--------------------------------------------------------\n")
  cat(paste("Relatório de Estatísticas Descritivas para:", nome_dataset, "\n\n"))
  
  # Resumo básico utilizando summary()
  cat("Resumo básico (summary):\n")
  print(summary(df))
  cat("\n")
  
  # Cálculo de estatísticas adicionais para colunas numéricas
  numeric_vars <- sapply(df, is.numeric)
  if (sum(numeric_vars) > 0) {
    cat("Estatísticas adicionais para variáveis numéricas:\n")
    stats <- df %>%
      select(which(numeric_vars)) %>%
      summarise_all(list(
        mean   = ~mean(. , na.rm = TRUE),
        sd     = ~sd(. , na.rm = TRUE),
        median = ~median(. , na.rm = TRUE),
        min    = ~min(. , na.rm = TRUE),
        max    = ~max(. , na.rm = TRUE)
      ))
    print(stats)
    cat("\n")
  }
  
  # Gerar relatório HTML com o pacote DataExplorer
  ## O arquivo HTML será salvo com o nome "relatorio_<nome_dataset>.html"
  relatorio_html <- paste0("relatorio_", nome_dataset, ".html")
  create_report(df, output_file = relatorio_html)
  cat(paste("Relatório HTML salvo em:", relatorio_html, "\n"))
  
  cat("--------------------------------------------------------\n\n")
}

# # Gerar relatório para cada um dos data frames
gera_relatorio(ans_15.23, "ans_15.23")
gera_relatorio(ans_cmed_15_23_intersecao, "ans_cmed_15_23_intersecao")


gera_relatorio(bps_97.24, "bps_97.24")
gera_relatorio(bps.cmed_15.23_intersecao, "bps.cmed_15.23_intersecao")

gera_relatorio(cmed_10.24, "cmed_10.24")


# __________________________ ----
# 2. Limpeza ----

## 2.1 ANS----

non_green_columns_ans = c( 
  "OBS", "DATA_ENTRADA", "TEMPO_INTERNACAO",
  "UNIDADE_MEDIDA", "VALOR_PAGO"
)

# Keep only these columns
ans_cmed_clean <- ans_cmed_15_23_intersecao %>%
  select(!all_of(non_green_columns_ans))

# Remover valores ausentes
ans_cmed_clean1 <- ans_cmed_clean %>%
  filter(!is.na(PRECO_UNITARIO), PRECO_UNITARIO > 0)
names(ans_cmed_clean1)

ans_cmed_clean12 <- ans_cmed_clean1 %>%
  filter(!is.na(PRECO_UNITARIO), PRECO_UNITARIO > 0,
         !is.na(PRECO_TETO_UNITARIO), PRECO_TETO_UNITARIO > 0,
         !is.na(ANO_MES),
         !is.na(CATEGORIA)) %>%
  select(-c(COMERCIALIZACAO, IHH)) %>%
  mutate(RAZAO_PRECO = PRECO_UNITARIO / PRECO_TETO_UNITARIO)

num_registros_unicos_ans12 <- ans_cmed_clean12 %>%
  summarise(total_unicos = n_distinct(REGISTRO))
num_registros_unicos_ans12

## 2.2. BPS----
non_green_columns_bps <- c(
  "OBS"
  
)

names(bps.cmed_15.23_intersecao)
# Filter
bps_cmed_clean <- bps.cmed_15.23_intersecao %>%
  select(!all_of(non_green_columns_bps))
names(bps_cmed_clean)

bps_cmed_clean1 <- bps_cmed_clean %>%
  filter(!is.na(PRECO_UNITARIO), PRECO_UNITARIO > 0,
         !is.na(PRECO_TETO_UNITARIO), PRECO_TETO_UNITARIO > 0) %>%
  mutate(RAZAO_PRECO = PRECO_UNITARIO / PRECO_TETO_UNITARIO) %>%
  select(-c(COMERCIALIZACAO, IHH))

num_registros_unicos_bps1 <- bps_cmed_clean1 %>%
  summarise(total_unicos = n_distinct(REGISTRO))
num_registros_unicos_bps1
colSums(is.na(bps_cmed_clean1))

unique(bps_cmed_clean1$TIPO_COMPRA)
levels(bps_cmed_clean1$TIPO_COMPRA) <- 
  c(levels(bps_cmed_clean1$TIPO_COMPRA), "NI")

# 2. Substituir os NA pelo novo nível
bps_cmed_clean1$TIPO_COMPRA[
  is.na(bps_cmed_clean1$TIPO_COMPRA)
] <- "NI"

# Verificação
unique(bps_cmed_clean1$TIPO_COMPRA)


# __________________________ ----
# 3.Estatísticas ----

# Agregado por grupo, faixa e tipo de produto

# Adicionar Times New Roman
font_add("Times New Roman", regular = "C:/Windows/Fonts/times.ttf")
showtext_auto()

count(agg$REG_APR)


# __________________________ ----
## 3.1. ANS ----
# ¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨----
### 3.1.1.Gráficos ----
hist(ans_cmed_clean12$RAZAO_PRECO)
quantile(ans_cmed_clean12$RAZAO_PRECO, probs = seq(0, 1, 0.1), na.rm = TRUE)

ans_plot <- ans_cmed_clean12 %>%
  filter(RAZAO_PRECO <= 17.07907) %>%  # até o percentil 90%
  mutate(
    GRUPO = case_when(
      RAZAO_PRECO < 1 ~ "<1",
      RAZAO_PRECO == 1 ~ "=1",
      RAZAO_PRECO > 1 ~ ">1",
      TRUE ~ NA_character_
    )
  )

hist(ans_plot$RAZAO_PRECO,
     breaks = 100,
     main = "Distribuição da Razão Preço Observado ÷ Teto (sem outliers)",
     xlab = "Razão Observado ÷ Teto",
     col = "skyblue",
     border = "gray")
abline(v = 1, col = "red", lty = 2)

#### 3.1.1.1) Boxplot of Observed Price ÷ Cap Price Ratio by Group  ----

ans_plot_eq1 <- ans_plot %>%
  filter(GRUPO != "=1")

ans_bp_descontos = ggplot(ans_plot_eq1, aes(x = GRUPO, y = RAZAO_PRECO, fill = GRUPO)) +
  geom_boxplot(outlier.shape = NA, color = "black") +
  coord_cartesian(ylim = c(0, 5)) +
  scale_fill_manual(
    name   = NULL,
    values = c(
      "<1" = "#b2df8a",
      "=1" = "#fdbf6f",
      ">1" = "#fb9a99" 
    )
  ) +
  labs(
    x = NULL,
    y = NULL,
    title = NULL
  ) +
  theme_minimal(base_family = "Times New Roman") +
  theme(
    # reexibe grid de fundo
    panel.grid.major = element_line(color = "grey95"),
    panel.grid.minor = element_line(color = "grey95"),
    panel.background = element_blank(),
    axis.title.x    = element_text(family = "Times New Roman", size = 10),
    axis.title.y    = element_text(family = "Times New Roman", size = 10),
    axis.text.x     = element_text(family = "Times New Roman", size = 32),
    axis.text.y     = element_text(family = "Times New Roman", size = 32),
    legend.position = "right"
  )

#### 3.1.1.2) Boxplot of Observed ÷ Ceiling Ratio by State (UF) ----

ans_bp_uf = ggplot(ans_plot, aes(x = UF, y = RAZAO_PRECO, fill = UF)) +
  geom_boxplot(outlier.shape = NA, color = "black") +
  geom_boxplot(outlier.shape = NA, color = "black") +
  geom_hline(yintercept = 1,
             color      = "darkred",
             #linetype   = "dashed",
             linewidth  = 1) +
  labs(
    x = "State (UF)",
    y = "Observed ÷ Cap Ratio"
  ) +
  theme_minimal(base_family = "Times New Roman") +
  theme(
    panel.grid.major = element_line(color = "grey95"),
    panel.grid.minor = element_line(color = "grey95"),
    panel.background = element_blank(),
    axis.title.x     = element_blank(),
    axis.title.y     = element_blank(),
    axis.text.x      = element_text(angle = 45, hjust = 1,
                                    family = "Times New Roman", size = 32),
    axis.text.y      = element_text(family = "Times New Roman", size = 32),
    legend.position  = "none"
  )
ans_bp_uf

names(ans_plot)
ans_plot = ans_plot |>
  mutate(ano =year(DATA))
regiao_lookup <- tibble(
  UF     = c("AC","AM","AP","PA","RO","RR","TO",
             "AL","BA","CE","MA","PB","PE","PI","RN","SE",
             "DF","GO","MS","MT",
             "ES","MG","RJ","SP",
             "PR","RS","SC"),
  regiao = c(rep("North", 7),
             rep("Northeast", 9),
             rep("Midwest", 4),
             rep("Southeast", 4),
             rep("South", 3))
)
unique(ans_plot$CATEGORIA)
ans_media_regiao <- ans_plot %>%
  mutate(
    data = as.Date(paste0(ANO_MES, "-01")),
    ano  = year(data)
  ) %>%
  left_join(regiao_lookup, by = "UF") %>%
  group_by(regiao, ano) %>%
  summarise(
    media_razao = mean(RAZAO_PRECO, na.rm = TRUE),
    .groups = "drop"
  )
ans_media_regiao

colSums(is.na(ans_plot))

# 1. Visão geral: quantos NAs em cada coluna
colSums(is.na(ans_media_regiao))

# 2. Percentual de NAs por coluna
round(colSums(is.na(ans_media_regiao)) / nrow(ans_media_regiao) * 100, 2)

# 3. Resumo estatístico (inclui contagem de NAs)
summary(ans_media_regiao)


# 3. Gráfico de linha
ans_st_reg = ggplot(ans_media_regiao,
       aes(x     = factor(ano),
           y     = media_razao,
           color = regiao,
           group = regiao)) +
  geom_line(linewidth = 1) +
  geom_point(size = 2) +
  scale_y_continuous(labels = percent_format(accuracy = 1)) +
  labs(
    x     = "Ano",
    y     = "Média da Razão Preço (Desconto)",
    color = "Região"
  ) +
  theme_minimal(base_family = "Times New Roman", base_size = 32) +
  theme(
    axis.text.x     = element_text(angle = 45, hjust = 1, size = 32),
    axis.title      = element_blank(),
    axis.text.y     = element_text(size = 32),
    legend.position = "right",
    legend.title    = element_blank(),
    legend.text     = element_text(size = 32, family = "Times New Roman")
  )

print(ans_st_reg)


#### 3.1.1.3) Distribution of Observed ÷ Cap Ratio (without outliers)  ----
ans_d_ratios = ggplot(ans_plot, aes(x = RAZAO_PRECO, fill = GRUPO)) +
  geom_histogram(
    breaks = seq(0, 17.07907, by = 0.5),
    color = "white", boundary = 0
  ) +
  geom_vline(
    xintercept = 1,
    color      = "darkred",
    linewidth  = 1
  ) +
  scale_fill_manual(
    name   = NULL,
    values = c("<1" = "#b2df8a", "=1" = "#fdbf6f", ">1" = "#fb9a99"),
    labels = c("<1" = "<1×", "=1" = "=1×", ">1" = ">1×")
  ) +
  scale_x_continuous(
    name   = NULL,
    limits = c(0, 17.07907),
    breaks = seq(0, 17, by = 1),
    minor_breaks = NULL
  ) +
  scale_y_continuous(
    name   = NULL,
    labels = label_comma()
  ) +
  theme_minimal(base_family = "Times New Roman") +
  theme(
    plot.title        = element_text(size = 32, hjust = 0.5),
    axis.title.x      = element_text(size = 32),
    axis.title.y      = element_text(size = 32),
    axis.text.x       = element_text(size = 32,  hjust = 1),
    axis.text.y       = element_text(size = 32),
    axis.ticks.x = element_blank(),
    legend.text     = element_text(size = 32, family = "Times New Roman")
  )


ans_plot %>%
  count(GRUPO) %>%
  mutate(Proporcao = n / sum(n))


#### 3.1.1.4) Quantity‐Weighted Distribution by Observed ÷ Cap Ratio Range(ANS) ----
ans_plot1 <- ans_plot %>%
  mutate(
    BIN = cut(RAZAO_PRECO, breaks = seq(0, 17.07907, by = 0.5), right = FALSE)
  )%>%
  filter(!is.na(BIN))

ans_peso <- ans_plot1 %>%
  group_by(GRUPO, BIN) %>%
  summarise(QTD_TOTAL = sum(QUANTIDADE, na.rm = TRUE), .groups = "drop")



bin_at_1 <- match("[0.5,1)", levels(ans_peso$BIN))

# 3) Plot
ans_dq_ratio <- ggplot(ans_peso, aes(x = BIN, y = QTD_TOTAL, fill = GRUPO)) +
  geom_col(position = "stack", color = "white") +
  geom_vline(
    xintercept = bin_at_1 + 0.5,
    color      = "darkred",
    linewidth  = 1
  ) +
  scale_fill_manual(
    name   = NULL,
    values = c("<1" = "#b2df8a", "=1" = "#fdbf6f", ">1" = "#fb9a99"),
    labels = c("<1" = "<1×", "=1" = "=1×", ">1" = ">1×")
  ) +
  scale_y_continuous(
    name   = NULL,
    labels = label_comma()
  ) +
  scale_x_discrete(
    name = NULL
  ) +
  theme_minimal(base_family = "Times New Roman") +
  theme(
    panel.background    = element_blank(),
    panel.grid.major.x  = element_line(color = "grey95"),
    panel.grid.major.y  = element_line(color = "grey95"),
    panel.grid.minor.x  = element_line(color = "grey95"),
    panel.grid.minor.y  = element_blank(),
    axis.text.x         = element_text(angle = 45, hjust = 1, size = 32),
    axis.text.y         = element_text(size = 32),
    legend.position     = "right",
    legend.text         = element_text(size = 32)
  )


#### _ TIPO----

# Criar faixas (bins) e grupo
ans_plot_tipo <- ans_cmed_clean12 %>%
  filter(RAZAO_PRECO <= 17.07907) %>%
  mutate(
    BIN = cut(RAZAO_PRECO, breaks = seq(0, 17.07907, by = 0.5), right = FALSE),
    GRUPO = case_when(
      RAZAO_PRECO < 1 ~ "<1",
      RAZAO_PRECO == 1 ~ "=1",
      RAZAO_PRECO > 1 ~ ">1",
      TRUE ~ NA_character_
    )
  ) %>%
  filter(!is.na(BIN), !is.na(TIPO_PRODUTO))


ans_tipo_qtd <- ans_plot %>%
  mutate(
    TIPO_PRODUTO_LABEL = if_else(is.na(TIPO_PRODUTO), "NOT INFORMED", TIPO_PRODUTO)
  ) %>%
  group_by(TIPO_PRODUTO_LABEL) %>%
  summarise(QTD_TOTAL = sum(QUANTIDADE, na.rm = TRUE), .groups = "drop")



# 1) Tradução para inglês
ans_tipo_qtd <- ans_tipo_qtd %>%
  mutate(
    TIPO_PRODUTO_LABEL = case_when(
      TIPO_PRODUTO_LABEL == "SIMILAR"        ~ "SIMILAR",
      TIPO_PRODUTO_LABEL == "BIOLÓGICO NOVO" ~ "NEW BIOLOGIC",
      TIPO_PRODUTO_LABEL == "BIOLÓGICO"      ~ "BIOLOGIC",
      TIPO_PRODUTO_LABEL == "GENÉRICO"       ~ "GENERIC",
      TIPO_PRODUTO_LABEL == "NOVO"           ~ "NEW",
      TIPO_PRODUTO_LABEL == "ESPECÍFICO"     ~ "SPECIALIZED",
      TRUE                                   ~ toupper(TIPO_PRODUTO_LABEL)
    )
  )

# 2) Reordenar fator por quantidade crescente, mantendo "NOT INFORMED" no fim
ordem_normais <- ans_tipo_qtd %>%
  filter(TIPO_PRODUTO_LABEL != "NOT INFORMED") %>%
  arrange(QTD_TOTAL) %>%
  pull(TIPO_PRODUTO_LABEL)

niveis_finais <- c("NOT INFORMED", ordem_normais)
niveis_finais
ans_tipo_qtd <- ans_tipo_qtd %>%
  mutate(
    TIPO_PRODUTO_LABEL = factor(TIPO_PRODUTO_LABEL, levels = niveis_finais)
  )


#### 3.1.1.4) Bar chart: Total Quantity by Product Type ----
ans_d_type = ggplot(ans_tipo_qtd, aes(x = TIPO_PRODUTO_LABEL, y = QTD_TOTAL)) +
  geom_col(fill = "#a6cee3", color = "#a6cee3") +
  geom_text(aes(label = comma(QTD_TOTAL)),
            hjust = -0.1,
            size   = 15,
            family = "Times New Roman") +
  coord_flip() +
  scale_y_continuous(
    labels = label_comma(),
    expand = expansion(mult = c(0, 0.2))
  ) +
  theme_minimal(base_family = "Times New Roman") +
  theme(
    panel.grid       = element_blank(),
    panel.background = element_blank(),
    axis.title       = element_blank(),
    axis.text.x      = element_blank(),
    axis.ticks.x     = element_blank(),
    axis.text.y      = element_text(size = 32, family = "Times New Roman")
  )

ans_d_type

#### 3.1.1.5) Stacked histogram: Quantity-Weighted Distribution by Product Type (ANS)----
ans_plot_tipo <- ans_plot %>%
  filter(RAZAO_PRECO <= 17.07907) %>%
  mutate(
    BIN = cut(RAZAO_PRECO, breaks = seq(0, 17.07907, by = 0.5), right = FALSE),
    TIPO_PRODUTO_LABEL = case_when(
      TIPO_PRODUTO == "SIMILAR"        ~ "SIMILAR",
      TIPO_PRODUTO == "BIOLÓGICO NOVO" ~ "NEW BIOLOGIC",
      TIPO_PRODUTO == "BIOLÓGICO"      ~ "BIOLOGIC",
      TIPO_PRODUTO == "GENÉRICO"       ~ "GENERIC",
      TIPO_PRODUTO == "NOVO"           ~ "NEW",
      TIPO_PRODUTO == "ESPECÍFICO"     ~ "SPECIALIZED",
      is.na(TIPO_PRODUTO)              ~ "NOT INFORMED",
      TRUE                              ~ toupper(TIPO_PRODUTO)
    )
  )  %>%
  filter(
    !is.na(BIN),
    TIPO_PRODUTO_LABEL != "NOT INFORMED"
  )

# Agregar por faixa, grupo e label traduzido
ans_tipo_peso <- ans_plot_tipo %>%
  group_by(TIPO_PRODUTO_LABEL, GRUPO, BIN) %>%
  summarise(QTD_TOTAL = sum(QUANTIDADE, na.rm = TRUE), .groups = "drop")

# Stacked histogram
bin_at_1 <- match("[0.5,1)", levels(ans_tipo_peso$BIN))

ans_dq_type <- ggplot(ans_tipo_peso, aes(x = BIN, y = QTD_TOTAL, fill = GRUPO)) +
  geom_col(position = "stack", color = "white") +
  geom_vline(
    xintercept = bin_at_1 + 0.5,
    color      = "darkred",
    linewidth  = 1
  ) +
  facet_wrap(~ TIPO_PRODUTO_LABEL, scales = "free_y") +
  scale_fill_manual(
    values = c("<1" = "#b2df8a", "=1" = "#fdbf6f", ">1" = "#fb9a99"),
    labels = c("<1" = "<1×", "=1" = "=1×", ">1" = ">1×")
  ) +
  scale_y_continuous(labels = label_comma(), expand = expansion(mult = c(0, 0.1))) +
  theme_minimal(base_family = "Times New Roman") +
  theme(
    panel.grid       = element_blank(),
    panel.background = element_blank(),
    strip.text       = element_text(size = 32),
    axis.text.x      = element_text(angle = 45, hjust = 1, size = 32),
    axis.text.y      = element_text(size = 32),
    axis.title       = element_blank(),
    axis.ticks       = element_blank(),
    plot.title       = element_text(size = 32, hjust = 0.5),
    legend.title    = element_blank(),
    legend.text     = element_text(size = 32, family = "Times New Roman")
  )

ans_dq_type

#### _CLASSE TERAPEUTICA ----

#### 3.1.1.6) Bar chart: Total Quantity by Therapeutic Class ----
ans_classe_qtd <- ans_plot %>%
  mutate(
    CLASS_LABEL = if_else(
      is.na(CLASSE_PRINCIPAL_DESCRICAO),
      "NOT INFORMED",
      CLASSE_PRINCIPAL_DESCRICAO
    )
  ) %>%
  group_by(CLASS_LABEL) %>%
  summarise(QTY_TOTAL = sum(QUANTIDADE, na.rm = TRUE), .groups = "drop") %>%
  arrange(QTY_TOTAL)

levels_cls <- ans_classe_qtd %>%
  filter(CLASS_LABEL != "NOT INFORMED") %>%
  pull(CLASS_LABEL)
ans_classe_qtd <- ans_classe_qtd %>%
  mutate(
    CLASS_LABEL = factor(CLASS_LABEL, levels = c("NOT INFORMED", levels_cls))
  )

ans_d_class = ggplot(ans_classe_qtd, aes(x = CLASS_LABEL, y = QTY_TOTAL)) +
  geom_col(fill = "#a6cee3", color = "#a6cee3") +
  geom_text(aes(label = comma(QTY_TOTAL)),
            hjust = -0.1,
            size = 3.2,
            family = "Times New Roman") +
  coord_flip() +
  scale_y_continuous(
    labels = label_comma(),
    expand = expansion(mult = c(0, 0.2))
  ) +
  theme_minimal(base_family = "Times New Roman") +
  theme(
    panel.grid       = element_blank(),
    panel.background = element_blank(),
    axis.title       = element_blank(),
    axis.text.x      = element_blank(),
    axis.ticks.x     = element_blank(),
    axis.text.y      = element_text(size = 32, family = "Times New Roman")
  )

#### 3.1.1.7) Stacked histogram: Quantity-Weighted Distribution by Therapeutic Class ----

ans_plot_classe <- ans_plot %>%
  filter(RAZAO_PRECO <= 17.1) %>%
  mutate(
    BIN = cut(RAZAO_PRECO, breaks = seq(0, 17.1, by = 0.5), right = FALSE),
    CLASS_LABEL = if_else(
      is.na(CLASSE_PRINCIPAL_DESCRICAO),
      "NOT INFORMED",
      CLASSE_PRINCIPAL_DESCRICAO
    )
  ) %>%
  filter(!is.na(BIN))

ans_classe_peso <- ans_plot_classe %>%
  group_by(CLASS_LABEL, GRUPO, BIN) %>%
  summarise(QTY_TOTAL = sum(QUANTIDADE, na.rm = TRUE), .groups = "drop")

ggplot(ans_classe_peso, aes(x = BIN, y = QTY_TOTAL, fill = GRUPO)) +
  geom_col(position = "stack", color = "white") +
  geom_vline(
    xintercept = bin_at_1 + 0.5,
    color      = "darkred",
    linewidth  = 1
  ) +
  facet_wrap(~ CLASS_LABEL, scales = "free_y") +
  scale_fill_manual(
    values = c("<1" = "#b2df8a", "=1" = "#fdbf6f", ">1" = "#fb9a99"),
    labels = c("<1" = "<1×", "=1" = "=1×", ">1" = ">1×")
  ) +
  scale_y_continuous(labels = label_comma(), expand = expansion(mult = c(0, 0.1))) +
  theme_minimal(base_family = "Times New Roman") +
  theme(
    panel.grid       = element_blank(),
    panel.background = element_blank(),
    strip.text       = element_text(family = "Times New Roman", size = 10),
    axis.text.x      = element_text(angle = 45, hjust = 1,
                                    family = "Times New Roman", size = 8),
    axis.text.y      = element_text(family = "Times New Roman", size = 8),
    axis.title       = element_blank(),
    axis.ticks       = element_blank(),
    plot.title       = element_blank(),
    legend.position  = "none"
  )


ans_dq_class = ggplot(ans_classe_peso, aes(x = BIN, y = QTY_TOTAL, fill = GRUPO)) +
  geom_col(position = "stack", color = "white") +
  geom_vline(
    xintercept = bin_at_1 + 0.5,
    color      = "darkred",
    linewidth  = 1
  ) +
  facet_wrap(~ CLASS_LABEL, scales = "free_y", ncol = 3) +
  scale_fill_manual(
    values = c("<1" = "#b2df8a", "=1" = "#fdbf6f", ">1" = "#fb9a99"),
    labels = c("<1" = "<1×", "=1" = "=1×", ">1" = ">1×")
  ) +
  scale_y_continuous(labels = label_comma(), expand = expansion(mult = c(0, 0.1))) +
  theme_minimal(base_family = "Times New Roman") +
  theme(
    panel.grid       = element_blank(),
    panel.background = element_blank(),
    strip.text       = element_text(family = "Times New Roman", size = 32),
    axis.text.x      = element_text(angle = 45, hjust = 1, family = "Times New Roman", size = 32),
    axis.text.y      = element_text(family = "Times New Roman", size = 32),
    axis.title       = element_blank(),
    axis.ticks       = element_blank(),
    plot.title       = element_blank(),
    legend.position  = "none",
    legend.title    = element_blank(),
    legend.text     = element_text(size = 32, family = "Times New Roman")
  )
ans_dq_class

ans_bp_descontos
ans_bp_uf
ans_d_ratios
ans_dq_ratio
ans_d_type
ans_dq_type
ans_d_class
ans_dq_class


#Salvar


# Lista de objetos de gráfico
plots_list <- list(
  ans_bp_descontos = ans_bp_descontos,
  ans_bp_uf        = ans_bp_uf,
  ans_st_reg       = ans_st_reg,
  ans_d_ratios     = ans_d_ratios,
  ans_dq_ratio     = ans_dq_ratio,
  ans_d_type       = ans_d_type,
  ans_dq_type      = ans_dq_type,
  ans_d_class      = ans_d_class,
  ans_dq_class     = ans_dq_class
)

# Diretório de saída
out_dir <- "../results/midia"

if (!dir.exists(out_dir)) dir.create(out_dir)

# Loop para salvar cada gráfico
for (name in names(plots_list)) {
  ggsave(
    filename = file.path(out_dir, paste0(name, ".png")),
    plot     = plots_list[[name]],
    device   = "png",
    width    = 10,      
    height   = 8,       
    dpi      = 300      
  )
}


### 3.1.2. Tabelas ----
ans_cmed_clean12 = combined_data_filter |>
  filter(SETOR_PUB == 0)


### 1. Total de registros
total_registros_ans <- nrow(ans_cmed_clean12)
total_registros_ans
total_unicos_ans <- ans_cmed_clean12 %>%
  distinct(REGISTRO_ANVISA) %>%
  nrow()
total_unicos_ans
totalqde_ans <- sum(ans_cmed_clean12$QUANTIDADE)
totalqde_ans

### 2. Registros hospitalares
hospitalares_ans <- sum(ans_cmed_clean12$TIPO == "Hospitalar", na.rm = TRUE)
hospitalares_ans
perc_hospitalar_ans <- round(hospitalares_ans / total_registros_ans * 100, 1)
perc_hospitalar_ans

### 3. Registros com preço zero
valores_zero_ans <- sum(ans_cmed_clean$PRECO_UNITARIO == 0, na.rm = TRUE)
valores_zero_ans
perc_zero_ans <- round(valores_zero_ans / total_registros_ans * 100, 1)
perc_zero_ans

### 4. Estatísticas gerais da razão

media_razao_ans <- round(mean(ans_cmed_clean12$RAZAO_PRECO, na.rm = TRUE), 2)
media_razao_ans

mediana_razao_ans <- round(median(ans_cmed_clean12$RAZAO_PRECO, na.rm = TRUE), 2)
mediana_razao_ans

names(ans_cmed_clean12)


### 5. Top classes terapêuticas (por quantidade) + razão média
top_classes_ans <- ans_cmed_clean12 %>%
  group_by(CLASSE_PRINCIPAL_DESCRICAO) %>%
  summarise(
    QTD_TOTAL = sum(QUANTIDADE, na.rm = TRUE),
    RAZAO_MEDIA = round(mean(RAZAO_PRECO, na.rm = TRUE), 2),
    RAZAO_MEDIANA = round(median(RAZAO_PRECO, na.rm = TRUE), 2)
  ) %>%
  mutate(perc = round(100 * QTD_TOTAL / sum(QTD_TOTAL), 3)) %>%
  arrange(desc(QTD_TOTAL)) 

top_classes_ans

### 6. Top estados (por quantidade) + razão média
top_estados_ans <- ans_cmed_clean12 %>%
  group_by(UF) %>%
  summarise(
    QTD_TOTAL = sum(QUANTIDADE, na.rm = TRUE),
    RAZAO_MEDIA = round(mean(RAZAO_PRECO, na.rm = TRUE), 2),
    RAZAO_MEDIANA = round(median(RAZAO_PRECO, na.rm = TRUE), 2)
  ) %>%
  mutate(perc = round(100 * QTD_TOTAL / sum(QTD_TOTAL), 3)) %>%
  arrange(desc(QTD_TOTAL)) 

top_estados_ans

### 7. Top tipos de produto (por quantidade) + razão média + proporção
top_tipos_ans <- ans_cmed_clean12 %>%
  group_by(TIPO_PRODUTO) %>%
  summarise(
    QTD_TOTAL = sum(QUANTIDADE, na.rm = TRUE),
    RAZAO_MEDIA = round(mean(RAZAO_PRECO, na.rm = TRUE), 2),
    RAZAO_MEDIANA = round(median(RAZAO_PRECO, na.rm = TRUE), 2)
  ) %>%
  mutate(perc = round(100 * QTD_TOTAL / sum(QTD_TOTAL), 2)) %>%
  arrange(desc(QTD_TOTAL)) 
top_tipos_ans


### 9. Top estados (por quantidade) + razão média
top_nivel_ans <- ans_cmed_clean12 %>%
  group_by(NÍVEL) %>%
  summarise(
    QTD_TOTAL = sum(QUANTIDADE, na.rm = TRUE),
    RAZAO_MEDIA = round(mean(RAZAO_PRECO, na.rm = TRUE), 2),
    RAZAO_MEDIANA = round(median(RAZAO_PRECO, na.rm = TRUE), 2)
  ) %>%
  mutate(perc = round(100 * QTD_TOTAL / sum(QTD_TOTAL), 3))%>%
  arrange(desc(QTD_TOTAL)) 

top_nivel_ans


### 10. Top Faixa (por quantidade) + razão média
# Faixas de preços:

# i) Defina os limites e rótulos das faixas de preço
breaks_faixa <- c(-Inf, 20, 50, 250, 500, 1000, 5000, 20000, 50000, 100000, Inf)
labels_faixa <- c(
  "<= R$ 20,00",
  "R$ 20,01 – R$ 50,00",
  "R$ 50,01 – R$ 250,00",
  "R$ 250,01 – R$ 500,00",
  "R$ 500,01 – R$ 1.000,00",
  "R$ 1.000,01 – R$ 5.000,00",
  "R$ 5.000,01 – R$ 20.000,00",
  "R$ 20.000,01 – R$ 50.000,00",
  "R$ 50.000,01 – R$ 100.000,00",
  ">= R$ 100.000,00"
)
names(ans_cmed_clean12)

# ii) Calcule faturamento bruto e unidades por faixa
ans_cmed_clean12 <- ans_cmed_clean12 %>%
  mutate(
    FAIXA_PA = cut(
      PRECO_TETO_APRESENTACAO,
      breaks = breaks_faixa,
      labels = labels_faixa,
      right = FALSE,
      include.lowest = TRUE),
      FAIXA_PU = cut(
        PRECO_TETO_UNITARIO,
        breaks = breaks_faixa,
        labels = labels_faixa,
        right = FALSE,
        include.lowest = TRUE
    ))
  

# iii) Visualize o resultado
names(ans_cmed_clean12)

top_faixa_ans <- ans_cmed_clean12 %>%
  group_by(FAIXA_PA) %>%
  summarise(
    QTD_TOTAL = sum(QUANTIDADE, na.rm = TRUE),
    RAZAO_MEDIA = round(mean(RAZAO_PRECO, na.rm = TRUE), 2),
    RAZAO_MEDIANA = round(median(RAZAO_PRECO, na.rm = TRUE), 2)
  ) %>%
  mutate(perc = round(100 * QTD_TOTAL / sum(QTD_TOTAL), 3))%>%
  arrange(desc(QTD_TOTAL)) 

top_faixa_ans



### 11. Saída de resumo
list(
  total_registros_ans = total_registros_ans,
  hospitalares_ans = hospitalares_ans,
  perc_hospitalar_ans = perc_hospitalar_ans,
  valores_zero_ans = valores_zero_ans,
  perc_zero_ans = perc_zero_ans,
  media_razao_ans = media_razao_ans,
  mediana_razao_ans = mediana_razao_ans,
  top_classes_ans = top_classes_ans,
  top_estados_ans = top_estados_ans,
  top_tipos_ans = top_tipos_ans,
  top_nivel_ans = top_nivel_ans,
  top_faixa_ans = top_faixa_ans
)
resultados_ans <- list(
  total_registros_ans = as.data.frame(total_registros_ans),
  hospitalares_ans    = as.data.frame(hospitalares_ans),
  perc_hospitalar_ans = as.data.frame(perc_hospitalar_ans),
  valores_zero_ans    = as.data.frame(valores_zero_ans),
  perc_zero_ans       = as.data.frame(perc_zero_ans),
  media_razao_ans     = as.data.frame(media_razao_ans),
  mediana_razao_ans   = as.data.frame(mediana_razao_ans),
  top_classes_ans     = top_classes_ans,
  top_estados_ans     = top_estados_ans,
  top_tipos_ans       = top_tipos_ans,
  top_nivel_ans       = top_nivel_ans,
  top_faixa_ans       = top_faixa_ans
)



# __________________________ ----
## 3.2. BPS ----
# ¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨----
### 3.2.1.Gráficos ----

hist(bps_cmed_clean1$RAZAO_PRECO)
quantile(bps_cmed_clean1$RAZAO_PRECO, probs = seq(0, 1, 0.1), na.rm = TRUE)

bps_plot <- bps_cmed_clean1 %>%
  filter(RAZAO_PRECO <= 1.114053) %>%  # até o percentil 90%
  mutate(
    GRUPO = case_when(
      RAZAO_PRECO < 1 ~ "<1",
      RAZAO_PRECO == 1 ~ "=1",
      RAZAO_PRECO > 1 ~ ">1",
      TRUE ~ NA_character_
    )
  )

#### 3.2.1.1) Boxplot of Ratio by Group (BPS) ----
bps_plot_eq1 <- bps_plot %>%
  filter(GRUPO != "=1")

bps_bp_descontos <- ggplot(bps_plot_eq1, aes(x = GRUPO, y = RAZAO_PRECO, fill = GRUPO)) +
  geom_boxplot(outlier.shape = NA, color = "black") +
  coord_cartesian(ylim = c(0, 1.2)) +
  scale_fill_manual(
    values = c("<1" = "#b2df8a", "=1" = "#fdbf6f", ">1" = "#fb9a99"),
    guide = "none"
  ) +
  labs(
    title = NULL,
    x = NULL,
    y = NULL
  ) +
  theme_minimal(base_family = "Times New Roman") +
  theme(
    panel.grid       = element_blank(),
    panel.background = element_blank(),
    axis.title       = element_blank(),
    axis.text.x      = element_blank(),
    axis.ticks.x     = element_blank(),
    axis.text.y      = element_text(family = "Times New Roman", size = 9)
  )
bps_bp_descontos


#### 3.1.1.2) Boxplot of Observed ÷ Cap Ratio by State (BPS) ----

bps_bp_uf = ggplot(bps_plot, aes(x = UF, y = RAZAO_PRECO, fill = UF)) +
  geom_boxplot(outlier.shape = NA, color = "black") +
  geom_hline(
    yintercept = 1,
    color      = "darkred",
    linewidth  = 1
  ) +
  labs(
    x = NULL,
    y = NULL
  ) +
  theme_minimal(base_family = "Times New Roman") +
  theme(
    panel.grid.major = element_line(color = "grey90"),
    panel.grid.minor = element_line(color = "grey95"),
    panel.background = element_blank(),
    axis.title.x     = element_text(family = "Times New Roman", size = 10),
    axis.title.y     = element_text(family = "Times New Roman", size = 10),
    axis.text.x      = element_text(
      angle  = 45,
      hjust  = 1,
      family = "Times New Roman",
      size   = 8
    ),
    axis.text.y      = element_text(family = "Times New Roman", size = 9),
    legend.position  = "none"
  )
bps_bp_uf



names(bps_plot)
bps_plot = bps_plot |>
  mutate(ano =year(DATA_COMPRA))
regiao_lookup <- tibble(
  UF     = c("AC","AM","AP","PA","RO","RR","TO",
             "AL","BA","CE","MA","PB","PE","PI","RN","SE",
             "DF","GO","MS","MT",
             "ES","MG","RJ","SP",
             "PR","RS","SC"),
  regiao = c(rep("North", 7),
             rep("Northeast", 9),
             rep("Midwest", 4),
             rep("Southeast", 4),
             rep("South", 3))
)
unique(bps_plot$CATEGORIA)
bps_media_regiao <- bps_plot %>%
  mutate(
    data = as.Date(paste0(ANO_MES, "-01")),
    ano  = year(data)
  ) %>%
  left_join(regiao_lookup, by = "UF") %>%
  group_by(regiao, ano) %>%
  summarise(
    media_razao = mean(RAZAO_PRECO, na.rm = TRUE),
    .groups = "drop"
  )
bps_media_regiao

colSums(is.na(bps_plot))

# 1. Visão geral: quantos NAs em cada coluna
colSums(is.na(bps_media_regiao))

# 2. Percentual de NAs por coluna
round(colSums(is.na(bps_media_regiao)) / nrow(bps_media_regiao) * 100, 2)

# 3. Resumo estatístico (inclui contagem de NAs)
summary(bps_media_regiao)



# 3. Gráfico de linha
bps_st_reg = ggplot(bps_media_regiao,
                    aes(x     = factor(ano),
                        y     = media_razao,
                        color = regiao,
                        group = regiao)) +
  geom_line(linewidth = 1) +
  geom_point(size = 2) +
  scale_y_continuous(labels = percent_format(accuracy = 1)) +
  labs(
    x     = "Ano",
    y     = "Média da Razão Preço (Desconto)",
    color = "Região"
  ) +
  theme_minimal(base_family = "Times New Roman", base_size = 32) +
  theme(
    axis.text.x     = element_text(angle = 45, hjust = 1, size = 32),
    axis.title      = element_blank(),
    axis.text.y     = element_text(size = 32),
    legend.position = "right",
    legend.title    = element_blank(),
    legend.text     = element_text(size = 32, family = "Times New Roman")
  )

bps_st_reg


#### 3.2.1.3) Histogram of Observed÷Cap Ratio (BPS) ----

bps_d_ratios <- ggplot(bps_plot, aes(x = RAZAO_PRECO, fill = GRUPO)) +
  geom_histogram(breaks = seq(0, 1.2, by = 0.05),
                 color = "white", boundary = 0) +
  geom_vline(xintercept = 1,
             color = "darkred", 
             linewidth = 1) +
  scale_fill_manual(
    values = c("<1" = "#b2df8a",
               "=1" = "#fdbf6f",
               ">1" = "#fb9a99"),
    guide = "none"
  ) +
  scale_x_continuous(
    name        = NULL,
    limits      = c(0, 1.2),
    breaks      = seq(0, 1.2, by = 0.1),
    minor_breaks= seq(0, 1.2, by = 0.05),
    labels      = number_format(accuracy = 0.1)
  ) +
  scale_y_continuous(
    name   = NULL,
    labels = label_comma()
  ) +
  theme_minimal(base_family = "Times New Roman") +
  theme(
    panel.grid.major.x = element_line(color = "grey95"),
    panel.grid.major.y = element_line(color = "grey95"),
    panel.grid.minor.x = element_line(color = "grey95"),
    panel.grid.minor.y = element_blank(),
    panel.background   = element_rect(fill = "white", color = NA),
    axis.title.x    = element_text(size = 32, family = "Times New Roman"),
    axis.text.x     = element_text(size = 32, family = "Times New Roman"),
    axis.ticks.x    = element_blank(),
    axis.title.y    = element_text(size = 32, family = "Times New Roman"),
    axis.text.y     = element_text(size = 32, family = "Times New Roman"),
    axis.ticks.y    = element_blank()
  )


bps_d_ratios


#### 3.2.1.4) Quantity‐Weighted Ratio Distribution (BPS) ----

bps_plot1 <- bps_plot %>%
  mutate(
    BIN = cut(
      RAZAO_PRECO,
      breaks = seq(0, 1.114053, by = 0.05),
      right  = FALSE
    )
  ) %>%
  filter(!is.na(BIN))

bps_peso <- bps_plot1 %>%
  group_by(GRUPO, BIN) %>%
  summarise(QTD_TOTAL = sum(QUANTIDADE, na.rm = TRUE), .groups = "drop")

bin_at_1 <- match("[0.95,1)", levels(bps_peso$BIN))

bps_dq_ratio <- ggplot(bps_peso, aes(x = BIN, y = QTD_TOTAL, fill = GRUPO)) +
  geom_col(position = "stack", color = "white") +
  geom_vline(xintercept = bin_at_1 + 0.5,
             color      = "darkred",
             linewidth  = 1) +
  scale_fill_manual(
    values = c("<1" = "#b2df8a",
               "=1" = "#fdbf6f",
               ">1" = "#fb9a99"),
    guide = "none"
  ) +
  scale_y_continuous(labels = label_comma()) +
  scale_x_discrete(name = "Observed ÷ Cap Ratio Range",
                   expand = expansion(add = c(0, 0.5))) +  # espaço após a última barra
  theme_minimal(base_family = "Times New Roman") +
  theme(
    panel.background   = element_rect(fill = "white", color = NA),
    panel.grid.major.x = element_line(color = "grey95"),
    panel.grid.major.y = element_line(color = "grey95"),
    panel.grid.minor.x = element_line(color = "grey95"),
    panel.grid.minor.y = element_blank(),
    axis.text.x        = element_text(angle = 45, hjust = 1,
                                      family = "Times New Roman", size = 32),
    axis.text.y        = element_text(family = "Times New Roman", size = 32),
    axis.ticks         = element_blank(),
    axis.title         = element_blank(),
    plot.title         = element_blank()
  )

bps_dq_ratio


#### _ TIPO----
#### 3.2.1.5) Bar chart: Total Quantity by Product Type (BPS) ----

bps_tipo_qtd <- bps_plot %>%
  mutate(
    TIPO_PRODUTO_LABEL = case_when(
      TIPO_PRODUTO == "SIMILAR"        ~ "SIMILAR",
      TIPO_PRODUTO == "BIOLÓGICO NOVO" ~ "NEW BIOLOGIC",
      TIPO_PRODUTO == "BIOLÓGICO"      ~ "BIOLOGIC",
      TIPO_PRODUTO == "GENÉRICO"       ~ "GENERIC",
      TIPO_PRODUTO == "NOVO"           ~ "NEW",
      TIPO_PRODUTO == "ESPECÍFICO"     ~ "SPECIALIZED",
      is.na(TIPO_PRODUTO)              ~ "NOT INFORMED",
      TRUE                              ~ toupper(TIPO_PRODUTO)
    )
  ) %>%
  group_by(TIPO_PRODUTO_LABEL) %>%
  summarise(QTD_TOTAL = sum(QUANTIDADE, na.rm = TRUE), .groups = "drop") %>%
  arrange(QTD_TOTAL)

levels_tipo <- bps_tipo_qtd %>%
  filter(TIPO_PRODUTO_LABEL != "NOT INFORMED") %>%
  pull(TIPO_PRODUTO_LABEL)
bps_tipo_qtd <- bps_tipo_qtd %>%
  mutate(
    TIPO_PRODUTO_LABEL = factor(
      TIPO_PRODUTO_LABEL,
      levels = c(levels_tipo, "NOT INFORMED")
    )
  )

bps_d_type = ggplot(bps_tipo_qtd, aes(x = TIPO_PRODUTO_LABEL, y = QTD_TOTAL)) +
  geom_col(fill = "#a6cee3", color = "#a6cee3") +
  geom_text(aes(label = comma(QTD_TOTAL)),
            hjust = -0.1, size = 3.2, family = "Times New Roman") +
  coord_flip() +
  scale_y_continuous(labels = label_comma(),
                     expand = expansion(mult = c(0, 0.2))) +
  theme_minimal(base_family = "Times New Roman") +
  theme(
    panel.grid       = element_blank(),
    panel.background = element_blank(),
    axis.title       = element_blank(),
    axis.text.x      = element_blank(),
    axis.ticks.x     = element_blank(),
    axis.text.y      = element_text(size = 32, family = "Times New Roman")
  )
bps_d_type

#### 3.2.1.6) Stacked histogram: Quantity‐Weighted Distribution by Product Type (BPS) ----

bps_tipo_peso <- bps_plot1 %>%
  mutate(
    TIPO_PRODUTO_LABEL = case_when(
      TIPO_PRODUTO == "SIMILAR"        ~ "SIMILAR",
      TIPO_PRODUTO == "BIOLÓGICO NOVO" ~ "NEW BIOLOGIC",
      TIPO_PRODUTO == "BIOLÓGICO"      ~ "BIOLOGIC",
      TIPO_PRODUTO == "GENÉRICO"       ~ "GENERIC",
      TIPO_PRODUTO == "NOVO"           ~ "NEW",
      TIPO_PRODUTO == "ESPECÍFICO"     ~ "SPECIALIZED",
      is.na(TIPO_PRODUTO)              ~ "NOT INFORMED",
      TRUE                              ~ toupper(TIPO_PRODUTO)
    )
  ) %>%
  group_by(TIPO_PRODUTO_LABEL, GRUPO, BIN) %>%
  summarise(QTD_TOTAL = sum(QUANTIDADE, na.rm = TRUE), .groups = "drop")

bin_at_1 <- match("[0.95,1)", levels(bps_tipo_peso$BIN))

bps_dq_type = ggplot(bps_tipo_peso, aes(x = BIN, y = QTD_TOTAL, fill = GRUPO)) +
  geom_col(position = "stack", color = "white") +
  geom_vline(xintercept = bin_at_1 + 0.5,
             color      = "darkred",
             linewidth  = 1) +
  facet_wrap(~ TIPO_PRODUTO_LABEL, scales = "free_y", ncol = 3) +
  scale_fill_manual(
    values = c("<1" = "#b2df8a", "=1" = "#fdbf6f", ">1" = "#fb9a99"),
    labels = c("<1" = "<1×", "=1" = "=1×", ">1" = ">1×"),
    guide  = "none"
  ) +
  scale_y_continuous(labels = label_comma(),
                     expand = expansion(mult = c(0, 0.1))) +
  theme_minimal(base_family = "Times New Roman") +
  theme(
    panel.grid.major.x = element_line(color = "grey90"),
    panel.grid.major.y = element_line(color = "grey90"),
    panel.grid.minor.x = element_line(color = "grey90"),
    panel.grid.minor.y = element_blank(),
    panel.background   = element_blank(),
    axis.text.x        = element_text(angle = 45, hjust = 1,
                                      family = "Times New Roman", size = 32),
    axis.text.y        = element_text(family = "Times New Roman", size = 32),
    axis.title         = element_blank(),
    axis.ticks         = element_blank(),
    plot.title         = element_blank()
  )
bps_dq_type


#### _CLASSE TERAPEUTICA ----

#### 3.2.1.6) Bar chart: Total Quantity by Therapeutic Class (BPS) ----

bps_classe_qtd <- bps_plot %>%
  mutate(
    CLASS_LABEL = if_else(
      is.na(CLASSE_PRINCIPAL_DESCRICAO),
      "NOT INFORMED",
      CLASSE_PRINCIPAL_DESCRICAO
    )
  ) %>%
  group_by(CLASS_LABEL) %>%
  summarise(QTY_TOTAL = sum(QUANTIDADE, na.rm = TRUE), .groups = "drop") %>%
  arrange(QTY_TOTAL)

levels_cls <- bps_classe_qtd %>%
  filter(CLASS_LABEL != "NOT INFORMED") %>%
  pull(CLASS_LABEL)
bps_classe_qtd <- bps_classe_qtd %>%
  mutate(
    CLASS_LABEL = factor(CLASS_LABEL, levels = c(levels_cls, "NOT INFORMED"))
  )

bps_d_class = ggplot(bps_classe_qtd, aes(x = CLASS_LABEL, y = QTY_TOTAL)) +
  geom_col(fill = "#a6cee3", color = "#a6cee3") +
  geom_text(aes(label = comma(QTY_TOTAL)),
            hjust = -0.1, size = 3.2, family = "Times New Roman") +
  coord_flip() +
  scale_y_continuous(labels = label_comma(),
                     expand = expansion(mult = c(0, 0.2))) +
  theme_minimal(base_family = "Times New Roman") +
  theme(
    panel.grid       = element_blank(),
    panel.background = element_blank(),
    axis.title       = element_blank(),
    axis.text.x      = element_blank(),
    axis.ticks.x     = element_blank(),
    axis.text.y      = element_text(size = 32, family = "Times New Roman")
  )
bps_d_class


#### 3.2.1.7) Stacked histogram: Quantity‐Weighted Distribution by Therapeutic Class (BPS) ----

bps_classe_peso <- bps_plot1 %>%
  mutate(
    CLASS_LABEL = if_else(
      is.na(CLASSE_PRINCIPAL_DESCRICAO),
      "NOT INFORMED",
      CLASSE_PRINCIPAL_DESCRICAO
    )
  ) %>%
  group_by(CLASS_LABEL, GRUPO, BIN) %>%
  summarise(QTY_TOTAL = sum(QUANTIDADE, na.rm = TRUE), .groups = "drop")

bps_dq_class = ggplot(bps_classe_peso, aes(x = BIN, y = QTY_TOTAL, fill = GRUPO)) +
  geom_vline(
    xintercept = bin_at_1 + 0.5,
    color      = "darkred",
  ) +
  geom_col(position = "stack", color = "white") +
  facet_wrap(~ CLASS_LABEL, scales = "free_y", ncol = 3) +
  scale_fill_manual(
    values = c("<1" = "#b2df8a", "=1" = "#fdbf6f", ">1" = "#fb9a99"),
    labels = c("<1" = "<1×", "=1" = "=1×", ">1" = ">1×"),
    guide  = "none"
  ) +
  scale_y_continuous(labels = label_comma(),
                     expand = expansion(mult = c(0, 0.1))) +
  theme_minimal(base_family = "Times New Roman") +
  theme(
    panel.grid.major.x = element_line(color = "grey90"),
    panel.grid.major.y = element_line(color = "grey90"),
    panel.grid.minor.x = element_line(color = "grey90"),
    panel.grid.minor.y = element_blank(),
    panel.background   = element_blank(),
    axis.text.x        = element_text(angle = 45, hjust = 1, 
                                      family = "Times New Roman", size = 32),
    axis.text.y        = element_text(family = "Times New Roman", size = 32),
    axis.title         = element_blank(),
    axis.ticks         = element_blank(),
    plot.title         = element_blank()
  )

bps_dq_class


# Lista de objetos de gráfico
plots_list <- list(
  bps_bp_descontos = bps_bp_descontos,
  bps_bp_uf        = bps_bp_uf,
  bps_st_reg       = bps_st_reg,
  bps_d_ratios     = bps_d_ratios,
  bps_dq_ratio     = bps_dq_ratio,
  bps_d_type       = bps_d_type,
  bps_dq_type      = bps_dq_type,
  bps_d_class      = bps_d_class,
  bps_dq_class     = bps_dq_class
)


# Loop para salvar cada gráfico
for (name in names(plots_list)) {
  ggsave(
    filename = file.path(out_dir, paste0(name, ".png")),
    plot     = plots_list[[name]],
    device   = "png",
    width    = 10,
    height   = 8,
    dpi      = 300
  )
}


### 3.2.2. Tabelas ----

# 1. Total de registros
total_bps <- nrow(bps_cmed_clean1)
total_bps

total_bps_unicos <- bps_cmed_clean1 %>%
  distinct(REGISTRO_ANVISA) %>%
  nrow()
total_bps_unicos

totalqde_bps <- sum(bps_cmed_clean1$QUANTIDADE)
totalqde_bps

sort(unique(bps.cmed_15.23_intersecao$UF))

valores_zero_bps <- sum(bps_cmed_clean1$PRECO_UNITARIO == 0, na.rm = TRUE)
valores_zero_bps
perc_zero_bps <- round(valores_zero_bps / totalqde_bps * 100, 1)
perc_zero_bps

# 2. Estatísticas gerais da razão
media_razao_bps <- round(mean(bps_cmed_clean1$RAZAO_PRECO, na.rm = TRUE), 2)
media_razao_bps
mediana_razao_bps <- round(median(bps_cmed_clean1$RAZAO_PRECO, na.rm = TRUE), 2)
mediana_razao_bps

# 3. Top classes terapêuticas (por quantidade) + razão média
top_classes_bps <- bps_cmed_clean1 %>%
  group_by(CLASSE_PRINCIPAL_DESCRICAO) %>%
  summarise(
    QTD_TOTAL = sum(QUANTIDADE, na.rm = TRUE),
    RAZAO_MEDIA = round(mean(RAZAO_PRECO, na.rm = TRUE), 2),
    RAZAO_MEDIANA = round(median(RAZAO_PRECO, na.rm = TRUE), 2)
  ) %>%
  mutate(perc = round(100 * QTD_TOTAL / sum(QTD_TOTAL), 3)) %>%
  arrange(desc(QTD_TOTAL)) 

top_classes_bps

# 4. Top UFs (por quantidade) + razão média
top_ufs_bps <- bps_cmed_clean1 %>%
  group_by(UF) %>%
  summarise(
    QTD_TOTAL = sum(QUANTIDADE, na.rm = TRUE),
    RAZAO_MEDIA = round(mean(RAZAO_PRECO, na.rm = TRUE), 2),
    RAZAO_MEDIANA = round(median(RAZAO_PRECO, na.rm = TRUE), 2)
  ) %>%
  mutate(perc = round(100 * QTD_TOTAL / sum(QTD_TOTAL), 3)) %>%
  arrange(desc(QTD_TOTAL)) 

top_ufs_bps
print(top_ufs_bps, n = Inf)

# 5. Top tipos de produto (por quantidade) + razão média + proporção
top_tipos_bps <- bps_cmed_clean1 %>%
  group_by(TIPO_PRODUTO) %>%
  summarise(
    QTD_TOTAL = sum(QUANTIDADE, na.rm = TRUE),
    RAZAO_MEDIA = round(mean(RAZAO_PRECO, na.rm = TRUE), 2),
    RAZAO_MEDIANA = round(median(RAZAO_PRECO, na.rm = TRUE), 2)
  ) %>%
  mutate(perc = round(100 * QTD_TOTAL / sum(QTD_TOTAL), 3)) %>%
  arrange(desc(QTD_TOTAL)) 

top_tipos_bps


# 7. Proporção por modalidade de compra administrativa ("A")
n_administrativa <- sum(bps_cmed_clean1$TIPO_COMPRA == "A", na.rm = TRUE)
n_administrativa
perc_administrativa <- round(100 * n_administrativa / total_bps, 1)
perc_administrativa

top_tipo_compra_bps <- bps_cmed_clean1 %>%
  group_by(TIPO_COMPRA) %>%
  summarise(
    N = n(),
    PERCENTUAL = round(100 * N / total_bps, 1),
    RAZAO_MEDIA = round(mean(RAZAO_PRECO, na.rm = TRUE), 2),
    RAZAO_MEDIANA = round(median(RAZAO_PRECO, na.rm = TRUE), 2),
    .groups = "drop"
  )
top_tipo_compra_bps

### 8. Top estados (por quantidade) + razão média
top_nivel_bps <- bps_cmed_clean1 %>%
  group_by(NÍVEL) %>%
  summarise(
    QTD_TOTAL = sum(QUANTIDADE, na.rm = TRUE),
    RAZAO_MEDIA = round(mean(RAZAO_PRECO, na.rm = TRUE), 2),
    RAZAO_MEDIANA = round(median(RAZAO_PRECO, na.rm = TRUE), 2)
  ) %>%
  mutate(perc = round(100 * QTD_TOTAL / sum(QTD_TOTAL), 3))%>%
  arrange(desc(QTD_TOTAL)) 

top_nivel_bps


### 10. Top Faixa (por quantidade) + razão média
# Faixas de preços:

# i) Defina os limites e rótulos das faixas de preço
breaks_faixa <- c(-Inf, 20, 50, 250, 500, 1000, 5000, 20000, 50000, 100000, Inf)
labels_faixa <- c(
  "<= R$ 20,00",
  "R$ 20,01 – R$ 50,00",
  "R$ 50,01 – R$ 250,00",
  "R$ 250,01 – R$ 500,00",
  "R$ 500,01 – R$ 1.000,00",
  "R$ 1.000,01 – R$ 5.000,00",
  "R$ 5.000,01 – R$ 20.000,00",
  "R$ 20.000,01 – R$ 50.000,00",
  "R$ 50.000,01 – R$ 100.000,00",
  ">= R$ 100.000,00"
)
names(bps_cmed_clean1)
# ii) Calcule faturamento bruto e unidades por faixa
bps_cmed_clean1 <- bps_cmed_clean1 %>%
  mutate(
    FAIXA_PU = cut(
      PRECO_TETO_UNITARIO,
      breaks = breaks_faixa,
      labels = labels_faixa,
      right = FALSE,
      include.lowest = TRUE
    )
  ) 
bps_cmed_clean1 <- bps_cmed_clean1 %>%
  mutate(
    FAIXA_PA = cut(
      PRECO_TETO_APRESENTACAO,
      breaks = breaks_faixa,
      labels = labels_faixa,
      right = FALSE,
      include.lowest = TRUE
    )
  ) 
# iii) Visualize o resultado
names(bps_cmed_clean1)

top_faixa_bps <- bps_cmed_clean1 %>%
  group_by(FAIXA_PA) %>%
  summarise(
    QTD_TOTAL = sum(QUANTIDADE, na.rm = TRUE),
    RAZAO_MEDIA = round(mean(RAZAO_PRECO, na.rm = TRUE), 2),
    RAZAO_MEDIANA = round(median(RAZAO_PRECO, na.rm = TRUE), 2)
  ) %>%
  mutate(perc = round(100 * QTD_TOTAL / sum(QTD_TOTAL), 4))%>%
  arrange(desc(QTD_TOTAL)) 

top_faixa_bps


# 11. Saída de resumo
list(
  total_registros = total_bps,
  media_razao = media_razao_bps,
  mediana_razao = mediana_razao_bps,
  top_classes = top_classes_bps,
  top_ufs = top_ufs_bps,
  top_tipos = top_tipos_bps,
  top_tipo_compra_bps = top_tipo_compra_bps,
  top_nivel = top_nivel_bps,
  top_faixa = top_faixa_bps
)

resultados_bps <- list(
  total_registros = as.data.frame(total_bps),
  media_razao     = as.data.frame(media_razao_bps),
  mediana_razao   = as.data.frame(mediana_razao_bps),
  top_classes_bps     = top_classes_bps,
  top_ufs         = top_ufs_bps,
  top_tipos_bps       = top_tipos_bps,
  top_tipo_compra_bps = top_tipo_compra_bps,
  top_nivel_bps       = top_nivel_bps,
  top_faixa_bps       = top_faixa_bps
)

# #__----


# __________________________ ----
## 4. COMBINED DATA ----
# ¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨----
# 4.1. Combinar interseções dos arquivos ----
# Novos relatórios
gera_relatorio(ans_cmed_clean12, "ans_cmed_clean")
gera_relatorio(bps_cmed_clean1, "bps_cmed_clean")

ans_cmed_clean <- ans_cmed_clean12 %>%
  mutate(SETOR = "Privado") 
names(ans_cmed_clean)
bps_cmed_clean <- bps_cmed_clean1 %>%
  mutate(SETOR = "Público")
names(bps_cmed_clean)

intersect_count <- length(intersect(ans_cmed_clean$REGISTRO, bps_cmed_clean$REGISTRO))
print(intersect_count)

intesect_registros <- intersect(
  ans_cmed_clean$REGISTRO,
  bps_cmed_clean$REGISTRO
)
length(intesect_registros)

common_cols <- intersect(names(ans_cmed_clean), names(bps_cmed_clean))

ans_filtrada <- ans_cmed_clean %>%
  filter(REGISTRO %in% intesect_registros) %>%
  select(all_of(common_cols))

bps_filtrada <- bps_cmed_clean %>%
  filter(REGISTRO %in% intesect_registros) %>%
  select(all_of(common_cols))

combined_data <- bind_rows(ans_filtrada, bps_filtrada)
names(combined_data_test)
length(unique(combined_data_test$REGISTRO))


na_counts <- sapply(combined_data, function(x) sum(is.na(x)))
na_counts
na_df <- data.frame(variavel = names(na_counts), n_na = na_counts)
na_df <- na_df[order(-na_df$n_na), ]

print(na_df)

## 4.2. Salvar----

nome_combined_data <- "combined_data_final.parquet"
dir.parquet_combined_data <- file.path(caminho_resultados, nome_combined_data)
write_parquet(combined_data, sink = dir.parquet_combined_data)
combined_data = read_parquet(dir.parquet_combined_data)

