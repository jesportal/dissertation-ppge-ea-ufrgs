rm(list = ls())

# ___________________________ ----
# HETEROGENEOUS ANALYSIS ----
# ¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨----
#______________________----
# 0. Pacotes ----
#¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨-----

library(arrow)
library(car)
library(data.table)
library(devtools)
library(dplyr)
library(DoubleML)
library(fixest)
library(geosphere)
library(ggplot2)
library(glmnet)
library(kableExtra)
library(lubridate)
library(mlr3)
library(mlr3learners)
library(mlr3pipelines)
library(modelsummary)
library(performance)
library(plm)
library(purrr)
library(ranger)
library(readxl)
library(sidrar)
library(stringr)
library(showtext)
library(tibble)
library(tidyr)
library(xgboost)
library(zoo)



#______________________----
# 1. Preparação dos dados ----
#¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨-----

# Carregar ----
caminho_draft <- "../data/02_interim"
caminho_resultados <- "../data/03_processed"

nome_combined_data <- "combined_data_final.parquet"
dir.parquet_combined_data <- file.path(caminho_resultados, nome_combined_data)
combined_data_backup = read_parquet(dir.parquet_combined_data)

names(combined_data_backup)

combined_data = combined_data_backup
names(combined_data)

print(combined_data |>
        summarise(across(everything(), ~sum(is.na(.)))) |>
        pivot_longer(everything(), names_to = "variable", values_to = "n_missing"), n = 100)



# Tratamentos ----

# definir efeitos fixos e controles
vars_fe   <- c("REGISTRO", "ANO_MES")
vars_ctrl <- c(
  "UF", "TIPO_PRODUTO", "TARJA", "RESTRICAO_HOSPITALAR", "REGIAO","REGISTRO_ANVISA",
  "CLASSE_PRINCIPAL_CODIGO","CLASSE_TERAPEUTICA", "CLASSE_TERAPEUTICA_CODIGO",
  "CLASSE_TERAPEUTICA_DESCRICAO", "CLASSE_PRINCIPAL_CODIGO" , "CLASSE_PRINCIPAL_DESCRICAO",
  "LISTA_CREDITO_PIS_COFINS", "CNPJ", "SUBSTANCIA", "PRODUTO", "APRESENTACAO_PADRAO", 
  "CAP", "CONFAZ_87", "ICMS_0", "REGIME", "NIVEL_LAG","GRUPO_DESCONTO_LAG", 
  "REAJUSTE", "REAJUSTE_INICIO", "ANO",
  "CATEGORIA","CATEGORIA_PRECO", "REGIME_PRECO", "REG_APR","REGISTRO_APRESENTACAO",
  "LABORATORIO", "CATEGORIA_PRECO", "SETOR", "CHAVE_PRECO", "CHAVE_JUNCAO", "APRESENTACAO"
)

combined_data = combined_data |>
  mutate(
    across(
      .cols = all_of(c(vars_fe, vars_ctrl)),
      .fns  = as.factor
    )
  )
unique(combined_data$SETOR)

names(combined_data)


vars <- c(
  "UF", "TIPO_PRODUTO", "TARJA", "RESTRICAO_HOSPITALAR", "REGIAO","REGISTRO_ANVISA",
  "CLASSE_PRINCIPAL_CODIGO","CLASSE_TERAPEUTICA", "CLASSE_TERAPEUTICA_CODIGO",
  "CLASSE_TERAPEUTICA_DESCRICAO", "CLASSE_PRINCIPAL_CODIGO" , "CLASSE_PRINCIPAL_DESCRICAO",
  "LISTA_CREDITO_PIS_COFINS", "CNPJ", "SUBSTANCIA", "PRODUTO", "APRESENTACAO_PADRAO", 
  "CAP", "CONFAZ_87", "ICMS_0", "REGIME", "NIVEL_LAG","GRUPO_DESCONTO_LAG", 
  "REAJUSTE", "REAJUSTE_INICIO",
  "CATEGORIA","CATEGORIA_PRECO", "REGIME_PRECO", "REG_APR","REGISTRO_APRESENTACAO",
  "LABORATORIO", "CATEGORIA_PRECO", "SETOR", "CHAVE_PRECO", "CHAVE_JUNCAO", "APRESENTACAO"
)
setDT(combined_data)

for (v in vars) {
  dt_var <- combined_data[, .(n_levels = uniqueN(get(v))), by = REGISTRO]
  pct_varia <- dt_var[, mean(n_levels > 1)] * 100
  cat(sprintf(
    "  • %s varia dentro de registro em %.1f%% dos registros\n",
    v, pct_varia
  ))
}


# Filtrar outliers ----

quantile(combined_data$RAZAO_PRECO, probs = seq(0, 1, 0.1), na.rm = TRUE)
hist(combined_data$RAZAO_PRECO)

combined_data_1 = combined_data |>
  filter(REGIME == "REGULADO",
         REGIME_PRECO == "REGULADO") |>
  mutate(REGIME_PRECO = as.factor(REGIME_PRECO),
         REGIME = as.factor(REGIME))

names(combined_data_1)
unique(combined_data_1$REGIME)
unique(combined_data_1$REGIME_PRECO)


combined_data_filter = combined_data_1 |>
  filter(RAZAO_PRECO <= quantile(RAZAO_PRECO, 0.99, na.rm = TRUE)) 



quantile(combined_data_filter$RAZAO_PRECO, probs = seq(0, 1, 0.1), na.rm = TRUE)
hist(combined_data_filter$RAZAO_PRECO)

combined_data_filter= combined_data_filter |>
  select(- c(CHAVE_PRECO,CHAVE_JUNCAO, CHAVE_CLASSE_ANOMES_LAG, CHAVE_CLASSE_ANOMES))
rm(sum_dt)

# Agregar ----
unique(combined_data_filter$TIPO_PRODUTO)
unique(combined_data_filter$CATEGORIA_PRECO)
unique(combined_data_filter$ICMS)

dt_agg_fe <- combined_data_filter |>
  mutate(
    UNIT    = as.factor(paste(REGISTRO, UF, SETOR, sep = "_")),
    ANO_MES = as.yearmon(ANO_MES, "%Y-%m")
  ) |>
  group_by(UNIT, REGISTRO, REAJUSTE, CATEGORIA_PRECO, ICMS, SETOR, 
           REGIAO, NIVEL_LAG, GRUPO_DESCONTO, ANO_MES) %>%
  summarise(
    QUANTIDADE         = sum(QUANTIDADE, na.rm = TRUE),
    PRECO_UNITARIO     = mean(PRECO_UNITARIO, na.rm = TRUE),
    TETO_UNITARIO      = mean(PRECO_TETO_UNITARIO),
    TETO_APRES         = mean(PRECO_TETO_APRESENTACAO),
    RAZAO_PRECO        = mean(RAZAO_PRECO, na.rm = TRUE),
    GRUPO_DESCONTO_LAG = first(GRUPO_DESCONTO_LAG),
    RAZAO_PRECO_LAG    = first(RAZAO_PRECO_LAG),
    REAJUSTE_INICIO    = first(REAJUSTE_INICIO),
    VPP                = first(VPP),
    MESES_DESDE_REAJ   = first(MESES_DESDE_REAJ),
    ICMS               = first(ICMS),
    TIPO_PRODUTO       = first(TIPO_PRODUTO),
    SUBSTANCIA         = first(SUBSTANCIA),
    .groups            = "drop"
  ) |>
  mutate(
    SETOR_PUB         = ifelse(SETOR == "Público", 1L, 0L),
    LOG_QUANTIDADE    = log(QUANTIDADE),
    LOG_RAZAO_PRECO   = log(RAZAO_PRECO),
    LOG_PU            = log(PRECO_UNITARIO),
    LOG_CAP_U         = log(TETO_UNITARIO),
    LOG_QUANTIDADE    = log(QUANTIDADE),
    LOG_RAZAO_PRECO   = log(RAZAO_PRECO)
  ) |>
  as.data.table()


dt_agg_fe <- as.data.table(dt_agg_fe)


dt_agg_fe |>
  summarise(
    mean_log_q = mean(LOG_QUANTIDADE, na.rm = TRUE),
    mean_log_p = mean(LOG_RAZAO_PRECO, na.rm = TRUE)
  )

names(dt_agg_fe)

#----
# 3) Para cada fator, encontra o nível cujo sum(QUANTIDADE) é máximo e relevel
factors_to_relevel <- c(
  "UF", "REGIAO", "NIVEL_LAG", "GRUPO_DESCONTO_LAG", "SETOR", "FAIXA_PA", "REAJUSTE", "REAJUSTE_INICIO"
)
setDT(combined_data_filter)
names(combined_data_filter)

for (f in factors_to_relevel) {
  if (f %in% names(combined_data_filter)) {
    sum_dt     <- combined_data_filter[, list(sumQ = sum(QUANTIDADE, na.rm = TRUE)), by = f]
    base_level <- sum_dt[which.max(sumQ), get(f)]
    other_lvls <- setdiff(sum_dt[[f]], base_level)
    combined_data_filter[, (f) := factor(get(f), levels = c(base_level, other_lvls))]
  }
}

for (f in factors_to_relevel) {
  if (f %in% names(dt_agg_fe)) {
    sum_dt     <- dt_agg_fe[, list(sumQ = sum(QUANTIDADE, na.rm = TRUE)), by = f]
    base_level <- sum_dt[which.max(sumQ), get(f)]
    other_lvls <- setdiff(sum_dt[[f]], base_level)
    dt_agg_fe[, (f) := factor(get(f), levels = c(base_level, other_lvls))]
  }
}
rm(factors_to_relevel)
rm(sum_dt)


print(combined_data_filter |>
        summarise(across(everything(), ~sum(is.na(.)))) |>
        pivot_longer(everything(), names_to = "variable", values_to = "n_missing"), n = 100)
names(combined_data_filter)

#______________________----
# 2. Modelo de Efeitos Fixos (FEOLS) ----
#¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨-----

## 2.1 Escalonado ----

# M0: básico (apenas razão preço-cap)
m0 <- feols(LOG_QUANTIDADE ~ LOG_RAZAO_PRECO,
            data = dt_agg_fe, cluster = ~ REGISTRO)

# M1: adiciona setor + interação (elasticidade difere por setor)
m1 <- feols(LOG_QUANTIDADE ~ LOG_RAZAO_PRECO * SETOR_PUB,
            data = dt_agg_fe, cluster = ~ REGISTRO)

# M2: adiciona REGIÃO
m2 <- feols(LOG_QUANTIDADE ~ LOG_RAZAO_PRECO * SETOR_PUB + REGIAO,
            data = dt_agg_fe, cluster = ~ REGISTRO)

# M3: adiciona defasagens (nível)
m3 <- feols(LOG_QUANTIDADE ~ LOG_RAZAO_PRECO * SETOR_PUB +
              REGIAO + NIVEL_LAG,
            data = dt_agg_fe, cluster = ~ REGISTRO)

# M4: adiciona defasagens (grupos de desconto)
m4 <- feols(LOG_QUANTIDADE ~ LOG_RAZAO_PRECO * SETOR_PUB +
              REGIAO + NIVEL_LAG + GRUPO_DESCONTO_LAG,
            data = dt_agg_fe, cluster = ~ REGISTRO)
# M5: adiciona FEs de REGISTRO + REAJUSTE
m5 <- feols(LOG_QUANTIDADE ~ LOG_RAZAO_PRECO * SETOR_PUB +
              REGIAO + NIVEL_LAG + GRUPO_DESCONTO_LAG
            | REGISTRO + REAJUSTE,
            data = dt_agg_fe, cluster = ~ REGISTRO)

# M6 (final): adiciona FE de REGISTRO também (seu modelo original)
m6 <- feols(LOG_QUANTIDADE ~ LOG_RAZAO_PRECO * SETOR_PUB +
              REGIAO + NIVEL_LAG + GRUPO_DESCONTO_LAG
            | REGISTRO + CATEGORIA_PRECO + ICMS + REAJUSTE,
            data = dt_agg_fe, cluster = ~ REGISTRO)

summary(m6)
print(m6)


## 2.2. Robustez ----

check_collinearity(m6)
wald_car_F <- linearHypothesis(
  m6,
  "LOG_RAZAO_PRECO:SETOR_PUB = 0",
  test = "F"
)
print(wald_car_F)


results_feols = etable(m0, m1, m2, m3, m4, m5,m6,
       se = "cluster", cluster = ~ REGISTRO,
       fitstat = c("n","r2","wr2","rmse"),  # todos válidos na sua versão
       dict = c("LOG_QUANTIDADE"="log(Q)",
                "LOG_RAZAO_PRECO"="log(price/cap)",
                "SETOR_PUB"="Public",
                "LOG_RAZAO_PRECO:SETOR_PUB"="log(price/cap) × Public",
                "REGIAO"="Region",
                "NIVEL_LAG"="Lag level",
                "GRUPO_DESCONTO_LAG"="Lag discount grp"))
print(results_feols)

# (M1 vs M0): setor e interação
linearHypothesis(m1, c("SETOR_PUB = 0", "LOG_RAZAO_PRECO:SETOR_PUB = 0"))

# (M2 vs M1): REGIÃO
regs <- grep("^REGIAO", names(coef(m2)), value = TRUE)
linearHypothesis(m2, paste0(regs, " = 0"))

# (M3 vs M2): lags
lags <- grep("^(NIVEL_LAG)", names(coef(m3)), value = TRUE)
linearHypothesis(m3, paste0(lags, " = 0"))

# (M4 vs M3): lags
lags <- grep("^(GRUPO_DESCONTO_LAG)", names(coef(m4)), value = TRUE)
linearHypothesis(m4, paste0(lags, " = 0"))

# (M5 vs M4): lags
lags <- grep("^(GRUPO_DESCONTO_LAG)", names(coef(m5)), value = TRUE)
linearHypothesis(m5, paste0(lags, " = 0"))

# (M6 vs M5): lags
lags <- grep("^(GRUPO_DESCONTO_LAG)", names(coef(m6)), value = TRUE)
linearHypothesis(m6, paste0(lags, " = 0"))

car::linearHypothesis(m6, c("NIVEL_LAG2 = 0", "NIVEL_LAG3 = 0"))



## 2.3. Gráficos -----

mods <- list(M1 = m1, M2 = m2, M3 = m3, M4 = m4, M5 = m5, m6=m6)

mods <- list(
  "M1: +Public interaction"          = m1,
  "M2: +Regions"                       = m2,
  "M3: +Concentration Level"         = m3,
  "M4: +Discount Interval Lag"         = m4,
  "M5: +FE {Registry, Adjustment Validity}" = m5,
  "M6: +FE +{Price Category, ICMS}"                 = m6
)

est_three <- function(mod, mname){
  b <- coef(mod)
  V <- vcov(mod)  
  
  lrp   <- "LOG_RAZAO_PRECO"
  inter <- "LOG_RAZAO_PRECO:SETOR_PUB"
  stopifnot(lrp %in% names(b), inter %in% names(b))
  
  priv_b  <- b[lrp]
  priv_se <- sqrt(V[lrp, lrp])
  
  pub_b   <- priv_b + diff_b
  pub_se  <- sqrt(V[lrp, lrp] + V[inter, inter] + 2 * V[lrp, inter])
  
  tibble(
    model = mname,
    term  = c("Private", "Public"), 
    beta  = c(priv_b, pub_b, diff_b),
    se    = c(priv_se, pub_se, diff_se)
  ) |>
    mutate(ci_lo = beta - 1.96*se,
           ci_hi = beta + 1.96*se)
}

df_plot <- bind_rows(lapply(names(mods), function(nm) est_three(mods[[nm]], nm))) |>
  mutate(model = factor(model, levels = names(mods)),
         term  = factor(term, levels = c("Private","Public")))

# Plot
ggplot(df_plot, aes(x = model, y = beta, color = term)) +
  geom_hline(yintercept = 0, linetype = "dashed", color = "grey40") +
  geom_point(position = position_dodge(width = 0.45), size = 2.5) +
  geom_errorbar(aes(ymin = ci_lo, ymax = ci_hi),
                position = position_dodge(width = 0.45), width = 0.15, linewidth = 0.6) +
  scale_color_manual(values = c("Private" = "#1f77b4",
                                "Public"  = "#2ca02c",
                                "Difference" = "#d62728")) +
  labs(x = "Model",
       y = "Elasticity w.r.t. log(price/cap)",
       color = NULL,
       title = "Elasticities by sector and difference (M1–M6)",
       subtitle = "Points = estimate; bars = 95% CI; SEs clustered at REGISTRO") +
  theme_minimal(base_size = 13) +
  theme(legend.position = "top")


## Garantir Times New Roman disponível
font_add("Times New Roman", regular = "C:/Windows/Fonts/times.ttf")
showtext_auto()

df_plot_clean <- df_plot %>%
  filter(term %in% c("Private", "Public")) %>%
  droplevels()

g_fe_ols <- ggplot(df_plot_clean, aes(x = model, y = beta, color = term)) +
  geom_hline(yintercept = 0, linetype = "dashed", color = "grey40") +
  geom_point(position = position_dodge(width = 0.45), size = 2.5) +
  geom_errorbar(aes(ymin = ci_lo, ymax = ci_hi),
                position = position_dodge(width = 0.45),
                width = 0.15, linewidth = 0.6) +
  scale_color_manual(
    values = c("Private" = "#1f77b4", "Public" = "#2ca02c"),
    breaks = c("Private", "Public")
  ) +
  labs(
    x = "Model",
    y = "log(P_obs/P_cap)",
    color = NULL,
    title = element_blank(),
    subtitle = "Points = estimate; bars = 95% CI; SEs clustered at REGISTRO"
  ) +
  theme_minimal(base_family = "Times New Roman") +
  theme(
    axis.text.x  = element_text(family = "Times New Roman", size = 45),
    axis.text.y  = element_text(family = "Times New Roman", size = 40),
    axis.title.x = element_blank(),
    axis.title.y = element_text(family = "Times New Roman", size = 40),
    plot.title   = element_text(family = "Times New Roman", size = 45, face = "bold"),
    plot.subtitle= element_blank(),
    legend.position = "top",
    legend.text     = element_text(family = "Times New Roman", size = 40),
    panel.grid.major = element_line(color = "grey90"),
    panel.grid.minor = element_blank()
  )
g_fe_ols

plots_list <- list(
  g_fe_ols = g_fe_ols
)

# Diretório de saída (opcional; crie se não existir)
out_dir <- "../results/midia"

if (!dir.exists(out_dir)) dir.create(out_dir)

for (name in names(plots_list)) {
  ggsave(
    filename = file.path(out_dir, paste0(name, ".png")),
    plot     = plots_list[[name]],
    device   = "png",
    width    = 14,      # largura em polegadas
    height   = 10,      # altura em polegadas
    dpi      = 300,     # resolução em pontos por polegada
    units    = "in"     # unidade explícita
  )
}

## 2.4. Instrumento ----

### a) Inovadores vs genéricos ----

# 1) Classificação: INOVADOR = {NOVO, BIOLÓGICO NOVO}; GENÉRICO = {GENÉRICO}; OUTROS = {BIOLÓGICO, ESPECÍFICO, SIMILAR}
dt_agg_fe[, CLASS_TIPO := fcase(
  TIPO_PRODUTO %in% c("NOVO", "BIOLÓGICO NOVO"), "INOV",
  TIPO_PRODUTO == "GENÉRICO",                    "GEN",
  default = "OUTROS"
)]

# 2) Dummies pontuais
dt_agg_fe[, IS_INOVADOR := as.integer(CLASS_TIPO == "INOV")]
dt_agg_fe[, IS_GENERICO := as.integer(CLASS_TIPO == "GEN")]

# 3) Instrumentos = VPP × dummy (um para inovadores, outro para genéricos)
dt_agg_fe[, Z_VPP_INOV := VPP * IS_INOVADOR]
dt_agg_fe[, Z_VPP_GEN  := VPP * IS_GENERICO]

mod_a <- feols(
  LOG_QUANTIDADE ~ 
    SETOR_PUB + REGIAO + NIVEL_LAG + GRUPO_DESCONTO_LAG
  | REGISTRO + REAJUSTE
  | (LOG_RAZAO_PRECO + LOG_RAZAO_PRECO:SETOR_PUB) ~
    (Z_VPP_INOV + Z_VPP_GEN) +
    (Z_VPP_INOV:SETOR_PUB + Z_VPP_GEN:SETOR_PUB),
  data    = dt_agg_fe,
  cluster = ~REGISTRO
)

summary(mod_a)


###b) ICMS ----
dt_agg_fe[, Z_ICMS := ICMS]

mod_b <- feols(
  LOG_QUANTIDADE ~ SETOR_PUB + REGIAO + NIVEL_LAG + GRUPO_DESCONTO_LAG
  | REGISTRO + REAJUSTE
  | (LOG_RAZAO_PRECO + LOG_RAZAO_PRECO:SETOR_PUB) ~ 
    Z_ICMS + Z_ICMS:SETOR_PUB,
  data = dt_agg_fe, cluster = ~REGISTRO
)
summary(mod_b)

### c) PIM Farma----
# instrumento: produção nacional de produtos farmacêuticos
# Obter produção industrial (PIM-PF)
producao_ibge <- get_sidra(api = "/t/8888/n1/all/n2/all/n3/all/v/12606/p/all/c544/129314,129330/d/v12606%205")
names(producao_ibge)
producao_farma = producao_ibge |>
  filter( `Seções e atividades industriais (CNAE 2.0) (Código)` == "129330",
          `Nível Territorial` == "Brasil",
  )
producao_farma <- producao_farma %>%
  mutate(
    mes_codigo = as.character(`Mês (Código)`),
    data_mes   = ym(mes_codigo),
    ANO_MES     = as.yearmon(data_mes, "%Y-%m"),
    PROD_FARM = Valor
  ) %>%
  select(ANO_MES, PROD_FARM)

names(producao_farma)
names(dt_agg_fe)

producao_farma <- producao_farma %>%
  mutate(ANO_MES = as.yearmon(ANO_MES, "%Y-%m"))

dt_agg_fe = dt_agg_fe |>
  left_join(producao_farma, by = "ANO_MES")
names(dt_agg_fe)

fe_model_prod_iv2 <- feols(
  # 1. regressão estrutural: apenas variáveis exógenas
  LOG_QUANTIDADE ~ 
    REGIAO + NIVEL_LAG + GRUPO_DESCONTO_LAG
  # 2. efeitos fixos
  | REGISTRO + ANO_MES
  # 3. IV: preço e interação endógenos instrumentados por produtividade
  | (LOG_RAZAO_PRECO + LOG_RAZAO_PRECO:SETOR_PUB) 
  ~ (PROD_FARM + PROD_FARM:SETOR_PUB),
  data    = dt_agg_fe,
  cluster = ~REGISTRO
)

summary(fe_model_prod_iv2)

### d) Vigência reajuste ----
fe_model_iv1 <- feols(
  # bloco 1: só exógenas
  LOG_QUANTIDADE ~ 
    REGIAO + NIVEL_LAG + GRUPO_DESCONTO_LAG
  # bloco 2: efeitos fixos
  | REGISTRO 
  # bloco 3: endógenas ~ instrumentos
  | (LOG_RAZAO_PRECO + LOG_RAZAO_PRECO:SETOR_PUB) 
  ~ (REAJUSTE + REAJUSTE:SETOR_PUB),
  data    = dt_agg_fe,
  cluster = ~REGISTRO
)

summary(fe_model_iv1)


### e) N meses desde autorização ----

fe_model_iv3 <- feols(
  # (i) só exógenas
  LOG_QUANTIDADE ~ 
    REGIAO 
  + NIVEL_LAG 
  + GRUPO_DESCONTO_LAG
  
  # (ii) efeitos fixos de produto e mês-ano
  | REGISTRO + ANO_MES
  
  # (iii) preços endógenos ~ instrumentos
  | (LOG_RAZAO_PRECO + LOG_RAZAO_PRECO:SETOR_PUB) 
  ~ (MESES_DESDE_REAJ + MESES_DESDE_REAJ:SETOR_PUB),
  
  data    = dt_agg_fe,
  cluster = ~REGISTRO
)

summary(fe_model_iv3)

### f) dummy inicio reajuste ----
fe_model_iv4 <- feols(
  # bloco 1: só exógenas
  LOG_QUANTIDADE ~ 
    REGIAO + NIVEL_LAG + GRUPO_DESCONTO_LAG
  # bloco 2: efeitos fixos
  | REGISTRO + ANO_MES
  # bloco 3: endógenas ~ instrumentos
  | (LOG_RAZAO_PRECO + LOG_RAZAO_PRECO:SETOR_PUB) 
  ~ (REAJUSTE_INICIO + REAJUSTE_INICIO:SETOR_PUB),
  data    = dt_agg_fe,
  cluster = ~REGISTRO
)

# com setor público 
summary(fe_model_iv4)

fe_model_iv4.1 <- feols(
  LOG_QUANTIDADE ~
    SETOR_PUB                       # controle pelo setor
  + REGIAO                         # controles regionais
  + NIVEL_LAG 
  + GRUPO_DESCONTO_LAG
  | REGISTRO + ANO_MES             # 2) efeitos fixos
  | (LOG_RAZAO_PRECO               # 3) variáveis endógenas
     + LOG_RAZAO_PRECO:SETOR_PUB) 
  ~ (REAJUSTE_INICIO             #    instrumentos discretos
     + REAJUSTE_INICIO:SETOR_PUB),
  data    = dt_agg_fe,
  cluster = ~REGISTRO
)

summary(fe_model_iv4.1)


# instrumento: valor do reajuste autorizado
fe_model_iv5 <- feols(
  # bloco 1: só exógenas
  LOG_QUANTIDADE ~ 
    REGIAO + NIVEL_LAG + GRUPO_DESCONTO_LAG
  # bloco 2: efeitos fixos
  | REGISTRO + ANO_MES
  # bloco 3: endógenas ~ instrumentos
  | (LOG_RAZAO_PRECO + LOG_RAZAO_PRECO:SETOR_PUB) 
  ~ (VPP + VPP:SETOR_PUB),
  data    = dt_agg_fe,
  cluster = ~REGISTRO
)

summary(fe_model_iv5)


fe_model_iv6 <- feols(
  LOG_QUANTIDADE ~ 
    SETOR_PUB +
    REGIAO +
    NIVEL_LAG +
    GRUPO_DESCONTO_LAG
  | REGISTRO + ANO_MES
  | (LOG_RAZAO_PRECO + LOG_RAZAO_PRECO:SETOR_PUB) ~
    # Instrumentos principais
    (VPP + VPP:SETOR_PUB) +           # Valor efetivo do reajuste
    # Instrumentos adicionais para robustez
    (REAJUSTE_INICIO + REAJUSTE_INICIO:SETOR_PUB),  # Dummy temporal
  data = dt_agg_fe,
  cluster = ~REGISTRO,
  ssc = ssc(adj = TRUE, cluster.adj = TRUE))
summary(fe_model_iv6)


fe_model_iv_combined2 <- feols(
  # 1) regressão estrutural: só exógenas
  LOG_QUANTIDADE ~
    SETOR_PUB
  + REGIAO
  + NIVEL_LAG
  + GRUPO_DESCONTO_LAG
  
  # 2) efeitos fixos
  | REGISTRO + ANO_MES
  
  # 3) endógenas ~ instrumentos
  | (LOG_RAZAO_PRECO + LOG_RAZAO_PRECO:SETOR_PUB)
  ~ (VPP + MESES_DESDE_REAJ)
  + (VPP:SETOR_PUB + MESES_DESDE_REAJ:SETOR_PUB),
  
  data    = dt_agg_fe,
  cluster = ~REGISTRO
)

summary(fe_model_iv_combined2)


## 2.5. Outras interações ----
### Faixas de desconto ----
fe_model_agg_gd <- feols(
  LOG_QUANTIDADE ~ 
    i(GRUPO_DESCONTO_LAG, LOG_RAZAO_PRECO) * SETOR_PUB
  + REGIAO + NIVEL_LAG
  | REGISTRO + ANO_MES,
  data = dt_agg_fe,
  cluster = ~REGISTRO
)

summary(fe_model_agg_gd_c)
check_collinearity(fe_model_agg_gd)

fe_model_agg_gd <- feols(
  LOG_QUANTIDADE ~ 
    i(GRUPO_DESCONTO_LAG, LOG_RAZAO_PRECO) * SETOR_PUB
  + REGIAO + NIVEL_LAG
  | REGISTRO + ANO_MES,
  data = dt_agg_fe,
  cluster = ~REGISTRO
)

summary(fe_model_agg_gd)
check_collinearity(fe_model_agg_gd)


### Nível concorrencial ----
fe_model_agg_n_c <- feols(
  LOG_QUANTIDADE_C ~ 
    i(NIVEL_LAG, LOG_RAZAO_PRECO_C) * SETOR_PUB
  + REGIAO + GRUPO_DESCONTO_LAG
  | REGISTRO + ANO_MES,
  data = dt_agg_fe,
  cluster = ~REGISTRO
)
summary(fe_model_agg_n_c)
check_collinearity(fe_model_agg_n_c)


fe_model_agg_n <- feols(
  LOG_QUANTIDADE ~ 
    i(NIVEL_LAG, LOG_RAZAO_PRECO) * SETOR_PUB
  + REGIAO + GRUPO_DESCONTO_LAG
  | REGISTRO + ANO_MES,
  data = dt_agg_fe,
  cluster = ~REGISTRO
)
summary(fe_model_agg_n)
check_collinearity(fe_model_agg_n)


## 2.6 Separados por setor ----
dt_priv_c <- filter(dt_agg_fe, SETOR_PUB == 0)
dt_pub_c <- filter(dt_agg_fe, SETOR_PUB == 1)

fe_model_priv_c <- feols(
  LOG_QUANTIDADE_C ~ 
    LOG_RAZAO_PRECO_C
  + REGIAO + factor(NIVEL_LAG) + GRUPO_DESCONTO_LAG
  | REGISTRO + ANO_MES,
  data = dt_priv_c,
  cluster = ~REGISTRO
)

summary(fe_model_priv_c)
check_collinearity(fe_model_priv_c)

fe_model_pub_c <- feols(
  LOG_QUANTIDADE_C ~ 
    LOG_RAZAO_PRECO_C
  + REGIAO + factor(NIVEL_LAG) + GRUPO_DESCONTO_LAG
  | REGISTRO + ANO_MES,
  data = dt_pub_c,
  cluster = ~REGISTRO
)

summary(fe_model_pub_c)
check_collinearity(fe_model_pub_c)



#______________________----
# 3. Double Machine Learning (DML) ----
## DoubleML – PLR com D contínuo + interação (setor público)
## Learners: Lasso, CART, RF, Boosting (XGBoost), NN, Ensemble
## Robustez: 2 vs 5 folds e variações leves de hiperparâmetros

#¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨-----
## 3.1. Preparação dos dados -------------------------------------

drop_const_cols_matrix <- function(M) {
  if (!is.matrix(M)) M <- as.matrix(M)
  if (ncol(M) == 0) return(M)
  keep <- apply(M, 2, function(x) sd(x, na.rm = TRUE) > 0)
  if (!any(keep)) {
    warning("Todas as colunas são constantes. Retornando matriz vazia.")
    return(M[, 0, drop = FALSE])
  }
  M[, keep, drop = FALSE]
}
names(dt_agg_fe)


dt_dml_raw_1 <- dt_agg_fe %>%
  mutate(
    Y = LOG_QUANTIDADE,
    D = LOG_RAZAO_PRECO,
    D_INTERACAO = D * SETOR_PUB
  ) %>%
  select(
    Y, D, D_INTERACAO, SETOR_PUB,
    REGIAO, NIVEL_LAG, GRUPO_DESCONTO_LAG,
    REGISTRO, CATEGORIA_PRECO, ICMS, REAJUSTE
  ) %>%
  tidyr::drop_na() %>%
  mutate(
    GRUPO_DESCONTO_LAG = str_replace_all(GRUPO_DESCONTO_LAG, "[^[:alnum:]]", "_"),
    NIVEL_LAG          = str_replace_all(NIVEL_LAG,          "[^[:alnum:]]", "_"),
    REGIAO             = str_replace_all(REGIAO,             "[^[:alnum:]]", "_"),
    ICMS               = stringr::str_replace_all(ICMS,            "[^[:alnum:]]", "_"),
    REAJUSTE           = stringr::str_replace_all(REAJUSTE,        "[^[:alnum:]]", "_"),
    CATEGORIA_PRECO    = stringr::str_replace_all(CATEGORIA_PRECO, "[^[:alnum:]]", "_"),
    REGIAO             = factor(REGIAO),
    NIVEL_LAG          = factor(NIVEL_LAG),
    GRUPO_DESCONTO_LAG = factor(GRUPO_DESCONTO_LAG),
    ICMS               = factor(ICMS),
    REAJUSTE           = factor(REAJUSTE),
    CATEGORIA_PRECO    = factor(CATEGORIA_PRECO)
  )

mm_1 <- model.matrix(
  ~ REGIAO + NIVEL_LAG + GRUPO_DESCONTO_LAG + SETOR_PUB + 
    factor(CATEGORIA_PRECO) + factor(ICMS) + factor(REAJUSTE) - 1,
  data = dt_dml_raw_1
)
mm_1 <- drop_const_cols_matrix(mm_1)

# Após montar features:
base_df_1 <- cbind(
  dt_dml_raw_1[, c("Y","D","D_INTERACAO", "REGISTRO")],  
  as.data.frame(mm_1)                                    
)

dml_data_1 <- DoubleMLClusterData$new(
  data = base_df_1,
  y_col = "Y",
  d_cols = c("D","D_INTERACAO"),
  x_cols = setdiff(colnames(base_df_1), c("Y","D","D_INTERACAO","REGISTRO")),
  cluster_cols = "REGISTRO"     
)

p_1 <- length(dml_data_1$x_cols)
p_1

## 3.2. Learners (parâmetros fixos) ----------------

# Lasso (aproxima Belloni et al.)
lasso_1se <- lrn("regr.cv_glmnet", s = "lambda.1se", nfolds = 10)
lasso_min <- lrn("regr.cv_glmnet", s = "lambda.min", nfolds = 10)

# CART (CART único com xval)
cart_base <- lrn("regr.rpart",
                 cp = 0.001, minsplit = 20, maxdepth = 10, xval = 0)

# Random Forest (artigo trazia 1000 árvores, mas foi pesado demais)
rf_base <- lrn("regr.ranger",
               num.trees = 300,
               mtry = max(3, floor(sqrt(p_1))),
               min.node.size = 5)

# Boosting "GBM-like" via XGBoost
boost_base <- lrn("regr.xgboost",
                  objective = "reg:squarederror",
                  eta = 0.05, nrounds = 300,
                  max_depth = 3,
                  subsample = 0.8,
                  colsample_bytree = 0.8,
                  min_child_weight = 1)

# Neural Net 
nn_base <- lrn("regr.nnet",
               size = 2, decay = 0.02,
               maxit = 500, MaxNWts = 20000, trace = FALSE)

# Ensemble (stacking linear de predições CV: Lasso + RF + XGB + NN)
po_cv_lasso <- po("learner_cv", lasso_1se$clone(), id = "cv_lasso")
po_cv_rf    <- po("learner_cv", rf_base$clone(),    id = "cv_rf")
po_cv_xgb   <- po("learner_cv", boost_base$clone(), id = "cv_xgb")
po_cv_nn    <- po("learner_cv", nn_base$clone(),    id = "cv_nn")

graph_stack <- gunion(list(po_cv_lasso, po_cv_rf, po_cv_xgb, po_cv_nn)) %>>%
  po("featureunion") %>>%
  lrn("regr.lm")

ens_base <- GraphLearner$new(graph_stack)


## 3.3. Função DML (PLR, partialling out) – CIs manuais ----

run_dml_model_1 <- function(name, learner_l, learner_m, data_obj,
                          n_folds = 5, n_rep = 25, z = 1.96) {
  cat("\n\n--- Running:", name, "| K =", n_folds, "| n_rep =", n_rep, "---\n")
  dml <- DoubleMLPLR$new(
    data = data_obj,
    ml_l = learner_l,   # E[Y|X]
    ml_m = learner_m,   # E[D|X]
    n_folds = n_folds,
    n_rep = n_rep,                 
    score = "partialling out",
    dml_procedure = "dml2"
  )
  set.seed(123)
  dml$fit()

  est <- as.numeric(dml$coef)
  se  <- as.numeric(dml$se)
  terms <- names(dml$coef)
  
  tibble(
    model     = name,
    term      = terms,
    estimate  = est,
    std_error = se,
    p_value   = 2 * pnorm(-abs(est / se)),
    ci_lower  = est - z * se,
    ci_upper  = est + z * se,
    n_folds   = n_folds,
    n_rep     = n_rep
  )
}

run_dml_model_2 <- function(name, learner_l, learner_m, data_obj,
                            n_folds = 5, n_rep = 5, z = 1.96) {
  cat("\n\n--- Running:", name, "| K =", n_folds, "| n_rep =", n_rep, "---\n")
  dml <- DoubleMLPLR$new(
    data = data_obj,
    ml_l = learner_l,   # E[Y|X]
    ml_m = learner_m,   # E[D|X]
    n_folds = n_folds,
    n_rep = n_rep,                 
    score = "partialling out",
    dml_procedure = "dml2"
  )
  set.seed(123)
  dml$fit()
  
  est <- as.numeric(dml$coef)
  se  <- as.numeric(dml$se)
  terms <- names(dml$coef)
  
  tibble(
    model     = name,
    term      = terms,
    estimate  = est,
    std_error = se,
    p_value   = 2 * pnorm(-abs(est / se)),
    ci_lower  = est - z * se,
    ci_upper  = est + z * se,
    n_folds   = n_folds,
    n_rep     = n_rep
  )
}

## 3.4. Estimação principal (K=5) com os learners do artigo ----

set.seed(123)

dml_lasso_5 = run_dml_model_1("Lasso (λ.1se, K=5)", lasso_1se, lasso_1se, dml_data_1, n_folds = 5, n_rep = 25)
print(dml_lasso_5)
dml_cart_5 = run_dml_model_1("CART (K=5)",        cart_base,  cart_base,  dml_data_1, n_folds = 5, n_rep = 25)
print(dml_cart_5)
dml_rf_5 = run_dml_model_2("Random Forest (K=5)", rf_base,  rf_base,    dml_data_1, n_folds = 5, n_rep = 5)
print(dml_rf_5)
dml_xgb_5 =  run_dml_model_2("Boosting XGB (K=5)",  boost_base, boost_base, dml_data_1, n_folds = 5, n_rep = 5)
print(dml_xgb_5)
dml_nn_5 = run_dml_model_2("Neural Net (K=5)",    nn_base,   nn_base,     dml_data_1, n_folds = 5, n_rep = 5)
print(dml_nn_5)

results_dml_5 <- bind_rows(
  dml_lasso_5, dml_cart_5, dml_rf_5, dml_xgb_5, dml_nn_5
)
print(results_dml_5)



## 3.5. Robustez ----


### (a) K=2 ----

dml_lasso_2 = run_dml_model_1("Lasso (λ.1se, K=2)", lasso_1se, lasso_1se, dml_data_1, n_folds = 2, n_rep = 25)
print(dml_lasso_2)
dml_cart_2 = run_dml_model_1("CART (K=2)",        cart_base,  cart_base,  dml_data_1, n_folds = 2, n_rep = 25)
print(dml_cart_2)
dml_rf_2 = run_dml_model_2("Random Forest (K=2)", rf_base,  rf_base,  dml_data_1, n_folds = 2, n_rep = 5)
print(dml_rf_2)
dml_xgb_2 =  run_dml_model_2("Boosting XGB (K=2)",  boost_base, boost_base, dml_data_1, n_folds = 2, n_rep = 5)
print(dml_xgb_2)
dml_nn_2 = run_dml_model_2("Neural Net (K=2)",    nn_base,   nn_base,   dml_data_1, n_folds = 2, n_rep = 5)
print(dml_nn_2)

results_dml_rob_2 <- bind_rows(
  dml_lasso_2, dml_cart_2, dml_rf_2, dml_xgb_2, dml_nn_2
)
print(results_dml_rob_2)

### (b) variações de hiperparâmetros ----

# Variações simples (parâmetros fixos alternativos)
rf_shallow <- lrn("regr.ranger",
                  num.trees = 200,
                  mtry = max(3, floor(sqrt(p_1))),
                  min.node.size = 10,
                  max.depth = 12)

xgb_fast <- lrn("regr.xgboost",
                objective = "reg:squarederror",
                eta = 0.1,
                nrounds = 200,
                max_depth = 3,
                subsample = 0.8,
                colsample_bytree = 0.8,
                min_child_weight = 1)

nn_wider <- lrn("regr.nnet",
                size = 8, 
                decay = 0.01, 
                maxit = 500, 
                MaxNWts = 50000, 
                trace = FALSE)



dml_lasso_hip = run_dml_model_1("Lasso (λ.min, K=5)", lasso_min, lasso_min, dml_data_1, n_folds = 5, n_rep = 25)
dml_rf_hip = run_dml_model_2("RF (shallow, K=5)",  rf_shallow, rf_shallow, dml_data_1, n_folds = 5, n_rep = 5)
dml_xgb_hip = run_dml_model_2("XGB (eta=0.1, K=5)", xgb_fast, xgb_fast, dml_data_1, n_folds = 5, n_rep = 5)
dml_nn_hip = run_dml_model_2("NN (size=8, K=5)",   nn_wider, nn_wider, dml_data_1, n_folds = 5, n_rep = 5)
results_dml_rob_hip <- bind_rows(
  dml_lasso_hip, dml_rf_hip, dml_xgb_hip, dml_nn_hip
)
print(results_dml_rob_hip)

## 3.6. Consolidação ----

results_all <- bind_rows(results_dml_5, results_dml_rob_2, results_dml_rob_hip) %>%
  mutate(significance = case_when(
    p_value < 0.01 ~ "***",
    p_value < 0.05 ~ "**",
    p_value < 0.10 ~ "*",
    TRUE ~ ""
  ))

print(results_all, n = 100)

# MAIN: K=5 only (five learners)
results_main_k5 <- results_all %>%
  filter(n_folds == 5,
         model %in% c("Lasso (λ.1se, K=5)",
                      "CART (K=5)",
                      "Random Forest (K=5)",
                      "Boosting XGB (K=5)",
                      "Neural Net (K=5)")) %>%
  mutate(term_label = ifelse(term == "D",
                             "Log(Price Ratio)",
                             "Log(Price Ratio) × Public Sector"),
         model = factor(model,
                        levels = c("Lasso (λ.1se, K=5)",
                                   "CART (K=5)",
                                   "Random Forest (K=5)",
                                   "Boosting XGB (K=5)",
                                   "Neural Net (K=5)")))

## 3.7. Gráficos----

p_main <- ggplot(results_main_k5,
                 aes(x = estimate, y = model, color = term_label)) +
  geom_point(size = 2.5) +
  geom_errorbarh(aes(xmin = ci_lower, xmax = ci_upper), height = 0.2) +
  facet_wrap(~term_label, scales = "free_x") +
  labs(title = "DML (K = 5): Coefficients for D and D × Public Sector",
       x = "Coefficient", y = "Learner", color = "Term") +
  theme_minimal(base_size = 12) +
  theme(legend.position = "top")
p_main

results_k2 <- results_all %>%
  filter(n_folds == 2) %>%
  mutate(term_label = ifelse(term == "D",
                             "Log(Price Ratio)",
                             "Log(Price Ratio) × Public Sector"))

p_k2 <- ggplot(results_k2,
               aes(x = estimate, y = model, color = term_label)) +
  geom_point(size = 2.5) +
  geom_errorbarh(aes(xmin = ci_lower, xmax = ci_upper), height = 0.2) +
  facet_wrap(~term_label, scales = "free_x") +
  labs(title = "Stability (K = 2): Coefficients for D and D × Public Sector",
       x = "Coefficient", y = "Learner", color = "Term") +
  theme_minimal(base_size = 12) +
  theme(legend.position = "top")
p_k2

results_hypers <- results_all %>%
  filter(n_folds == 5,
         model %in% c("Lasso (λ.min, K=5)",
                      "RF (shallow, K=5)",
                      "XGB (eta=0.1, K=5)",
                      "NN (size=8, K=5)")) %>%
  mutate(term_label = ifelse(term == "D",
                             "Log(Price Ratio)",
                             "Log(Price Ratio) × Public Sector"))

p_hypers <- ggplot(results_hypers,
                   aes(x = estimate, y = model, color = term_label)) +
  geom_point(size = 2.5) +
  geom_errorbarh(aes(xmin = ci_lower, xmax = ci_upper), height = 0.2) +
  facet_wrap(~term_label, scales = "free_x") +
  labs(title = "Stability (Hyperparameters, K = 5): D and D × Public",
       x = "Coefficient", y = "Learner", color = "Term") +
  theme_minimal(base_size = 12) +
  theme(legend.position = "top")


p_main
p_k2
p_hypers

 plots_list <- list(
   p_main = p_main,
   p_k2 = p_k2,
   p_hypers = p_hypers
 )
 
 for (name in names(plots_list)) {
   ggsave(
     filename = file.path(out_dir, paste0(name, ".png")),
     plot     = plots_list[[name]],
     device   = "png",
     width    = 14,
     height   = 10,
     dpi      = 300,    
     units    = "in"     
   )
 }

 ## ¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨----