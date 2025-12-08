rm(list = ls())

# ___________________ ----
# UNIFICAÇÃO BASES ----
# ¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨----

# 0. Carregar Pacotes ----

library(arrow)
library(dplyr)
library(janitor)
library(lubridate)
library(openxlsx)
library(purrr)
library(readxl)
library(stringr)
library(tidyr)
library(writexl)
library(zoo)

#____________________----
# 1. CARREGAMENTO----
caminho_draft <- "../data/02_interim"
caminho_resultados <- "../data/03_processed"

## 1.1. ANS ----
nome_arquivo_ans <- "ans_15_23_completa.parquet"
caminho_ans <- file.path(caminho_draft, nome_arquivo_ans)
ans_15.23 <- read_parquet(caminho_ans)

names(ans_15.23)

ans_15.23 <- ans_15.23 %>%
  clean_names()
colnames(ans_15.23) <- toupper(colnames(ans_15.23))
names(ans_15.23)

ans_15.23 <- ans_15.23 %>%
  mutate(DATA = coalesce(DT_REALIZACAO, ANO_MES_EVENTO)) %>%
  select(-c(DT_REALIZACAO, ANO_MES_EVENTO))

ans_15.23 <- ans_15.23 %>%
  mutate(DATA = as.Date(paste0(DATA, "-01")),
         ANO_MES_ANS = format(as.Date(`DATA`, format="%Y-%m-%d"), "%Y-%m")) %>%
  rename(
    "ID_TISS" = "ID_EVENTO_ATENCAO_SAUDE",
    "UF" = "UF_PRESTADOR",
    "PRODUTO_ANS" = "TERMO",
    "APRESENTACAO_ANS" = "APRESENTACAO",
    "LABORATORIO_ANS" = "LABORATORIO",
    "TABELA_TISS" = "CD_TABELA_REFERENCIA",
    "TEMPO_INTERNACAO" = "TEMPO_DE_PERMANENCIA",
    "DATA_INCORPORACAO"="DATA_DE_INICIO_DE_VIGENCIA",
    "DATA_ENTRADA" = "DATA_DE_FIM_DE_VIGENCIA",
    "DATA_SAIDA" = "DATA_DE_FIM_DE_IMPLANTACAO",
    "QUANTIDADE" = "QT_ITEM_EVENTO_INFORMADO",
    "VALOR" = "VL_ITEM_EVENTO_INFORMADO",
    "VALOR_PAGO" = "VL_ITEM_PAGO_FORNECEDOR")

names(ans_15.23)

ans_15.23 <- ans_15.23 %>%
  mutate(TIPO = as.factor(TIPO))
summary(ans_15.23)

colunas_ordenadas_ans <- c(
  "TIPO",
  "DATA", "ANO_MES_ANS", "UF", "LABORATORIO_ANS",
  "REGISTRO_ANVISA", "ID_TISS","CD_PROCEDIMENTO",
  "PRODUTO_ANS", "APRESENTACAO_ANS", "UNIDADE_MEDIDA",
  "IND_PACOTE", "IND_TABELA_PROPRIA", "TABELA_TISS",
  "TEMPO_INTERNACAO",
  "DATA_INCORPORACAO","DATA_ENTRADA", "DATA_SAIDA",
  "QUANTIDADE", "VALOR", "VALOR_PAGO")

ans_15.23 <- ans_15.23  %>%
  select(all_of(colunas_ordenadas_ans))

# Visualizar o resultado
names(ans_15.23)
unique(ans_15.23$DATA)

## 1.2. BPS ----
nome_arquivo_bps <- "bps_97.24_completa.parquet"
caminho_bps <- file.path(caminho_draft, nome_arquivo_bps)
bps_97.24 <- read_parquet(caminho_bps)
names(bps_97.24)

bps_97.24 <- bps_97.24 %>%
  clean_names()
colnames(bps_97.24) <- toupper(colnames(bps_97.24))
names(bps_97.24)
summary(bps_97.24)

bps_97.24 <- bps_97.24 %>%
  mutate(
    # Atualizar Registro ANVISA para os casos específicos (pode haver erros não identificado)
    REGISTRO_ANVISA = case_when(
      REGISTRO_ANVISA == "1029804320059" & 
        str_detect(NOME_FABRICANTE, "ROCHE") ~ "1010005390020",
      
      REGISTRO_ANVISA == "1211004520045" &
        str_detect(NOME_FABRICANTE, "PFIZER") ~ "1021602400080",
      
      REGISTRO_ANVISA == "1163701340010" & 
        str_detect(NOME_FABRICANTE, "UNIAO")  ~ "1049714060044",
      
      CNPJ_FABRICANTE == "3978166000175" & 
        str_detect(NOME_FABRICANTE, "DRREDDYS")  ~ "03.978.166/0001-75",
      
      CNPJ_FABRICANTE == "3978166000175"  & 
        str_detect(NOME_FABRICANTE, "EMS")  ~ "45.992.062/0001-65",
      
      TRUE ~ REGISTRO_ANVISA
    ))

bps_15.23 <- bps_97.24 %>%
  filter(`DATA_COMPRA` >= as.Date("2015-01-01") & 
           `DATA_COMPRA` <= as.Date("2023-12-31"))


## 1.3. CMED ----
nome_arquivo_cmed <- "cmed_10.24_completa.parquet"
dir.parquet_cmed_10.24 <- file.path(caminho_draft, nome_arquivo_cmed)
cmed_10.24 <- read_parquet(dir.parquet_cmed_10.24)

names(cmed_10.24)

summary(cmed_10.24)

cmed_15.23 <- cmed_10.24 %>%
  filter(`DATA_PUBLICADA` >= as.Date("2015-01-01")& 
           `DATA_PUBLICADA` <= as.Date("2023-12-31"))

cmed_15.23 <- cmed_15.23 %>%
  mutate(
    CLASSE_TERAPEUTICA = as.character(CLASSE_TERAPEUTICA),
    CLASSE_TERAPEUTICA = ifelse(
      CLASSE_TERAPEUTICA == "L1H9 - INIBIDORES PREOTEÍNA KINASE ANTINEOPLÁSICOS, OUTROS",
      "L1H9 - OUTROS ANTINEOPLÁSICOS INIBIDORES DA PROTEÍNA KINASE",
      CLASSE_TERAPEUTICA
    )
  ) 

cmed_15.23 = cmed_15.23%>%
  mutate(
     CHAVE_CLASSE_ANOMES = paste0(ANO_MES, "_", CLASSE_TERAPEUTICA_CODIGO)
     )
names(cmed_15.23)


### 1.3.1. Reajuste ----
path_reajuste <- "../data/00_auxiliar/reajuste_anual_07-24.xlsx"
reajuste_anual <- read_excel(path_reajuste)
names(reajuste_anual)


### 1.3.2. Fator Z ----
path_z <- "../data/00_auxiliar/z_14-24.xlsx"
fator_z  <- read_excel(path_z)
names(fator_z)


### _merge z e reajuste ----

# Ajuste inicial em fator_z: datas, código e IHH
fator_z_adj <- fator_z %>%
  mutate(
    `Data Início`               = as.Date(`Data Início`),
    `Data Fim`                  = as.Date(`Data Fim`),
    CLASSE_TERAPEUTICA_CODIGO   = str_remove(`Classe Terapêutica`, " -.*$"),
    IHH                         = if_else(IHH > 10000, 10000L, IHH)
  )

# Expansão mensal de cada linha
fator_z_expanded <- fator_z_adj %>%
  mutate(
    `Data Início`             = floor_date(as.Date(`Data Início`), "month"),
    `Data Fim`                = floor_date(as.Date(`Data Fim`),    "month"),
    CLASSE_TERAPEUTICA_CODIGO = str_remove(`Classe Terapêutica`, " -.*$")
  ) %>%
  pmap_dfr(function(...){
    row        <- tibble(...)
    meses_seq  <- seq(row$`Data Início`, row$`Data Fim`, by = "1 month")
    row[rep(1, length(meses_seq)), ] %>%
      mutate(
        Data    = meses_seq,
        Ano_Mes = format(meses_seq, "%Y-%m")
      )
  }) %>%
  select(-`Data Início`, -`Data Fim`) %>%
  mutate(
    CHAVE_CLASSE_ANOMES = paste0(Ano_Mes, "_", CLASSE_TERAPEUTICA_CODIGO)
  )


df_chaves_cmed <- cmed_15.23 %>%
  distinct(CHAVE_CLASSE_ANOMES)

# Junte as chaves completas ao fator_z_expanded e aplique regra de nível = 3
fator_z_completo <- df_chaves_cmed %>%
  left_join(fator_z_expanded, by = "CHAVE_CLASSE_ANOMES") %>%
  mutate(
    # nível 3 para quem não estava no fator_z original
    Nível        = if_else(is.na(Nível), 3L, Nível),
    # preencha Ano_Reajuste e Ano_Mes a partir da chave, se estiverem faltando
    `Ano Reajuste` = if_else(
      is.na(`Ano Reajuste`),
      as.integer(substr(CHAVE_CLASSE_ANOMES, 1, 4)),
      `Ano Reajuste`
    ),
    Ano_Mes      = if_else(
      is.na(Ano_Mes),
      sub("_.*$", "", CHAVE_CLASSE_ANOMES),
      Ano_Mes
    ),
    CLASSE_TERAPEUTICA_CODIGO = if_else(
      is.na(CLASSE_TERAPEUTICA_CODIGO),
      sub("^.*_", "", CHAVE_CLASSE_ANOMES),
      CLASSE_TERAPEUTICA_CODIGO
    )
  )

# Junte com reajuste_anual para trazer IPCA, Fator X/Y/Z e VPP
z_reajuste <- fator_z_completo %>%
  left_join(
    reajuste_anual %>%
      select(Ano, Nível, IPCA, `Fator X`, `Fator Y`, `Fator Z`, VPP),
    by = c("Ano Reajuste" = "Ano", "Nível" = "Nível")
  )

# Verifique nomes e primeiras linhas
names(z_reajuste)
names(cmed_15.23)


cmed_15.23_joined <- cmed_15.23 %>%
  left_join(
    z_reajuste,
    by = "CHAVE_CLASSE_ANOMES"
  )
names(cmed_15.23_joined)

cmed_15.23_joined <- cmed_15.23_joined %>%
  rename(
    # colunas originais do cmed
    CLASSE_TERAPEUTICA_CODIGO = CLASSE_TERAPEUTICA_CODIGO.x,
    # colunas vindas de z_reajuste
    CLASSE_TERAPEUTICA_CODIGO_Z   = CLASSE_TERAPEUTICA_CODIGO.y,
    ANO_MES_Z = Ano_Mes
  )

names(cmed_15.23_joined) <- toupper(names(cmed_15.23_joined))
names(cmed_15.23_joined)

# Definir vetor com as novas colunas a verificar
novas <- c(
  "DATA",
  "ANO REAJUSTE",
  "Nº",
  "CLASSE TERAPÊUTICA",
  "IHH",
  "NÍVEL",
  "IPCA",
  "FATOR X",
  "FATOR Y",
  "FATOR Z",
  "VPP",
  "CLASSE_TERAPEUTICA_CODIGO_Z",
  "ANO_MES_Z"
)

# Contar NAs em cada uma
na_counts <- cmed_15.23_joined %>%
  summarise(
    across(
      all_of(novas),
      ~ sum(is.na(.)),
      .names = "{col}"
    )
  )

# “Desempacota” em formato longo
na_counts %>%
  pivot_longer(
    cols      = everything(),
    names_to  = "column",
    values_to = "n_na"
  ) %>%
  arrange(desc(n_na))

cmed_15.23_backup = cmed_15.23

cmed_15.23 = cmed_15.23_joined

#_________________----
# 2.PREPARAÇÃO_----

## 2.1. Identificar Interseção ----

# Selecionar os registros únicos de cada dataframe
registros_ans <- ans_15.23 %>%
  select(`REGISTRO_ANVISA`) %>%
  distinct()

registros_bps <- bps_15.23 %>%
  select(`REGISTRO_ANVISA`) %>%
  distinct()

registros_cmed <- cmed_15.23 %>%
  select(REGISTRO) %>%
  distinct() %>%
  rename(REGISTRO_ANVISA = REGISTRO)


# Encontrar a interseção dos valores únicos
intersecao_registros <- registros_cmed %>%
  inner_join(registros_ans, by = "REGISTRO_ANVISA") %>%
  inner_join(registros_bps, by = "REGISTRO_ANVISA") %>%
  filter(!is.na(REGISTRO_ANVISA) & REGISTRO_ANVISA != "")

unique(intersecao_registros$REGISTRO_ANVISA)


## 2.2. Filtrar Bases ----

### 2.2.1. ANS interseção ----
ans_15.23_intersecao <- ans_15.23 %>%
  semi_join(intersecao_registros, by = "REGISTRO_ANVISA")

ans_15.23_intersecao <- ans_15.23_intersecao %>%
  mutate(REGISTRO_APRESENTACAO_ANS = paste(`REGISTRO_ANVISA`, APRESENTACAO_ANS, sep = " "),
         REGISTRO_APRESENTACAO_ANS = stringr::str_trim(REGISTRO_APRESENTACAO_ANS)
         )

names(ans_15.23_intersecao)


### 2.2.2. BPS interseção ----
bps_15.23_intersecao <- bps_15.23 %>%
  semi_join(intersecao_registros, by = "REGISTRO_ANVISA")

bps_15.23_intersecao <- bps_15.23_intersecao %>%
  mutate(REGISTRO_APRESENTACAO_BPS = paste(`REGISTRO_ANVISA`, `UNIDADE_FORNECIMENTO`, sep = " "),
         REGISTRO_APRESENTACAO_BPS = stringr::str_trim(REGISTRO_APRESENTACAO_BPS))

names(bps_15.23_intersecao)


### 2.2.3. CMED interseção ----
cmed_15.23_intersecao <- cmed_15.23 %>%
  semi_join(intersecao_registros, by = c("REGISTRO" = "REGISTRO_ANVISA"))

cmed_15.23_intersecao <- cmed_15.23_intersecao %>%
  mutate(REGISTRO_APRESENTACAO = paste(REGISTRO, APRESENTACAO, sep = " "),
         REGISTRO_APRESENTACAO = stringr::str_trim(REGISTRO_APRESENTACAO))

names(cmed_15.23_intersecao)

sort(unique(cmed_15.23_intersecao$ARQUIVO))


cmed_15.23_intersecao.1.1 <- cmed_15.23_intersecao %>%
  filter(ARQUIVO != "2015.11.20_pmc.xls")

# Criando os dados faltantes para 2015-10
missings_pmc_2015.10 <- cmed_15.23_intersecao %>%
  filter(ARQUIVO == "2015.12.18_pmc.xls") %>%
  mutate(
    DATA_PUBLICADA = "2015-10-01",
    DATA_ATUALIZADA = NA,
    ARQUIVO = "2015.10.01_pmc.xls",
    ANO_MES = "2015-10"
  )

# Criando os dados faltantes para 2015-11
missings_pmc_2015.11 <- cmed_15.23_intersecao %>%
  filter(ARQUIVO == "2015.12.18_pmc.xls") %>%
  mutate(
    DATA_PUBLICADA = "2015-11-20",
    DATA_ATUALIZADA = NA,
    ARQUIVO = str_replace(as.character(ARQUIVO), "^2015\\.12\\.18", "2015.11.20"),
    ANO_MES = "2015-11"
  )

# Verificando se os valores foram corretamente alterados
unique(missings_pmc_2015.10$ARQUIVO)
unique(missings_pmc_2015.10$DATA_PUBLICADA)
unique(missings_pmc_2015.10$ANO_MES)

unique(missings_pmc_2015.11$ARQUIVO)
unique(missings_pmc_2015.11$DATA_PUBLICADA)
unique(missings_pmc_2015.11$ANO_MES)

# Função para garantir que todas as colunas são caracteres
convert_to_character <- function(df) {
  df %>% mutate(across(everything(), as.character))
}

# Convertendo os data frames para garantir compatibilidade
cmed_15.23_intersecao.1.1 <- convert_to_character(cmed_15.23_intersecao.1.1)
missings_pmc_2015.10 <- convert_to_character(missings_pmc_2015.10)
missings_pmc_2015.11 <- convert_to_character(missings_pmc_2015.11)

# Unindo os dados corrigidos
cmed_15.23_intersecao1 <- bind_rows(cmed_15.23_intersecao.1.1, missings_pmc_2015.10, missings_pmc_2015.11)

# Reajustar formatos
cmed_15.23_intersecao1 <- cmed_15.23_intersecao1 %>%
  mutate(
    # Converter datas para formato Date
    DATA_PUBLICADA = as.Date(DATA_PUBLICADA),
    DATA_ATUALIZADA = as.Date(DATA_ATUALIZADA),
    ANO_MES = format(DATA_PUBLICADA, "%Y-%m"))

cmed_15.23_intersecao1 <- cmed_15.23_intersecao1 %>%
  mutate(
    # Converter colunas que representam categorias em fatores
    `CLASSE_TERAPEUTICA` = as.factor(`CLASSE_TERAPEUTICA`),
    `CLASSE_TERAPEUTICA_CODIGO` = as.factor(`CLASSE_TERAPEUTICA_CODIGO`),
    `CLASSE_TERAPEUTICA_DESCRICAO` = as.factor(`CLASSE_TERAPEUTICA_DESCRICAO`),
    `CLASSE_PRINCIPAL_CODIGO` = as.factor(`CLASSE_PRINCIPAL_CODIGO`),
    `CLASSE_PRINCIPAL_DESCRICAO` = as.factor(`CLASSE_PRINCIPAL_DESCRICAO`),
    `TIPO_PRODUTO` = as.factor(`TIPO_PRODUTO`),
    `TARJA` = as.factor(TARJA),
    `DESTINACAO_COMERCIAL` = as.factor(`DESTINACAO_COMERCIAL`),
    `REGIME_PRECO` = as.factor(`REGIME_PRECO`),
    `REGIME` = as.factor(REGIME),
    `RESTRICAO_HOSPITALAR` = as.factor(`RESTRICAO_HOSPITALAR`),
    `LISTA_CRÉDITO_PIS/COFINS` = as.factor(`LISTA_CRÉDITO_PIS/COFINS`),
    `ICMS_0%` = as.factor(`ICMS_0%`),
    `CAP` = as.factor(CAP),
    `CONFAZ_87` = as.factor(`CONFAZ_87`)
  )

tratar_valores <- function(valor) {
  valor <- suppressWarnings(as.numeric(valor)) # Converte para numérico
  return(valor)
}

cmed_15.23_intersecao1 <- cmed_15.23_intersecao1 %>%
  mutate(across(
    matches("^(PF|PMC|PMVG)"), 
    ~ tratar_valores(.)
  ))


## 2.3. Criar Apresentação Padrão ----

### _(Primário) ----

# Criar DataFrames filtrando apenas REGISTRO e APRESENTAÇÃO e adicionando a fonte
cmed_df <- cmed_15.23_intersecao1 %>%
  select(`REGISTRO`, APRESENTACAO) %>%
  mutate(REGISTRO_APRESENTACAO = paste(`REGISTRO`, APRESENTACAO, sep = " "),
         REGISTRO_APRESENTACAO = stringr::str_trim(REGISTRO_APRESENTACAO))  %>%
  mutate(FONTE = "CMED")

ans_df <- ans_15.23_intersecao %>%
  select(`REGISTRO_ANVISA`, APRESENTACAO_ANS) %>%
  rename(REGISTRO = `REGISTRO_ANVISA`,
         APRESENTACAO = APRESENTACAO_ANS) %>%
  mutate(REGISTRO_APRESENTACAO = paste(`REGISTRO`, APRESENTACAO, sep = " "),
         REGISTRO_APRESENTACAO = stringr::str_trim(REGISTRO_APRESENTACAO)) %>%
  mutate(FONTE = "ANS")

ans_df <- ans_df %>%
  distinct(REGISTRO_APRESENTACAO,REGISTRO, APRESENTACAO, FONTE)

bps_df <- bps_15.23_intersecao %>%
  select(`REGISTRO_ANVISA`, `UNIDADE_FORNECIMENTO`) %>%
  rename(REGISTRO = `REGISTRO_ANVISA`, 
         APRESENTACAO = `UNIDADE_FORNECIMENTO`)%>%
  mutate(REGISTRO_APRESENTACAO = paste(`REGISTRO`, APRESENTACAO, sep = " "),
         REGISTRO_APRESENTACAO = stringr::str_trim(REGISTRO_APRESENTACAO))  %>%
  mutate(FONTE = "BPS")

# Unir os DataFrames
intersecao_cmed_ans_bps <- bind_rows(cmed_df, ans_df, bps_df)

# Remover duplicatas caso existam
intersecao_cmed_ans_bps <- intersecao_cmed_ans_bps %>%
  distinct(REGISTRO_APRESENTACAO,REGISTRO, APRESENTACAO, FONTE)

names(intersecao_cmed_ans_bps)
write.xlsx(intersecao_cmed_ans_bps, file = file.path(caminho_draft, "25.03.16_intersecao_cmed_ans_bps.xlsx"), overwrite = TRUE)

# # Em excel, foi criada manualmente a coluna "APRESENTAÇÃO PADRÃO", para consolidar as diferentes bases
# # Assim, é necessário baixar a versão consolidada
# 
# arquivo_excel <- file.path(caminho_draft, "25.03.11_intersecao_cmed_ans_bps.xlsx")
# intersecao_cmed_ans_bps_lido <- read_xlsx(arquivo_excel, sheet = 1)
# names(intersecao_cmed_ans_bps_lido)
# 
# # Criar nova coluna baseada na última ocorrência de "X"
# intersecao_cmed_ans_bps_lido <- intersecao_cmed_ans_bps_lido |>
#   mutate(`APRESENTACAO_PADRAO` = stringr::str_trim(`APRESENTACAO_PADRAO`),
#          REGISTRO_APRESENTACAO = stringr::str_trim(REGISTRO_APRESENTACAO))
# 
# intersecao_cmed_ans_bps_lido <- intersecao_cmed_ans_bps_lido %>%
#   mutate(
#     QDE = as.numeric(stringr::str_trim(QDE)), # Remove espaços e converte para número
#     )
# write.xlsx(intersecao_cmed_ans_bps, file = file.path(caminho_draft, "intersecao_cmed_ans_bps1.xlsx"), overwrite = TRUE)


### _Secundário ----

# Após consolidado, precisa apenas baixar e checar

arquivo_excel <- file.path(caminho_resultados, "25.03.16_intersecao_cmed_ans_bps_final.xlsx")
intersecao_cmed_ans_bps_lido <- read_xlsx(arquivo_excel, sheet = 1)
names(intersecao_cmed_ans_bps_lido)
intersecao_cmed_ans_bps_lido <- intersecao_cmed_ans_bps_lido |>
  mutate(`APRESENTACAO_PADRAO` = stringr::str_trim(`APRESENTACAO_PADRAO`),
         REGISTRO_APRESENTACAO = stringr::str_trim(REGISTRO_APRESENTACAO))

intersecao_cmed_ans_bps = intersecao_cmed_ans_bps_lido
names(intersecao_cmed_ans_bps)

reg = unique(intersecao_cmed_ans_bps$REGISTRO)


## 2.4.Consolidar bases ----

# Garantir que os dataframes têm a mesma estrutura e adicionar QDE aos originais

### 2.4.1. ANS interseção ----

ans_15.23_intersecao <- ans_15.23_intersecao %>%
  mutate(REGISTRO_APRESENTACAO_ANS = stringr::str_trim(REGISTRO_APRESENTACAO_ANS))

ans_15.23_intersecao <- ans_15.23_intersecao %>%
  left_join(
    intersecao_cmed_ans_bps_lido %>% 
      filter(FONTE == "ANS") %>%  # Filtrar antes da junção
      distinct(REGISTRO_APRESENTACAO, .keep_all = TRUE) %>%  # Remover duplicatas mantendo todas as colunas
      select(REGISTRO_APRESENTACAO, APRESENTACAO_PADRAO, QDE), 
    by = c("REGISTRO_APRESENTACAO_ANS"="REGISTRO_APRESENTACAO")
  )

ans_15.23_intersecao_nas <- ans_15.23_intersecao %>%
  summarise(across(everything(), ~ sum(is.na(.)))) %>%
  pivot_longer(cols = everything(), names_to = "coluna", values_to = "na_count")
print(ans_15.23_intersecao_nas, n = Inf)

ans_15.23_intersecao <- ans_15.23_intersecao %>%
  rename(
    APRESENTACAO_PADRAO_ANS = "APRESENTACAO_PADRAO") %>%
  mutate(
    `REGISTRO_ANVISA` = str_trim(`REGISTRO_ANVISA`),
    CHAVE_JUNCAO = paste0(ANO_MES_ANS,"_",  `REGISTRO_ANVISA`, "_", APRESENTACAO_PADRAO_ANS))

names(ans_15.23_intersecao)


### 2.4.2. BPS interseção ----

bps_15.23_intersecao <- bps_15.23_intersecao %>%
  left_join(
    intersecao_cmed_ans_bps %>% 
      filter(FONTE == "BPS") %>%  # Filtrar antes da junção
      distinct(REGISTRO_APRESENTACAO, .keep_all = TRUE) %>%  # Remover duplicatas mantendo todas as colunas
      select(REGISTRO_APRESENTACAO, APRESENTACAO_PADRAO, QDE), 
    by = c("REGISTRO_APRESENTACAO_BPS"= "REGISTRO_APRESENTACAO")
  )

names(bps_15.23_intersecao)

bps_15.23_intersecao <- bps_15.23_intersecao %>%
  rename(
    APRESENTACAO_PADRAO_BPS = "APRESENTACAO_PADRAO") |>
  mutate(
    CHAVE_JUNCAO = paste0(ANO_MES_BPS,"_",  `REGISTRO_ANVISA`, "_", APRESENTACAO_PADRAO_BPS))

names(bps_15.23_intersecao)


bps_15.23_intersecao_nasuf = bps_15.23_intersecao |>
  filter(is.na(UF))


# Vetores com os municípios de cada Unidade Federativa
mun_pi <- c("ALTO LONGA", "ALVORADA DO GURGUEIA", "ANISIO DE ABREU", "ANTONIO ALMEIDA", "AROAZES", "ARRAIAL",
            "BAIXA GRANDE DO RIBEIRO", "BATALHA", "BERTOLINIA", "BURITI DOS MONTES", "CAJUEIRO DA PRAIA",
            "CAMPINAS DO PIAUI", "CAMPO ALEGRE DO FIDALGO", "CAMPO DO BRITO", "CAMPO MAIOR", "CAPELA",
            "COLONIA DO GURGUEIA", "COLONIA DO PIAUI", "CONCEICAO DO CANINDE", "CURRAL NOVO DO PIAUI",
            "CURRALINHOS", "DOM INOCENCIO", "ESPERANTINA", "FLORES DO PIAUI", "FLORIANO", "FRANCISCO AYRES",
            "ITABAIANA", "ITABAIANINHA", "ITAINOPOLIS", "ITAUEIRA", "JOAO COSTA", "LAGOA ALEGRE",
            "LAGOA DE SAO FRANCISCO", "LAGOA DO BARRO DO PIAUI", "MACAMBIRA", "MADEIRO", "MARCOS PARENTE",
            "MIGUEL ALVES", "MONSENHOR GIL", "NAZARE DO PIAUI", "NOVA SANTA RITA", "NOVO SANTO ANTONIO",
            "OEIRAS", "PALMEIRAIS", "PARNAGUA", "PAULISTANA", "PAVUSSU", "PORTO", "PORTO ALEGRE DO PIAUI",
            "REDENCAO DO GURGUEIA", "SANTO INACIO DO PIAUI", "SAO BRAZ DO PIAUI",
            "SAO FRANCISCO DE ASSIS DO PIAUI", "SAO FRANCISCO DO PIAUI", "SAO GONCALO DO PIAUI",
            "SAO JOAO DA VARJOTA", "SAO JOSE DO PEIXE", "SAO MIGUEL DO FIDALGO", "SEBASTIAO LEAL",
            "SIMPLICIO MENDES", "SOCORRO DO PIAUI", "TERESINA", "TOMAR DO GERU", "VERA MENDES")

mun_se <- c("AMPARO DE SAO FRANCISCO", "ARACAJU", "PORTO DA FOLHA")

mun_mt <- c("CUIABA", "VARZEA GRANDE")

mun_ms <- c("CAMPO VERDE")

# Adicionar a coluna 'UF' à base de dados
bps_15.23_intersecao <- bps_15.23_intersecao %>%
  mutate(
    UF = if_else(
      is.na(UF),
      case_when(
        MUNICIPIO_INSTITUICAO %in% mun_pi ~ "PI",
        MUNICIPIO_INSTITUICAO %in% mun_se ~ "SE",
        MUNICIPIO_INSTITUICAO %in% mun_mt ~ "MT",
        MUNICIPIO_INSTITUICAO %in% mun_ms ~ "MS",
        TRUE ~ NA_character_
      ),
      UF
    )
  )

bps_15.23_intersecao <- bps_15.23_intersecao %>%
  mutate(REGIAO = case_when(
    UF %in% c("AC", "AP", "AM", "PA", "RO", "RR", "TO") ~ "NORTE",
    UF %in% c("AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN") ~ "NORDESTE",
    UF %in% c("DF", "GO", "MT", "MS") ~ "CENTRO-OESTE",
    UF %in% c("ES", "MG", "RJ", "SP") ~ "SUDESTE",
    UF %in% c("PR", "RS", "SC") ~ "SUL",
    TRUE ~ "ESTRANGEIRO"
  ))

bps_15.23_intersecao <- bps_15.23_intersecao %>%
  mutate(PAIS                           = case_when(
    !is.na(UF) ~ "BRASIL",
    TRUE ~ NA_character_
  ))

bps_15.23_intersecao_nas <- bps_15.23_intersecao %>%
  summarise(across(everything(), ~ sum(is.na(.)))) %>%
  pivot_longer(cols = everything(), names_to = "coluna", values_to = "na_count")

print(bps_15.23_intersecao_nas, n = Inf)

bps_15.23_intersecao = bps_15.23_intersecao |>
  select(-c(MEDIA_PONDERADA,MAIOR_PRECO,MENOR_PRECO, CMED_PRECO_REGULADO, COMPETENCIA_CMED,VALOR_TOTAL,
            QUALIFICACAO,CLASSE,ESFERA,LICITACAO, NOTA_FISCAL,DATA_INSERCAO,GENERICO))

### 2.4.3. CMED interseção ----

cmed_15.23_intersecao1 <- cmed_15.23_intersecao %>%
  left_join(
    intersecao_cmed_ans_bps %>%
      filter(FONTE == "CMED") %>%  # Filtrar antes da junção
      distinct(REGISTRO_APRESENTACAO, .keep_all = TRUE) %>%
      select(REGISTRO_APRESENTACAO, APRESENTACAO_PADRAO, QDE), 
    by = "REGISTRO_APRESENTACAO"
  )

cmed_15.23_intersecao1 <- cmed_15.23_intersecao1 %>%
  mutate(
    REG_APR = paste0(REGISTRO, "_", APRESENTACAO_PADRAO),
    CHAVE_JUNCAO = paste0(ANO_MES,"_", REGISTRO, "_", APRESENTACAO_PADRAO)
    )

names(cmed_15.23_intersecao1)
cmed_15.23_intersecao_nas <- cmed_15.23_intersecao1 %>%
  summarise(across(everything(), ~ sum(is.na(.)))) %>%
  pivot_longer(cols = everything(), names_to = "coluna", values_to = "na_count")
print(cmed_15.23_intersecao_nas, n = Inf)

sort(unique(cmed_15.23_intersecao1$DATA_PUBLICADA))
sum(is.na(cmed_15.23_intersecao1$`ICMS_0%`))

cmed_ordenada = c(
  "ARQUIVO", 
  "DATA_PUBLICADA", "ANO_MES",  
  "CNPJ","LABORATORIO", 
  "CLASSE_TERAPEUTICA","CLASSE_TERAPEUTICA_CODIGO", "CLASSE_TERAPEUTICA_DESCRICAO",
  "CLASSE_PRINCIPAL_CODIGO", "CLASSE_PRINCIPAL_DESCRICAO", "REG_APR", "CHAVE_JUNCAO",
  "SUBSTANCIA", "PRODUTO", "APRESENTACAO", "APRESENTACAO_PADRAO", 
  "REGISTRO", "REGISTRO_APRESENTACAO", 
  "CODIGO_GGREM", 
  "TIPO_PRODUTO", "TARJA", "RESTRICAO_HOSPITALAR",
  "LISTA_CRÉDITO_PIS/COFINS", "ICMS_0%", "CAP", "CONFAZ_87",
  "REGIME", "REGIME_PRECO", "QDE",
  "IHH", "NÍVEL", "IPCA", "FATOR X", "FATOR Y", "FATOR Z", "VPP", "ANO_MES_REAJUSTE", 
  
  "PF_SEM IMPOSTOS", "PF_0%", "PF_12%", "PF_12% ALC", "PF_17%", "PF_17% ALC", "PF_17.5%", "PF_17.5% ALC", 
  "PF_18%", "PF_18% ALC", "PF_19%", "PF_19% ALC", "PF_19.5%", "PF_19.5% ALC",
  "PF_20%", "PF_20% ALC", "PF_20.5%", "PF_21%", "PF_21% ALC", "PF_22%", "PF_22% ALC",
  
  "PMC_SEM IMPOSTOS", "PMC_0%", "PMC_12%", "PMC_12% ALC", "PMC_17%", "PMC_17% ALC", "PMC_17.5%", "PMC_17.5% ALC", 
  "PMC_18%", "PMC_18% ALC", "PMC_19%", "PMC_19% ALC", "PMC_19.5%", "PMC_19.5% ALC",
  "PMC_20%", "PMC_20% ALC", "PMC_20.5%", "PMC_21%", "PMC_21% ALC", "PMC_22%", "PMC_22% ALC",
  
  "PMVG_SEM IMPOSTOS", "PMVG_0%", "PMVG_12%", "PMVG_12% ALC", "PMVG_17%", "PMVG_17% ALC", "PMVG_17.5%", "PMVG_17.5% ALC", 
  "PMVG_18%", "PMVG_18% ALC", "PMVG_19%", "PMVG_19% ALC", "PMVG_19.5%", "PMVG_19.5% ALC",
  "PMVG_20%", "PMVG_20% ALC", "PMVG_20.5%", "PMVG_21%", "PMVG_21% ALC", "PMVG_22%", "PMVG_22% ALC",
  
 # "DESTINACAO_COMERCIAL", "ANALISE_RECURSAL",
  "COMERCIALIZACAO" #"ULTIMA_ALTERACAO"
)


# Reorganizar o DataFrame de acordo com a nova lista de colunas completas
cmed_15.23_intersecao1 <- cmed_15.23_intersecao1  %>%
  select(all_of(cmed_ordenada))
names(cmed_15.23_intersecao1)


#### 2.4.3.1. CMED Descritivo ----

cmed_15.23_intersecao_descritivo <- cmed_15.23_intersecao1 %>%
  select(
    "DATA_PUBLICADA", "ANO_MES",  
    "CNPJ","LABORATORIO", 
    "CLASSE_TERAPEUTICA","CLASSE_TERAPEUTICA_CODIGO", "CLASSE_TERAPEUTICA_DESCRICAO",
    "CLASSE_PRINCIPAL_CODIGO", "CLASSE_PRINCIPAL_DESCRICAO", "REG_APR", "CHAVE_JUNCAO",
    "SUBSTANCIA", "PRODUTO", "APRESENTACAO", "APRESENTACAO_PADRAO",
    "REGISTRO", "REGISTRO_APRESENTACAO", 
    "CODIGO_GGREM",
    "TIPO_PRODUTO", "TARJA", "RESTRICAO_HOSPITALAR",
    "LISTA_CRÉDITO_PIS/COFINS", "ICMS_0%", "CAP", "CONFAZ_87",
    "REGIME", "REGIME_PRECO",
    "IHH", "NÍVEL", "IPCA", "FATOR X", "FATOR Y", "FATOR Z", "VPP", "ANO_MES_Z", 
    "COMERCIALIZACAO"
  ) %>%
  group_by(CHAVE_JUNCAO) %>%
  summarise(across(everything(), last, .names = "{.col}"), .groups = "drop")

names(cmed_15.23_intersecao_descritivo)
cmed_15.23_intersecao_descritivo_nas <- cmed_15.23_intersecao_descritivo %>%
  summarise(across(everything(), ~ sum(is.na(.)))) %>%
  pivot_longer(cols = everything(), names_to = "coluna", values_to = "na_count")
print(cmed_15.23_intersecao_descritivo_nas, n = Inf)



#### 2.4.3.2. CMED Teto ----

cmed_15.23_intersecao_teto = cmed_15.23_intersecao1 |>
  select(
    "ARQUIVO", 
    "DATA_PUBLICADA", "ANO_MES",  
    "CNPJ","LABORATORIO", 
    "CLASSE_TERAPEUTICA","CLASSE_TERAPEUTICA_CODIGO", "CLASSE_TERAPEUTICA_DESCRICAO",
    "CLASSE_PRINCIPAL_CODIGO", "CLASSE_PRINCIPAL_DESCRICAO", "REG_APR", "CHAVE_JUNCAO",
    "SUBSTANCIA", "PRODUTO", "APRESENTACAO", "APRESENTACAO_PADRAO", 
    "REGISTRO", "REGISTRO_APRESENTACAO", 
    "CODIGO_GGREM", 
    "TIPO_PRODUTO", "TARJA", "RESTRICAO_HOSPITALAR",
    "LISTA_CRÉDITO_PIS/COFINS", "ICMS_0%", "CAP", "CONFAZ_87",
    "REGIME", "REGIME_PRECO", "QDE",
    
    "PF_SEM IMPOSTOS", "PF_0%", "PF_12%", "PF_12% ALC", "PF_17%", "PF_17% ALC", "PF_17.5%", "PF_17.5% ALC", 
    "PF_18%", "PF_18% ALC", "PF_19%", "PF_19% ALC", "PF_19.5%", "PF_19.5% ALC",
    "PF_20%", "PF_20% ALC", "PF_20.5%", "PF_21%", "PF_21% ALC", "PF_22%", "PF_22% ALC",
    
    "PMC_SEM IMPOSTOS", "PMC_0%", "PMC_12%", "PMC_12% ALC", "PMC_17%", "PMC_17% ALC", "PMC_17.5%", "PMC_17.5% ALC", 
    "PMC_18%", "PMC_18% ALC", "PMC_19%", "PMC_19% ALC", "PMC_19.5%", "PMC_19.5% ALC",
    "PMC_20%", "PMC_20% ALC", "PMC_20.5%", "PMC_21%", "PMC_21% ALC", "PMC_22%", "PMC_22% ALC",
    
    "PMVG_SEM IMPOSTOS", "PMVG_0%", "PMVG_12%", "PMVG_12% ALC", "PMVG_17%", "PMVG_17% ALC", "PMVG_17.5%", "PMVG_17.5% ALC", 
    "PMVG_18%", "PMVG_18% ALC", "PMVG_19%", "PMVG_19% ALC", "PMVG_19.5%", "PMVG_19.5% ALC",
    "PMVG_20%", "PMVG_20% ALC", "PMVG_20.5%", "PMVG_21%", "PMVG_21% ALC", "PMVG_22%", "PMVG_22% ALC"
  )


#### 2.4.3.3. CMED Empilhada ----

# Lista de colunas a empilhar
colunas_empilhar <- names(cmed_15.23_intersecao_teto) %>%
  str_subset("^PF|^PMC|^PMVG")
colunas_empilhar

# Transformação para formato longo
cmed_15.23_intersecao_long <- cmed_15.23_intersecao_teto %>%
  pivot_longer(cols = all_of(colunas_empilhar), 
               names_to = "TIPO", 
               values_to = "PRECO_TETO_APRESENTACAO") %>%
  filter(!is.na(`PRECO_TETO_APRESENTACAO`)) %>%
  separate(TIPO, into = c("CATEGORIA", "ICMS"), sep = "_", extra = "merge", fill = "right") %>%
  mutate(
    `QDE` = as.numeric(QDE),  # Garante que QDE seja numérico
    PRECO_TETO_APRESENTACAO = as.numeric(PRECO_TETO_APRESENTACAO),
    PRECO_TETO_UNITARIO = ifelse(QDE > 0, PRECO_TETO_APRESENTACAO / QDE, NA)
  )

names(cmed_15.23_intersecao_long)

cmed_15.23_intersecao_long_nas <- cmed_15.23_intersecao_long %>%
  summarise(across(everything(), ~ sum(is.na(.)))) %>%
  pivot_longer(cols = everything(), names_to = "coluna", values_to = "na_count")
print(cmed_15.23_intersecao_long_nas, n = Inf)

# Remover duplicatas do dataframe
cmed_15.23_intersecao_long <- cmed_15.23_intersecao_long %>%
  mutate(
    CHAVE_PRECO = paste0(ANO_MES,"_", REGISTRO, "_", CATEGORIA, " ", ICMS, "_", APRESENTACAO_PADRAO)
  )
cmed_15.23_intersecao_long %>%
  count(CHAVE_PRECO) %>%
  filter(n > 1)

cmed_15.23_intersecao_long_unique <- cmed_15.23_intersecao_long %>%
  distinct(CHAVE_PRECO, .keep_all = TRUE)

cmed_15.23_intersecao_long_unique_nas <- cmed_15.23_intersecao_long_unique %>%
  summarise(across(everything(), ~ sum(is.na(.)))) %>%
  pivot_longer(cols = everything(), names_to = "coluna", values_to = "na_count")
print(cmed_15.23_intersecao_long_unique_nas, n = Inf)



## 2.5. ICMS ----

caminho_arquivo <- "../data/00_auxiliar/Alíquotas UF.xlsx"

# Ler a primeira aba (ajuste conforme necessário)
icms_uf <- read_excel(caminho_arquivo, sheet = 1)
names(icms_uf)

icms_uf <- icms_uf  %>%
  mutate(
    DATA = as.Date(DATA),
    ICMS = str_to_upper(ICMS),
    UF = str_to_upper(UF),
    MUNICIPIO = str_to_upper(MUNICIPIO),
    OBS = str_to_upper(OBS),
    
    # Remover caracteres especiais das colunas de texto
    UF = str_replace_all(UF, "[^A-Z0-9 ]", ""),
    MUNICIPIO = stringi::stri_trans_general(MUNICIPIO, "Latin-ASCII"),
    
    # Adicionar ANO_MES
    ANO_MES = format(as.Date(DATA, format="%d/%m/%Y"), "%Y-%m")
    )
unique(icms_uf$ICMS)

icms_uf_sm = icms_uf |>
  filter(is.na(OBS)) |>
  select(-c(DATA,MUNICIPIO))

icms_uf_m = icms_uf |>
  filter(!is.na(MUNICIPIO))|>
  select(-c(DATA,OBS))

icms_uf_g = icms_uf |>
  filter(OBS == "GENÉRICO") |>
  select(-c(DATA,MUNICIPIO))

icms_uf_ans = icms_uf |>
  select(-c(DATA,MUNICIPIO)) |>
  distinct()
head(icms_uf_ans)

# Visualizar os primeiros registros
names(cmed_15.23_intersecao_long)
names(icms_uf)

#___________________----
# 3. UNIFICAÇÃO_----

# 3.1. BPS e CMED----

names(cmed_15.23_intersecao_descritivo)
names(bps_15.23_intersecao)

# ===
#  Juntar colunas explicativas CMED antes de aplicar ICMS
# ===

bps.cmed_15.23_intersecao <- bps_15.23_intersecao %>%
  left_join(
    cmed_15.23_intersecao_descritivo,
    by = c("CHAVE_JUNCAO")
  )

bps.cmed_15.23_intersecao <- bps.cmed_15.23_intersecao %>%
  mutate(
    ANO_MES_BPS = trimws(as.character(ANO_MES_BPS)),
    UF = trimws(as.character(UF)),
    MUNICIPIO_INSTITUICAO = trimws(as.character(MUNICIPIO_INSTITUICAO))
  )

names(bps.cmed_15.23_intersecao)
sum(is.na(bps.cmed_15.23_intersecao$REGISTRO))

bps.cmed_15.23_intersecao_nas = bps.cmed_15.23_intersecao |>
  filter(is.na(bps.cmed_15.23_intersecao$REGISTRO))

bps.cmed_15.23_intersecao_filter = bps.cmed_15.23_intersecao |>
  filter(!is.na(bps.cmed_15.23_intersecao$REGISTRO))

names(bps.cmed_15.23_intersecao_filter)
sum(is.na(bps.cmed_15.23_intersecao_filter$REGISTRO))

bps.cmed_15.23_intersecao_filter_nas <- bps.cmed_15.23_intersecao_filter %>%
  summarise(across(everything(), ~ sum(is.na(.)))) %>%
  pivot_longer(cols = everything(), names_to = "coluna", values_to = "na_count")
print(bps.cmed_15.23_intersecao_filter_nas, n = Inf)


### 3.1.1. Corrigir CNPJ ----
bps.cmed_15.23_intersecao_filter <- bps.cmed_15.23_intersecao_filter %>%
  mutate(VERIFICA_CNPJ = ifelse(CNPJ_FABRICANTE == CNPJ, "SIM", "NÃO"))

# Contar quantos "NÃO" existem na coluna VERIFICA_CNPJ
sum(bps.cmed_15.23_intersecao_filter$VERIFICA_CNPJ == "NÃO", na.rm = TRUE)

bps.cmed_15.23_intersecao_filter = bps.cmed_15.23_intersecao_filter |>
  mutate(
    lab_check = str_replace_all(LABORATORIO, "-", ""),
    primeira_palavra_fabricante = word(NOME_FABRICANTE, 1),
    CNPJ_FABRICANTE = ifelse(str_detect(lab_check, primeira_palavra_fabricante), CNPJ, CNPJ_FABRICANTE)
  ) %>%
  select(-c(primeira_palavra_fabricante,lab_check))  |>
  mutate(VERIFICA_CNPJ = ifelse(CNPJ_FABRICANTE == CNPJ, "SIM", "NÃO"))

nas_bps.cmed_15.23_intersecao_filter = bps.cmed_15.23_intersecao_filter |>
  filter(bps.cmed_15.23_intersecao_filter$VERIFICA_CNPJ == "NÃO")
names(nas_bps.cmed_15.23_intersecao_filter)

bps.cmed_15.23_intersecao_filter = bps.cmed_15.23_intersecao_filter %>%
  filter(VERIFICA_CNPJ == "SIM") |>
  select(
    -c(
      "VERIFICA_CNPJ"
    ))

names(bps.cmed_15.23_intersecao_filter)
sum(is.na(bps.cmed_15.23_intersecao_filter$REGISTRO))

bps.cmed_15.23_intersecao_m = bps.cmed_15.23_intersecao_filter
names(bps.cmed_15.23_intersecao_m)

bps.cmed_15.23_intersecao_m_nas <- bps.cmed_15.23_intersecao_m %>%
  summarise(across(everything(), ~ sum(is.na(.)))) %>%
  pivot_longer(cols = everything(), names_to = "coluna", values_to = "na_count")
print(bps.cmed_15.23_intersecao_m_nas, n = Inf)

names(bps.cmed_15.23_intersecao_m)


# ===
### 3.1.2. Ajustar Municípios ----
# ===

# Primeiro, precisamos consertar os estados em branco:
municipios <- read.csv("../data/00_auxiliar/Municipios_Brasileiros.csv", sep = ";")
municipios <- municipios %>%
  mutate(
    MUNICIPIO = iconv(MUNICIPIO, from = "latin1", to = "UTF-8", sub = ""),
    MUNICIPIO = toupper(MUNICIPIO),
    MUNICIPIO = stringi::stri_trans_general(MUNICIPIO, "Latin-ASCII"),
    REGIAO = toupper(REGIAO),
    ESTADO = toupper(ESTADO),
    ESTADO = stringi::stri_trans_general(ESTADO, "Latin-ASCII"),
  ) |>
  select("MUNICIPIO", "UF", "ESTADO", "REGIAO")

sum(is.na(bps.cmed_15.23_intersecao_m$UF))
    
bps_15.23_intersecao_semuf = bps.cmed_15.23_intersecao_m |>
  filter(is.na(UF))
unique(bps_15.23_intersecao_semuf$MUNICIPIO_INSTITUICAO)

# Lista de municípios a serem filtrados
municipios_lista <- c(
  "CUIABA", "REDENCAO DO GURGUEIA", "AROAZES", "NOVO SANTO ANTONIO",
  "ALTO LONGA", "SAO JOAO DA VARJOTA", "VARZEA GRANDE", "SAO BRAZ DO PIAUI",
  "SAO FRANCISCO DE ASSIS DO PIAUI", "LAGOA DO BARRO DO PIAUI",
  "COLONIA DO GURGUEIA", "ANISIO DE ABREU", "ALVORADA DO GURGUEIA", "MADEIRO",
  "TERESINA", "CAMPO MAIOR", "CAMPO VERDE", "AMPARO DE SAO FRANCISCO", "ITABAIANA",
  "ITAINOPOLIS", "ANTONIO ALMEIDA", "CAPELA", "PALMEIRAIS", "MARCOS PARENTE",
  "BATALHA", "SIMPLICIO MENDES", "CONCEICAO DO CANINDE", "SAO FRANCISCO DO PIAUI",
  "PORTO ALEGRE DO PIAUI", "PAVUSSU", "DOM INOCENCIO", "CURRALINHOS", "MIGUEL ALVES",
  "PORTO DA FOLHA", "ARACAJU", "OEIRAS", "FRANCISCO AYRES", "ESPERANTINA",
  "LAGOA DE SAO FRANCISCO", "BAIXA GRANDE DO RIBEIRO", "JOAO COSTA",
  "SAO GONCALO DO PIAUI", "NOVA SANTA RITA", "LAGOA ALEGRE", "BURITI DOS MONTES",
  "PARNAGUA", "SEBASTIAO LEAL", "SAO JOSE DO PEIXE", "CAMPO ALEGRE DO FIDALGO",
  "COLONIA DO PIAUI", "FLORES DO PIAUI", "SANTO INACIO DO PIAUI", "NAZARE DO PIAUI",
  "CURRAL NOVO DO PIAUI", "BERTOLINIA", "SAO MIGUEL DO FIDALGO", "ITABAIANINHA",
  "FLORIANO", "MONSENHOR GIL", "CAJUEIRO DA PRAIA", "TOMAR DO GERU", "CAMPO DO BRITO",
  "ARRAIAL", "CAMPINAS DO PIAUI", "PORTO", "VERA MENDES", "SOCORRO DO PIAUI",
  "PAULISTANA", "ITAUEIRA", "MACAMBIRA", "NOSSA SENHORA DA GLORIA"
)
# Filtrar a base municipios apenas para os municípios listados
municipios_filtrados <- municipios %>%
  filter(MUNICIPIO %in% municipios_lista)

# Verificar duplicatas na base filtrada
mun_dup <- municipios_filtrados %>%
  group_by(MUNICIPIO) %>%
  summarise(quantidade = n()) %>%
  filter(quantidade > 1)

# Exibir os municípios duplicados
print(mun_dup)


# Lista de municípios duplicados
municipios_duplicados_lista <- c(
  "BATALHA", "CAPELA", "ESPERANTINA", 
  "ITABAIANA", "NOVA SANTA RITA", "NOVO SANTO ANTONIO", 
  "VARZEA GRANDE"
)
municipios_duplicados <- municipios %>%
  filter(MUNICIPIO %in% municipios_duplicados_lista)


bps.cmed_15.23_intersecao_m <- bps.cmed_15.23_intersecao_m %>%
  mutate(UF = ifelse(is.na(UF) 
                     & NOME_INSTITUICAO == "SECRETARIA MUNICIPAL DE SAUDE" 
                     & MUNICIPIO_INSTITUICAO == "ESPERANTINA", 
                     "PI", UF),
         UF = ifelse(is.na(UF) 
                     & NOME_INSTITUICAO == "MUNICIPIO DE BATALHASECRETARIA MUNICIPAL DE SAUDE E SANEAMENTO BASICO" 
                     & MUNICIPIO_INSTITUICAO == "BATALHA", 
                     "PI", UF),
         UF = ifelse(is.na(UF) 
                     & NOME_INSTITUICAO == "MUNICIPIO DE CAPELA" 
                     & MUNICIPIO_INSTITUICAO == "CAPELA", 
                     "SE", UF),
         UF = ifelse(is.na(UF) 
                     & NOME_INSTITUICAO == "FUNDO MUNICIPAL DE SAUDE DE ITABAIANA SERGIPE" 
                     & MUNICIPIO_INSTITUICAO == "ITABAIANA", 
                     "SE", UF),
         UF = ifelse(is.na(UF) 
                     & NOME_INSTITUICAO == "MUNICIPIO DE NOVA SANTA RITA SECRETARIA DE SAUDE" 
                     & MUNICIPIO_INSTITUICAO == "NOVA SANTA RITA", 
                     "PI", UF),
         
         UF = ifelse(is.na(UF) 
                     & NOME_INSTITUICAO == "FUNDO MUNICIPAL DE SAUDE" 
                     & MUNICIPIO_INSTITUICAO == "NOVO SANTO ANTONIO", 
                     "MT", UF),
         UF = ifelse(is.na(UF) 
                     & NOME_INSTITUICAO == "FUNDO MUNICIPAL DE SAUDE DE VARZEA GRANDE" 
                     & MUNICIPIO_INSTITUICAO == "VARZEA GRANDE", 
                     "MT", UF))
sum(is.na(bps.cmed_15.23_intersecao_m$UF))

# Usar df de municipios para completar faltantes
municipios_sem_duplicados_lista <- data.frame(MUNICIPIO = as.character(
  setdiff(municipios_lista, municipios_duplicados_lista)))

municipios_semduplicados <- municipios %>%
  filter(MUNICIPIO %in% municipios_sem_duplicados_lista$MUNICIPIO)
unique(municipios_sem_duplicados_lista$MUNICIPIO)

bps.cmed_15.23_intersecao_m <- bps.cmed_15.23_intersecao_m %>%
  left_join(municipios_semduplicados %>% select(MUNICIPIO, UF), by = c("MUNICIPIO_INSTITUICAO" = "MUNICIPIO")) 

names(bps.cmed_15.23_intersecao_m)
names(municipios_semduplicados)

bps.cmed_15.23_intersecao_m <- bps.cmed_15.23_intersecao_m %>%
  mutate(UF = ifelse(!is.na(UF.x), UF.x, UF.y)) %>%
  select(-UF.x, -UF.y)

sum(is.na(bps.cmed_15.23_intersecao_m$UF))


# ===
### 3.1.3. Ajustar ICMS ----
# ===

#### _ aliquota zero (CONFAZ) ----
names(bps.cmed_15.23_intersecao_m)

bps.cmed_15.23_intersecao_1 <- bps.cmed_15.23_intersecao_m %>%
  mutate(
    # CATEGORIA_PRECO: Define PMVG para CAP e decisões judiciais (Tipo Compra J), PF para os demais
    CATEGORIA_PRECO = case_when(
      CAP == "SIM" ~ "PMVG",
      `TIPO_COMPRA` == "J" ~ "PMVG",
      TRUE ~ "PF"
    ),
    
    # ICMS: Define como "0%" se ICMS_0% ou CONFAZ_87 forem "SIM"
    ICMS = case_when(
      `ICMS_0%` == "SIM" ~ "0%",
      CONFAZ_87 == "SIM" ~ "0%",
      TRUE ~ NA
    )
  )

sum(is.na(bps.cmed_15.23_intersecao_1$ICMS))
sum(is.na(bps.cmed_15.23_intersecao_1$CATEGORIA_PRECO))
sum(is.na(bps.cmed_15.23_intersecao_1$CAP))


#### _ genéricos PR, MG e SP ----
bps.cmed_15.23_intersecao_1 <- bps.cmed_15.23_intersecao_1 %>%
  left_join(icms_uf_ans, 
            by = c("TIPO_PRODUTO" = "OBS", "UF","ANO_MES_BPS"="ANO_MES")) %>%
  mutate(ICMS = ifelse(is.na(ICMS.x), ICMS.y, ICMS.x)) %>%
  select(-c(ICMS.y,ICMS.x))

sum(is.na(bps.cmed_15.23_intersecao_1$ICMS))


#### _ ALC ----

bps.cmed_15.23_intersecao_1 <- bps.cmed_15.23_intersecao_1 %>%
  left_join(icms_uf_m, 
            by = c("MUNICIPIO_INSTITUICAO" = "MUNICIPIO", "UF","ANO_MES_BPS"="ANO_MES")) %>%
  mutate(ICMS = ifelse(is.na(ICMS.x), ICMS.y, ICMS.x)) %>%
  select(-c(ICMS.y,ICMS.x))

sum(is.na(bps.cmed_15.23_intersecao_1$ICMS))


#### _ estados ----

# Completar ICMS

bps.cmed_15.23_intersecao_1 <- bps.cmed_15.23_intersecao_1 %>%
  left_join(icms_uf_sm, 
            by = c("UF","ANO_MES_BPS"="ANO_MES")) %>%
  mutate(ICMS = ifelse(is.na(ICMS.x), ICMS.y, ICMS.x)) %>%
  select(-c(ICMS.y,ICMS.x))

bps.cmed_15.23_intersecao_nas1 <- bps.cmed_15.23_intersecao_1 %>%
  filter(is.na(bps.cmed_15.23_intersecao_1$ICMS))

sum(is.na(bps.cmed_15.23_intersecao_1$ICMS))
names(bps.cmed_15.23_intersecao_1)


### 3.1.4. União bases ----

bps.cmed_15.23_intersecao_1 = bps.cmed_15.23_intersecao_1 |>
  mutate(
    CHAVE_PRECO = paste0(ANO_MES_BPS, "_", REGISTRO_ANVISA, "_", CATEGORIA_PRECO, " ", 
                         ICMS, "_", APRESENTACAO_PADRAO_BPS)
    )

bps.cmed_15.23_intersecao_2 = bps.cmed_15.23_intersecao_1 %>%
  left_join(
    cmed_15.23_intersecao_long_unique %>%
      select(CHAVE_PRECO, CHAVE_JUNCAO, CATEGORIA, ICMS, PRECO_TETO_APRESENTACAO, PRECO_TETO_UNITARIO),
    by = c("CHAVE_PRECO")
  )
sum(is.na(bps.cmed_15.23_intersecao_2$PRECO_TETO_UNITARIO))
bps.cmed_15.23_intersecao_2_nas = bps.cmed_15.23_intersecao_2 |>
  filter(is.na(bps.cmed_15.23_intersecao_2$PRECO_TETO_UNITARIO))

bps.cmed_15.23_intersecao_final <- bps.cmed_15.23_intersecao_1 %>%
  mutate(
    CHAVE_PRECO = str_replace(CHAVE_PRECO, "^2019-12", "2020-01"),
    CHAVE_PRECO = str_replace(CHAVE_PRECO, "^2019-06", "2019-07"),
    CHAVE_PRECO = str_replace(CHAVE_PRECO, "^2019-11_1942700230041_PMVG 18%", "2019-11_1942700230041_PMVG 0%")
  ) %>%
  left_join(
    cmed_15.23_intersecao_long_unique %>%
      select(CHAVE_PRECO, CHAVE_JUNCAO, CATEGORIA, ICMS, PRECO_TETO_APRESENTACAO, PRECO_TETO_UNITARIO),
    by = "CHAVE_PRECO"
  )

bps.cmed_15.23_intersecao_final_nas = bps.cmed_15.23_intersecao_final |>
  filter(is.na(bps.cmed_15.23_intersecao_final$PRECO_TETO_UNITARIO))


names(bps.cmed_15.23_intersecao_final)
sum(is.na(bps.cmed_15.23_intersecao_final$PRECO_TETO_UNITARIO))
sum(is.na(bps.cmed_15.23_intersecao_final$PRECO_TETO_APRESENTACAO))

bps.cmed_15.23_intersecao_final = bps.cmed_15.23_intersecao_final |>
  rename(
    "ICMS" = "ICMS.x",
    "CHAVE_JUNCAO" = "CHAVE_JUNCAO.x"
  ) |>
  select(-c("ICMS.y","CHAVE_JUNCAO.y"))



## 3.2. ANS e CMED ----

# Medicamento de uso exclusivamente hospitalar não pode aplicar PMC

names(ans_15.23_intersecao)

ans_15.23_intersecao_ = ans_15.23_intersecao |>
  filter(IND_PACOTE == 0, na.rm = TRUE)
ans_contagem_nas <- ans_15.23_intersecao %>%
  summarise(across(everything(), ~ sum(is.na(.)))) %>%
  pivot_longer(cols = everything(), names_to = "coluna", values_to = "na_count")
print(ans_contagem_nas, n = Inf)
names(ans_15.23_intersecao)

ans.cmed_15.23_intersecao <- ans_15.23_intersecao %>%
  left_join(
    cmed_15.23_intersecao_descritivo,
    by = c("CHAVE_JUNCAO")
  )

names(ans.cmed_15.23_intersecao)
ans.cmed_contagem_nas <- ans.cmed_15.23_intersecao %>%
  summarise(across(everything(), ~ sum(is.na(.)))) %>%
  pivot_longer(cols = everything(), names_to = "coluna", values_to = "na_count")
print(ans.cmed_contagem_nas, n = Inf)

unique(ans.cmed_15.23_intersecao$UF)


# ===
### 3.2.1. Ajustar ICMS ----
# ===
names(ans.cmed_15.23_intersecao)

#### _ uso hospitalar: PF|PMC ----
ans.cmed_15.23_intersecao_1 <- ans.cmed_15.23_intersecao %>%
  mutate(
    CATEGORIA_PRECO = case_when(
      `RESTRICAO_HOSPITALAR` == "SIM" ~ "PF",
      TRUE ~ "PMC"
    ))

#### _ alíquota zero (CONFAZ) ----
ans.cmed_15.23_intersecao_1 <- ans.cmed_15.23_intersecao_1 %>%
  mutate(
    # ICMS: Define como "0%" se ICMS_0% ou CONFAZ_87 forem "SIM"
    ICMS = case_when(
      `ICMS_0%` == "SIM" ~ "0%",
      CONFAZ_87 == "SIM" ~ "0%",
      TRUE ~ NA # Mantém o valor original nos demais casos
    )
  )
sum(is.na(ans.cmed_15.23_intersecao_1$ICMS))

#### _ genéricos MG e SP ----

ans.cmed_15.23_intersecao_1 <- ans.cmed_15.23_intersecao_1 %>%
  left_join(icms_uf_ans, 
            by = c("TIPO_PRODUTO" = "OBS","UF","ANO_MES_ANS"="ANO_MES")) %>%
  mutate(ICMS = ifelse(is.na(ICMS.x), ICMS.y, ICMS.x)) %>%
  select(-c(ICMS.y,ICMS.x))

sum(is.na(ans.cmed_15.23_intersecao_1$ICMS))


#### _ estados ----

# Completar ICMS

ans.cmed_15.23_intersecao_1 <- ans.cmed_15.23_intersecao_1 %>%
  left_join(icms_uf_sm, 
            by = c("UF","ANO_MES_ANS"="ANO_MES")) %>%
  mutate(ICMS = ifelse(is.na(ICMS.x), ICMS.y, ICMS.x)) %>%
  select(-c(ICMS.y,ICMS.x))

sum(is.na(ans.cmed_15.23_intersecao_1$ICMS))

names(ans.cmed_15.23_intersecao_1)


### 3.2.2. União bases ----

ans.cmed_15.23_intersecao_1 <- ans.cmed_15.23_intersecao_1 %>%
mutate(
  CHAVE_PRECO = paste0(ANO_MES_ANS, "_", REGISTRO_ANVISA, "_", CATEGORIA_PRECO, " ", ICMS, "_", APRESENTACAO_PADRAO_ANS)
)

# Ajustar e padronizar `CHAVE_PRECO`
padronizar_chave_preco <- function(df) {
  df %>%
    mutate(
      CHAVE_PRECO = str_trim(CHAVE_PRECO),
      CHAVE_PRECO = str_replace_all(CHAVE_PRECO, "\\s+", " "),
      CHAVE_PRECO = str_to_upper(CHAVE_PRECO)
    )
}

ans.cmed_15.23_intersecao_1 <- padronizar_chave_preco(ans.cmed_15.23_intersecao_1)
cmed_15.23_intersecao_long_unique <- padronizar_chave_preco(cmed_15.23_intersecao_long_unique)

# Tentar unir novamente  usando `CHAVE_PRECO`
ans.cmed_15.23_intersecao_2 <- ans.cmed_15.23_intersecao_1 %>%
  left_join(
    cmed_15.23_intersecao_long_unique %>%
      select(CHAVE_PRECO, CHAVE_JUNCAO, CATEGORIA, ICMS, PRECO_TETO_APRESENTACAO, PRECO_TETO_UNITARIO),
    by = "CHAVE_PRECO"
  )

# Relatório de valores ausentes após o left_join()
ans.cmed_15.23_intersecao_2_nas <- ans.cmed_15.23_intersecao_2 %>%
  summarise(across(everything(), ~ sum(is.na(.)))) %>%
  pivot_longer(cols = everything(), names_to = "coluna", values_to = "na_count")

print(ans.cmed_15.23_intersecao_2_nas, n = Inf)

# Criar um relatório por mês para visualizar os valores ausentes
relatorio_nas <- ans.cmed_15.23_intersecao_2 %>%
  filter(is.na(PRECO_TETO_UNITARIO)) %>%
  group_by(ANO_MES_ANS) %>%
  summarise(n = n())

print(relatorio_nas, n = Inf)

# Estratégia de Preenchimento Dinâmico
## **Preencher com o mês anterior**
ans.cmed_15.23_intersecao_2 <- ans.cmed_15.23_intersecao_2 %>%
  arrange(ANO_MES_ANS) %>%
  group_by(REGISTRO_ANVISA) %>%
  mutate(
    PRECO_TETO_UNITARIO = ifelse(is.na(PRECO_TETO_UNITARIO), lag(PRECO_TETO_UNITARIO), PRECO_TETO_UNITARIO),
    PRECO_TETO_APRESENTACAO = ifelse(is.na(PRECO_TETO_APRESENTACAO), lag(PRECO_TETO_APRESENTACAO), PRECO_TETO_APRESENTACAO)
  ) %>%
  ungroup()


## **Preencher com o mês seguinte se o anterior não existia**
ans.cmed_15.23_intersecao_2 <- ans.cmed_15.23_intersecao_2 %>%
  arrange(desc(ANO_MES_ANS)) %>%
  group_by(REGISTRO_ANVISA) %>%
  mutate(
    PRECO_TETO_UNITARIO = ifelse(is.na(PRECO_TETO_UNITARIO), lead(PRECO_TETO_UNITARIO), PRECO_TETO_UNITARIO),
    PRECO_TETO_APRESENTACAO = ifelse(is.na(PRECO_TETO_APRESENTACAO), lead(PRECO_TETO_APRESENTACAO), PRECO_TETO_APRESENTACAO)
  ) %>%
  ungroup()

# Ajustar nomes de colunas duplicadas se necessário
ans.cmed_15.23_intersecao_final <- ans.cmed_15.23_intersecao_2 %>%
  rename(
    "ICMS" = "ICMS.x",
    "CHAVE_JUNCAO" = "CHAVE_JUNCAO.x"
  ) %>%
  select(-c("ICMS.y", "CHAVE_JUNCAO.y"))

# Última verificação de valores ausentes após todas as correções
ans.cmed_15.23_intersecao_final_nas <- ans.cmed_15.23_intersecao_final %>%
  filter(is.na(PRECO_TETO_UNITARIO))

print(ans.cmed_15.23_intersecao_final_nas, n = 50) 

# Exibir o número final de valores ausentes após todas as correções
sum(is.na(ans.cmed_15.23_intersecao_final$PRECO_TETO_UNITARIO))
sum(is.na(ans.cmed_15.23_intersecao_final$PRECO_TETO_APRESENTACAO))


names(ans.cmed_15.23_intersecao_final)

num_registros_unicos_ans <- ans.cmed_15.23_intersecao_final %>%
  summarise(total_unicos = n_distinct(REGISTRO))
num_registros_unicos_ans

num_registros_unicos_bps <- bps.cmed_15.23_intersecao_final %>%
  summarise(total_unicos = n_distinct(REGISTRO))
num_registros_unicos_bps


#___________________----
# # SALVAR  ----

write_parquet(ans.cmed_15.23_intersecao_final, file.path(caminho_resultados,"ans_cmed_15_23_intersecao_final.parquet"))
write_parquet(bps.cmed_15.23_intersecao_final, file.path(caminho_resultados,"bps.cmed_15.23_intersecao_final.parquet"))
