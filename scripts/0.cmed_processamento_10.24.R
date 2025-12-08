rm(list = ls())

# ___________________ ----
# CMED ----
# ¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨----
# Baixar dados brutos em: https://www.gov.br/anvisa/pt-br/assuntos/medicamentos/cmed/precos/anos-anteriores/anos-anteriores

# ___________________ ----
# 1. Carregar Pacotes ----
library(readxl)
library(dplyr) 
library(stringr)
library(writexl)
library(purrr)  
library(openxlsx)
library(arrow)
library(stats)
library(tidytext)
library(tm)        
library(tidyr)
library(lubridate)
library(janitor)
library(foreign)

# ___________________ ----
# 2. Carregar dados ----


## 2.1. Carregar meses ----

## Função para processar uma planilha
processar_planilha <- function(caminho_arquivo) {
  
  conteudo <- read_excel(caminho_arquivo, col_names = FALSE)
  
  # Padrões de cabeçalho que procuramos
  padroes_cabecalho <- c("CÓDIGO GGREM","SUBSTÂNCIA", "SUBSTANCIA", "PRINCÍPIO ATIVO", "PRINCIPIO ATIVO")
  linha_cabecalho <- NA
  for (i in 1:nrow(conteudo)) {
    linha <- conteudo[i, ]
    
    if (!is.na(linha[[1]]) && any(str_starts(linha[[1]], padroes_cabecalho))) {
      linha_cabecalho <- i
      break
    }
  }
  
  if (is.na(linha_cabecalho)) {
    cat("Aviso: Não foi encontrada a linha de cabeçalho no arquivo:", caminho_arquivo, "\n")
    return(NULL)
  }
  
  cat("Número da linha do cabeçalho encontrado:", linha_cabecalho, "\n")
  
  dados <- read_excel(caminho_arquivo, skip = linha_cabecalho - 1)
  
  data_valor <- NA
  data_publicada <- NA
  data_atualizada <- NA
  vetor_texto <- na.omit(unlist(conteudo))
  
  vetor_texto <- stringr::str_trim(vetor_texto)
  
  for (linha in vetor_texto) {
    if (str_detect(linha, "^4[0-9]*$")) {
      # Converter o valor da linha que começa com "4" em data
      data_valor <- as.Date(as.numeric(linha), origin = "1899-12-30")
      # Formatar a data no formato "dd/mm/yyyy"
      data_valor <- format(data_valor, "%d/%m/%Y")
      break
    }
  }
  
  if (!is.na(data_valor)) {
    cat("Valor detectado que começa com '4':", data_valor, "\n")
  }
  
  for (linha in vetor_texto) {
    if (str_detect(linha, "^Publicada em:")) {
      data_publicada <- str_extract(linha, "[0-9]{2}/[0-9]{2}/[0-9]{4}")
    }
    if (str_detect(linha, "^Atualizada em:")) {
      data_atualizada <- str_extract(linha, "[0-9]{2}/[0-9]{2}/[0-9]{4}")
    }
  }
  
  for (linha in vetor_texto) {
    if (str_detect(linha, "^Publicada em")) {
      padrao_publicada_atualizada <- "Publicada em\\s+([0-9]{2}/[0-9]{2}/[0-9]{4}),?\\s*(?:atualizada em\\s+([0-9]{2}/[0-9]{2}/[0-9]{4}))?"
      captura <- str_match(linha, padrao_publicada_atualizada)
      
      if (!is.na(captura[1, 2])) {
        data_publicada <- captura[1, 2]
      }
      if (!is.na(captura[1, 3])) {
        data_atualizada <- captura[1, 3]
      }
      break
    }
  }
  
  if (is.na(data_publicada)) {
    data_publicada <- data_valor
  }
  
  # Exibir as datas capturadas
  cat("Data de publicação:", data_publicada, "\n")
  cat("Data de atualização:", data_atualizada, "\n")
  
  dados <- dados %>%
    mutate(Data_Publicada = data_publicada,
           Data_Atualizada = data_atualizada,
           .before = 1)
  
  print(head(dados))
  
  return(dados)
}


# Função para processar todas as planilhas em um diretório
processar_todas_planilhas <- function(diretorio) {
  arquivos <- list.files(diretorio, pattern = "\\.(xlsx|xls)$", full.names = TRUE)
  
  lista_dados <- list()
  
  for (arquivo in arquivos) {
    cat("Processando arquivo:", arquivo, "\n")
    dados_processados <- processar_planilha(arquivo)
    
    if (is.null(dados_processados) || length(dados_processados) == 0) {
      cat("Aviso: Nenhuma aba processada no arquivo:", arquivo, "\n")
      next
    }
    
    lista_dados[[basename(arquivo)]] <- dados_processados
  }
  
  return(lista_dados)
}


### 2.1.1. PMC----

#### ___2010-2024 ----

diretorio_cmed_pmc_10 <- #Identificar caminhos dos arquivos brutos
cmed_pmc_10 <- processar_todas_planilhas(diretorio_cmed_pmc_10)
summary(cmed_pmc_10)

diretorio_cmed_pmc_11 <- #Identificar caminhos dos arquivos brutos
cmed_pmc_11 <- processar_todas_planilhas(diretorio_cmed_pmc_11)
summary(cmed_pmc_11)

diretorio_cmed_pmc_12 <- #Identificar caminhos dos arquivos brutos
cmed_pmc_12 <- processar_todas_planilhas(diretorio_cmed_pmc_12)
summary(cmed_pmc_12)

diretorio_cmed_pmc_13 <- #Identificar caminhos dos arquivos brutos
cmed_pmc_13 <- processar_todas_planilhas(diretorio_cmed_pmc_13)
summary(cmed_pmc_13)

diretorio_cmed_pmc_14 <- #Identificar caminhos dos arquivos brutos
cmed_pmc_14 <- processar_todas_planilhas(diretorio_cmed_pmc_14)
summary(cmed_pmc_14)

diretorio_cmed_pmc_15 <- #Identificar caminhos dos arquivos brutos
cmed_pmc_15 <- processar_todas_planilhas(diretorio_cmed_pmc_15)
summary(cmed_pmc_15)

diretorio_cmed_pmc_16 <- #Identificar caminhos dos arquivos brutos
cmed_pmc_16 <- processar_todas_planilhas(diretorio_cmed_pmc_16)
summary(cmed_pmc_16)

diretorio_cmed_pmc_17 <- #Identificar caminhos dos arquivos brutos
cmed_pmc_17 <- processar_todas_planilhas(diretorio_cmed_pmc_17)
summary(cmed_pmc_17)

diretorio_cmed_pmc_18 <- #Identificar caminhos dos arquivos brutos
cmed_pmc_18 <- processar_todas_planilhas(diretorio_cmed_pmc_18)
summary(cmed_pmc_18)

diretorio_cmed_pmc_19 <- #Identificar caminhos dos arquivos brutos
cmed_pmc_19 <- processar_todas_planilhas(diretorio_cmed_pmc_19)
summary(cmed_pmc_19)

diretorio_cmed_pmc_20 <- #Identificar caminhos dos arquivos brutos
cmed_pmc_20 <- processar_todas_planilhas(diretorio_cmed_pmc_20)
summary(cmed_pmc_20)

diretorio_cmed_pmc_21 <- #Identificar caminhos dos arquivos brutos
cmed_pmc_21 <- processar_todas_planilhas(diretorio_cmed_pmc_21)
summary(cmed_pmc_21)

diretorio_cmed_pmc_22 <- #Identificar caminhos dos arquivos brutos
cmed_pmc_22 <- processar_todas_planilhas(diretorio_cmed_pmc_22)
summary(cmed_pmc_22)

diretorio_cmed_pmc_23 <- #Identificar caminhos dos arquivos brutos
cmed_pmc_23 <- processar_todas_planilhas(diretorio_cmed_pmc_23)
summary(cmed_pmc_23)

diretorio_cmed_pmc_24 <- #Identificar caminhos dos arquivos brutos
cmed_pmc_24 <- processar_todas_planilhas(diretorio_cmed_pmc_24)
summary(cmed_pmc_24)



### 2.1.2. PMVG ----

#### ___2011-2024 ----

diretorio_cmed_pmvg_11 <- #Identificar caminhos dos arquivos brutos
cmed_pmvg_11 <- processar_todas_planilhas(diretorio_cmed_pmvg_11)
summary(cmed_pmvg_11)

diretorio_cmed_pmvg_12 <- #Identificar caminhos dos arquivos brutos
cmed_pmvg_12 <- processar_todas_planilhas(diretorio_cmed_pmvg_12)
summary(cmed_pmvg_12)

diretorio_cmed_pmvg_13 <- #Identificar caminhos dos arquivos brutos
cmed_pmvg_13 <- processar_todas_planilhas(diretorio_cmed_pmvg_13)
summary(cmed_pmvg_13)

diretorio_cmed_pmvg_14 <- #Identificar caminhos dos arquivos brutos
cmed_pmvg_14 <- processar_todas_planilhas(diretorio_cmed_pmvg_14)
summary(cmed_pmvg_14)

diretorio_cmed_pmvg_15 <- #Identificar caminhos dos arquivos brutos
cmed_pmvg_15 <- processar_todas_planilhas(diretorio_cmed_pmvg_15)
summary(cmed_pmvg_15)

diretorio_cmed_pmvg_16 <- #Identificar caminhos dos arquivos brutos
cmed_pmvg_16 <- processar_todas_planilhas(diretorio_cmed_pmvg_16)
summary(cmed_pmvg_16)

diretorio_cmed_pmvg_17 <- #Identificar caminhos dos arquivos brutos
cmed_pmvg_17 <- processar_todas_planilhas(diretorio_cmed_pmvg_17)
summary(cmed_pmvg_17)

diretorio_cmed_pmvg_18 <- #Identificar caminhos dos arquivos brutos
cmed_pmvg_18 <- processar_todas_planilhas(diretorio_cmed_pmvg_18)
summary(cmed_pmvg_18)

diretorio_cmed_pmvg_19 <- #Identificar caminhos dos arquivos brutos
cmed_pmvg_19 <- processar_todas_planilhas(diretorio_cmed_pmvg_19)
summary(cmed_pmvg_19)

diretorio_cmed_pmvg_20 <- #Identificar caminhos dos arquivos brutos
cmed_pmvg_20 <- processar_todas_planilhas(diretorio_cmed_pmvg_20)
summary(cmed_pmvg_20)

diretorio_cmed_pmvg_21 <- #Identificar caminhos dos arquivos brutos
cmed_pmvg_21 <- processar_todas_planilhas(diretorio_cmed_pmvg_21)
summary(cmed_pmvg_21)

diretorio_cmed_pmvg_22 <- #Identificar caminhos dos arquivos brutos
cmed_pmvg_22 <- processar_todas_planilhas(diretorio_cmed_pmvg_22)
summary(cmed_pmvg_22)

diretorio_cmed_pmvg_23 <- #Identificar caminhos dos arquivos brutos
cmed_pmvg_23 <- processar_todas_planilhas(diretorio_cmed_pmvg_23)
summary(cmed_pmvg_23)

diretorio_cmed_pmvg_24 <- #Identificar caminhos dos arquivos brutos
cmed_pmvg_24 <- processar_todas_planilhas(diretorio_cmed_pmvg_24)
summary(cmed_pmvg_24)


# ¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨----
## 2.2. Criar DFs ----

padronizar_conflitos <- function(df) {
  for (col in colnames(df)) {
    df[[col]] <- as.character(df[[col]])
  }
  return(df)
}


### 2.2.1. PMC ----

# Padronizar conflitos
cmed_pmc_10 <- map(cmed_pmc_10, padronizar_conflitos)
cmed_pmc_11 <- map(cmed_pmc_11, padronizar_conflitos)
cmed_pmc_12 <- map(cmed_pmc_12, padronizar_conflitos)
cmed_pmc_13 <- map(cmed_pmc_13, padronizar_conflitos)
cmed_pmc_14 <- map(cmed_pmc_14, padronizar_conflitos)
cmed_pmc_15 <- map(cmed_pmc_15, padronizar_conflitos)
cmed_pmc_16 <- map(cmed_pmc_16, padronizar_conflitos)
cmed_pmc_17 <- map(cmed_pmc_17, padronizar_conflitos)
cmed_pmc_18 <- map(cmed_pmc_18, padronizar_conflitos)
cmed_pmc_19 <- map(cmed_pmc_19, padronizar_conflitos)
cmed_pmc_20 <- map(cmed_pmc_20, padronizar_conflitos)
cmed_pmc_21 <- map(cmed_pmc_21, padronizar_conflitos)
cmed_pmc_22 <- map(cmed_pmc_22, padronizar_conflitos)
cmed_pmc_23 <- map(cmed_pmc_23, padronizar_conflitos)
cmed_pmc_24 <- map(cmed_pmc_24, padronizar_conflitos)


# Converter Listas em Dataframes
df_cmed_pmc_10 <- bind_rows(cmed_pmc_10, .id = "Arquivo")
df_cmed_pmc_11 <- bind_rows(cmed_pmc_11, .id = "Arquivo")
df_cmed_pmc_12 <- bind_rows(cmed_pmc_12, .id = "Arquivo")
df_cmed_pmc_13 <- bind_rows(cmed_pmc_13, .id = "Arquivo")
df_cmed_pmc_14 <- bind_rows(cmed_pmc_14, .id = "Arquivo")
df_cmed_pmc_15 <- bind_rows(cmed_pmc_15, .id = "Arquivo")
df_cmed_pmc_16 <- bind_rows(cmed_pmc_16, .id = "Arquivo")
df_cmed_pmc_17 <- bind_rows(cmed_pmc_17, .id = "Arquivo")
df_cmed_pmc_18 <- bind_rows(cmed_pmc_18, .id = "Arquivo")
df_cmed_pmc_19 <- bind_rows(cmed_pmc_19, .id = "Arquivo")
df_cmed_pmc_20 <- bind_rows(cmed_pmc_20, .id = "Arquivo")
df_cmed_pmc_21 <- bind_rows(cmed_pmc_21, .id = "Arquivo")
df_cmed_pmc_21 <- df_cmed_pmc_21 %>% select(-`...7`)
df_cmed_pmc_22 <- bind_rows(cmed_pmc_22, .id = "Arquivo")
df_cmed_pmc_23 <- bind_rows(cmed_pmc_23, .id = "Arquivo")
df_cmed_pmc_24 <- bind_rows(cmed_pmc_24, .id = "Arquivo")

### 2.2.2. PMVG ----
cmed_pmvg_11 <- map(cmed_pmvg_11, padronizar_conflitos)
cmed_pmvg_12 <- map(cmed_pmvg_12, padronizar_conflitos)
cmed_pmvg_13 <- map(cmed_pmvg_13, padronizar_conflitos)
cmed_pmvg_14 <- map(cmed_pmvg_14, padronizar_conflitos)
cmed_pmvg_15 <- map(cmed_pmvg_15, padronizar_conflitos)
cmed_pmvg_16 <- map(cmed_pmvg_16, padronizar_conflitos)
cmed_pmvg_17 <- map(cmed_pmvg_17, padronizar_conflitos)
cmed_pmvg_18 <- map(cmed_pmvg_18, padronizar_conflitos)
cmed_pmvg_19 <- map(cmed_pmvg_19, padronizar_conflitos)
cmed_pmvg_20 <- map(cmed_pmvg_20, padronizar_conflitos)
cmed_pmvg_21 <- map(cmed_pmvg_21, padronizar_conflitos)
cmed_pmvg_22 <- map(cmed_pmvg_22, padronizar_conflitos)
cmed_pmvg_23 <- map(cmed_pmvg_23, padronizar_conflitos)
cmed_pmvg_24 <- map(cmed_pmvg_24, padronizar_conflitos)


#### ___Completar faltantes ----

abril_df <- cmed_pmvg_21[[4]]
maio_df <- cmed_pmvg_21[[5]]

# Duplicatas 
abril_duplicados <- abril_df %>%
  group_by(REGISTRO, `CÓDIGO GGREM`, `PF 0%`) %>%
  filter(n() > 1)
maio_duplicados <- maio_df %>%
  group_by(REGISTRO, `CÓDIGO GGREM`) %>%
  filter(n() > 1)

# Identificar medicamentos adicionados 
medicamentos_adicionados_21 <- maio_df %>%
  anti_join(abril_df, by = c("REGISTRO", "CÓDIGO GGREM")) %>% 
  select(-contains("PMC"))


# Realizar o join com os dados de abril para manter os PMVGs existentes
mai_df <- bind_rows(abril_df, medicamentos_adicionados_21)%>% 
  select(-contains("PMC")) %>%
  mutate(
    SUBSTANCIA = coalesce(`PRINCÍPIO ATIVO`, SUBSTÂNCIA),
    Data_Publicada  = "05/05/2021",
    Data_Atualizada = NA
  )  # Opcional: Remover as colunas originais


# Verificar os nomes das colunas no resultado
names(mai_df)

# Substituir o conteúdo 
cmed_pmvg_21[[5]] <- mai_df

# Verificar se a substituição foi bem-sucedida
print(cmed_pmvg_21[[5]])


## Converter Lista em Dataframe
df_cmed_pmvg_11 <- bind_rows(cmed_pmvg_11, .id = "Arquivo")
df_cmed_pmvg_12 <- bind_rows(cmed_pmvg_12, .id = "Arquivo")
df_cmed_pmvg_13 <- bind_rows(cmed_pmvg_13, .id = "Arquivo")
df_cmed_pmvg_14 <- bind_rows(cmed_pmvg_14, .id = "Arquivo")
df_cmed_pmvg_15 <- bind_rows(cmed_pmvg_15, .id = "Arquivo")
df_cmed_pmvg_16 <- bind_rows(cmed_pmvg_16, .id = "Arquivo")
df_cmed_pmvg_17 <- bind_rows(cmed_pmvg_17, .id = "Arquivo")
df_cmed_pmvg_18 <- bind_rows(cmed_pmvg_18, .id = "Arquivo")
df_cmed_pmvg_19 <- bind_rows(cmed_pmvg_19, .id = "Arquivo")
df_cmed_pmvg_20 <- bind_rows(cmed_pmvg_20, .id = "Arquivo")
df_cmed_pmvg_20 <- df_cmed_pmvg_20 %>% select(-c(Coluna1,`...44`,`...45`,`...46`,`...47`,
                                                 `...48`,`...49`))
df_cmed_pmvg_21 <- bind_rows(cmed_pmvg_21, .id = "Arquivo")
df_cmed_pmvg_21 <- df_cmed_pmvg_21 %>% select(-c(`...44`,`...45`))
df_cmed_pmvg_22 <- bind_rows(cmed_pmvg_22, .id = "Arquivo")
df_cmed_pmvg_22 <- df_cmed_pmvg_22 %>% select(-c(Coluna1,`...44`,`...45`,`...46`,`...47`,
                                                 `...48`,`...49`,Show,`520701001158112`))
df_cmed_pmvg_23 <- bind_rows(cmed_pmvg_23, .id = "Arquivo")
df_cmed_pmvg_23 <- df_cmed_pmvg_23 %>% select(-c(`...50`,`...51`))
df_cmed_pmvg_24 <- bind_rows(cmed_pmvg_24, .id = "Arquivo")
df_cmed_pmvg_24 <- df_cmed_pmvg_24 %>% 
  mutate (TARJA = `...64`)%>%
  select(-c(`...64`,`...65`))
names(df_cmed_pmvg_24)


# ¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨----
## 2.3. Agrupar DFs ----

### ___PMC ----
df_cmed_pmc_10 <- padronizar_conflitos(df_cmed_pmc_10)
df_cmed_pmc_11 <- padronizar_conflitos(df_cmed_pmc_11)
df_cmed_pmc_12 <- padronizar_conflitos(df_cmed_pmc_12)
df_cmed_pmc_13 <- padronizar_conflitos(df_cmed_pmc_13)
df_cmed_pmc_14 <- padronizar_conflitos(df_cmed_pmc_14)
df_cmed_pmc_15 <- padronizar_conflitos(df_cmed_pmc_15)
df_cmed_pmc_16 <- padronizar_conflitos(df_cmed_pmc_16)
df_cmed_pmc_17 <- padronizar_conflitos(df_cmed_pmc_17)
df_cmed_pmc_18 <- padronizar_conflitos(df_cmed_pmc_18)
df_cmed_pmc_19 <- padronizar_conflitos(df_cmed_pmc_19)
df_cmed_pmc_20 <- padronizar_conflitos(df_cmed_pmc_20)
df_cmed_pmc_21 <- padronizar_conflitos(df_cmed_pmc_21)
df_cmed_pmc_22 <- padronizar_conflitos(df_cmed_pmc_22)
df_cmed_pmc_23 <- padronizar_conflitos(df_cmed_pmc_23)
df_cmed_pmc_24 <- padronizar_conflitos(df_cmed_pmc_24)

names(df_cmed_pmc_18)
names(df_cmed_pmc_19)
names(df_cmed_pmc_20)
names(df_cmed_pmc_21)
names(df_cmed_pmc_22)
names(df_cmed_pmc_23)
names(df_cmed_pmc_24)

df_cmed_pmc_10.24 <- bind_rows(df_cmed_pmc_10, df_cmed_pmc_11,df_cmed_pmc_12, 
                               df_cmed_pmc_13,df_cmed_pmc_14, df_cmed_pmc_15,
                               df_cmed_pmc_16, df_cmed_pmc_17,
                               df_cmed_pmc_18, df_cmed_pmc_19, df_cmed_pmc_20, 
                               df_cmed_pmc_21, df_cmed_pmc_22, df_cmed_pmc_23, 
                               df_cmed_pmc_24)

### ___PMVG ----
df_cmed_pmvg_11 <- padronizar_conflitos(df_cmed_pmvg_11)
df_cmed_pmvg_12 <- padronizar_conflitos(df_cmed_pmvg_12)
df_cmed_pmvg_13 <- padronizar_conflitos(df_cmed_pmvg_13)
df_cmed_pmvg_14 <- padronizar_conflitos(df_cmed_pmvg_14)
df_cmed_pmvg_15 <- padronizar_conflitos(df_cmed_pmvg_15)
df_cmed_pmvg_16 <- padronizar_conflitos(df_cmed_pmvg_16)
df_cmed_pmvg_17 <- padronizar_conflitos(df_cmed_pmvg_17)
df_cmed_pmvg_18 <- padronizar_conflitos(df_cmed_pmvg_18)
df_cmed_pmvg_19 <- padronizar_conflitos(df_cmed_pmvg_19)
df_cmed_pmvg_20 <- padronizar_conflitos(df_cmed_pmvg_20)
df_cmed_pmvg_21 <- padronizar_conflitos(df_cmed_pmvg_21)
df_cmed_pmvg_22 <- padronizar_conflitos(df_cmed_pmvg_22)
df_cmed_pmvg_23 <- padronizar_conflitos(df_cmed_pmvg_23)
df_cmed_pmvg_24 <- padronizar_conflitos(df_cmed_pmvg_24)

df_cmed_pmvg_11.24 <- bind_rows(df_cmed_pmvg_11,df_cmed_pmvg_12, df_cmed_pmvg_13,
                                df_cmed_pmvg_14, df_cmed_pmvg_15, df_cmed_pmvg_16, 
                                df_cmed_pmvg_17,df_cmed_pmvg_18, df_cmed_pmvg_19, 
                                df_cmed_pmvg_20,df_cmed_pmvg_21, df_cmed_pmvg_22,
                                df_cmed_pmvg_23,df_cmed_pmvg_24)

names(df_cmed_pmvg_11.24)

# ¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨----
## 2.4. Ajustar Cabeçalhos ----

### ___PMC ----
names(df_cmed_pmc_10.24)

colnames(df_cmed_pmc_10.24) <- toupper(colnames(df_cmed_pmc_10.24))
colnames(df_cmed_pmc_10.24) <- stringr::str_trim(colnames(df_cmed_pmc_10.24))
colnames(df_cmed_pmc_10.24) <- gsub(",", ".", colnames(df_cmed_pmc_10.24))
colnames(df_cmed_pmc_10.24) <- make.unique(colnames(df_cmed_pmc_10.24))

names(df_cmed_pmc_10.24)

df_cmed_pmc_10.24 <- df_cmed_pmc_10.24 %>%
  mutate_all(as.character)

df_tratada_cmed_pmc_10.24 = df_cmed_pmc_10.24 |>
  mutate(`PF_0%` = coalesce(`PF 0%`, `PFC0`),
         `PF_12%` = coalesce(`PF 12%`, `PF12`),
         `PF_17%` = coalesce(`PF 17%`, `PF17`),
         `PF_18%` = coalesce(`PF 18%`, `PF18`),
         `PF_19%` = coalesce(`PF 19%`, `PF19`),
         `PF_17% ALC`= coalesce(`PF 17% ZONA FRANCA DE MANAUS`,`PF17ZFM`,`PF 17% ALC`),
         `PF_18% ALC`= coalesce(`PF 18% ZONA FRANCA DE MANAUS`,`PF 18% ALC`),
         `PMC_0%` = coalesce(`PMC 0%`, `PMC0_P`),
         `PMC_12%` = coalesce(`PMC 12%`, `PMC12_P`),
         `PMC_17%` = coalesce(`PMC 17%`, `PMC17_P`),
         `PMC_18%` = coalesce(`PMC 18%`, `PMC18_P`),
         `PMC_19%` = coalesce(`PMC 19%`, `PMC19_P`),
         `PMC_17% ALC`= coalesce(`PMC 17% ZONA FRANCA DE MANAUS`,`PMC17ZFM_P`,`PMC 17% ALC`),
         `PMC_18% ALC`= coalesce(`PMC 18% ZONA FRANCA DE MANAUS`,`PMC 18% ALC`),
         `CAP1` = coalesce(CAP, CAP.1),
         `CODIGO_GGREM` = coalesce(`CÓDIGO GGREM`, COD_GGREM),
         `ANALISE_RECURSAL1` = coalesce(`ANÁLISE RECURSAL`,ANALISE_RECURSAL,`ANÁLISE RECURSAL CMED / PREÇO AJUSTADO POR DECISÃO JUDICIAL`),
         `APRESENTACAO1` = coalesce(APRESENTAÇÃO,`APRSENTAÇÃO`,APRESENTACAO),
         `TIPO_PRODUTO` = coalesce(`TIPO DE PRODUTO`,`TIPO DE PRODUTO (STATUS DO PRODUTO)`),
         `EAN_1` = coalesce(EAN, `EAN 1`),
         `SUBSTANCIA1` = coalesce(`PRINCÍPIO ATIVO`, `SUBSTÂNCIA`, SUBSTANCIA),
         `LABORATORIO` = coalesce(RAZAO_SOCIAL, LABORATÓRIO),
         `CONFAZ_871` = coalesce(`CONFAZ 87`,CONFAZ_87),
         `ULTIMA_ALTERACAO` = coalesce(`ULTIMA ALTERAÇÃO` , `ÚLTIMA ALTERAÇÃO`),
         `RESTRICAO_HOSPITALAR` = coalesce(REST_HOSP, `RESTRIÇÃO HOSPITALAR`)
         ) |>
  select(
    -c(`PF 0%`, `PFC0`,
       `PF 12%`, `PF12`,
       `PF 17%`, `PF17`,
       `PF 18%`, `PF18`, 
       `PF 19%`, `PF19`,
       `PF 17% ZONA FRANCA DE MANAUS`,`PF17ZFM`,`PF 17% ALC`,
       `PF 18% ZONA FRANCA DE MANAUS`,`PF 18% ALC`,
       `PMC 0%`, `PMC0_P`,
       `PMC 12%`, `PMC12_P`,
       `PMC 17%`, `PMC17_P`,
       `PMC 18%`, `PMC18_P`,
       `PMC 19%`, `PMC19_P`,
       `PMC 17% ZONA FRANCA DE MANAUS`,`PMC17ZFM_P`,`PMC 17% ALC`,
       `PMC 18% ZONA FRANCA DE MANAUS`,`PMC 18% ALC`,
       CAP, CAP.1,`CÓDIGO GGREM`, COD_GGREM,
       `ANÁLISE RECURSAL`,ANALISE_RECURSAL,`ANÁLISE RECURSAL CMED / PREÇO AJUSTADO POR DECISÃO JUDICIAL`,
       APRESENTAÇÃO,`APRSENTAÇÃO`,APRESENTACAO, `TIPO DE PRODUTO`,`TIPO DE PRODUTO (STATUS DO PRODUTO)`,
       EAN, `EAN 1`, `PRINCÍPIO ATIVO`, `SUBSTÂNCIA`, SUBSTANCIA,
       RAZAO_SOCIAL, LABORATÓRIO,
       `CONFAZ 87`,CONFAZ_87,
       `ULTIMA ALTERAÇÃO` , `ÚLTIMA ALTERAÇÃO`,
       REST_HOSP, `RESTRIÇÃO HOSPITALAR`
       )
    )

names(df_tratada_cmed_pmc_10.24)

df_tratada_cmed_pmc_10.24 = df_tratada_cmed_pmc_10.24 |>
    rename(`PMVG_0%` = `PMVG 0%`,
           `PMVG_12%` = `PMVG 12%`,
           `PMVG_17%` = `PMVG 17%`,
           `PMVG_18%` = `PMVG 18%`,
           `PMVG_19%` = `PMVG 19%`,
           
           `PF_12% ALC` = `PF 12% ALC`,
           `PF_17.5%` = `PF 17.5%`,
           `PF_17.5% ALC` = `PF 17.5% ALC`,
           `PF_19% ALC` = `PF 19% ALC`,
           `PF_19.5%` = `PF 19.5%`,
           `PF_19.5% ALC` = `PF 19.5% ALC`,
           `PF_20%` = `PF 20%`,
           `PF_20% ALC` = `PF 20% ALC`,
           `PF_20.5%` = `PF 20.5%`,
           `PF_21%` = `PF 21%`,
           `PF_21% ALC` = `PF 21% ALC`,
           `PF_22%` = `PF 22%`,
           `PF_22% ALC` = `PF 22% ALC`,
           
           `PMC_SEM IMPOSTOS` = `PMC SEM IMPOSTO`,
           `PMC_12% ALC` = `PMC 12% ALC`,
           `PMC_17.5%` = `PMC 17.5%`,
           `PMC_17.5% ALC` = `PMC 17.5% ALC`,
           `PMC_19% ALC` = `PMC 19% ALC`,
           `PMC_19.5%` = `PMC 19.5%`,
           `PMC_19.5% ALC` = `PMC 19.5% ALC`,
           `PMC_20%` = `PMC 20%`,
           `PMC_20% ALC` = `PMC 20% ALC`,
           `PMC_20.5%` = `PMC 20.5%`,
           `PMC_21%` = `PMC 21%`,
           `PMC_21% ALC` = `PMC 21% ALC`,
           `PMC_22%` = `PMC 22%`,
           `PMC_22% ALC` = `PMC 22% ALC`,
           
           `CLASSE_TERAPEUTICA` = `CLASSE TERAPÊUTICA`,
           `ANALISE_RECURSAL` = `ANALISE_RECURSAL1`,
           `LISTA_CRÉDITO_PIS/COFINS` = `LISTA DE CONCESSÃO DE CRÉDITO TRIBUTÁRIO (PIS/COFINS)`,
           `COMERCIALIZACAO_2016` = `COMERCIALIZAÇÃO 2016`,
           `PF_SEM IMPOSTOS` = `PF SEM IMPOSTOS`,
           `EAN_2` = `EAN 2`,
           `EAN_3` = `EAN 3`,
           `COMERCIALIZACAO_2017` = `COMERCIALIZAÇÃO 2017`,
           `REGIME_PRECO` = `REGIME DE PREÇO`,
           `COMERCIALIZACAO_2018` = `COMERCIALIZAÇÃO 2018`,
           `ICMS_0%` = `ICMS 0%`,
           `COMERCIALIZACAO_2019` = `COMERCIALIZAÇÃO 2019`,
           `COMERCIALIZACAO_2020` = `COMERCIALIZAÇÃO 2020`,
           `COMERCIALIZACAO_2021` = `COMERCIALIZAÇÃO 2021`,
           `COMERCIALIZACAO_2022` = `COMERCIALIZAÇÃO 2022`,
           `DESTINACAO_COMERCIAL` = `DESTINAÇÃO COMERCIAL`,
           `CAP` = `CAP1`,
           `APRESENTACAO` = `APRESENTACAO1`,
           `SUBSTANCIA` = `SUBSTANCIA1`,
           `CONFAZ_87` = `CONFAZ_871`
           )

names(df_tratada_cmed_pmc_10.24)

pmc_ordenada = c(
  "ARQUIVO", "DATA_PUBLICADA", "DATA_ATUALIZADA",  "CNPJ", "LABORATORIO", 
  "CLASSE_TERAPEUTICA", "SUBSTANCIA", "PRODUTO", "APRESENTACAO",
  "REGISTRO", "CODIGO_GGREM", "EAN_1", "EAN_2", "EAN_3",
  "TIPO_PRODUTO", "TARJA", "RESTRICAO_HOSPITALAR",
  "LISTA_CRÉDITO_PIS/COFINS", "REGIME_PRECO", "ICMS_0%", "CAP", "CONFAZ_87",
  
  "PF_SEM IMPOSTOS", "PF_0%", "PF_12%", "PF_12% ALC", "PF_17%", "PF_17% ALC", "PF_17.5%", "PF_17.5% ALC", 
  "PF_18%", "PF_18% ALC", "PF_19%", "PF_19% ALC", "PF_19.5%", "PF_19.5% ALC",
  "PF_20%", "PF_20% ALC", "PF_20.5%", "PF_21%", "PF_21% ALC", "PF_22%", "PF_22% ALC",
  
  "PMC_SEM IMPOSTOS", "PMC_0%", "PMC_12%", "PMC_12% ALC", "PMC_17%", "PMC_17% ALC", "PMC_17.5%", "PMC_17.5% ALC", 
  "PMC_18%", "PMC_18% ALC", "PMC_19%", "PMC_19% ALC", "PMC_19.5%", "PMC_19.5% ALC",
  "PMC_20%", "PMC_20% ALC", "PMC_20.5%", "PMC_21%", "PMC_21% ALC", "PMC_22%", "PMC_22% ALC",
  
  "PMVG_0%", "PMVG_12%", "PMVG_17%", "PMVG_18%","PMVG_19%",
  
  "DESTINACAO_COMERCIAL", "ANALISE_RECURSAL",
  "COMERCIALIZACAO_2016", "COMERCIALIZACAO_2017", "COMERCIALIZACAO_2018",
  "COMERCIALIZACAO_2019", "COMERCIALIZACAO_2020", "COMERCIALIZACAO_2021",
  "COMERCIALIZACAO_2022", "ULTIMA_ALTERACAO"
)

df_tratada_cmed_pmc_10.24 <- df_tratada_cmed_pmc_10.24 %>%
  select(all_of(pmc_ordenada))

names(df_tratada_cmed_pmc_10.24)


### ___PMVG ----
names(df_cmed_pmvg_11.24)

colnames(df_cmed_pmvg_11.24) <- toupper(colnames(df_cmed_pmvg_11.24))
colnames(df_cmed_pmvg_11.24) <- stringr::str_trim(colnames(df_cmed_pmvg_11.24))
colnames(df_cmed_pmvg_11.24) <- gsub(",", ".", colnames(df_cmed_pmvg_11.24))
colnames(df_cmed_pmvg_11.24) <- make.unique(colnames(df_cmed_pmvg_11.24))

names(df_cmed_pmvg_11.24)

df_cmed_pmvg_11.24 <- df_cmed_pmvg_11.24 %>%
  mutate_all(as.character)

df_tratada_cmed_pmvg_11.24 = df_cmed_pmvg_11.24 |>
  mutate(`PF_0%` = coalesce(`PF 0%`,`PF0`),
         `PF_12%` = coalesce(`PF 12%`,`PF12`),
         `PF_17%` = coalesce(`PF 17%`,`PF17`),
         `PF_18%` = coalesce(`PF 18%`,`PF18`),
         `PF_19%` = coalesce(`PF 19%`,`PF19`),
         `PF_17% ALC`= coalesce(`PF 17% ZONA FRANCA DE MANAUS`,`PF 17% ALC`),
         `PF_18% ALC`= coalesce(`PF 18% ZONA FRANCA DE MANAUS`,`PF 18% ALC`),
         `PMVG_SEM IMPOSTOS` = coalesce(`PMVG SEM IMPOSTOS`,`PMVG SEM IMPOSTO`),
         `PMVG_0%` = coalesce(`PMVG 0%`,`PMVGC0`),
         `PMVG_12%` = coalesce(`PMVG 12%`,`PMVG12`),
         `PMVG_17%` = coalesce(`PMVG 17%`,`PMVG17`),
         `PMVG_18%` = coalesce(`PMVG 18%`,`PMVG18`),
         `PMVG_19%` = coalesce(`PMVG 19%`,`PMVG19`),
         `PMVG_17% ALC`= coalesce(`PMVG 17% ZONA FRANCA DE MANAUS`,`PMVG 17% ALC`),
         `PMVG_18% ALC`= coalesce(`PMVG 18% ZONA FRANCA DE MANAUS`,`PMVG 18% ALC`),
         `CAP1` = coalesce(`CAP`,`CAP.1`),
         `CODIGO_GGREM` = coalesce(`CÓDIGO GGREM`,`COD_GGREM`),
         `ANALISE_RECURSAL1` = coalesce(`ANÁLISE RECURSAL`,`ANALISE_RECURSAL`,`ANÁLISE RECURSAL CMED / PREÇO AJUSTADO POR DECISÃO JUDICIAL`),
         `APRESENTACAO1` = coalesce(APRESENTAÇÃO,`APRSENTAÇÃO`,`APRESENTACAO`),
         `TIPO_PRODUTO` = coalesce(`TIPO DE PRODUTO`,`TIPO DE PRODUTO (STATUS DO PRODUTO)`),
         `EAN_1` = coalesce(`EAN`,`EAN 1`),
         `SUBSTANCIA1` = coalesce(`PRINCÍPIO ATIVO`,`SUBSTÂNCIA`,`SUBSTANCIA`),
         `LABORATORIO` = coalesce(`RAZAO_SOCIAL`,`LABORATÓRIO`),
         `CONFAZ_871` = coalesce(`CONFAZ 87`,`CONFAZ_87`),
         `ULTIMA_ALTERACAO` = coalesce(`ULTIMA ALTERAÇÃO`,`ÚLTIMA ALTERAÇÃO`),
         `RESTRICAO_HOSPITALAR` = coalesce(`REST_HOSP`,`RESTRIÇÃO HOSPITALAR`)
  ) |>
  select(
    -c(`PF 0%`, `PF0`,
       `PF 12%`, `PF12`,
       `PF 17%`, `PF17`,
       `PF 18%`, `PF18`, 
       `PF 19%`, `PF19`,
       `PF 17% ZONA FRANCA DE MANAUS`,`PF 17% ALC`,
       `PF 18% ZONA FRANCA DE MANAUS`,`PF 18% ALC`,
       `PMVG SEM IMPOSTOS`, `PMVG SEM IMPOSTO`,
       `PMVG 0%`, `PMVGC0`,
       `PMVG 12%`, `PMVG12`,
       `PMVG 17%`, `PMVG17`,
       `PMVG 18%`, `PMVG18`,
       `PMVG 19%`, `PMVG19`,
       `PMVG 17% ZONA FRANCA DE MANAUS`,`PMVG 17% ALC`,
       `PMVG 18% ZONA FRANCA DE MANAUS`,`PMVG 18% ALC`,
       CAP, CAP.1,`CÓDIGO GGREM`, COD_GGREM,
       `ANÁLISE RECURSAL`,ANALISE_RECURSAL,`ANÁLISE RECURSAL CMED / PREÇO AJUSTADO POR DECISÃO JUDICIAL`,
       APRESENTAÇÃO,`APRSENTAÇÃO`,APRESENTACAO, `TIPO DE PRODUTO`,`TIPO DE PRODUTO (STATUS DO PRODUTO)`,
       EAN, `EAN 1`, `PRINCÍPIO ATIVO`, `SUBSTÂNCIA`, SUBSTANCIA,
       RAZAO_SOCIAL, LABORATÓRIO,
       `CONFAZ 87`,CONFAZ_87,
       `ULTIMA ALTERAÇÃO` , `ÚLTIMA ALTERAÇÃO`,
       REST_HOSP, `RESTRIÇÃO HOSPITALAR`
    )
  )

names(df_tratada_cmed_pmvg_11.24)

df_tratada_cmed_pmvg_11.24 = df_tratada_cmed_pmvg_11.24 |>
  rename(`PMC_0%` = `PMC 0%`,
         `PMC_12%` = `PMC 12%`,
         `PMC_17%` = `PMC 17%`,
         `PMC_17% ALC` = `PMC 17% ALC`,
         `PMC_17.5%` = `PMC 17.5%`,
         `PMC_17.5% ALC` = `PMC 17.5% ALC`,
         `PMC_18%` = `PMC 18%`,
         `PMC_18% ALC` = `PMC 18% ALC`,
         `PMC_20%` = `PMC 20%`,
         
         `PF_SEM IMPOSTOS` = `PF SEM IMPOSTOS`,
         `PF_12% ALC` = `PF 12% ALC`,
         `PF_17.5%` = `PF 17.5%`,
         `PF_17.5% ALC` = `PF 17.5% ALC`,
         `PF_19% ALC` = `PF 19% ALC`,
         `PF_19.5%` = `PF 19.5%`,
         `PF_19.5% ALC` = `PF 19.5% ALC`,
         `PF_20%` = `PF 20%`,
         `PF_20% ALC` = `PF 20% ALC`,
         `PF_20.5%` = `PF 20.5%2`,
         `PF_21%` = `PF 21%`,
         `PF_21% ALC` = `PF 21% ALC`,
         `PF_22%` = `PF 22%`,
         `PF_22% ALC` = `PF 22% ALC`,
         
         `PMVG_12% ALC` = `PMVG 12% ALC`,
         `PMVG_17.5%` = `PMVG 17.5%`,
         `PMVG_17.5% ALC` = `PMVG 17.5% ALC`,
         `PMVG_19% ALC` = `PMVG 19% ALC`,
         `PMVG_19.5%` = `PMVG 19.5%`,
         `PMVG_19.5% ALC` = `PMVG 19.5% ALC`,
         `PMVG_20%` = `PMVG 20%`,
         `PMVG_20% ALC` = `PMVG 20% ALC`,
         `PMVG_20.5%` = `PMVG 20.5%`,
         `PMVG_21%` = `PMVG 21%`,
         `PMVG_21% ALC` = `PMVG 21% ALC`,
         `PMVG_22%` = `PMVG 22%`,
         `PMVG_22% ALC` = `PMVG 22% ALC`,
         
         `CLASSE_TERAPEUTICA` = `CLASSE TERAPÊUTICA`,
         `ANALISE_RECURSAL` = `ANALISE_RECURSAL1`,
         `LISTA_CRÉDITO_PIS/COFINS` = `LISTA DE CONCESSÃO DE CRÉDITO TRIBUTÁRIO (PIS/COFINS)`,
         `COMERCIALIZACAO_2016` = `COMERCIALIZAÇÃO 2016`,
         `PF_SEM IMPOSTOS` = `PF SEM IMPOSTOS`,
         `EAN_2` = `EAN 2`,
         `EAN_3` = `EAN 3`,
         `COMERCIALIZACAO_2017` = `COMERCIALIZAÇÃO 2017`,
         `REGIME_PRECO` = `REGIME DE PREÇO`,
         `COMERCIALIZACAO_2018` = `COMERCIALIZAÇÃO 2018`,
         `ICMS_0%` = `ICMS 0%`,
         `COMERCIALIZACAO_2019` = `COMERCIALIZAÇÃO 2019`,
         `COMERCIALIZACAO_2020` = `COMERCIALIZAÇÃO 2020`,
         `COMERCIALIZACAO_2021` = `COMERCIALIZAÇÃO 2021`,
         `COMERCIALIZACAO_2022` = `COMERCIALIZAÇÃO 2022`,
         `DESTINACAO_COMERCIAL` = `DESTINAÇÃO COMERCIAL`,
         `CAP` = `CAP1`,
         `APRESENTACAO` = `APRESENTACAO1`,
         `SUBSTANCIA` = `SUBSTANCIA1`,
         `CONFAZ_87` = `CONFAZ_871`
  )

names(df_tratada_cmed_pmvg_11.24)

pmvg_ordenada = c(
  "ARQUIVO", "DATA_PUBLICADA", "DATA_ATUALIZADA",  "CNPJ","LABORATORIO", 
  "CLASSE_TERAPEUTICA", "SUBSTANCIA", "PRODUTO", "APRESENTACAO",
  "REGISTRO", "CODIGO_GGREM", "EAN_1", "EAN_2", "EAN_3",
  "TIPO_PRODUTO", "TARJA", "RESTRICAO_HOSPITALAR",
  "LISTA_CRÉDITO_PIS/COFINS", "REGIME_PRECO", "ICMS_0%", "CAP", "CONFAZ_87",
  
  "PF_SEM IMPOSTOS", "PF_0%", "PF_12%", "PF_12% ALC", "PF_17%", "PF_17% ALC", "PF_17.5%", "PF_17.5% ALC", 
  "PF_18%", "PF_18% ALC", "PF_19%", "PF_19% ALC", "PF_19.5%", "PF_19.5% ALC",
  "PF_20%", "PF_20% ALC", "PF_20.5%", "PF_21%", "PF_21% ALC", "PF_22%", "PF_22% ALC",
  
  "PMC_0%", "PMC_12%", "PMC_17%","PMC_17% ALC","PMC_17.5%", "PMC_17.5% ALC",
  "PMC_18%","PMC_18% ALC", "PMC_20%",
  
    "PMVG_SEM IMPOSTOS", "PMVG_0%", "PMVG_12%", "PMVG_12% ALC", "PMVG_17%", "PMVG_17% ALC", "PMVG_17.5%", "PMVG_17.5% ALC", 
  "PMVG_18%", "PMVG_18% ALC", "PMVG_19%", "PMVG_19% ALC", "PMVG_19.5%", "PMVG_19.5% ALC",
  "PMVG_20%", "PMVG_20% ALC", "PMVG_20.5%", "PMVG_21%", "PMVG_21% ALC", "PMVG_22%", "PMVG_22% ALC",
 
  "DESTINACAO_COMERCIAL", "ANALISE_RECURSAL",
  "COMERCIALIZACAO_2016", "COMERCIALIZACAO_2017", "COMERCIALIZACAO_2018",
  "COMERCIALIZACAO_2019", "COMERCIALIZACAO_2020", "COMERCIALIZACAO_2021",
  "COMERCIALIZACAO_2022", "ULTIMA_ALTERACAO"
)

df_tratada_cmed_pmvg_11.24 <- df_tratada_cmed_pmvg_11.24 %>%
  select(all_of(pmvg_ordenada))

names(df_tratada_cmed_pmvg_11.24)


# ¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨----
## 2.5. Criar CMED Agrupada ----

cmed_10.24 = bind_rows(
  df_tratada_cmed_pmc_10.24, df_tratada_cmed_pmvg_11.24
)

names(cmed_10.24)

cmed_ordenada = c(
  "ARQUIVO", "DATA_PUBLICADA", "DATA_ATUALIZADA", "ANO_MES", "CNPJ","LABORATORIO", 
  "CLASSE_TERAPEUTICA", "SUBSTANCIA", "PRODUTO", "APRESENTACAO",
  "REGISTRO", "CODIGO_GGREM", "EAN_1", "EAN_2", "EAN_3",
  "TIPO_PRODUTO", "TARJA", "RESTRICAO_HOSPITALAR",
  "LISTA_CRÉDITO_PIS/COFINS", "REGIME_PRECO", "ICMS_0%", "CAP", "CONFAZ_87",
  
  "PF_SEM IMPOSTOS", "PF_0%", "PF_12%", "PF_12% ALC", "PF_17%", "PF_17% ALC", "PF_17.5%", "PF_17.5% ALC", 
  "PF_18%", "PF_18% ALC", "PF_19%", "PF_19% ALC", "PF_19.5%", "PF_19.5% ALC",
  "PF_20%", "PF_20% ALC", "PF_20.5%", "PF_21%", "PF_21% ALC", "PF_22%", "PF_22% ALC",
  
  "PMC_SEM IMPOSTOS", "PMC_0%", "PMC_12%", "PMC_12% ALC", "PMC_17%", "PMC_17% ALC", "PMC_17.5%", "PMC_17.5% ALC", 
  "PMC_18%", "PMC_18% ALC", "PMC_19%", "PMC_19% ALC", "PMC_19.5%", "PMC_19.5% ALC",
  "PMC_20%", "PMC_20% ALC", "PMC_20.5%", "PMC_21%", "PMC_21% ALC", "PMC_22%", "PMC_22% ALC",
  
  "PMVG_SEM IMPOSTOS", "PMVG_0%", "PMVG_12%", "PMVG_12% ALC", "PMVG_17%", "PMVG_17% ALC", "PMVG_17.5%", "PMVG_17.5% ALC", 
  "PMVG_18%", "PMVG_18% ALC", "PMVG_19%", "PMVG_19% ALC", "PMVG_19.5%", "PMVG_19.5% ALC",
  "PMVG_20%", "PMVG_20% ALC", "PMVG_20.5%", "PMVG_21%", "PMVG_21% ALC", "PMVG_22%", "PMVG_22% ALC",
  
  "DESTINACAO_COMERCIAL", "ANALISE_RECURSAL",
  "COMERCIALIZACAO_2016", "COMERCIALIZACAO_2017", "COMERCIALIZACAO_2018",
  "COMERCIALIZACAO_2019", "COMERCIALIZACAO_2020", "COMERCIALIZACAO_2021",
  "COMERCIALIZACAO_2022", "ULTIMA_ALTERACAO"
)

# Reorganizar o DataFrame de acordo com a nova lista de colunas completas
cmed_10.24_tratada <- cmed_10.24_tratada  %>%
  select(all_of(cmed_ordenada))
  
names(cmed_10.24)


#### ___Completar faltantes ----
sort(unique(cmed_10.24_tratada$ARQUIVO))

cmed_10.24_tratada = cmed_10.24 |>
  filter(!ARQUIVO %in% c("2024.12.05_pmvg - Copia.xls",
                         "2023.12.09_pmvg - Copia.xls",
                         "2022.12.13_pmvg - Copia.xls"))

unique(cmed_10.24$ARQUIVO)
unique(cmed_10.24$DATA_PUBLICADA)

missings_2012.02 = cmed_10.24_tratada |>
  filter(DATA_PUBLICADA == "14/03/2012")
missings_2012.02 = missings_2012.02 |>
  mutate(
    DATA_PUBLICADA = "01/02/2012",
    DATA_ATUALIZADA = NA,
    ARQUIVO = str_replace(ARQUIVO, "^2012\\.03\\.14", "2012.02.01")
  )
unique(missings_2012.02$ARQUIVO)
unique(missings_2012.02$DATA_PUBLICADA)

missings_2013.08 = cmed_10.24_tratada |>
  filter(DATA_PUBLICADA == "11/09/2013")
missings_2013.08 = missings_2013.08 |>
  mutate(
    DATA_PUBLICADA = "01/08/2013",
    DATA_ATUALIZADA = NA,
    ARQUIVO = str_replace(ARQUIVO, "^2013\\.09\\.11", "2013.08.01")
  )
unique(missings_2013.08$ARQUIVO)
unique(missings_2013.08$DATA_PUBLICADA)

missings_2013.12 = cmed_10.24_tratada |>
  filter(DATA_PUBLICADA == "08/01/2014")
missings_2013.12 = missings_2013.12 |>
  mutate(
    DATA_PUBLICADA = "01/12/2013",
    DATA_ATUALIZADA = NA,
    ARQUIVO = str_replace(ARQUIVO, "^2014\\.01\\.08", "2013.12.01")
  )
unique(missings_2013.12$ARQUIVO)
unique(missings_2013.12$DATA_PUBLICADA)

missings_2015.10 = cmed_10.24_tratada |>
  filter(DATA_PUBLICADA == "20/11/2015")
missings_2015.10 = missings_2015.10 |>
  mutate(
    DATA_PUBLICADA = "01/10/2015",
    DATA_ATUALIZADA = NA,
    ARQUIVO = str_replace(ARQUIVO, "^2015\\.11\\.20", "2015.10.01")
  )
unique(missings_2015.10$ARQUIVO)
unique(missings_2015.10$DATA_PUBLICADA)

cmed_10.24_tratada = cmed_10.24_tratada |>
  filter(!ARQUIVO == "2019.12.10_pmvg.xls")

missings_2019.12 = cmed_10.24_tratada |>
  filter(ARQUIVO == "2020.01.07_pmvg.xls")

missings_2019.12 = missings_2019.12 |>
  mutate(
    DATA_PUBLICADA = as.Date("2019-12-10"),
    DATA_ATUALIZADA = NA,
    ANO_MES = format(DATA_PUBLICADA, "%Y-%m"),
    ARQUIVO = str_replace(ARQUIVO, "^2020\\.01\\.07", "2019.12.10")
  )
unique(missings_2019.12$ARQUIVO)
unique(missings_2019.12$DATA_PUBLICADA)

cmed_10.24_tratada = bind_rows(cmed_10.24_tratada,
                               missings_2019.12)

cmed_10.24_tratada <- padronizar_conflitos(cmed_10.24_tratada)
missings_2012.02 <- padronizar_conflitos(missings_2012.02)
missings_2013.08 <- padronizar_conflitos(missings_2013.08)
missings_2013.12 <- padronizar_conflitos(missings_2013.12)
missings_2015.10 <- padronizar_conflitos(missings_2015.10)

cmed_10.24_tratada = bind_rows(cmed_10.24_tratada,
                               missings_2012.02, missings_2013.08,
                               missings_2013.12,missings_2015.10) 

cmed_10.24_tratada = cmed_10.24_tratada |>
  arrange(ARQUIVO)

unique(cmed_10.24_tratada$DATA_PUBLICADA)

nome_arquivo_cmed <- "cmed_10.24_tratada.parquet"
dir.parquet_cmed_10.24 <- file.path(caminho_draft, nome_arquivo_cmed)
write_parquet(cmed_10.24_tratada, sink = dir.parquet_cmed_10.24)
cmed_10.24_tratada <- read_parquet(dir.parquet_cmed_10.24)

names(cmed_10.24_tratada)

# ___________________ ----
# 3. Tratar Dados ----

cmed_10.24_1 = cmed_10.24_tratada #backup
# cmed_10.24_tratada = cmed_10.24_1

# Checagem de nulos
nas_cmed_10.24 <- cmed_10.24_tratada %>%
  summarise(across(everything(), ~ sum(is.na(.)))) %>%
  pivot_longer(cols = everything(), names_to = "coluna", values_to = "0_nulos") %>%
  arrange(desc(`0_nulos`))
print(nas_cmed_10.24, n = Inf)


## 3.1. Tratar Datas ----
cmed_10.24_tratada <- cmed_10.24_tratada %>%
  mutate(
    DATA_PUBLICADA = dmy(DATA_PUBLICADA),
    DATA_ATUALIZADA = dmy(DATA_ATUALIZADA),
    ANO_MES = format(DATA_PUBLICADA, "%Y-%m"))

names(cmed_10.24_tratada)
summary(cmed_10.24_tratada)

# ¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨----
## 3.2. Tratar Numéricos ----

tratar_valores <- function(valor) {
  valor <- stringr::str_trim(valor)
  valor <- ifelse(str_detect(valor, ","), 
                  str_remove_all(valor, "\\."),
                  valor)
  valor <- str_replace_all(valor, ",", ".")
  valor <- suppressWarnings(as.numeric(valor))
  return(valor)
}

cmed_10.24_tratada <- cmed_10.24_tratada %>%
  mutate(across(
    matches("^(PF|PMC|PMVG)"), 
    ~ tratar_valores(.)
  ))

summary(cmed_10.24_tratada)
names(cmed_10.24_tratada)

# ¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨----
## 3.3. Tratar Textos----

### 3.3.1. APRESENTAÇÃO ----
sort(unique(cmed_10.24_tratada$`APRESENTACAO`))

cat("Nulos em APRESENTACAO:", sum(is.na(cmed_10.24_tratada$APRESENTACAO)), "\n")

cmed_10.24_tratada <- cmed_10.24_tratada %>%
  mutate(
    `APRESENTACAO` = toupper(`APRESENTACAO`),
    `APRESENTACAO` = stringi::stri_trans_general(`APRESENTACAO`, "Latin-ASCII"))
sort(unique(cmed_10.24_tratada$`APRESENTACAO`))

cmed_10.24_tratada <- cmed_10.24_tratada %>%
  filter(!is.na(APRESENTACAO))

cat("Nulos em APRESENTACAO:", sum(is.na(cmed_10.24_tratada$APRESENTACAO)), "\n")


### 3.3.2. PRODUTO ----
unique(cmed_10.24_tratada$`PRODUTO`)
cat("Nulos em PRODUTO:", sum(is.na(cmed_10.24_tratada$PRODUTO)), "\n")

cmed_10.24_tratada <- cmed_10.24_tratada %>%
  mutate(
    `PRODUTO` = stringr::str_trim(`PRODUTO`),
    `PRODUTO` = toupper(`PRODUTO`))

#### ___corrigir NAs ----
cmed_10.24_tratada <- cmed_10.24_tratada %>%
  group_by(CODIGO_GGREM) %>%
  mutate(PRODUTO = ifelse(is.na(PRODUTO) | PRODUTO == "", first(na.omit(PRODUTO)), PRODUTO)) %>%
  ungroup()
cat("Nulos em PRODUTO:", sum(is.na(cmed_10.24_tratada$PRODUTO)), "\n")


### 3.3.3. CODIGO_GGREM ----
unique(cmed_10.24_tratada$`CODIGO_GGREM`)
cat("Nulos em CODIGO_GGREM:", sum(is.na(cmed_10.24_tratada$CODIGO_GGREM)), "\n")
cmed_10.24_tratada <- cmed_10.24_tratada %>%
  mutate(
    `CODIGO_GGREM` = stringr::str_trim(`CODIGO_GGREM`)
  )

### 3.3.4. REGISTRO ANVISA ----
unique(cmed_10.24_tratada$`REGISTRO`)

padronizar_registro_anvisa <- function(registros) {
  registros <- toupper(registros)
  registros <- gsub("[^0-9]", "", registros)
  registros <- trimws(registros)
  registros <- ifelse(registros == "" | registros == "NA" | registros == "NAOINFORMADO", NA, registros)
  registros <- ifelse(!is.na(registros), sprintf("%013s", registros), registros)
  registros <- gsub(" ", "0", registros)
  return(registros)
}

cmed_10.24_tratada$`REGISTRO` = padronizar_registro_anvisa(cmed_10.24_tratada$`REGISTRO`)


#### ___corrigir NAs----

cat("Nulos em REGISTRO:", sum(is.na(cmed_10.24_tratada$REGISTRO)), "\n")

#Investigação de Padrões de Nulos
# Objetivo: Verificar se os valores nulos ocorrem em registros específicos ou aleatoriamente.

cmed_10.24_tratada <- cmed_10.24_tratada %>%
  group_by(PRODUTO, APRESENTACAO) %>%
  mutate(REGISTRO = ifelse(is.na(REGISTRO) | REGISTRO == "", first(na.omit(REGISTRO)), REGISTRO)) %>%
  ungroup()
cat("Nulos em REGISTRO:", sum(is.na(cmed_10.24_tratada$REGISTRO)), "\n")

cmed_10.24_tratada <- cmed_10.24_tratada %>%
  group_by(CODIGO_GGREM) %>%
  mutate(REGISTRO = ifelse(is.na(REGISTRO) | REGISTRO == "", first(na.omit(REGISTRO)), REGISTRO)) %>%
  ungroup()

cat("Nulos em REGISTRO:", sum(is.na(cmed_10.24_tratada$REGISTRO)), "\n")

nas_registro <- cmed_10.24_tratada %>%
  filter(is.na(REGISTRO))

names(cmed_10.24_tratada)


### 3.3.5. SUBSTANCIA ----

unique(cmed_10.24_tratada$`SUBSTANCIA`)

cmed_10.24_tratada <- cmed_10.24_tratada %>%
  mutate(
    `SUBSTANCIA` = stringr::str_trim(`SUBSTANCIA`),
    `SUBSTANCIA` = toupper(`SUBSTANCIA`)
  )

unique(cmed_10.24_tratada$`SUBSTANCIA`)


#### ___corrigir NAs----
cat("Nulos em SUBSTANCIA:", sum(is.na(cmed_10.24_tratada$SUBSTANCIA)), "\n")

cmed_10.24_tratada <- cmed_10.24_tratada %>%
  group_by(REGISTRO) %>%
  mutate(SUBSTANCIA = ifelse(is.na(SUBSTANCIA) | SUBSTANCIA == "", first(na.omit(SUBSTANCIA)), SUBSTANCIA)) %>%
  ungroup()
cat("Nulos em SUBSTANCIA:", sum(is.na(cmed_10.24_tratada$SUBSTANCIA)), "\n")


### 3.3.6. LABORATÓRIO ----
unique(cmed_10.24_tratada$`LABORATORIO`)
cat("Nulos em LABORATORIO:", sum(is.na(cmed_10.24_tratada$LABORATORIO)), "\n")

padronizar_nomes <- function(nomes) {
  nomes <- toupper(nomes)
  nomes <- stringi::stri_trans_general(nomes, "Latin-ASCII")
  nomes <- gsub("[^A-Z0-9/%& -]", "", nomes)
  nomes <- gsub("\\s+", " ", nomes)
  nomes <- stringr::str_trim(nomes)
  return(nomes)
}

cmed_10.24_tratada$`LABORATORIO` = padronizar_nomes(cmed_10.24_tratada$`LABORATORIO`)
unique(cmed_10.24_tratada$`LABORATORIO`)


### 3.3.6. CNPJ ----
unique(cmed_10.24_tratada$`CNPJ`)

cmed_10.24_tratada <- cmed_10.24_tratada %>%
  mutate(CNPJ = ifelse(!is.na(CNPJ) & nchar(CNPJ) == 14,
                       str_replace_all(CNPJ, "(\\d{2})(\\d{3})(\\d{3})(\\d{4})(\\d{2})", "\\1.\\2.\\3/\\4-\\5"),
                       CNPJ)) %>%
  mutate(CNPJ = ifelse(!is.na(CNPJ) & nchar(CNPJ) != 18,
                       str_replace_all(CNPJ, "[^0-9]", ""),
                       CNPJ)) %>%
  mutate(CNPJ = ifelse(!is.na(CNPJ) & nchar(CNPJ) == 14,
                       str_replace_all(CNPJ, "(\\d{2})(\\d{3})(\\d{3})(\\d{4})(\\d{2})", "\\1.\\2.\\3/\\4-\\5"),
                       CNPJ))

unique(cmed_10.24_tratada$`CNPJ`)

cnpj = data.frame(cnpj = unique(cmed_10.24_tratada$`CNPJ`))
rm(cnpj)

#### ___corrigir NAs ----

cat("Nulos em CNPJ:", sum(is.na(cmed_10.24_tratada$CNPJ)), "\n")

cmed_10.24_tratada <- cmed_10.24_tratada %>%
  group_by(LABORATORIO) %>%
  mutate(CNPJ = ifelse(is.na(CNPJ) | CNPJ == "", first(na.omit(CNPJ)), CNPJ)) %>%
  ungroup()

cat("Nulos em CNPJ:", sum(is.na(cmed_10.24_tratada$CNPJ)), "\n")


### 3.3.7. ANALISE_RECURSAL----
cmed_10.24_tratada <- cmed_10.24_tratada %>%
  mutate(
    ANALISE_RECURSAL = str_replace_all(ANALISE_RECURSAL, "\\bAR\\b", "(AR)"),
    ANALISE_RECURSAL = str_replace_all(ANALISE_RECURSAL, "\\(\\(AR\\)\\)", "(AR)"),
    ANALISE_RECURSAL = str_replace_all(ANALISE_RECURSAL, "\\(\\(\\(AR\\)\\)\\)", "(AR)")
  ) 
sort(unique(cmed_10.24_tratada$ANALISE_RECURSAL))

#### ___corrigir NAs----
cat("Nulos em ANALISE_RECURSAL:", sum(is.na(cmed_10.24_tratada$ANALISE_RECURSAL)), "\n")

cmed_10.24_tratada <- cmed_10.24_tratada %>%
  group_by(REGISTRO) %>%
  mutate(ANALISE_RECURSAL = ifelse(is.na(ANALISE_RECURSAL) | ANALISE_RECURSAL == "", first(na.omit(ANALISE_RECURSAL)), ANALISE_RECURSAL)) %>%
  ungroup()
cat("Nulos em ANALISE_RECURSAL:", sum(is.na(cmed_10.24_tratada$ANALISE_RECURSAL)), "\n")
cmed_10.24_tratada <- cmed_10.24_tratada %>%
  group_by(CODIGO_GGREM) %>%
  mutate(ANALISE_RECURSAL = ifelse(is.na(ANALISE_RECURSAL) | ANALISE_RECURSAL == "", first(na.omit(ANALISE_RECURSAL)), ANALISE_RECURSAL)) %>%
  ungroup()
cat("Nulos em ANALISE_RECURSAL:", sum(is.na(cmed_10.24_tratada$ANALISE_RECURSAL)), "\n")
cmed_10.24_tratada <- cmed_10.24_tratada %>%
  group_by(PRODUTO,APRESENTACAO) %>%
  mutate(ANALISE_RECURSAL = ifelse(is.na(ANALISE_RECURSAL) | ANALISE_RECURSAL == "", first(na.omit(ANALISE_RECURSAL)), ANALISE_RECURSAL)) %>%
  ungroup()
cat("Nulos em ANALISE_RECURSAL:", sum(is.na(cmed_10.24_tratada$ANALISE_RECURSAL)), "\n")

### Formatar ----
names(cmed_10.24_tratada)
cmed_10.24_tratada <- cmed_10.24_tratada %>%
  mutate(
    `ARQUIVO` = as.character(ARQUIVO),
    `SUBSTANCIA` = as.character(SUBSTANCIA),
    `PRODUTO` = as.character(PRODUTO),
    `APRESENTACAO` = as.character(APRESENTACAO),
    `CNPJ` = as.character(CNPJ),
    `LABORATORIO` = as.character(LABORATORIO),
    `CODIGO_GGREM` = as.character(`CODIGO_GGREM`),
    `REGISTRO` = as.character(REGISTRO),
    `EAN_1` = as.character(`EAN_1`),
    `EAN_2` = as.character(`EAN_2`),
    `EAN_3` = as.character(`EAN_3`),
    `COMERCIALIZACAO_2016` = as.character(`COMERCIALIZACAO_2016`),
    `COMERCIALIZACAO_2017` = as.character(`COMERCIALIZACAO_2017`),
    `COMERCIALIZACAO_2018` = as.character(`COMERCIALIZACAO_2018`),
    `COMERCIALIZACAO_2019` = as.character(`COMERCIALIZACAO_2019`),
    `COMERCIALIZACAO_2020` = as.character(`COMERCIALIZACAO_2020`),
    `COMERCIALIZACAO_2021` = as.character(`COMERCIALIZACAO_2021`),
    `COMERCIALIZACAO_2022` = as.character(`COMERCIALIZACAO_2022`),
    `ANALISE_RECURSAL` = as.character(`ANALISE_RECURSAL`),
    `DESTINACAO_COMERCIAL` = as.character(`DESTINACAO_COMERCIAL`)
  )

# ¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨----
## 3.4. Tratar Fatores----

### 3.4.1. CLASSE TERAPÊUTICA ----
names(cmed_10.24_tratada)
sort(unique(cmed_10.24_tratada$`CLASSE_TERAPEUTICA`))

cmed_10.24_tratada <- cmed_10.24_tratada |>
  mutate(
    `CLASSE_TERAPEUTICA` = stringr::str_trim(`CLASSE_TERAPEUTICA`),
    `CLASSE_TERAPEUTICA` = toupper(`CLASSE_TERAPEUTICA`))

# Dicionário para descritivo baseado na primeira letra do código
classe_dict <- c(
  A = "ALIMENTARY TRACT AND METABOLISM",
  B = "BLOOD AND BLOOD FORMING ORGANS",
  C = "CARDIOVASCULAR SYSTEM",
  D = "DERMATOLOGICALS",
  F = "PHYTOTHERAPEUTICS",
  G = "GENITO-URINARY SYSTEM AND SEX HORMONES",
  H = "SYSTEMIC HORMONAL PREPARATIONS, EXCL. SEX HORMONES AND INSULINS",
  J = "ANTIINFECTIVES FOR SYSTEMIC USE",
  K = "SOLUTIONS (NON-ATC)",
  L = "ANTINEOPLASTIC AND IMMUNOMODULATING AGENTS",
  M = "MUSCULO-SKELETAL SYSTEM",
  N = "NERVOUS SYSTEM",
  P = "ANTIPARASITIC PRODUCTS, INSECTICIDES AND REPELLENTS",
  R = "RESPIRATORY SYSTEM",
  S = "SENSORY ORGANS",
  T = "DIAGNOSTIC PRODUCTS (NON-ATC)",
  V = "VARIOUS"
)

#### ___corrigir NAs ----
cat("Nulos em CLASSE_TERAPEUTICA:", sum(is.na(cmed_10.24_tratada$CLASSE_TERAPEUTICA)), "\n")

cmed_10.24_tratada <- cmed_10.24_tratada %>%
  group_by(REGISTRO) %>%
  mutate(
    `CLASSE_TERAPEUTICA` = ifelse(`CLASSE_TERAPEUTICA` == "" | is.na(`CLASSE_TERAPEUTICA`), first(na.omit(`CLASSE_TERAPEUTICA`)), `CLASSE_TERAPEUTICA`)
  ) %>%
  ungroup()
cat("Nulos em CLASSE_TERAPEUTICA:", sum(is.na(cmed_10.24_tratada$CLASSE_TERAPEUTICA)), "\n")

# Processar a tabela
cmed_10.24_tratada <- cmed_10.24_tratada %>%
  # Criar colunas separadas
  mutate(
    CLASSE_TERAPEUTICA_CODIGO = str_extract(CLASSE_TERAPEUTICA, "^[A-Z0-9]+"),
    CLASSE_TERAPEUTICA_DESCRICAO = str_extract(CLASSE_TERAPEUTICA, "(?<=- ).*"),
    CLASSE_PRINCIPAL_CODIGO = substr(CLASSE_TERAPEUTICA_CODIGO, 1, 1),
    CLASSE_PRINCIPAL_DESCRICAO = classe_dict[substr(CLASSE_TERAPEUTICA_CODIGO, 1, 1)]
  )
sort(unique(cmed_10.24_tratada$CLASSE_PRINCIPAL_CODIGO))

cat("Nulos em CLASSE_TERAPEUTICA:", sum(is.na(cmed_10.24_tratada$CLASSE_TERAPEUTICA)), "\n")
cat("Nulos em CLASSE_TERAPEUTICA_CODIGO:", sum(is.na(cmed_10.24_tratada$CLASSE_TERAPEUTICA_CODIGO)), "\n")
cat("Nulos em CLASSE_TERAPEUTICA_DESCRICAO:", sum(is.na(cmed_10.24_tratada$CLASSE_TERAPEUTICA_DESCRICAO)), "\n")
cat("Nulos em CLASSE_PRINCIPAL_CODIGO:", sum(is.na(cmed_10.24_tratada$CLASSE_PRINCIPAL_CODIGO)), "\n")
cat("Nulos em CLASSE_PRINCIPAL_DESCRICAO:", sum(is.na(cmed_10.24_tratada$CLASSE_PRINCIPAL_DESCRICAO)), "\n")

nas1_cmed_10.24 <- cmed_10.24_tratada %>%
  summarise(across(everything(), ~ sum(is.na(.)))) %>%
  pivot_longer(cols = everything(), names_to = "coluna", values_to = "na_count")
print(nas_cmed_10.24, n = Inf)
print(nas1_cmed_10.24, n = Inf)


### 3.4.2. TIPO DE PRODUTO ----
sort(unique(cmed_10.24_tratada$`TIPO_PRODUTO`))

cmed_10.24_tratada <- cmed_10.24_tratada %>%
  mutate(
    `TIPO_PRODUTO` = stringr::str_trim(`TIPO_PRODUTO`),
    `TIPO_PRODUTO` = case_when(
      # Agrupando Biológicos
      `TIPO_PRODUTO` %in% c("Biológico", "Biológicos") ~ "Biológico",
      # Agrupando Biológico Novo
      `TIPO_PRODUTO` %in% c("Biológico Novo", "Biológico Novo(Patente)", "Biológicos(Patente)") ~ "Biológico Novo",
      # Agrupando Genéricos
      `TIPO_PRODUTO` %in% c("Genérico", "Genérico (Referência)", "Genérico(Referência)", 
                                                   "Genérico(Patente)", "Genérico(Alopático)", 
                                                   "Genérico(Referência e Patente)") ~ "Genérico",
      # Agrupando Similares
      `TIPO_PRODUTO` %in% c("Similar", "Similar (Referência)", "Similar(Referência)", 
                                                   "Similar(Patente)", "Similar(Referência e Patente)", 
                                                   "Similar(Alopático)", "Similar Referência e") ~ "Similar",
      # Agrupando Novos
      `TIPO_PRODUTO` %in% c("Novo", "Novo (Referência)", "Novo(Referência)", 
                                                   "Novo(Patente)", "Novo (Referência e Patente)",
                                                   "Novo(Referência e Patente)", "Novo Referência e") ~ "Novo",
      # Agrupando Específicos
      `TIPO_PRODUTO` %in% c("Específico", "Específico (Referência)", "Específico(Referência e Patente)",
                                                   "Específico(Patente)", "Específico Referência e", "Específico(Referência)") ~ "Específico",
      # Agrupando outros valores
      `TIPO_PRODUTO` %in% c("Radiofármaco", "Fitoterápico", "Produto de Terapia Avançada") ~ `TIPO_PRODUTO`,
      # Tratamento de valores inválidos
      `TIPO_PRODUTO` %in% c("-", "-(*)", "0", NA) ~ NA_character_,
      TRUE ~ `TIPO_PRODUTO`
    )
  )

cmed_10.24_tratada <- cmed_10.24_tratada %>%
  mutate(
    `TIPO_PRODUTO` = toupper(`TIPO_PRODUTO`)
  )
sort(unique(cmed_10.24_tratada$`TIPO_PRODUTO`))


#### ___corrigir NAs ----
cat("Nulos em TIPO_PRODUTO:", sum(is.na(cmed_10.24_tratada$TIPO_PRODUTO)), "\n")

cmed_10.24_tratada <- cmed_10.24_tratada %>%
  group_by(REGISTRO) %>%
  mutate(
    `TIPO_PRODUTO` = ifelse(`TIPO_PRODUTO` == "" | is.na(`TIPO_PRODUTO`), first(na.omit(`TIPO_PRODUTO`)), `TIPO_PRODUTO`)
  ) %>%
  ungroup()

cat("Nulos em TIPO_PRODUTO:", sum(is.na(cmed_10.24_tratada$TIPO_PRODUTO)), "\n")

cmed_10.24_tratada <- cmed_10.24_tratada %>%
  group_by(CODIGO_GGREM) %>%
  mutate(
    `TIPO_PRODUTO` = ifelse(`TIPO_PRODUTO` == "" | is.na(`TIPO_PRODUTO`), first(na.omit(`TIPO_PRODUTO`)), `TIPO_PRODUTO`)
  ) %>%
  ungroup()

cat("Nulos em TIPO_PRODUTO:", sum(is.na(cmed_10.24_tratada$TIPO_PRODUTO)), "\n")

cmed_10.24_tratada <- cmed_10.24_tratada %>%
  group_by(PRODUTO,LABORATORIO) %>%
  mutate(
    `TIPO_PRODUTO` = ifelse(`TIPO_PRODUTO` == "" | is.na(`TIPO_PRODUTO`), first(na.omit(`TIPO_PRODUTO`)), `TIPO_PRODUTO`)
  ) %>%
  ungroup()

cat("Nulos em TIPO_PRODUTO:", sum(is.na(cmed_10.24_tratada$TIPO_PRODUTO)), "\n")


### 3.4.3. TARJA ----
sort(unique(cmed_10.24_tratada$`TARJA`))

cmed_10.24_tratada <- cmed_10.24_tratada %>%
  mutate(
    `TARJA` = stringr::str_trim(`TARJA`),
    `TARJA` = case_when(
      `TARJA` %in% c("Tarja Vermelha", "Tarja  Vermelha", "Tarja Vermelha (*)", 
                     "Tarja Vermelha(*)", "Tarja Vermelha (**)", "Tarja Vermelha sob restrição") ~ "Tarja Vermelha",
      `TARJA` %in% c("Tarja Preta", "Tarja  Preta", "Tarja Preta(*)", 
                     "Tarja Preta (*)", "Tarja Preta (**)", "Tarja Preta sob restrição") ~ "Tarja Preta",
      `TARJA` %in% c("Venda Livre", "Tarja Venda Livre", "Venda Livre/Sem Tarja (*)", 
                     "Não", "Venda Liberada (**)", "Venda Livre", "Sem Tarja(*)", 
                     "Tarja Sem Tarja", "Tarja Venda Livre/Sem Tarja (*)") ~ "MIP",
      `TARJA` %in% c("Tarja -(*)", "Tarja - (*)", "Tarja", "Tarja (*)", "Sim") ~ "Tarja Não Especificada",
      `TARJA` %in% c("-", "- (*)", "Informação Indisponível nos Registros da Anvisa", 
                     "Sem Informações de tarja (*)", "Sem informações de tarja (*)") ~ NA_character_,
      TRUE ~ `TARJA`
    )
  )

cmed_10.24_tratada <- cmed_10.24_tratada |>
  mutate(
    `TARJA` = toupper(`TARJA`))

sort(unique(cmed_10.24_tratada$`TARJA`))

names(cmed_10.24_tratada)

#### ___corrigir NAs ----
cat("Nulos em TARJA:", sum(is.na(cmed_10.24_tratada$TARJA)), "\n")

cmed_10.24_tratada <- cmed_10.24_tratada %>%
  group_by(REGISTRO) %>%
  mutate(
    `TARJA` = ifelse(`TARJA` == "" | is.na(`TARJA`), first(na.omit(`TARJA`)), `TARJA`)
  ) %>%
  ungroup()

cat("Nulos em TARJA:", sum(is.na(cmed_10.24_tratada$TARJA)), "\n")

cmed_10.24_tratada <- cmed_10.24_tratada %>%
  group_by(CODIGO_GGREM) %>%
  mutate(
    `TARJA` = ifelse(`TARJA` == "" | is.na(`TARJA`), first(na.omit(`TARJA`)), `TARJA`)
  ) %>%
  ungroup()
cat("Nulos em TARJA:", sum(is.na(cmed_10.24_tratada$TARJA)), "\n")

cmed_10.24_tratada <- cmed_10.24_tratada %>%
  group_by(PRODUTO,APRESENTACAO) %>%
  mutate(
    `TARJA` = ifelse(`TARJA` == "" | is.na(`TARJA`), first(na.omit(`TARJA`)), `TARJA`)
  ) %>%
  ungroup()

cat("Nulos em TARJA:", sum(is.na(cmed_10.24_tratada$TARJA)), "\n")


### 3.4.4. DESTINAÇÃO COMERCIAL ----
sort(unique(cmed_10.24_tratada$`DESTINACAO_COMERCIAL`))

cmed_10.24_tratada <- cmed_10.24_tratada %>%
  mutate(
    `DESTINACAO_COMERCIAL` = toupper(`DESTINACAO_COMERCIAL`))
sort(unique(cmed_10.24_tratada$`DESTINACAO_COMERCIAL`))

#### ___corrigir NAs ----
cat("Nulos em DESTINACAO_COMERCIAL:", sum(is.na(cmed_10.24_tratada$DESTINACAO_COMERCIAL)), "\n")

cmed_10.24_tratada <- cmed_10.24_tratada %>%
  group_by(REGISTRO) %>%
  mutate(
    `DESTINACAO_COMERCIAL` = ifelse(`DESTINACAO_COMERCIAL` == "" | is.na(`DESTINACAO_COMERCIAL`), 
                                    first(na.omit(`DESTINACAO_COMERCIAL`)), `DESTINACAO_COMERCIAL`)
  ) %>%
  ungroup()
cat("Nulos em DESTINACAO_COMERCIAL:", sum(is.na(cmed_10.24_tratada$DESTINACAO_COMERCIAL)), "\n")

cmed_10.24_tratada <- cmed_10.24_tratada %>%
  group_by(CODIGO_GGREM) %>%
  mutate(
    `DESTINACAO_COMERCIAL` = ifelse(`DESTINACAO_COMERCIAL` == "" | is.na(`DESTINACAO_COMERCIAL`), 
                                    first(na.omit(`DESTINACAO_COMERCIAL`)), `DESTINACAO_COMERCIAL`)
  ) %>%
  ungroup()
cat("Nulos em DESTINACAO_COMERCIAL:", sum(is.na(cmed_10.24_tratada$DESTINACAO_COMERCIAL)), "\n")

cmed_10.24_tratada <- cmed_10.24_tratada %>%
  group_by(PRODUTO,APRESENTACAO) %>%
  mutate(
    `DESTINACAO_COMERCIAL` = ifelse(`DESTINACAO_COMERCIAL` == "" | is.na(`DESTINACAO_COMERCIAL`), 
                                    first(na.omit(`DESTINACAO_COMERCIAL`)), `DESTINACAO_COMERCIAL`)
  ) %>%
  ungroup()
cat("Nulos em DESTINACAO_COMERCIAL:", sum(is.na(cmed_10.24_tratada$DESTINACAO_COMERCIAL)), "\n")


### 3.4.5. RESTRIÇÃO HOSPITALAR ----
names(cmed_10.24_tratada)
sort(unique(cmed_10.24_tratada$`RESTRICAO_HOSPITALAR`))

cmed_10.24_tratada <- cmed_10.24_tratada %>%
  mutate(
   `RESTRICAO_HOSPITALAR` = ifelse(`RESTRICAO_HOSPITALAR` == "SM", "SIM", `RESTRICAO_HOSPITALAR`),
   `RESTRICAO_HOSPITALAR` = toupper(`RESTRICAO_HOSPITALAR`))
sort(unique(cmed_10.24_tratada$`RESTRICAO_HOSPITALAR`))

#### ___corrigir NAs ----
cat("Nulos em RESTRICAO_HOSPITALAR:", sum(is.na(cmed_10.24_tratada$RESTRICAO_HOSPITALAR)), "\n")

cmed_10.24_tratada <- cmed_10.24_tratada %>%
  group_by(REGISTRO) %>%
  mutate(
    `RESTRICAO_HOSPITALAR` = ifelse(`RESTRICAO_HOSPITALAR` == "" | is.na(`RESTRICAO_HOSPITALAR`), 
                                    first(na.omit(`RESTRICAO_HOSPITALAR`)), `RESTRICAO_HOSPITALAR`)
  ) %>%
  ungroup()
cat("Nulos em RESTRICAO_HOSPITALAR:", sum(is.na(cmed_10.24_tratada$RESTRICAO_HOSPITALAR)), "\n")


### 3.4.6. LISTA DE CONCESSÃO DE CRÉDITO TRIBUTÁRIO (PIS/COFINS) ----
sort(unique(cmed_10.24_tratada$`LISTA_CRÉDITO_PIS/COFINS`))

cmed_10.24_tratada <- cmed_10.24_tratada %>%
  mutate(
    `LISTA_CRÉDITO_PIS/COFINS` = toupper(`LISTA_CRÉDITO_PIS/COFINS`))

sort(unique(cmed_10.24_tratada$`LISTA_CRÉDITO_PIS/COFINS`))

#### ___corrigir NAs ----
cat("Nulos em LISTA_CRÉDITO_PIS/COFINS:", sum(is.na(cmed_10.24_tratada$`LISTA_CRÉDITO_PIS/COFINS`)), "\n")

cmed_10.24_tratada <- cmed_10.24_tratada %>%
  group_by(REGISTRO) %>%
  mutate(
    `LISTA_CRÉDITO_PIS/COFINS` = ifelse(`LISTA_CRÉDITO_PIS/COFINS` == "" | is.na(`LISTA_CRÉDITO_PIS/COFINS`), 
                                        first(na.omit(`LISTA_CRÉDITO_PIS/COFINS`)), `LISTA_CRÉDITO_PIS/COFINS`)
  ) %>%
  ungroup()
cat("Nulos em LISTA_CRÉDITO_PIS/COFINS:", sum(is.na(cmed_10.24_tratada$`LISTA_CRÉDITO_PIS/COFINS`)), "\n")

cmed_10.24_tratada <- cmed_10.24_tratada %>%
  group_by(CODIGO_GGREM) %>%
  mutate(
    `LISTA_CRÉDITO_PIS/COFINS` = ifelse(`LISTA_CRÉDITO_PIS/COFINS` == "" | is.na(`LISTA_CRÉDITO_PIS/COFINS`), 
                                        first(na.omit(`LISTA_CRÉDITO_PIS/COFINS`)), `LISTA_CRÉDITO_PIS/COFINS`)
  ) %>%
  ungroup()
cat("Nulos em LISTA_CRÉDITO_PIS/COFINS:", sum(is.na(cmed_10.24_tratada$`LISTA_CRÉDITO_PIS/COFINS`)), "\n")

cmed_10.24_tratada <- cmed_10.24_tratada %>%
  group_by(PRODUTO,APRESENTACAO) %>%
  mutate(
    `LISTA_CRÉDITO_PIS/COFINS` = ifelse(`LISTA_CRÉDITO_PIS/COFINS` == "" | is.na(`LISTA_CRÉDITO_PIS/COFINS`), 
                                        first(na.omit(`LISTA_CRÉDITO_PIS/COFINS`)), `LISTA_CRÉDITO_PIS/COFINS`)
  ) %>%
  ungroup()
cat("Nulos em LISTA_CRÉDITO_PIS/COFINS:", sum(is.na(cmed_10.24_tratada$`LISTA_CRÉDITO_PIS/COFINS`)), "\n")


### 3.4.7. ICMS 0% ----
sort(unique(cmed_10.24_tratada$`ICMS_0%`))

cmed_10.24_tratada <- cmed_10.24_tratada %>%
  mutate(
    `ICMS_0%` = toupper(`ICMS_0%`))
sort(unique(cmed_10.24_tratada$`ICMS_0%`))

#### ___corrigir NAs ----
cat("Nulos em ICMS_0%:", sum(is.na(cmed_10.24_tratada$`ICMS_0%`)), "\n")

cmed_10.24_tratada <- cmed_10.24_tratada %>%
  group_by(REGISTRO) %>%
  mutate(
    `ICMS_0%` = ifelse(`ICMS_0%` == "" | is.na(`ICMS_0%`), first(na.omit(`ICMS_0%`)), `ICMS_0%`)
  ) %>%
  ungroup()

cat("Nulos em ICMS_0%:", sum(is.na(cmed_10.24_tratada$`ICMS_0%`)), "\n")

cmed_10.24_tratada <- cmed_10.24_tratada %>%
  group_by(CODIGO_GGREM) %>%
  mutate(
    `ICMS_0%` = ifelse(`ICMS_0%` == "" | is.na(`ICMS_0%`), first(na.omit(`ICMS_0%`)), `ICMS_0%`)
  ) %>%
  ungroup()

cat("Nulos em ICMS_0%:", sum(is.na(cmed_10.24_tratada$`ICMS_0%`)), "\n")

cmed_10.24_tratada <- cmed_10.24_tratada %>%
  group_by(PRODUTO,APRESENTACAO) %>%
  mutate(
    `ICMS_0%` = ifelse(`ICMS_0%` == "" | is.na(`ICMS_0%`), first(na.omit(`ICMS_0%`)), `ICMS_0%`)
  ) %>%
  ungroup()

cat("Nulos em ICMS_0%:", sum(is.na(cmed_10.24_tratada$`ICMS_0%`)), "\n")


### 3.4.8. CAP ----
sort(unique(cmed_10.24_tratada$`CAP`))

cmed_10.24_tratada <- cmed_10.24_tratada %>%
  mutate(
    `CAP` = case_when(
      toupper(`CAP`) == "0" ~ NA_character_,
      TRUE ~ toupper(`CAP`)
    )
  )
sort(unique(cmed_10.24_tratada$`CAP`))

#### ___corrigir NAs ----

cat("Nulos em CAP:", sum(is.na(cmed_10.24_tratada$`CAP`)), "\n")

cmed_10.24_tratada <- cmed_10.24_tratada %>%
  group_by(REGISTRO) %>%
  mutate(
    `CAP` = ifelse(`CAP` == "" | is.na(`CAP`), first(na.omit(`CAP`)), `CAP`)
  ) %>%
  ungroup()

cat("Nulos em CAP:", sum(is.na(cmed_10.24_tratada$`CAP`)), "\n")



### 3.4.9. CONFAZ 87 ----
sort(unique(cmed_10.24_tratada$`CONFAZ_87`))

cmed_10.24_tratada <- cmed_10.24_tratada %>%
  mutate(
    `CONFAZ_87` = case_when(
      toupper(`CONFAZ_87`) == "0" ~ NA_character_,
      TRUE ~ toupper(`CONFAZ_87`)
    )
  )

sort(unique(cmed_10.24_tratada$`CONFAZ_87`))

#### ___corrigir NAs ----

cat("Nulos em CONFAZ_87:", sum(is.na(cmed_10.24_tratada$`CONFAZ_87`)), "\n")

cmed_10.24_tratada <- cmed_10.24_tratada %>%
  group_by(REGISTRO) %>%
  mutate(
    `CONFAZ_87` = ifelse(`CONFAZ_87` == "" | is.na(`CONFAZ_87`), first(na.omit(`CONFAZ_87`)), `CONFAZ_87`)
  ) %>%
  ungroup()

cat("Nulos em CONFAZ_87:", sum(is.na(cmed_10.24_tratada$`CONFAZ_87`)), "\n")


### 3.4.10. REGIME DE PREÇO ----
names(cmed_10.24_tratada)
sort(unique(cmed_10.24_tratada$`REGIME_PRECO`))

cmed_10.24_tratada <- cmed_10.24_tratada %>%
  mutate(
    `REGIME_PRECO` = toupper(`REGIME_PRECO`))

sort(unique(cmed_10.24_tratada$`REGIME_PRECO`))
sort(unique(cmed_10.24_tratada$`REGIME`))

#### ___corrigir NAs ----

sort(unique(naregime_18.24$REGISTRO))
cat("Nulos em REGIME_PRECO:", sum(is.na(cmed_10.24_tratada$`REGIME_PRECO`)), "\n")
cat("Nulos em REGIME:", sum(is.na(cmed_10.24_tratada$`REGIME`)), "\n")

cmed_10.24_tratada <- cmed_10.24_tratada %>%
  group_by(REGISTRO) %>%
  mutate(
    `REGIME_PRECO` = ifelse(`REGIME_PRECO` == "" | is.na(`REGIME_PRECO`), first(na.omit(`REGIME_PRECO`)), `REGIME_PRECO`),
    ,
    `REGIME_PRECO` = ifelse(is.na(`REGIME_PRECO`) & TARJA %in% c("Tarja Vermelha", "Tarja Preta"), 
                            "REGULADO", `REGIME_PRECO`)
  ) %>%
  ungroup()

cat("Nulos em REGIME_PRECO:", sum(is.na(cmed_10.24_tratada$`REGIME_PRECO`)), "\n")
cat("Nulos em REGIME:", sum(is.na(cmed_10.24_tratada$`REGIME`)), "\n")

cmed_10.24_tratada <- cmed_10.24_tratada %>%
  mutate(
    `REGIME` = case_when(
      is.na(`REGIME_PRECO`) ~ NA_character_,
      `REGIME_PRECO` %in% c("LIBERADO", "MONITORADO") ~ "LIBERADO",
      `REGIME_PRECO` == "REGULADO" ~ "REGULADO",
      TRUE ~ `REGIME_PRECO` 
    )
  )

cat("Nulos em REGIME_PRECO:", sum(is.na(cmed_10.24_tratada$`REGIME_PRECO`)), "\n")
cat("Nulos em REGIME:", sum(is.na(cmed_10.24_tratada$`REGIME`)), "\n")

nas_regime = cmed_10.24_tratada |>
  filter(is.na(`REGIME_PRECO`))

# Liberados
caminho_liberados <- "../data/00_auxiliar/cmed_liberados.xlsx"
liberados <- read_excel(caminho_liberados)
names(liberados)
names(cmed_10.24_tratada)
liberados <- liberados %>%
  row_to_names(row_number = 1) 

liberados <- liberados %>%
  select(-which(names(liberados) == "" | is.na(names(liberados)))) %>%
  mutate(Data = as.Date(as.numeric(Data), origin = "1899-12-30"))

print(sort(unique(liberados$Data)))
sort(unique(liberados$Data))


# Converter colunas de data para o formato Date (se necessário)
liberados$Data <- as.Date(liberados$Data, format = "%d/%m/%Y")
cmed_10.24_tratada$DATA_PUBLICADA <- as.Date(cmed_10.24_tratada$DATA_PUBLICADA, format = "%d/%m/%Y")

# Função para verificar se uma string está contida em outra (vetorizada)
contido_em <- function(texto1, texto2) {
  sapply(texto1, function(x) grepl(x, texto2, ignore.case = TRUE))
}

# Adicionar uma coluna temporária em liberados para facilitar a comparação
liberados <- liberados %>%
  mutate(
    LABORATORIO = EMPRESA,
    APRESENTACAO = APRESENTAÇÃO 
  )

# Realizar a junção e comparação de forma vetorizada
resultado <- cmed_10.24_tratada %>%
  rowwise() %>% 
  mutate(
    REGIME = ifelse(
      any(
        contido_em(liberados$LABORATORIO, LABORATORIO) &
          contido_em(liberados$PRODUTO, PRODUTO) &
          contido_em(liberados$APRESENTACAO, APRESENTACAO) &
          DATA_PUBLICADA >= liberados$Data
      ),
      "Liberado",
      REGIME
    )
  ) %>%
  ungroup()  # Remover o agrupamento

# Remover colunas temporárias de liberados
liberados <- liberados %>%
  select(-LABORATORIO, -APRESENTACAO)

# Exibir o resultado
print(resultado)

ls()

# Visualizar as primeiras linhas para confirmar a adição da coluna REGISTRO
sort(unique(liberados1$`REGISTRO`))
names(liberados1)
names(cmed_10.24_tratada)

rm(liberados11)

# Formatar

cmed_10.24_tratada <- cmed_10.24_tratada %>%
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

# ___________________ ----
# 4. Ordenação final ----
cmed_ordenada = c(
  "ARQUIVO", "DATA_PUBLICADA", "DATA_ATUALIZADA", "ANO_MES",  
  "CNPJ","LABORATORIO", 
  "CLASSE_TERAPEUTICA","CLASSE_TERAPEUTICA_CODIGO", "CLASSE_TERAPEUTICA_DESCRICAO",
  "CLASSE_PRINCIPAL_CODIGO", "CLASSE_PRINCIPAL_DESCRICAO",
  "SUBSTANCIA", "PRODUTO", "APRESENTACAO",
  "REGISTRO", "CODIGO_GGREM", "EAN_1", "EAN_2", "EAN_3",
  "TIPO_PRODUTO", "TARJA", "RESTRICAO_HOSPITALAR",
  "LISTA_CRÉDITO_PIS/COFINS", "ICMS_0%", "CAP", "CONFAZ_87",
  "REGIME", "REGIME_PRECO", 

  "PF_SEM IMPOSTOS", "PF_0%", "PF_12%", "PF_12% ALC", "PF_17%", "PF_17% ALC", "PF_17.5%", "PF_17.5% ALC", 
  "PF_18%", "PF_18% ALC", "PF_19%", "PF_19% ALC", "PF_19.5%", "PF_19.5% ALC",
  "PF_20%", "PF_20% ALC", "PF_20.5%", "PF_21%", "PF_21% ALC", "PF_22%", "PF_22% ALC",
  
  "PMC_SEM IMPOSTOS", "PMC_0%", "PMC_12%", "PMC_12% ALC", "PMC_17%", "PMC_17% ALC", "PMC_17.5%", "PMC_17.5% ALC", 
  "PMC_18%", "PMC_18% ALC", "PMC_19%", "PMC_19% ALC", "PMC_19.5%", "PMC_19.5% ALC",
  "PMC_20%", "PMC_20% ALC", "PMC_20.5%", "PMC_21%", "PMC_21% ALC", "PMC_22%", "PMC_22% ALC",
  
  "PMVG_SEM IMPOSTOS", "PMVG_0%", "PMVG_12%", "PMVG_12% ALC", "PMVG_17%", "PMVG_17% ALC", "PMVG_17.5%", "PMVG_17.5% ALC", 
  "PMVG_18%", "PMVG_18% ALC", "PMVG_19%", "PMVG_19% ALC", "PMVG_19.5%", "PMVG_19.5% ALC",
  "PMVG_20%", "PMVG_20% ALC", "PMVG_20.5%", "PMVG_21%", "PMVG_21% ALC", "PMVG_22%", "PMVG_22% ALC",
  
  "DESTINACAO_COMERCIAL", "ANALISE_RECURSAL",
  "COMERCIALIZACAO_2016", "COMERCIALIZACAO_2017", "COMERCIALIZACAO_2018",
  "COMERCIALIZACAO_2019", "COMERCIALIZACAO_2020", "COMERCIALIZACAO_2021",
  "COMERCIALIZACAO_2022", "ULTIMA_ALTERACAO"
)

# Reorganizar o DataFrame de acordo com a nova lista de colunas completas
cmed_10.24_tratada <- cmed_10.24_tratada  %>%
  select(all_of(cmed_ordenada))

names(cmed_10.24_tratada)

# ___________________ ----
# 5. Salvar Resultados ----

caminho_draft <- "../data/02_interim"

nome_arquivo_cmed <- "cmed_10.24_completa.parquet"
dir.parquet_cmed_10.24 <- file.path(caminho_draft, nome_arquivo_cmed)
write_parquet(cmed_10.24_tratada, sink = dir.parquet_cmed_10.24)