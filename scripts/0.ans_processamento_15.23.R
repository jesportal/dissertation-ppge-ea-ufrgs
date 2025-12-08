rm(list = ls())

# ___________________ ----
# ANS ----
# ¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨----
# Baixar dados brutos em: https://dadosabertos.ans.gov.br/FTP/PDA/TISS/

# 1.Processamento ANS ----
 
# Pacotes
library(arrow)
library(dplyr)
library(readr)


# Função que descompacta cada ZIP, lê e filtra CSVs
processar_zip <- function(arquivo_zip) {
  cat("\nProcessando arquivo ZIP:", arquivo_zip, "\n")
  
  diretorio_temp <- tempfile("zip_")
  dir.create(diretorio_temp)
  
  unzip(arquivo_zip, exdir = diretorio_temp)
  
  arquivos_csv <- list.files(
    path       = diretorio_temp,
    pattern    = "\\.csv$",
    full.names = TRUE
  )
  
  cat("   CSVs encontrados:", length(arquivos_csv), "\n")
  
  dfs_filtrados <- lapply(arquivos_csv, function(arq_csv) {
    df <- suppressMessages(
      read_csv2(
        file           = arq_csv,
        show_col_types = FALSE,
        locale         = locale(decimal_mark = ",", grouping_mark = ".")
      )
    )
    
    df_filtrado <- df %>%
      mutate(CD_TABELA_REFERENCIA = trimws(as.character(CD_TABELA_REFERENCIA))) %>%
      filter(CD_TABELA_REFERENCIA == "20")
    
    df_filtrado
  })
  
  resultado_parcial <- bind_rows(dfs_filtrados)
  
  cat("   Linhas filtradas neste ZIP:", nrow(resultado_parcial), "\n")
  
  resultado_parcial
}


# 1.1. Ambulatorial ----

# Caminho base (onde estão as pastas/arquivos .zip) - Atualizar caminho a cada ano
caminho_base_amb_ano <- #Identificar caminhos dos arquivos brutos por ano -> ajustar ano

# Lista todos os arquivos ZIP cujo nome inclua '_DET',
# percorrendo recursivamente subdiretórios.
arquivos_zip <- list.files(
  path        = caminho_base_amb_ano,
  pattern     = "_DET.*\\.zip$",   # nomes que contenham _DET e terminem em .zip
  full.names  = TRUE,
  ignore.case = TRUE,
  recursive   = TRUE
)

# Mostra quantos arquivos ZIP foram encontrados
cat("Arquivos ZIP encontrados:", length(arquivos_zip), "\n")
print(arquivos_zip)

# Aplica a função 'processar_zip' a cada arquivo ZIP encontrado
lista_resultados_2015 <- lapply(arquivos_zip, processar_zip)
lista_resultados_2016 <- lapply(arquivos_zip, processar_zip)
lista_resultados_2017 <- lapply(arquivos_zip, processar_zip)
lista_resultados_2018 <- lapply(arquivos_zip, processar_zip)
lista_resultados_2019 <- lapply(arquivos_zip, processar_zip)
lista_resultados_2020 <- lapply(arquivos_zip, processar_zip)
lista_resultados_2021 <- lapply(arquivos_zip, processar_zip)
lista_resultados_2022 <- lapply(arquivos_zip, processar_zip)
lista_resultados_2023 <- lapply(arquivos_zip, processar_zip)

# Junta todos os resultados em um único data frame
res_amb_2015 <- bind_rows(lista_resultados_2015)
res_amb_2016 <- bind_rows(lista_resultados_2016)
res_amb_2017 <- bind_rows(lista_resultados_2017)
res_amb_2018 <- bind_rows(lista_resultados_2018)
res_amb_2019 <- bind_rows(lista_resultados_2019)
res_amb_2020 <- bind_rows(lista_resultados_2020)
res_amb_2021 <- bind_rows(lista_resultados_2021)
res_amb_2022 <- bind_rows(lista_resultados_2022)
res_amb_2023 <- bind_rows(lista_resultados_2023)

ans_amb_15.23 <- bind_rows(res_amb_2015,res_amb_2016,res_amb_2017,
                           res_amb_2018,res_amb_2019,res_amb_2020,
                           res_amb_2021,res_amb_2022,res_amb_2023)


# Visualiza as primeiras linhas
head(ans_amb_15.23)

# 1.2. Hospitalar ----

# Caminho base (onde estão as pastas/arquivos .zip) - Atualizar caminho a cada ano
caminho_base_hos_ano <- #Identificar caminho dos arquivos brutos

# Lista todos os arquivos ZIP cujo nome inclua '_DET',
#    percorrendo recursivamente subdiretórios.
arquivos_zip <- list.files(
  path        = caminho_base_hos_ano,
  pattern     = "_DET.*\\.zip$",   # nomes que contenham _DET e terminem em .zip
  full.names  = TRUE,
  ignore.case = TRUE,
  recursive   = TRUE
)

# Mostra quantos arquivos ZIP foram encontrados
cat("Arquivos ZIP encontrados:", length(arquivos_zip), "\n")
print(arquivos_zip)


# Aplica a função 'processar_zip' a cada arquivo ZIP encontrado
lista_resultados_hos_2015 <- lapply(arquivos_zip, processar_zip)
lista_resultados_hos_2016 <- lapply(arquivos_zip, processar_zip)
lista_resultados_hos_2017 <- lapply(arquivos_zip, processar_zip)
lista_resultados_hos_2018 <- lapply(arquivos_zip, processar_zip)
lista_resultados_hos_2019 <- lapply(arquivos_zip, processar_zip)
lista_resultados_hos_2020 <- lapply(arquivos_zip, processar_zip)
lista_resultados_hos_2021 <- lapply(arquivos_zip, processar_zip)
lista_resultados_hos_2022 <- lapply(arquivos_zip, processar_zip)
lista_resultados_hos_2023 <- lapply(arquivos_zip, processar_zip)

# Junta todos os resultados em um único data frame
res_hos_2015 <- bind_rows(lista_resultados_hos_2015)
res_hos_2016 <- bind_rows(lista_resultados_hos_2016)
res_hos_2017 <- bind_rows(lista_resultados_hos_2017)
res_hos_2018 <- bind_rows(lista_resultados_hos_2018)
res_hos_2019 <- bind_rows(lista_resultados_hos_2019)
res_hos_2020 <- bind_rows(lista_resultados_hos_2020)
res_hos_2021 <- bind_rows(lista_resultados_hos_2021)
res_hos_2022 <- bind_rows(lista_resultados_hos_2022)
res_hos_2023 <- bind_rows(lista_resultados_hos_2023)

ans_hos_15.23 <- bind_rows(res_hos_2015,res_hos_2016,res_hos_2017,
                           res_hos_2018,res_hos_2019,res_hos_2020,
                           res_hos_2021,res_hos_2022,res_hos_2023)

ans_amb_15.23 <- ans_amb_15.23 |>
  mutate(Tipo = "Ambulatorial")

ans_hos_15.23 <- ans_hos_15.23 |>
  mutate(Tipo = "Hospitalar")

ans_15.23 = bind_rows(ans_amb_15.23,ans_hos_15.23)

head(ans_15.23)

# 2. Juntar Registro ANVISA ----
# Ler o CSV da Tabela 20 (medicamentos)
caminho_csv <- "../data/00_auxiliar/Tabela_20_Medicamentos.csv"
tabela_20 <- read_csv2(caminho_csv, show_col_types = FALSE)
tabela_20 <- tabela_20 %>%
  mutate(`Código do Termo` = as.character(`Código do Termo`),
         `REGISTRO ANVISA` = as.character(`REGISTRO ANVISA`)) |>
  rename(Apresentacao = `Apresentação`)

names(tabela_20)

ans_15.23_com_meds <- ans_15.23 %>%
  left_join(tabela_20, by = c("CD_PROCEDIMENTO" = "Código do Termo"))


head(ans_15.23_com_meds)

colnames(ans_15.23) <- toupper(colnames(ans_15.23))
names(ans_15.23)

# 3. Salvar ----

# Caminho onde o arquivo Parquet será salvo
caminho_draft <- "../data/02_interim"

nome_arquivo <- "ans_15_23_completa.parquet"
caminho_15_23 <- file.path(caminho_draft, nome_arquivo)
write_parquet(ans_15.23_com_meds, sink = caminho_15_23)