rm(list = ls())

# ___________________ ----
# BPS ----
# ¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨----
# Baixar dados brutos em: https://www.gov.br/saude/pt-br/acesso-a-informacao/banco-de-precos/bases-anuais-compiladas

# Pacotes
library(arrow)
library(dplyr)
library(readxl)
library(stringr)

# 1. Diretório com planilhas ----
dir_bps <- #Identificar caminhos dos arquivos brutos

# 2. Lista todos os arquivos .xls ou .xlsx ----
arquivos_excel <- list.files(
  path        = dir_bps,
  pattern     = "\\.xls[x]?$",
  full.names  = TRUE,
  ignore.case = TRUE
)

# 3. Ler arquivos ----

achar_cabecalho <- function(df) {
  first_row <- unlist(df[1, ])
  if (any(nchar(first_row) > 0)) {
    colnames(df) <- make.names(first_row, unique = TRUE)
    df <- df[-1, ]
  } else {
    colnames(df) <- paste0("Coluna_", seq_along(first_row))
  }
  return(df)
}


lista_primaria_dfs <- lapply(arquivos_excel, function(arquivo) {
  cat("Processando:", arquivo, "\n")
  df <- read_excel(arquivo, col_names = FALSE)
  achar_cabecalho(df)
})


# 4. Ajustar cabeçalhos ----

## 4.1. Selecionar cabeçalhos ----

# Ajustar cabeçalhos para os 18 primeiros itens
for (i in 1:18) {
  first_row <- as.character(unlist(lista_primaria_dfs[[i]][1, ]))
  if (any(nchar(first_row) > 0)) {
    colnames(lista_primaria_dfs[[i]]) <- make.names(first_row, unique = TRUE)
    lista_primaria_dfs[[i]] <- lista_primaria_dfs[[i]][-1, ]
  } else {
    colnames(lista_primaria_dfs[[i]]) <- paste0("Coluna_", seq_along(first_row))
  }
}

# Exibir os novos cabeçalhos para confirmação
for (i in 1:18) {
  cat("\nNovos cabeçalhos para o arquivo", i, ":\n")
  print(names(lista_primaria_dfs[[i]]))
}

for (i in 17:18) {
  first_row <- as.character(unlist(lista_primaria_dfs[[i]][1, ]))
  if (any(nchar(first_row) > 0)) {
    colnames(lista_primaria_dfs[[i]]) <- make.names(first_row, unique = TRUE)
    lista_primaria_dfs[[i]] <- lista_primaria_dfs[[i]][-1, ]
  } else {
    colnames(lista_primaria_dfs[[i]]) <- paste0("Coluna_", seq_along(first_row))
  }
}

# Exibir os novos cabeçalhos para confirmação
for (i in 17:18) {
  cat("\nNovos cabeçalhos para o arquivo", i, ":\n")
  print(names(lista_primaria_dfs[[i]]))
}

for (i in 27:27) {
  first_row <- as.character(unlist(lista_primaria_dfs[[i]][1, ]))
  if (any(nchar(first_row) > 0)) {
    colnames(lista_primaria_dfs[[i]]) <- make.names(first_row, unique = TRUE)
    lista_primaria_dfs[[i]] <- lista_primaria_dfs[[i]][-1, ]
  } else {
    colnames(lista_primaria_dfs[[i]]) <- paste0("Coluna_", seq_along(first_row))
  }
}

# Exibir os novos cabeçalhos para confirmação
for (i in 27:27) {
  cat("\nNovos cabeçalhos para o arquivo", i, ":\n")
  print(names(lista_primaria_dfs[[i]]))
}

## 4.2. Remover prefixo 'X.' ----

# Remover prefixo 'X.' dos nomes das colunas para os primeiros 26 itens
lista_dfs <- lista_primaria_dfs

for (i in seq_along(lista_dfs)) {
  colunas_atuais <- names(lista_dfs[[i]])
  novas_colunas <- gsub("^X\\.", "", colunas_atuais)
    names(lista_dfs[[i]]) <- novas_colunas
}

# Exibir os novos cabeçalhos para confirmação
for (i in seq_along(lista_dfs)) {
  cat("\nNovos cabeçalhos para o arquivo", i, ":\n")
  print(names(lista_dfs[[i]]))
}


  ## 4.3. Substituir pontos ----

# Substituir grupos de pontos por um único espaço e remover espaços no início e no final
for (i in seq_along(lista_dfs)) {
  colunas_atuais <- names(lista_dfs[[i]])
  novas_colunas <- trimws(gsub("\\.+", " ", colunas_atuais))
  names(lista_dfs[[i]]) <- novas_colunas
}

# Exibir os novos cabeçalhos para confirmação
for (i in seq_along(lista_dfs)) {
  cat("\nNovos cabeçalhos para o arquivo", i, ":\n")
  print(names(lista_dfs[[i]]))
}


## 4.4 Remover NA's ----

# Função para remover colunas cujo nome contém "NA"
remover_colunas_com_na_nome <- function(df) {
  df[, !grepl("^NA", names(df)), drop = FALSE]
}

# Aplicar a função a todos os data frames na lista
lista_dfs <- lapply(lista_dfs, remover_colunas_com_na_nome)

# Exibir os cabeçalhos ajustados para validação
for (i in seq_along(lista_dfs)) {
  cat("\nCabeçalhos ajustados para o arquivo", i, ":\n")
  print(names(lista_dfs[[i]]))
}


## 4.5. Padronizar nomes ----
# Renomear uma coluna específica
# Verificar os nomes atuais das colunas do item X

 print(names(lista_dfs[[22]]))

# Renomear uma coluna específica
names(lista_dfs[[1]])[names(lista_dfs[[1]]) == "Fornecedor"] <- "Nome Fornecedor"
names(lista_dfs[[2]])[names(lista_dfs[[2]]) == "Fornecedor"] <- "Nome Fornecedor"
names(lista_dfs[[3]])[names(lista_dfs[[3]]) == "Fornecedor"] <- "Nome Fornecedor"
names(lista_dfs[[4]])[names(lista_dfs[[4]]) == "Fornecedor"] <- "Nome Fornecedor"
names(lista_dfs[[5]])[names(lista_dfs[[5]]) == "Fornecedor"] <- "Nome Fornecedor"
names(lista_dfs[[6]])[names(lista_dfs[[6]]) == "Fornecedor"] <- "Nome Fornecedor"
names(lista_dfs[[7]])[names(lista_dfs[[7]]) == "Fornecedor"] <- "Nome Fornecedor"
names(lista_dfs[[8]])[names(lista_dfs[[8]]) == "Fornecedor"] <- "Nome Fornecedor"
names(lista_dfs[[9]])[names(lista_dfs[[9]]) == "Fornecedor"] <- "Nome Fornecedor"
names(lista_dfs[[10]])[names(lista_dfs[[10]]) == "Fornecedor"] <- "Nome Fornecedor"
names(lista_dfs[[11]])[names(lista_dfs[[11]]) == "Fornecedor"] <- "Nome Fornecedor"
names(lista_dfs[[12]])[names(lista_dfs[[12]]) == "Fornecedor"] <- "Nome Fornecedor"
names(lista_dfs[[13]])[names(lista_dfs[[13]]) == "Fornecedor"] <- "Nome Fornecedor"
names(lista_dfs[[14]])[names(lista_dfs[[14]]) == "Fornecedor"] <- "Nome Fornecedor"
names(lista_dfs[[15]])[names(lista_dfs[[15]]) == "Fornecedor"] <- "Nome Fornecedor"
names(lista_dfs[[16]])[names(lista_dfs[[16]]) == "Fornecedor"] <- "Nome Fornecedor"
names(lista_dfs[[17]])[names(lista_dfs[[17]]) == "Fornecedor"] <- "Nome Fornecedor"
names(lista_dfs[[18]])[names(lista_dfs[[18]]) == "Fornecedor"] <- "Nome Fornecedor"
names(lista_dfs[[19]])[names(lista_dfs[[19]]) == "Fornecedor"] <- "Nome Fornecedor"
names(lista_dfs[[20]])[names(lista_dfs[[20]]) == "Fornecedor"] <- "Nome Fornecedor"
names(lista_dfs[[21]])[names(lista_dfs[[21]]) == "Fornecedor"] <- "Nome Fornecedor"
names(lista_dfs[[25]])[names(lista_dfs[[25]]) == "Fornecedor"] <- "Nome Fornecedor"
names(lista_dfs[[26]])[names(lista_dfs[[26]]) == "Fornecedor"] <- "Nome Fornecedor"


names(lista_dfs[[13]])[names(lista_dfs[[13]]) == "Fabricante"] <- "Nome Fabricante"
names(lista_dfs[[14]])[names(lista_dfs[[14]]) == "Fabricante"] <- "Nome Fabricante"
names(lista_dfs[[15]])[names(lista_dfs[[15]]) == "Fabricante"] <- "Nome Fabricante"
names(lista_dfs[[16]])[names(lista_dfs[[16]]) == "Fabricante"] <- "Nome Fabricante"
names(lista_dfs[[17]])[names(lista_dfs[[17]]) == "Fabricante"] <- "Nome Fabricante"
names(lista_dfs[[18]])[names(lista_dfs[[18]]) == "Fabricante"] <- "Nome Fabricante"
names(lista_dfs[[19]])[names(lista_dfs[[19]]) == "Fabricante"] <- "Nome Fabricante"
names(lista_dfs[[20]])[names(lista_dfs[[20]]) == "Fabricante"] <- "Nome Fabricante"
names(lista_dfs[[21]])[names(lista_dfs[[21]]) == "Fabricante"] <- "Nome Fabricante"
names(lista_dfs[[25]])[names(lista_dfs[[25]]) == "Fabricante"] <- "Nome Fabricante"
names(lista_dfs[[26]])[names(lista_dfs[[26]]) == "Fabricante"] <- "Nome Fabricante"


names(lista_dfs[[22]])[names(lista_dfs[[22]]) == "Instituição"] <- "CNPJ Instituição"
names(lista_dfs[[23]])[names(lista_dfs[[23]]) == "Instituição"] <- "CNPJ Instituição"
names(lista_dfs[[24]])[names(lista_dfs[[24]]) == "Instituição"] <- "CNPJ Instituição"

names(lista_dfs[[22]])[names(lista_dfs[[22]]) == "Fornecedor"] <- "CNPJ Fornecedor"
names(lista_dfs[[23]])[names(lista_dfs[[23]]) == "Fornecedor"] <- "CNPJ Fornecedor"
names(lista_dfs[[24]])[names(lista_dfs[[24]]) == "Fornecedor"] <- "CNPJ Fornecedor"

names(lista_dfs[[22]])[names(lista_dfs[[22]]) == "Fabricante"] <- "CNPJ Fabricante"
names(lista_dfs[[23]])[names(lista_dfs[[23]]) == "Fabricante"] <- "CNPJ Fabricante"
names(lista_dfs[[24]])[names(lista_dfs[[24]]) == "Fabricante"] <- "CNPJ Fabricante"


# Inicializa um vetor para armazenar os nomes das colunas
todos_cabecalhos <- c()

# Extrai os nomes das colunas de cada data frame na lista
for (i in seq_along(lista_dfs)) {
  todos_cabecalhos <- c(todos_cabecalhos, names(lista_dfs[[i]]))
}

# Remove duplicatas e ordena os cabeçalhos
cabecalhos_unicos <- sort(unique(todos_cabecalhos))

# Cria um data frame com os cabeçalhos únicos
df_cabecalhos <- data.frame(Cabecalhos = cabecalhos_unicos, stringsAsFactors = FALSE)

# Exibe o data frame
print(df_cabecalhos)

# Lista de mapeamento de nomes equivalentes para um único padrão
dicionario <- list(
  "Classe" = c("Classe"),
  "CMED Preço Regulado" = c("CMED Preço Regulado"),
  "CNPJ Fabricante" = c("CNPJ Fabricante"),
  "CNPJ Fornecedor" = c("CNPJ Fornecedor"),
  "CNPJ Instituição" = c("CNPJ Instituição"),
  "Código BR" = c("Código BR"),
  "Competência CMED" = c("Competência CMED"),
  "Data Compra" = c("Compra", "Data Compra", "Data da compra"),
  "Data Inserção" = c("Data Inserção", "Inserção"),
  "Descrição" = c("Descrição", "Descrição do item", "Descrição Item", "Descrição CATMAT"),
  "Esfera" = c("Esfera"),
  "Genérico" = c("Genérico"),
  "Licitação" = c("Licitação"),
  "Maior Preço" = c("Maior Preço"),
  "Menor Preço" = c("Menor Preço"),
  "Média Ponderada" = c("Média Ponderada"),
  "Modalidade Compra" = c("Modalidade da compra", "Modalidade da Compra", 
                          "Modalidade de compra", "Modalidade de Compra"),
  "Município Instituição" = c("Município", "Município Instituição"),
  "Nota Fiscal" = c("Nota Fiscal"),
  "Nome Fabricante" = c("Nome do fabricante", "Nome Fabricante"),
  "Nome Fornecedor" = c("Nome do fornecedor", "Nome Fornecedor"),
  "Nome Instituição" = c("Instituição compradora", "Nome da instituição", 
                         "Nome da Instituição", "Nome Instituição"),
  "País" = c("País"),
  "Preço Unitário" = c("Pago", "Preço unitário", "Preço Unitário", "Unitário"),
  "Quantidade" = c("Qtd Itens Comprados", "Quantidade", "Quantidade comprada"),
  "Qualificação" = c("Qualificação"),
  "Região" = c("Região"),
  "Registro Anvisa" = c("Anvisa", "Registro Anvisa", "ANVISA"),
  "Tipo Compra" = c("Tipo", "Tipo Compra"),
  "Unidade Fornecimento" = c("Unidade de fornecimento", "Unidade de Fornecimento"),
  "Valor Total" = c("Preço Total", "Total", "Valor total do item"),
  "UF" = c("UF")
)


# Função para unir colunas com nomes equivalentes
unir_colunas_equivalentes <- function(df, dicionario) {
  df_unidos <- df
  
  for (coluna_padrao in names(dicionario)) {
    colunas_equivalentes <- dicionario[[coluna_padrao]]
    colunas_presentes <- colunas_equivalentes[colunas_equivalentes %in% names(df)]
    
    if (length(colunas_presentes) > 1) {
      df_unidos[[coluna_padrao]] <- apply(df[, colunas_presentes, drop = FALSE], 1, function(row) {
        if (length(valores) > 0) return(valores[1]) else return(NA)
      })
      
      df_unidos <- df_unidos[, !(names(df_unidos) %in% colunas_presentes)]
    } else if (length(colunas_presentes) == 1) {
      names(df_unidos)[names(df_unidos) == colunas_presentes] <- coluna_padrao
    }
  }
  
  return(df_unidos)
}

lista_dfs_unidos <- lapply(seq_along(lista_dfs), function(i) {
    unir_colunas_equivalentes(lista_dfs[[i]], dicionario)
})

for (i in seq_along(lista_dfs_unidos)) {
  cat("\nCabeçalhos ajustados para o arquivo", i, ":\n")
  print(names(lista_dfs_unidos[[i]]))
}


# 5. Criar data frame ----

bps_97.24 <- bind_rows(lista_dfs_unidos)

cat("\nData frame combinado com sucesso!\n")
colnames(bps_97.24) <- toupper(colnames(bps_97.24))
names(bps_97.24)
summary(bps_97.24)


bps_97.24 <- bps_97.24 %>%
  mutate(
    `REGISTRO ANVISA` = na_if(`REGISTRO ANVISA`, "Não Informado"),
    `REGISTRO ANVISA` = na_if(`REGISTRO ANVISA`, "No Informado"),
    `REGISTRO ANVISA` = na_if(`REGISTRO ANVISA`, "NoInformado"),
    `REGISTRO ANVISA` = str_remove_all(`REGISTRO ANVISA`, "[^0-9A-Za-z]") 
  )


# 6. Ajustar formatos ----


names(bps_97.24)
summary(bps_97.24)


#Reordenar colunas
colunas_ordenadas <- c("DATA COMPRA",  "REGISTRO ANVISA", "CÓDIGO BR",
                       "DESCRIÇÃO", "UNIDADE FORNECIMENTO", "GENÉRICO",
                       "CNPJ INSTITUIÇÃO","NOME INSTITUIÇÃO", 
                       "CNPJ FABRICANTE", "NOME FABRICANTE",
                       "CNPJ FORNECEDOR", "NOME FORNECEDOR", 
                       "MUNICÍPIO INSTITUIÇÃO", "UF", "REGIÃO", "PAÍS",
                       "DATA INSERÇÃO","TIPO COMPRA", "MODALIDADE COMPRA",
                       "QUALIFICAÇÃO", "CLASSE",
                       "ESFERA", "LICITAÇÃO", "NOTA FISCAL",
                       "QUANTIDADE", "PREÇO UNITÁRIO", "VALOR TOTAL",
                       "MÉDIA PONDERADA", "MAIOR PREÇO", "MENOR PREÇO",
                       "CMED PREÇO REGULADO", "COMPETÊNCIA CMED")


colnames(bps_97.24) <- stringr::str_trim(colnames(bps_97.24))

bps_97.24 <- bps_97.24 %>%
  select(all_of(colunas_ordenadas))



## DATAS ----
datasvazias <- data.frame(
  Coluna = "DATA COMPRA",
  Valores_Vazios = sum(is.na(bps_97.24$`DATA COMPRA`) | bps_97.24$`DATA COMPRA` == "")
) |>
  data.frame(
    Coluna = "DATA INSERÇÃO",
    Valores_Vazios = sum(is.na(bps_97.24$`DATA INSERÇÃO`) | bps_97.24$`DATA INSERÇÃO` == "")
  ) |>
  data.frame(
    Coluna = "COMPETÊNCIA CMED",
    Valores_Vazios = sum(is.na(bps_97.24$`COMPETÊNCIA CMED`) | bps_97.24$`COMPETÊNCIA CMED` == "")
  )

print(datasvazias)
rm(datasvazias)

### _COMPRA ----
bps_97.24 <- bps_97.24 %>%
  mutate(
    `DATA COMPRA` = str_trim(`DATA COMPRA`),
    `DATA COMPRA` = case_when(
      !is.na(as.numeric(`DATA COMPRA`)) ~ as.Date(as.numeric(`DATA COMPRA`), origin = "1899-12-30"),
      TRUE ~ as.Date(`DATA COMPRA`, format = "%d/%m/%Y")
    ),
    ANO_MES_BPS = format(`DATA COMPRA`, "%Y-%m"),
    `DATA COMPRA` = format(`DATA COMPRA`, "%Y-%m-%d")
  )

vazios <- data.frame(
  Coluna = "DATA COMPRA",
  Valores_Vazios = sum(is.na(bps_97.24$`DATA COMPRA`) | bps_97.24$`DATA COMPRA` == "")
)

print(vazios)
rm(vazios)

### _INSERÇÃO ----
bps_97.24 <- bps_97.24 %>%
  mutate(
    `DATA INSERÇÃO` = str_trim(`DATA INSERÇÃO`),
    `DATA INSERÇÃO` = case_when(
      !is.na(as.numeric(`DATA INSERÇÃO`)) ~ as.Date(as.numeric(`DATA INSERÇÃO`), origin = "1899-12-30"),
      TRUE ~ as.Date(`DATA INSERÇÃO`, format = "%d/%m/%Y")
    ),
    `DATA INSERÇÃO` = format(`DATA INSERÇÃO`, "%Y-%m-%d")
  )

vazios <- data.frame(
  Coluna = "DATA INSERÇÃO",
  Valores_Vazios = sum(is.na(bps_97.24$`DATA INSERÇÃO`) | bps_97.24$`DATA INSERÇÃO` == "")
)

print(vazios)
rm(vazios)

### _COMPETÊNCIA CMED ----
bps_97.24 <- bps_97.24 %>%
  mutate(
    `COMPETÊNCIA CMED` = str_trim(`COMPETÊNCIA CMED`),
    `COMPETÊNCIA CMED` = case_when(
      !is.na(as.numeric(`COMPETÊNCIA CMED`)) ~ as.Date(as.numeric(`COMPETÊNCIA CMED`), origin = "1899-12-30"),
      TRUE ~ as.Date(`COMPETÊNCIA CMED`, format = "%d/%m/%Y")
    ),
    `COMPETÊNCIA CMED` = format(`COMPETÊNCIA CMED`, "%Y-%m-%d")
  )

vazios <- data.frame(
  Coluna = "COMPETÊNCIA CMED",
  Valores_Vazios = sum(is.na(bps_97.24$`COMPETÊNCIA CMED`) | bps_97.24$`COMPETÊNCIA CMED` == "")
)
print(vazios)
rm(vazios)


bps_97.24_formatada <- bps_97.24 %>%
  mutate(
    `COMPETÊNCIA CMED` = as.Date(`COMPETÊNCIA CMED`),
    `DATA COMPRA` = as.Date(`DATA COMPRA`),
    `DATA INSERÇÃO` = as.Date(`DATA INSERÇÃO`),
    ANO_MES_BPS = format(`DATA COMPRA`, "%Y-%m")
  )
summary(bps_97.24_formatada)
names(bps_97.24_formatada)

## NUMÉRICOS ----

### _Preço Unitário ----
bps_97.24_formatada <- bps_97.24_formatada %>%
  mutate(
    `PREÇO UNITÁRIO` = str_trim(`PREÇO UNITÁRIO`))

bps_97.24_formatada <- bps_97.24_formatada %>%
  mutate(revisao1_virgulas = str_count(`PREÇO UNITÁRIO`, "[,]"),
         revisao1_pontos = str_count(`PREÇO UNITÁRIO`, "[.]")) %>%
  mutate(
    `PREÇO UNITÁRIO` = case_when(
      revisao1_virgulas >= 1 & revisao1_pontos >= 1 ~ str_replace_all(`PREÇO UNITÁRIO`, "\\.", ""), # Remover pontos
      TRUE ~ `PREÇO UNITÁRIO`
    ))

bps_97.24_formatada <- bps_97.24_formatada %>%
  mutate(
    `CMED PREÇO REGULADO` = str_trim(`CMED PREÇO REGULADO`),
    q_esp_cpr = str_count(`CMED PREÇO REGULADO`, " "),
    `MAIOR PREÇO` = str_trim(`MAIOR PREÇO`),
    q_esp_map = str_count(`MAIOR PREÇO`, " "),
    `MÉDIA PONDERADA` = str_trim(`MÉDIA PONDERADA`),
    q_esp_avp = str_count(`MÉDIA PONDERADA`, " "),
    `MENOR PREÇO` = str_trim(`MENOR PREÇO`),
    q_esp_mep = str_count(`MENOR PREÇO`, " "),
    `PREÇO UNITÁRIO` = str_trim(`PREÇO UNITÁRIO`),
    q_esp_pu = str_count(`PREÇO UNITÁRIO`, " "),
    `QUANTIDADE` = str_trim(`QUANTIDADE`),
    q_esp_q = str_count(`QUANTIDADE`, " "),
    `VALOR TOTAL` = str_trim(`VALOR TOTAL`),
    q_esp_vt = str_count(`VALOR TOTAL`, " ")
  )
bps_97.24_formatada = bps_97.24_formatada |>
  select(-c(q_esp_cpr,q_esp_map,q_esp_avp,q_esp_mep,q_esp_pu,q_esp_q,q_esp_vt))

bps_97.24_formatada <- bps_97.24_formatada %>%
  mutate(revisao2_virgulas = str_count(`PREÇO UNITÁRIO`, "[,]"),
         revisao2_pontos = str_count(`PREÇO UNITÁRIO`, "[.]"))

bps_97.24_formatada <- bps_97.24_formatada %>%
  mutate(`PREÇO UNITÁRIO` = str_replace_all(`PREÇO UNITÁRIO`, ",", "."))


bps_97.24_formatada <- bps_97.24_formatada %>%
  mutate(revisao3_virgulas = str_count(`PREÇO UNITÁRIO`, "[,]"),
         revisao3_pontos = str_count(`PREÇO UNITÁRIO`, "[.]"),
         q_esp_pu = str_count(`PREÇO UNITÁRIO`, " "))

bps_97.24_formatada <- bps_97.24_formatada %>%
  select(-c(revisao1_virgulas, revisao1_pontos, revisao2_virgulas, 
            revisao2_pontos, revisao3_virgulas, revisao3_pontos, q_esp_pu))

pu = data.frame(valor= unique(bps_97.24_formatada$`PREÇO UNITÁRIO`)) |>
  mutate(
    pu= as.numeric(valor)
  )
names(pu)
rm(pu)


### _Quantidade ----

bps_97.24_formatada <- bps_97.24_formatada %>%
  mutate(revisao1_virgulas = str_count(`QUANTIDADE`, "[,]")) %>%
  mutate(
    `QUANTIDADE` = case_when(
      revisao1_virgulas == 1 ~ str_replace_all(`QUANTIDADE`, ",", "."),
      TRUE ~ `QUANTIDADE`
    ))
bps_97.24_formatada <- bps_97.24_formatada %>%
  mutate(revisao2_virgulas = str_count(`QUANTIDADE`, "[,]"))

bps_97.24_formatada <- bps_97.24_formatada %>%
  select(-c(revisao1_virgulas, revisao2_virgulas))

qd = data.frame(valor= unique(bps_97.24_formatada$`QUANTIDADE`)) |>
  mutate(
    qd= as.numeric(valor)
  )
names(qd)
rm(qd)


### _Valor Total ----

bps_97.24_formatada <- bps_97.24_formatada %>%
  mutate(revisao1_virgulas = str_count(`VALOR TOTAL`, "[,]"),
         revisao1_pontos = str_count(`VALOR TOTAL`, "[.]")) %>%
  mutate(
    `VALOR TOTAL` = case_when(
      revisao1_virgulas >= 1 & revisao1_pontos >= 1 ~ str_replace_all(`VALOR TOTAL`, "\\.", ""),
      TRUE ~ `VALOR TOTAL`
    ))
bps_97.24_formatada <- bps_97.24_formatada %>%
  mutate(revisao2_virgulas = str_count(`VALOR TOTAL`, "[,]"),
         revisao2_pontos = str_count(`VALOR TOTAL`, "[.]"))

bps_97.24_formatada <- bps_97.24_formatada %>%
  mutate(`VALOR TOTAL` = str_replace_all(`VALOR TOTAL`, ",", ".")) 

bps_97.24_formatada <- bps_97.24_formatada %>%
  mutate(revisao3_virgulas = str_count(`VALOR TOTAL`, "[,]"),
         revisao3_pontos = str_count(`VALOR TOTAL`, "[.]"))

bps_97.24_formatada <- bps_97.24_formatada %>%
  select(-c(revisao1_virgulas, revisao1_pontos, revisao2_virgulas, revisao2_pontos, revisao3_virgulas, revisao3_pontos))


### _CMED Preço Regulado ----

bps_97.24_formatada <- bps_97.24_formatada %>%
  mutate(revisao1_virgulas = str_count(`CMED PREÇO REGULADO`, "[,]"),
         revisao1_pontos = str_count(`CMED PREÇO REGULADO`, "[.]")) %>%
  mutate(
    `CMED PREÇO REGULADO` = case_when(
      revisao1_virgulas >= 1 & revisao1_pontos >= 1 ~ str_replace_all(`CMED PREÇO REGULADO`, "\\.", ""),
      TRUE ~ `CMED PREÇO REGULADO`
    ))
bps_97.24_formatada <- bps_97.24_formatada %>%
  mutate(revisao2_virgulas = str_count(`CMED PREÇO REGULADO`, "[,]"),
         revisao2_pontos = str_count(`CMED PREÇO REGULADO`, "[.]"))

bps_97.24_formatada <- bps_97.24_formatada %>%
  mutate(`CMED PREÇO REGULADO` = str_replace_all(`CMED PREÇO REGULADO`, ",", "."))

bps_97.24_formatada <- bps_97.24_formatada %>%
  mutate(revisao3_virgulas = str_count(`CMED PREÇO REGULADO`, "[,]"),
         revisao3_pontos = str_count(`CMED PREÇO REGULADO`, "[.]"))

bps_97.24_formatada <- bps_97.24_formatada %>%
  select(-c(revisao1_virgulas, revisao1_pontos, revisao2_virgulas, 
            revisao2_pontos, revisao3_virgulas, revisao3_pontos))


### _Média Ponderada ----

bps_97.24_formatada <- bps_97.24_formatada %>%
  mutate(revisao1_virgulas = str_count(`MÉDIA PONDERADA`, "[,]"),
         revisao1_pontos = str_count(`MÉDIA PONDERADA`, "[.]")) %>%
  mutate(
    `MÉDIA PONDERADA` = case_when(
      revisao1_virgulas >= 1 & revisao1_pontos >= 1 ~ str_replace_all(`MÉDIA PONDERADA`, "\\.", ""),
      TRUE ~ `MÉDIA PONDERADA`
    ))
bps_97.24_formatada <- bps_97.24_formatada %>%
  mutate(revisao2_virgulas = str_count(`MÉDIA PONDERADA`, "[,]"),
         revisao2_pontos = str_count(`MÉDIA PONDERADA`, "[.]"))

bps_97.24_formatada <- bps_97.24_formatada %>%
  mutate(`MÉDIA PONDERADA` = str_replace_all(`MÉDIA PONDERADA`, ",", "."))

bps_97.24_formatada <- bps_97.24_formatada %>%
  mutate(revisao3_virgulas = str_count(`MÉDIA PONDERADA`, "[,]"),
         revisao3_pontos = str_count(`MÉDIA PONDERADA`, "[.]"))

bps_97.24_formatada <- bps_97.24_formatada %>%
  select(-c(revisao1_virgulas, revisao1_pontos, revisao2_virgulas, 
            revisao2_pontos, revisao3_virgulas, revisao3_pontos))


### _Maior Preço ----

bps_97.24_formatada <- bps_97.24_formatada %>%
  mutate(revisao1_virgulas = str_count(`MAIOR PREÇO`, "[,]"),
         revisao1_pontos = str_count(`MAIOR PREÇO`, "[.]")) %>%
  mutate(
    `MAIOR PREÇO` = case_when(
      revisao1_virgulas >= 1 & revisao1_pontos >= 1 ~ str_replace_all(`MAIOR PREÇO`, "\\.", ""),
      TRUE ~ `MAIOR PREÇO`
    ))
bps_97.24_formatada <- bps_97.24_formatada %>%
  mutate(revisao2_virgulas = str_count(`MAIOR PREÇO`, "[,]"),
         revisao2_pontos = str_count(`MAIOR PREÇO`, "[.]"))

bps_97.24_formatada <- bps_97.24_formatada %>%
  mutate(`MAIOR PREÇO` = str_replace_all(`MAIOR PREÇO`, ",", "."))

bps_97.24_formatada <- bps_97.24_formatada %>%
  mutate(revisao3_virgulas = str_count(`MAIOR PREÇO`, "[,]"),
         revisao3_pontos = str_count(`MAIOR PREÇO`, "[.]"))

bps_97.24_formatada <- bps_97.24_formatada %>%
  select(-c(revisao1_virgulas, revisao1_pontos, revisao2_virgulas, 
            revisao2_pontos, revisao3_virgulas, revisao3_pontos))

ma = data.frame(valor= unique(bps_97.24_formatada$`MAIOR PREÇO`)) |>
  mutate(
    ma= as.numeric(valor)
  )
names(ma)
rm(ma)


### _Menor Preço ----

bps_97.24_formatada <- bps_97.24_formatada %>%
  mutate(revisao1_virgulas = str_count(`MENOR PREÇO`, "[,]"),
         revisao1_pontos = str_count(`MENOR PREÇO`, "[.]")) %>%
  mutate(
    `MENOR PREÇO` = case_when(
      revisao1_virgulas >= 1 & revisao1_pontos >= 1 ~ str_replace_all(`MENOR PREÇO`, "\\.", ""),
      TRUE ~ `MENOR PREÇO`
    ))
bps_97.24_formatada <- bps_97.24_formatada %>%
  mutate(revisao2_virgulas = str_count(`MENOR PREÇO`, "[,]"),
         revisao2_pontos = str_count(`MENOR PREÇO`, "[.]"))

bps_97.24_formatada <- bps_97.24_formatada %>%
  mutate(`MENOR PREÇO` = str_replace_all(`MENOR PREÇO`, ",", "."))

bps_97.24_formatada <- bps_97.24_formatada %>%
  mutate(revisao3_virgulas = str_count(`MENOR PREÇO`, "[,]"),
         revisao3_pontos = str_count(`MENOR PREÇO`, "[.]"))

bps_97.24_formatada <- bps_97.24_formatada %>%
  select(-c(revisao1_virgulas, revisao1_pontos, revisao2_virgulas, 
            revisao2_pontos, revisao3_virgulas, revisao3_pontos))


bps_97.24_formatada <- bps_97.24_formatada %>%
  mutate(
    `CMED PREÇO REGULADO` = as.numeric(`CMED PREÇO REGULADO`),
    `MAIOR PREÇO` = as.numeric(`MAIOR PREÇO`),
    `MÉDIA PONDERADA` = as.numeric(`MÉDIA PONDERADA`),
    `MENOR PREÇO` = as.numeric(`MENOR PREÇO`),
    `PREÇO UNITÁRIO` = as.numeric(`PREÇO UNITÁRIO`),
    `QUANTIDADE` = as.numeric(`QUANTIDADE`),
    `VALOR TOTAL` = as.numeric(`VALOR TOTAL`)
  )


##FATORES ----

### _Classe ----
unique(bps_97.24_formatada$`CLASSE`)

### _Esfera ----
unique(bps_97.24_formatada$`ESFERA`)
bps_97.24_formatada <- bps_97.24_formatada %>%
  mutate(
    `ESFERA` = str_trim(`ESFERA`),
    `ESFERA` = na_if(`ESFERA`, "")
  )
unique(bps_97.24_formatada$`ESFERA`)

### _Genérico ----
unique(bps_97.24_formatada$`GENÉRICO`)
bps_97.24_formatada <- bps_97.24_formatada %>%
  mutate(
    `GENÉRICO` = str_trim(`GENÉRICO`),
    `GENÉRICO` = case_when(
      `GENÉRICO` %in% c("NÃO", "N") ~ "Não",
      `GENÉRICO` %in% c("SIM", "S") ~ "Sim",
      TRUE ~ `GENÉRICO` 
    ),
    
  )
unique(bps_97.24_formatada$`GENÉRICO`)

### _Modalidade Compra ----
unique(bps_97.24_formatada$`MODALIDADE COMPRA`)
bps_97.24_formatada <- bps_97.24_formatada %>%
  mutate(
    `MODALIDADE COMPRA` = str_trim(`MODALIDADE COMPRA`),
    `MODALIDADE COMPRA` = case_when(
      `MODALIDADE COMPRA` %in% c("Pregão", " Pregão ") ~ "Pregão",
      `MODALIDADE COMPRA` %in% c("Convocação Geral", " Convocação Geral ") ~ "Convocação Geral",
      `MODALIDADE COMPRA` %in% c("Coleta de Preços", " Coleta de Preços ") ~ "Coleta de Preços",
      `MODALIDADE COMPRA` %in% c("Registro de Preço", " Registro de Preços ") ~ "Registro de Preços",
      `MODALIDADE COMPRA` %in% c("Pedido de Cotação", " Pedido de Cotação ") ~ "Pedido de Cotação",
      `MODALIDADE COMPRA` %in% c("Tomada de Preço", " Tomada de Preços ") ~ "Tomada de Preço",
      `MODALIDADE COMPRA` %in% c("Concorrência", " Concorrência ") ~ "Concorrência",
      `MODALIDADE COMPRA` %in% c("Compra Direta", " Compra Direta ") ~ "Compra Direta",
      `MODALIDADE COMPRA` %in% c("Dispensa de Licitação", " Dispensa de Licitação ") ~ "Dispensa de Licitação",
      `MODALIDADE COMPRA` %in% c("Cotação (Eletrônica) de Preços", " Cotação (Eletrônica) de Preços ") ~ "Cotação Eletrônica de Preços",
      `MODALIDADE COMPRA` %in% c("Licitación o concurso público internacional") ~ "Concurso Público Internacional",
      `MODALIDADE COMPRA` %in% c("Licitación o concurso abreviado") ~ "Concurso Abreviado",
      `MODALIDADE COMPRA` %in% c("Licitación o concurso público nacional") ~ "Concurso Público Nacional",
      `MODALIDADE COMPRA` %in% c("Licitación-Preadjud. Imediata") ~ "Licitação com Adjudicação Imediata",
      `MODALIDADE COMPRA` %in% c("Propuesta Pública") ~ "Proposta Pública",
      `MODALIDADE COMPRA` %in% c("Propuesta Privada") ~ "Proposta Privada",
      `MODALIDADE COMPRA` %in% c("Cotización Directa") ~ "Cotação Direta",
      TRUE ~ `MODALIDADE COMPRA`
    )
  )

unique(bps_97.24_formatada$`MODALIDADE COMPRA`)

### _Região ----
unique(bps_97.24_formatada$`REGIÃO`)

### _Tipo Compra ----
unique(bps_97.24_formatada$`TIPO COMPRA`)
bps_97.24_formatada <- bps_97.24_formatada %>%
  mutate(
    `TIPO COMPRA` = str_trim(`TIPO COMPRA`),
    `TIPO COMPRA` = case_when(
      `TIPO COMPRA` %in% c("Não Informado", " Não Informado ", "0") ~ NA_character_,
      `TIPO COMPRA` %in% c("A", "a", " A ", " a ") ~ "A",
      `TIPO COMPRA` %in% c("J", "j", " J ", " j ") ~ "J",
      TRUE ~ `TIPO COMPRA`
    )
  )

unique(bps_97.24_formatada$`TIPO COMPRA`)

### _UF ----
unique(bps_97.24_formatada$`UF`)
bps_97.24_formatada <- bps_97.24_formatada %>%
  mutate(
    `UF` = str_trim(`UF`),
    `UF` = case_when(
      `UF` == "" ~ NA_character_,
      `UF` %in% c("RJ", "RJ ") ~ "RJ",
      `UF` %in% c("SP", "SP ") ~ "SP",
      `UF` %in% c("MG", "MG ") ~ "MG",
      `UF` %in% c("DF", "DF ") ~ "DF",
      `UF` %in% c("GO", "GO ") ~ "GO",
      `UF` %in% c("CE", "CE ") ~ "CE",
      `UF` %in% c("PR", "PR ") ~ "PR",
      `UF` %in% c("RS", "RS ") ~ "RS",
      `UF` %in% c("SC", "SC ") ~ "SC",
      `UF` %in% c("PE", "PE ") ~ "PE",
      `UF` %in% c("BA", "BA ") ~ "BA",
      `UF` %in% c("PA", "PA ") ~ "PA",
      `UF` %in% c("AM", "AM ") ~ "AM",
      `UF` %in% c("MA", "MA ") ~ "MA",
      `UF` %in% c("MS", "MS ") ~ "MS",
      `UF` %in% c("TO", "TO ") ~ "TO",
      `UF` %in% c("ES", "ES ") ~ "ES",
      `UF` %in% c("AL", "AL ") ~ "AL",
      `UF` %in% c("RN", "RN ") ~ "RN",
      `UF` %in% c("PB", "PB ") ~ "PB",
      `UF` %in% c("RO", "RO ") ~ "RO",
      `UF` %in% c("RR", "RR ") ~ "RR",
      `UF` %in% c("AP") ~ "AP",
      `UF` %in% c("AC", "AC ") ~ "AC",
      `UF` %in% c("Bs.As", "ELS", "STG", "PRA", "MONT") ~ NA_character_,
      TRUE ~ NA_character_ 
    )
  )

unique(bps_97.24_formatada$`UF`)


bps_97.24_formatada <- bps_97.24_formatada %>%
  mutate(
    `CLASSE` = as.factor(`CLASSE`),
    `ESFERA` = as.factor(`ESFERA`),
    `GENÉRICO` = as.factor(`GENÉRICO`),
    `MODALIDADE COMPRA` = as.factor(`MODALIDADE COMPRA`),
    `REGIÃO` = as.factor(`REGIÃO`),
    `TIPO COMPRA` = as.factor(`TIPO COMPRA`),
    `UF` = as.factor(`UF`)
  )

summary(bps_97.24_formatada)


## TEXTOS ----

c <- bps_97.24_formatada %>%
  mutate(
    # Converter para texto
    `CNPJ FABRICANTE` = str_trim(`CNPJ FABRICANTE`),
    `CNPJ FORNECEDOR` = str_trim(`CNPJ FORNECEDOR`),
    `CNPJ INSTITUIÇÃO` = str_trim(`CNPJ INSTITUIÇÃO`),
    `CÓDIGO BR` = str_trim(`CÓDIGO BR`),
    `DESCRIÇÃO` = str_trim(`DESCRIÇÃO`),
    `LICITAÇÃO` = str_trim(`LICITAÇÃO`),
    `MUNICÍPIO INSTITUIÇÃO` = str_trim(`MUNICÍPIO INSTITUIÇÃO`),
    `NOME FABRICANTE` = str_trim(`NOME FABRICANTE`),
    `NOME FORNECEDOR` = str_trim(`NOME FORNECEDOR`),
    `NOME INSTITUIÇÃO` = str_trim(`NOME INSTITUIÇÃO`),
    `NOTA FISCAL` = str_trim(`NOTA FISCAL`),
    `QUALIFICAÇÃO` = str_trim(`QUALIFICAÇÃO`),
    `REGISTRO ANVISA` = str_trim(`REGISTRO ANVISA`),
    `UNIDADE FORNECIMENTO` = str_trim(`UNIDADE FORNECIMENTO`)
  )


### _CNPJ FABRICANTE ----
unique(bps_97.24_formatada$`CNPJ FABRICANTE`)

bps_97.24_formatada <- bps_97.24_formatada %>%
  mutate(
    `CNPJ FABRICANTE` = str_trim(`CNPJ FABRICANTE`),
    `CNPJ FABRICANTE` = case_when(
      str_detect(`CNPJ FABRICANTE`, "^\\d{2}\\.\\d{3}\\.\\d{3}/\\d{4}-\\d{2}$") ~ `CNPJ FABRICANTE`,
      str_detect(`CNPJ FABRICANTE`, "^\\d{14}$") ~ str_replace(`CNPJ FABRICANTE`, 
                 "(\\d{2})(\\d{3})(\\d{3})(\\d{4})(\\d{2})", "\\1.\\2.\\3/\\4-\\5"
      ),
      str_detect(`CNPJ FABRICANTE`, "^\\d{2}\\s*\\d{3}\\s*\\d{3}\\s*\\d{4}\\s*\\d{2}$") ~ str_replace_all(`CNPJ FABRICANTE`, 
                 c("\\s+" = "", "(\\d{2})(\\d{3})(\\d{3})(\\d{4})(\\d{2})" = "\\1.\\2.\\3/\\4-\\5")
      ),
      `CNPJ FABRICANTE` %in% c(NA, "", "NA") ~ NA_character_, 
      TRUE ~ `CNPJ FABRICANTE` 
    )
  )
unique(bps_97.24_formatada$`CNPJ FABRICANTE`)

  
### _CNPJ FORNECEDOR ----
unique(bps_97.24_formatada$`CNPJ FORNECEDOR`)

bps_97.24_formatada <- bps_97.24_formatada %>%
  mutate(
    `CNPJ FORNECEDOR` = str_trim(`CNPJ FORNECEDOR`),
    `CNPJ FORNECEDOR` = case_when(
      str_detect(`CNPJ FORNECEDOR`, "^\\d{2}\\.\\d{3}\\.\\d{3}/\\d{4}-\\d{2}$") ~ `CNPJ FORNECEDOR`,
      str_detect(`CNPJ FORNECEDOR`, "^\\d{14}$") ~ str_replace(`CNPJ FORNECEDOR`, 
                 "(\\d{2})(\\d{3})(\\d{3})(\\d{4})(\\d{2})", "\\1.\\2.\\3/\\4-\\5"
      ),
      str_detect(`CNPJ FORNECEDOR`, "^\\d{2}\\s*\\d{3}\\s*\\d{3}\\s*\\d{4}\\s*\\d{2}$") ~ str_replace_all(`CNPJ FORNECEDOR`, 
                 c("\\s+" = "",
                   "(\\d{2})(\\d{3})(\\d{3})(\\d{4})(\\d{2})" = "\\1.\\2.\\3/\\4-\\5" )
      ),
      `CNPJ FORNECEDOR` %in% c(NA, "", "NA") ~ NA_character_,
      TRUE ~ `CNPJ FORNECEDOR`
    )
  )
unique(bps_97.24_formatada$`CNPJ FORNECEDOR`)

### _CNPJ INSTITUIÇÃO ----
unique(bps_97.24_formatada$`CNPJ INSTITUIÇÃO`)
bps_97.24_formatada <- bps_97.24_formatada %>%
  mutate(
    `CNPJ INSTITUIÇÃO` = str_trim(`CNPJ INSTITUIÇÃO`),
    `CNPJ INSTITUIÇÃO` = case_when(
      str_detect(`CNPJ INSTITUIÇÃO`, "^\\d{2}\\.\\d{3}\\.\\d{3}/\\d{4}-\\d{2}$") ~ `CNPJ INSTITUIÇÃO`,
      str_detect(`CNPJ INSTITUIÇÃO`, "^\\d{14}$") ~ str_replace(`CNPJ INSTITUIÇÃO`, 
                 "(\\d{2})(\\d{3})(\\d{3})(\\d{4})(\\d{2})", "\\1.\\2.\\3/\\4-\\5"
      ),
      str_detect(`CNPJ INSTITUIÇÃO`, "^\\d{2}\\s*\\d{3}\\s*\\d{3}\\s*\\d{4}\\s*\\d{2}$") ~ str_replace_all(`CNPJ INSTITUIÇÃO`, 
                 c("\\s+" = "","(\\d{2})(\\d{3})(\\d{3})(\\d{4})(\\d{2})" = "\\1.\\2.\\3/\\4-\\5")
      ),
      `CNPJ INSTITUIÇÃO` %in% c(NA, "", "NA") ~ NA_character_,
      TRUE ~ `CNPJ INSTITUIÇÃO`
    )
  )
unique(bps_97.24_formatada$`CNPJ INSTITUIÇÃO`)

### _CÓDIGO BR ----
unique(bps_97.24_formatada$`CÓDIGO BR`)
bps_97.24_formatada$`CÓDIGO BR`= str_trim(bps_97.24_formatada$`CÓDIGO BR`)

### _DESCRIÇÃO ----
unique(bps_97.24_formatada$`DESCRIÇÃO`)

# Função para padronizar as descrições
padronizar_descricao <- function(descricoes) {
  descricoes <- toupper(descricoes)
  descricoes <- stringi::stri_trans_general(descricoes, "Latin-ASCII")
  descricoes <- gsub("[^A-Z0-9/% ]", "", descricoes)
  descricoes <- gsub("\\s+", " ", descricoes)
  descricoes <- str_trim(descricoes)
  return(descricoes)
}

bps_97.24_formatada$`DESCRIÇÃO` <- padronizar_descricao(bps_97.24_formatada$`DESCRIÇÃO`)
print(unique(bps_97.24_formatada$`DESCRIÇÃO`))

### _LICITAÇÃO ----
unique(bps_97.24_formatada$`LICITAÇÃO`)
bps_97.24_formatada$`LICITAÇÃO` = str_trim(bps_97.24_formatada$`LICITAÇÃO`)
unique(bps_97.24_formatada$`LICITAÇÃO`)

### _MUNICÍPIO INSTITUIÇÃO ----
unique(bps_97.24_formatada$`MUNICÍPIO INSTITUIÇÃO`)
bps_97.24_formatada$`MUNICÍPIO INSTITUIÇÃO` = str_trim(bps_97.24_formatada$`MUNICÍPIO INSTITUIÇÃO`)
unique(bps_97.24_formatada$`MUNICÍPIO INSTITUIÇÃO`)

### _NOME FABRICANTE ----
unique(bps_97.24_formatada$`NOME FABRICANTE`)
padronizar_nomes <- function(nomes) {
  nomes <- toupper(nomes)
  nomes <- stringi::stri_trans_general(nomes, "Latin-ASCII")
  nomes <- gsub("[^A-Z0-9/%& ]", "", nomes)
  nomes <- gsub("\\s+", " ", nomes)
  nomes <- str_trim(nomes)
  return(nomes)
}
bps_97.24_formatada$`NOME FABRICANTE` = padronizar_nomes(bps_97.24_formatada$`NOME FABRICANTE`)
unique(bps_97.24_formatada$`NOME FABRICANTE`)

### _NOME FORNECEDOR ----
unique(bps_97.24_formatada$`NOME FORNECEDOR`)
bps_97.24_formatada$`NOME FORNECEDOR` = padronizar_nomes(bps_97.24_formatada$`NOME FORNECEDOR`)
unique(bps_97.24_formatada$`NOME FORNECEDOR`)

### _NOME INSTITUIÇÃO ----
unique(bps_97.24_formatada$`NOME INSTITUIÇÃO`)
bps_97.24_formatada$`NOME INSTITUIÇÃO`= padronizar_nomes(bps_97.24_formatada$`NOME INSTITUIÇÃO`)
unique(bps_97.24_formatada$`NOME INSTITUIÇÃO`)

### _NOTA FISCAL ----
unique(bps_97.24_formatada$`NOTA FISCAL`)
bps_97.24_formatada$`NOTA FISCAL` = str_trim(bps_97.24_formatada$`NOTA FISCAL`)
unique(bps_97.24_formatada$`NOTA FISCAL`)

### _QUALIFICAÇÃO ----
unique(bps_97.24_formatada$`QUALIFICAÇÃO`)
bps_97.24_formatada$`QUALIFICAÇÃO` = str_trim(bps_97.24_formatada$`QUALIFICAÇÃO`)

### _REGISTRO ANVISA ----
unique(bps_97.24_formatada$`REGISTRO ANVISA`)

padronizar_registro_anvisa <- function(registros) {
  registros <- toupper(registros)
  registros <- gsub("[^0-9]", "", registros)
  registros <- str_trim(registros)
  registros <- ifelse(registros == "" | registros == "NA" | registros == "NAOINFORMADO", NA, registros)
  registros <- ifelse(!is.na(registros), sprintf("%013s", registros), registros)
  return(registros)
}

bps_97.24_formatada$`REGISTRO ANVISA` = padronizar_registro_anvisa(bps_97.24_formatada$`REGISTRO ANVISA`)
unique(bps_97.24_formatada$`REGISTRO ANVISA`)

### _UNIDADE FORNECIMENTO ----
unique(bps_97.24_formatada$`UNIDADE FORNECIMENTO`)
padronizar_unidade <- function(unidade) {
  unidade <- toupper(unidade)
  unidade <- stringi::stri_trans_general(unidade, "Latin-ASCII") 
  unidade <- gsub("[^A-Z0-9/% ,]", "", unidade)
  unidade <- gsub("\\s+", " ", unidade)
  unidade <- str_trim(unidade)
  return(unidade)
}

bps_97.24_formatada$`UNIDADE FORNECIMENTO` = padronizar_unidade(bps_97.24_formatada$`UNIDADE FORNECIMENTO`)
unique(bps_97.24_formatada$`UNIDADE FORNECIMENTO`)

summary(bps_97.24_formatada)

# 7. Salvar ----

caminho_draft <- "../data/02_interim"

nome_arquivo <- "bps_97.24_formatada.parquet"
caminho_bps_97.24 <- file.path(caminho_draft, nome_arquivo)
write_parquet(bps_97.24_formatada, sink = caminho_bps_97.24)
