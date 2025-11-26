#'---------------------------------------------------------
#'@projeto TCA utils
#'@responsavel Thiago Cordeiro-Almeida
#'@script TCA
#'@ultima-atualizacao 2025-11-26
#'@dados IBGE - PNADC
#'@script Importacao e exportacao de bases da pnad para atualizacao
#'@obs-ultima-atualizacao Atualizando toda a serie apos reponderacao da amostra
#'---------------------------------------------------------

# configuracoes gerais ----------------------------------------------------

options(scipen = 9999, timeout = 1000)
rm(list = ls())
invisible(gc())

# pacotes -----------------------------------------------------------------

library(pacman)
p_load(tidyverse, arrow, PNADcIBGE)

# visitas -----------------------------------------------------------------

###
# parametros
###

# ano
anos = c(2012:2024)

# visitas
visitas = c(1,5)

# diretorio

output_dir = "C:\\Users\\DELL\\OneDrive\\Trampos\\IPEA\\NINSOC\\bases\\PNADC\\Anual"


for(i in seq_along(anos)){
  ano = anos[i]
  for(v in seq_along(visitas)){
    visita = visitas[v]
    # condicionando para que nao faca a importacao em 2020 e 2021 da visita 1
    if(ano %in% c(2020,2021) & visita == 1){
      next
    } else{
      ###
      # Importacao dos dados
      ###

      # importacao dos dados

      pnadc <- PNADcIBGE::get_pnadc(
        year = ano,
        interview = visita,
        labels = FALSE,
        design = FALSE
      )

      # rapidas manipulacoes

      pnadc <- pnadc |>
        rename_with(tolower, .cols = everything()) |>
        mutate(across(everything(),~ as.numeric(.x)))

      ###
      # exportacao de dados
      ###

      write_parquet(pnadc, file.path(output_dir,glue::glue("pnadc_{ano}_visita_{visita}.parquet")))

      ###
      # next loop
      ###
      rm(pnadc)
      invisible(gc())
      print(paste0("Para bases de visitas, finalizamos: ",ano," - visita ",visita,"!"))
    }
  }
}


# trimestres - educacao ---------------------------------------------------

###
# parametros
###

# ano
anos = c(2016,2017,2018,2019, 2022,2023,2024)

# trimestre
trimestres = c(2)

# diretorio

output_dir = "C:\\Users\\DELL\\OneDrive\\Trampos\\IPEA\\NINSOC\\bases\\PNADC\\Trimestral"

for(i in seq_along(anos)){
  ano = anos[i]
  for(v in seq_along(trimestres)){
    trimestre = trimestres[v]
    ###
    # Importacao dos dados
    ###

    # importacao dos dados

    pnadc <- PNADcIBGE::get_pnadc(
      year = ano,
      topic = trimestre,
      labels = FALSE,
      design = FALSE
    )

    ###
    # exportacao de dados
    ###

    write_parquet(pnadc, file.path(output_dir,glue::glue("pnadc_0{trimestre}{ano}_educ.parquet")))

    ###
    # next loop
    ###
    rm(pnadc)
    invisible(gc())
    print(paste0("Para bases de trimestres, finalizamos: ",ano," - trimestre ",trimestre,"!"))
  }
}
