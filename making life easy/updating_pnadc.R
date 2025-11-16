#'---------------------------------------------------------
#'@projeto TCA utils
#'@responsavel Thiago Cordeiro-Almeida
#'@script TCA
#'@ultima-atualizacao 2025-11-16
#'@dados IBGE - PNADC
#'@script Importacao e exportacao de bases da pnad para atualizacao
#'---------------------------------------------------------

# configuracoes gerais ----------------------------------------------------

options(scipen = 99999)
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
anos = c(2023,2024)

# visitas
visitas = c(1,5)

# diretorio

output_dir = "C:\\Users\\DELL\\OneDrive\\Trampos\\IPEA\\NINSOC\\bases\\PNADC\\Anual"


for(i in seq_along(anos)){
  ano = anos[i]
  for(v in seq_along(visitas)){
    visita = visitas[v]
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

    write_parquet(pnadc, file.path(output_dir,glue::glue("pnadc_{trimestre}{ano}_educ.parquet")))

    ###
    # next loop
    ###
    rm(pnadc)
    invisible(gc())
    print(paste0("Para bases de trimestres, finalizamos: ",ano," - trimestre ",trimestre,"!"))
  }
}
