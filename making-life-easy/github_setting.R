# Configuracao do github em R
# Ref: https://beatrizmilz.github.io/RLadies-Git-RStudio-2019/#1

library(usethis)

usethis::create_project(
  file.path("C:\\Users\\tcordeiro\\OneDrive\\Estudos\\PhD_\\PhD_dissertation\\paper2\\analysis",
            "kinless_duration")
)

usethis::use_git_config(user.name = "thiagocalm", # Seu nome
                        user.email = "thiagocalm@outlook.com") # Seu email

usethis::git_sitrep() #verificar status

usethis::create_github_token() # criar um novo token de identificacao do computador

gitcreds::gitcreds_set() # atualizando credenciais/token do github

usethis::edit_r_environ() # abrir arquivo Renviron e incluir token

usethis::create_project() # criar projeto

usethis::use_git() # criar git (no computador)

usethis::use_github() # sincronizar com github

usethis::use_readme_md() # criar readme para repo

usethis::git_sitrep() #verificar status
# credentials::git_credential_forget() # apagar credencial
# credentials::set_github_pat() #inserir nova credecial
