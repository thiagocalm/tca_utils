#'#######################################################################
#'@code Functions for importing packages and other processes
#'######################################################################


# packages ----------------------------------------------------------------

# check and load package

check_and_load_package <- function(package_name, github = FALSE, package_rep = NULL) {
  # package
  require(pak)
  # function
  if(requireNamespace(package_name, quietly = TRUE)){
    cat("package was already installed!")
  }
  if(github == FALSE){
    if (!requireNamespace(package_name, quietly = TRUE)) {
      cat("package was not installed. initiating install process...")
      install.packages(package_name)
      cat("package installed and ready to use!")
    }
    library(package_name, character.only = TRUE)
  } else{
    if (!requireNamespace(package_name, quietly = TRUE)) {
      cat("package was not installed. initiating install process...")
      pak::pak(package_rep)
      cat("package installed and ready to use!")
    }
    library(package_name, character.only = TRUE)
  }
}
