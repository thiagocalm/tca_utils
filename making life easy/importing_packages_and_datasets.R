#'#######################################################################
#'@code Functions for importing packages and other processes
#'######################################################################


# packages ----------------------------------------------------------------

# check and load package

check_and_load_package <- function(package_name, github = FALSE, package_rep = NULL) {
  # package
  require(pak)
  # function
  if(github == FALSE){
    if (!requireNamespace(package_name, quietly = TRUE)) {
      install.packages(package_name)
    }
    library(package_name, character.only = TRUE)
  } else{
    if (!requireNamespace(package_name, quietly = TRUE)) {
      pak::pak(package_rep)
    }
    library(package_name, character.only = TRUE)
  }
}
