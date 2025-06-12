#'#######################################################################
#'@code Functions for importing packages and other processes
#'######################################################################


# packages ----------------------------------------------------------------

# check and load package

check_and_load_package <- function(package_name) {
  if (!requireNamespace(package_name, quietly = TRUE)) {
    install.packages(package_name)
  }
  library(package_name, character.only = TRUE)
}
