
# import package
source("https://github.com/thiagocalm/tca_utils/raw/refs/heads/master/making-life-easy/importing_packages_and_datasets.R")
check_and_load_package("wpp2024",github = TRUE, package_rep = "PPgp/wpp2024")

# data
data("age5categories")

# export
write.csv(age5categories, file = file.path("from-to-tables","age5categories.txt"))
