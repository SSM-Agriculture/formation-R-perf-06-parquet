library(arrow)
library(dplyr)
library(purrr)
library(openxlsx)
library(tictoc)

#### EXERCICE 1 ####

## Travail préalable ##

# Ouvrir le fichier parquet
RA2020 <- open_dataset("~/CERISE/03-Espace-de-Diffusion/030_Structures_exploitations/3020_Recensements/RA_2020/01_BASES DIFFUSION RA2020/RA_2020_parquet/RA2020_EXPLOITATIONS_240112.parquet")

# Consulter les métadonnées
RA2020$schema

# Consulter les 100 premières lignes du fichier
RA2020_extrait <- RA2020 |> slice_head(n = 100) |> collect()

# Récupération des vecteurs pour les codes et libellés des régions
COMPTAGES_REG <- RA2020 |> count(SIEGE_REG,SIEGE_LIB_REG) |> arrange(SIEGE_REG) |> collect() 
CODES_REGIONS <- COMPTAGES_REG|> pull(SIEGE_REG)
LIB_REGIONS <- COMPTAGES_REG|> pull(SIEGE_LIB_REG)

#' calculs_RA
#'
#' @param region chaine de caractères pour désigner la région
#' @param table data.frame sur laquelle s'applique la fonction
calculs_RA <- function(region,table){
  
  resultat <- table |> 
    filter(SIEGE_REG == region) |> 
    group_by(SIEGE_DEP) |>
    summarise(
      TOTAL_SAU = sum(SAU_TOT, na.rm = TRUE),
      TOTAL_CEREALES = sum(CEREALES_SUR_TOT, na.rm = TRUE),
      TOTAL_OLEAG = sum(OLEAG_SUR_TOT, na.rm = TRUE),
      PROP_SURF_CEREALES = (TOTAL_CEREALES / TOTAL_SAU * 100),
      PROP_SURF_OLEAG = (TOTAL_OLEAG / TOTAL_SAU * 100)) |>
    compute()
  
  return(resultat)
  
}

## Lancement des traitements
dir.create("~/Sorties_excel")

# Lecture des données
RA2020 <- open_dataset("~/CERISE/03-Espace-de-Diffusion/030_Structures_exploitations/3020_Recensements/RA_2020/01_BASES DIFFUSION RA2020/RA_2020_parquet/RA2020_EXPLOITATIONS_240112.parquet")

CODES_REGIONS |>
  map(
    calculs_RA,
    table = RA2020
  ) |>
  walk2(LIB_REGIONS, 
        \(x, y) write.xlsx(x, paste0("~/Sorties_excel/indicateurs ", y, ".xlsx")))


#### EXERCICE 2 ####

data_a <- tibble(
  id = rep(1:1000000, each = 10),
  annee = rep(2016:2025, times = 1000000),
  a = sample(letters, 10000000, replace = TRUE)
)

data_b <- tibble(
  id = rep(1:1000000, each = 10),
  annee = rep(2016:2025, times = 1000000),
  b = runif(10000000, 1, 100)
)

data_c <- tibble(
  lettres = sample(letters, 10000000, replace = TRUE),
  classe = sample(c("pommes","poires","melon","fraise"), 10000000, replace = TRUE)
)

write_parquet(data_a, "data_a.parquet")
write_parquet(data_b, "data_b.parquet")
write_parquet(data_c, "data_c.parquet")

# Liberation de la mémoire
rm(data_a)
rm(data_b)
rm(data_c)
gc()

# TRAITEMENT AVEC COLLECT()
tic()
# Lecture des fichiers parquet
ds1 <- open_dataset("data_a.parquet")
ds2 <- open_dataset("data_b.parquet")

etape1 <- ds1 |>
  left_join(ds2, by = c('id', 'annee')) |>
  collect()

ds3 <- open_dataset("data_c.parquet") |> 
  collect()

etape2 <- etape1 |> 
  filter(annee > 2020) |> 
  group_by(a) |> 
  summarise(sum(b, na.rm = TRUE)) |> 
  left_join(
    ds3, by = c('a' = 'lettres')
  )
toc()

# > 5.252 sec elapsed


# TRAITEMENT AVEC COMPUTE()
tic()
# Lecture des fichiers parquet
ds1 <- open_dataset("data_a.parquet")
ds2 <- open_dataset("data_b.parquet")

etape1 <- ds1 |>
  left_join(ds2, by = c('id', 'annee')) |>
  compute()

ds3 <- open_dataset("data_c.parquet")

etape2 <- etape1 |> 
  filter(annee > 2020) |> 
  group_by(a) |> 
  summarise(sum(b, na.rm = TRUE)) |> 
  left_join(
    ds3, by = c('a' = 'lettres')
  ) |> 
  collect()
toc()

# > 2.482 sec elapsed