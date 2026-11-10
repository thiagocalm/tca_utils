# merging pdf's

dir <- "C:/Users/DELL/OneDrive/Estudos/Mestrado - EDSD/Candidatura/Em processo/4. Official transcripts"

# file1 <- paste0(dir,"Letter of recommendation - Diego.pdf")

# file2 <- file.path(dir,"5_2 - english exam.pdf")
#
# file3 <- file.path(dir,"5_3 - Declaration of proficiency - original.pdf")
#
# file4 <- file.path(dir,"5_4 - Declaration of proficiency - translation.pdf")

file3 <- file.path(dir,"4_0 - index.pdf")

file4 <- file.path(dir,"4_2 - official transcripts.pdf")

file5 <- file.path(dir,"4_2_1 - official transcripts - undergraduate - pt.pdf")

file6 <- file.path(dir,"4_2_2 - official transcripts - undergraduate - en.pdf")

# file7 <- paste0(dir,"4_3 - degree certificates.pdf")
#
# file8 <- paste0(dir,"4_3_1 - degree certificates - master - pt.pdf")
#
# file9 <- paste0(dir,"4_3_2 - degree certificates - master - en.pdf")

file10 <- file.path(dir,"4_4 - official transcripts.pdf")

file11 <- file.path(dir,"4_4_1 - official transcripts - master - pt.pdf")

file12 <- file.path(dir,"4_4_2 - official transcripts - master - en.pdf")

file13 <- file.path(dir,"4_5 - official transcripts.pdf")

file14 <- file.path(dir,"4_5_1 - Equivalence of Bachelor's degree.pdf")

file15 <- file.path(dir,"4_5_2 - Equivalence of Master's degree.pdf")

qpdf::pdf_combine(
  input = c(
    # file2,
    file3, file4, file5, file6,
    # file7, file8, file9,
    file10, file11, file12,
    file13, file14, file15
  ),
  output = file.path(dir,"4 - Official transcripts - Thiago C. Almeida.pdf")
)

# converting jpeg to pdf

dir <- "C:/Users/User/OneDrive/Trampos/IPEA/chamada_002_23/Documentacao/"

file1 <- paste0(dir,"comprovante_bancario_1.jpg")

file2 <- paste0(dir,"comprovante_bancario_2.jpg")

daiR:: image_to_pdf(c(file1,file2), paste0(dir,"2.comprovante_bancario.pdf"))

pdf_combine(input = c(file1, file2),
            output = paste0(dir,"MPIDR - Thiago C. Almeida.pdf"))

# Splitting PDF and merging again

dir <- "/home/thiagocalm/Downloads"

# Parte 1
qpdf::pdf_subset(
  input = file.path(dir,"Trip - Rostock to Berlin.pdf"),
  pages = 2,
  output = file.path(dir,"Tripp - Rostock to Berlin.pdf")
)

# Parte 2
qpdf::pdf_subset(
  input = file.path(dir,"[versao_com_ata] dissertacao_Thiago_Cordeiro_Almeida.pdf"),
  pages = 4:132,
  output = file.path(dir,"pt_2.pdf")
)

# Unindo arquivos

file1 <- file.path(dir,"pt_1.pdf")
file2 <- file.path(dir,"M_Thiago Cordeiro Almeida.pdf")
file3 <- file.path(dir,"pt_2.pdf")

qpdf::pdf_combine(input = c(file1, file2,file3),
                  output = file.path(dir,"dissertacao_Thiago_Cordeiro_Almeida.pdf"))
