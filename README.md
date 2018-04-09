# finbins- the FinBins project's repo.

This repo contains a number of files that read data from tree datasets provided in csv format, i.e. FSS, IDBR and FCA register. Then it links and cleans the data to do an exhaustive feature selection search by training/evaluating a Random Forest (RF) classifier on each feature combination. Results are saved to the log file. Also a confusion matrix for each is created in each combination.

Main files are:

1. Ingestion - read the data from csv and enforces the schema
2. LinkFCA_IDBR, LinkFSS_IDBR - links the FCA and FSS datasets to IDBR
3. SIC_RF, SIC_RF_NORM, SIC_RF_RATIOS - trains and evaluates a RF classifier using a combination of features

