library(mapreduce)
library(jsonlite)

emitFunction <- function(row) {
  if(is.null(row[['Molecule']])||is.na(row[['Molecule']]))
    return (data.frame())
  retVal <- data.frame(molecule=row[['Molecule']],count=1)
  return (retVal)
}
ndjson_data <- stream_in(file("tests/drugdata.ndjson"))

list_data <- mapReduce_map(ndjson_data,emitFunction)

molecules <- mapReduce_reduce(list_data,c("molecule"),c("sum"),c("count"))