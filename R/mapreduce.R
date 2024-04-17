
pkg.env <- new.env()
if(Sys.info()["sysname"][1]=="Linux" || Sys.info()["sysname"][1]=="Darwin") {
  pkg.env$numCores <- detectCores()
}else {
  pkg.env$numCores <- 1
}
pkg.env$maxCores <- pkg.env$numCores
pkg.env$registered <- FALSE
#' Map Reduce Map Function
#'
#' parse a map function over source data
#' @param srcDoc the source document to parse
#' @param mapfunction the function to use, should emit a dataframe, empty if no items
#' @return a list of mapped items
#' @examples
#' emitFunction <- function(row) {
#'   if(is.null(row[['Molecule']]))
#'     return (data.frame())
#'   retVal <- data.frame(molecule=row[['Molecule']],count=1)
#'   return (retVal)
#' }
#'
#' list_data <- mapReduce_map(ndjson_data,emitFunction)
#'
#' @export
mapReduce_map<-function(srcDoc,mapFunction){
  if (is.data.frame(srcDoc)){
    inData <- split(srcDoc, 1:nrow(srcDoc))
  } else {
    inData <- srcDoc
  }
  retVal<-list(mclapply(inData, mapFunction,mc.cores = pkg.env$numCores))[[1]]
  retVal<-list_drop_empty(retVal)
  gc()
  return (retVal)
}
#' Map Reduce Map Function for ndjson files
#'
#' parse a map function over source data
#' @param srcDoc the source file connection to parse
#' @param mapfunction the function to use, should emit a dataframe, empty if no items
#' @return a list of mapped items
#' @examples
#' file<-"data/flight_data6.jdat"
#' processRow <- function(row){
#'   if(startsWith(row,"{")){
#'     json_final<-fromJSON(row)
#'     if(!is.null(json_final[["flightData"]])){
#'       timeStamp<-as.numeric(as.POSIXct(strptime(json_final[["time"]], "%Y/%m/%d %H:%M:%S")))
#'       if(timeStamp>=initial_time)
#'         return(data.frame(json_final[["flightData"]],
#'                time=json_final[["time"]],timestamp=((timeStamp-initial_time)/60)))
#'       else
#'         return(data.frame())
#'     }
#'     else
#'       return(data.frame())
#'   } else {
#'     return(data.frame())
#'   }
#' }
#' con = file(file, "r")
#' list_data <- mapReduce_map_ndjson(con,processRow)
#' close(con)
#'
#' @export
mapReduce_map_ndjson<-function(srcDoc,mapFunction){
  return(lapply(readLines(srcDoc, n=-1, warn=FALSE), mapFunction))
}
#' Map Reduce Reduce function
#'
#' Process a list of mapped dataframes and return a dataframe containing c(key) with c(functions) applied to c(summary_vars)
#' @param dt_s the list to reduce, result of mapReduce_map
#' @param key a c("key") containing the column names to group by
#' @param functions a c("functions") to apply to the reduction
#' @param summary_vars a c("variables") to apply the functions
#' @return a dataframe with the result of the reduction step
#' @examples
#' molecules <- mapReduce_reduce(list_data,c("molecule"),c("sum"),c("count"))
#'
#' @export
mapReduce_reduce<-function(dt_s,key, functions, summary_vars){
  if(is.data.frame(dt_s)){
    getRow<-function(x){
      return(data.frame(x))
    }
    dt_s<-mapReduce_map(dt_s,getRow)
  }
  if(!pkg.env$registered){
      registerDoParallel(pkg.env$numCores)
      pkg.env$registered <- TRUE
    }
  if(pkg.env$numCores>1){

    mapReducer <- function(x) {
      retVal<- foreach(i=x, .combine=rbind) %dopar% {
        dt_s[[i]]
      }
    }
    bins<-c()
    for (i in seq(1, length(dt_s), ceiling(length(dt_s)/pkg.env$numCores))){
      bin<-data.frame(i:min((i+ceiling(length(dt_s)/pkg.env$numCores))-1,length(dt_s)))
      bins<-append(bins,bin)
    }

    dt_s2<-list( mclapply(bins, mapReducer,mc.cores = pkg.env$numCores))[[1]]

    retVal<- foreach(i=1:length(dt_s2), .combine=rbind) %dopar% {
      dt_s2[[i]]
    }
    if(!missing(key) && !missing(functions)&& !missing(summary_vars)){
      print("reduce")
      key = rlang::syms(key)
      summary_exprs <- rlang::parse_exprs(glue::glue('{functions}({summary_vars}, na.rm = TRUE)'))
      names(summary_exprs) <- glue::glue('{functions}_{summary_vars}')
      retVal <- retVal %>% group_by(!!!key) %>% summarise(!!!summary_exprs, .groups = 'drop')
    }else{
      print("no reduce")
    }
    return(retVal)
  } else {
    retVal<- foreach(i=1:length(dt_s), .combine=rbind) %dopar% {
      dt_s[[i]]
    }
    if(!missing(key) && !missing(functions)&& !missing(summary_vars)){
      print("reduce")
      key = rlang::syms(key)
      summary_exprs <- rlang::parse_exprs(glue::glue('{functions}({summary_vars}, na.rm = TRUE)'))
      names(summary_exprs) <- glue::glue('{functions}_{summary_vars}')
      retVal <- retVal %>% group_by(!!!key) %>% summarise(!!!summary_exprs, .groups = 'drop')
    }else{
      print("no reduce")
    }
    gc()
    return(retVal)
  }
}
#' Map Reduce update number of worker
#'
#' By default, detectCores is used to set the number of workers, use this function to
#' modify this value.
#' @param numWorkers the number of workers
#' @return void
#' @param numWorkers the number of workers
#' @return void
#'
#' @export
mapReduce_numWorkers <- function(numWorkers){
  pkg.env$numCores <- min(c(numWorkers,pkg.env$maxCores))
}
