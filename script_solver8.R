# install.packages(c("jsonlite","plyr","data.table","lubridate","tidyr"))
library(jsonlite)
library(plyr)
library(data.table)
library(lubridate)
library(tidyr)
library(parallel)

#set configuration
#setwd('C:/Users/Leo/Documents/projekte/sbb/')
dir_raw_data <- './raw-data/'
dir_minizinc_model <- './minizinc-model/'
dir_flatzinc_model <- './flatzinc-model/'
dir_model_results <- './model-result/'
number_of_cpu_cores <- detectCores()
options(stringsAsFactors=FALSE)

#setup parallel
cl <- makeCluster(number_of_cpu_cores)
na <- clusterEvalQ(cl, library(plyr))
na <- clusterEvalQ(cl, library(lubridate))
na <- clusterEvalQ(cl, library(data.table))

#functions
is.na.nullSave <- function(x) is.null(x) || is.na(x)

secondsToHms <- function(seconds) {
  td <- seconds_to_period(seconds)
  sprintf('%02d:%02d:%02d', td@hour, lubridate::minute(td), lubridate::second(td))
}

# based on https://stackoverflow.com/a/35761217/55070
# for efficiency reson we only hold the last result in memory
lastResult <- function(filepath) {
  resultsFromFile <- c()
  newResult <- T 
  con = file(filepath, "r")
  while ( TRUE ) {
    line = readLines(con, n = 1)
    if ( length(line) == 0 ) {
      break
    } else if(line == "----------") {
      newResult <- T
    } else if(line == "==========") {
      print("optimal solution found.")
    } else {
      if(newResult) {
        resultsFromFile <- c()
        newResult <- F
      }
      resultsFromFile <- c(resultsFromFile, line);
    }
  }
  close(con)
  resultsFromFile
}

graphCreation <- function(routeSections, sectionId, train) {
  resList = data.frame()
  #for loop because access to prev and next is needed
  for (row in 1:nrow(routeSections)) {
    routeSection <- routeSections[row,]
    start <- if(!is.null(routeSection$route_alternative_marker_at_entry[[1]])) {
      routeSection$route_alternative_marker_at_entry[[1]]
    } else {
      if(row==1) {
        paste0(routeSections[row, 'sequence_number'],'_beginning')
      } else {
        paste0(routeSections[row-1, 'sequence_number'],'->',routeSections[row, 'sequence_number'])
      }
    }
    #end label
    end <- if(!is.null(routeSections[row, 'route_alternative_marker_at_exit'][[1]])) {
      routeSections[row, 'route_alternative_marker_at_exit'][[1]]
    } else {
      if(row==nrow(routeSections)) {
        paste0(routeSections[row, 'sequence_number'],'_end')
      } else {
        paste0(routeSections[row, 'sequence_number'],'->',routeSections[row+1, 'sequence_number'])
      }
    }
    
    result = data.frame(route = train, 
                        start = start,
                        end = end,
                        route_section_id = paste0(train,'#',routeSection$sequence_number),
                        route_path = sectionId,
                        section_marker = if(is.null(routeSections[[row, 'section_marker']]) | length(routeSections[[row, 'section_marker']]) == 0) 'NULL' else routeSections[[row, 'section_marker']],
                        minRunningTime = as.numeric(duration(routeSections[row, 'minimum_running_time'])), 
                        penalty = if(is.numeric(routeSections[row, "penalty"]) && !is.na(routeSections[row, "penalty"])) routeSections[row, "penalty"] else 0,
                        stringsAsFactors = F)
    result$resources <- list(routeSections[[row,"resource_occupations"]][,"resource"])
    resList <- rbind(resList, result);
  }
  resList
}

findPaths <- function(graph, nodes, prevPath, currentCost) {
  uniqueNodes <- unique(nodes);
  allPaths <- data.table(); 
  if(length(uniqueNodes) == 0) {
    #if no new nodes exist, we assume the end of the graph is reached. This assumption might be dangerous.
    return(data.table(cost = currentCost, path=list(prevPath)))
  }
  for(i in 1:length(uniqueNodes)) {
    n <- uniqueNodes[i]
    cost <- if(length(prevPath)>0) {
      prevNode <- tail(prevPath, n=1)
      edge <- graph[graph$start==prevNode & graph$end==n, ]
      penalty <- if(is.na(edge$penalty)) 0 else edge$penalty
      edge$minRunningTime + penalty*60 + currentCost
    } else {
      0
    }
    path <- c(prevPath,n);
    allPaths <- rbind(allPaths, 
                      if(endsWith(n,"_end")) {
                        data.table(cost, path=list(path))
                      } else {
                        newNodes <- graph[graph$start==n, "end"]
                        findPaths(graph, newNodes, path, cost)
                      }
    );
  }
  allPaths
}

#transform train names that minizinc can't handle
tt <- function(trainName) {
  gsub("\\[|\\]","_",trainName)
}

#code
createMinizincModel <- function(problem_instance, data, out, local_max_delay){
  print(problem_instance)
  clusterExport(cl, c("data", "graphCreation", "findPaths"))
  allTrainSchedule <- rbind.fill(clusterApply(cl, 1:nrow(data$routes), function(trainRow) {
    aTrain <- data$routes[trainRow,"id"]
    aTrainRoute <- data$routes[trainRow,"route_paths"][[1]]
    serviceIntentions <- data$service_intentions[[trainRow,"section_requirements"]]
    cat(aTrain,'\n')
    
    graph <- rbind.fill(lapply(1:nrow(aTrainRoute),function(row) {
      graphCreation(aTrainRoute[row,"route_sections"][[1]], aTrainRoute[row,"id"], aTrain)
    }))
    
    startNode <- if(nrow(graph[endsWith(graph$start,"_beginning"),]) == 1) graph[endsWith(graph$start,"_beginning"),] else graph[graph$section_marker == head(serviceIntentions$section_marker,1),]
    paths <- findPaths(graph, startNode[,"start"],c(),0)
    
    minCost <- min(paths$cost)
    winningPath <- paths[paths$cost == minCost,]
    set.seed(123)
    rndIdx <- sample(1:nrow(winningPath))
    idx <- (trainRow %% nrow(winningPath)) + 1
    aPath <- winningPath$path[[rndIdx[idx]]]
    
    fullDataPath <- graph[graph$start %in% head(aPath, n=-1) & graph$end %in% tail(aPath,n=-1),]
    
    #order alonge the path
    fullDataPath <- fullDataPath[match( head(aPath,-1), fullDataPath$start),]
    fullDataPath$sequence_number = 1:nrow(fullDataPath)
    
    serviceIntentions$section_requirement <- serviceIntentions$section_marker
    fullData <- merge(fullDataPath, serviceIntentions, by = 'section_marker', all.x=T, sort=F, suffixes = c('','.y'))
    fullData <- fullData[order(fullData$sequence_number),]
    fullData$min_stopping_time <- as.numeric(duration(fullData$min_stopping_time))
    fullData$min_stopping_time[is.na(fullData$min_stopping_time)] <- 0
    
    fullData$entry_earliest <- period_to_seconds(hms(fullData$entry_earliest,quiet = T))
    fullData$entry_latest   <- if(is.null(fullData$entry_latest)) NA else period_to_seconds(hms(fullData$entry_latest,quiet = T))
    fullData$exit_earliest  <- if(is.null(fullData$exit_earliest)) NA else period_to_seconds(hms(fullData$exit_earliest,quiet = T))
    fullData$exit_latest    <- period_to_seconds(hms(fullData$exit_latest,quiet = T))
    
    #assumption only one "entry earliest", and on the first position
    stopifnot( sum(!is.na(fullData$entry_earliest)) == 1 )
    stopifnot( !is.na(fullData$entry_earliest[1]) )
    
    fullData$exit_time <- 0
    fullData$entry_time <- 0
    
    # For Problemset 07_V1.22_FWA route 19320 entry_delay_weight is missing. therefore we extend the columns of fullData here
    if(!("entry_delay_weight" %in% names(fullData))) fullData[, "entry_delay_weight"] <- NA
    fullData
  }))
  cat("done train preparation\n")
  allTrains <- unique(allTrainSchedule$route)
  
  resources <- data$resources
  row.names(resources) <- resources$id
  
  resourcesForAllServices <- unnest(allTrainSchedule,resources)
  resourcesForAllServices <- subset(resourcesForAllServices, select = -c(route_section_id, minRunningTime, penalty, route_path, sequence_number.y))
  resourcesForAllServices$release_time <- as.numeric(duration(resources[resourcesForAllServices$resources,"release_time"]))
  resourcesForAllServices$exit_time      <- resourcesForAllServices$exit_time + resourcesForAllServices$release_time
  resourcesForAllServices <- resourcesForAllServices[order(resourcesForAllServices$route,resourcesForAllServices$resources,resourcesForAllServices$entry_time),]
  cat("done resource calculation\n")
  #output minizing model
  cat("%generated model with R script\n\n", file=out)
  
  trainDefs <- c()
  for(train in allTrains){
    topSequence = max(allTrainSchedule[allTrainSchedule$route==train,"sequence_number"])
    maxTime = max(allTrainSchedule[allTrainSchedule$route==train,"exit_latest"], na.rm = T) + local_max_delay;
    minTime = min(allTrainSchedule[allTrainSchedule$route==train,"entry_earliest"], na.rm = T);
    trainDefs <- c(trainDefs, paste0("\n% --- Train ",train,"------------------------\n"))
    times <- allTrainSchedule[allTrainSchedule$route==train,"minRunningTime"] + allTrainSchedule[allTrainSchedule$route==train,"min_stopping_time"];
    trainDefs <- c(trainDefs, paste0("var ",minTime,"..",maxTime,": T",tt(train),"_0;\n"))
    for(seq in allTrainSchedule[allTrainSchedule$route==train,"sequence_number"]) {
      exit <- allTrainSchedule[allTrainSchedule$route==train & allTrainSchedule$sequence_number == seq, "exit_earliest"]
      if(!is.na(exit)) {
        minTime <- exit
      }
      trainDefs <- c(trainDefs, paste0("var ",minTime,"..",maxTime,": T",tt(train),"_",seq,";\n"))
      trainDefs <- c(trainDefs, paste0("constraint T",tt(train),"_",seq," - T",tt(train),"_",(seq-1)," >= ",times[seq],";\n"))
    }
  }
  cat(trainDefs, sep = "", file=out, append = T)
  cat("done output trains\n")
  
  #required constrainst entry and exit earliest are not listed manualy, as they are taken in consideration above with reducing the range of the integer above.
  cat("\n\n%--entry earliest contraints-----------\n", file=out, append = T)
  entryEarliest <- allTrainSchedule[!is.na(allTrainSchedule$entry_earliest),c("route", "sequence_number", "entry_earliest")]
  for(i in 1:nrow(entryEarliest)) {
    cat("constraint T",tt(entryEarliest[i,"route"]),"_",entryEarliest[i,"sequence_number"]-1," >= ",entryEarliest[i,"entry_earliest"],";\n",sep = "", file=out, append = T)
  }
  
  cat("%--exit_earliest contraints-----------\n", file=out, append = T)
  exitEarliest <- allTrainSchedule[!is.na(allTrainSchedule$exit_earliest),c("route", "sequence_number", "exit_earliest")]
  for(i in 1:nrow(exitEarliest)) {
    cat("constraint T",tt(exitEarliest[i,"route"]),"_",exitEarliest[i,"sequence_number"]," >= ",exitEarliest[i,"exit_earliest"],";\n",sep = "", file=out, append = T)
  }
  
  cat("done earliest constraints\n")
  cat("%--optimize simplifiyed objective function-----------\n", file=out, append = T)
  # assumption that the delay weights are not used
  stopifnot(all(allTrainSchedule$entry_delay_weight == 1, na.rm = T))
  stopifnot(all(allTrainSchedule$exit_delay_weight == 1, na.rm = T))
  cat("var int: obj;\n", file=out, append = T);
  cat ("obj = ", file=out, append = T)
  exitLatest <- allTrainSchedule[!is.na(allTrainSchedule$exit_latest),c("route", "sequence_number", "exit_latest")]
  for(i in 1:nrow(exitLatest)) {
    cat("max(0, T",tt(exitLatest[i,"route"]),"_",exitLatest[i,"sequence_number"]," - ",exitLatest[i,"exit_latest"],") + ",sep = "", file=out, append = T)
  }
  
  entryLatest <- allTrainSchedule[!is.na(allTrainSchedule$entry_latest),c("route", "sequence_number", "entry_latest")]
  for(i in 1:nrow(entryLatest)) {
    # entry is in the minizinc model sequence number minus one as we only have exits and entry[i] == exits[i-1]
    cat("max(0, T",tt(entryLatest[i,"route"]),"_",(entryLatest[i,"sequence_number"] - 1)," - ",entryLatest[i,"entry_latest"],")",sep = "", file=out, append = T)
    if(i < nrow(entryLatest)) {
      cat(" + ", file=out, append = T)
    }
  }
  cat(";\n", file=out, append = T);
  
  cat("\n\nsolve minimize obj;\n\n\n", file=out, append = T)
  
  cat("start creating data for resource exclusion\n")
  resources$id <- sub("-","_",resources$id)
  dt <- data.table(resourcesForAllServices[,c("route", "resources", "sequence_number")])
  start <- reshape(dt[, list(sequence_number = min(sequence_number)), by = c("route", "resources")], idvar = "resources", timevar = "route", direction = "wide")
  end <- reshape(dt[, list(sequence_number = max(sequence_number)), by = c("route", "resources")], idvar = "resources", timevar = "route", direction = "wide")
  
  cat("start creating constraints for resource exclusion\n")
  #clusterExport(cl, c("start", "end", "resources", "tt"))
  exprs <- rbind.fill(lapply(1:nrow(start), function(i) {
    row <- unlist(start[i,-c("resources")])
    row <- row[!is.na(row)]
    endrow <- unlist(end[i,-c("resources")])
    endrow <- endrow[!is.na(endrow)]
    col <- length(row);
    names <- sub("^[^.]*.", "", names(row));
    releaseTime <- as.numeric(duration(resources[as.character(start[i,1]),"release_time"]))
    resource <-  sub("-","_",as.character(start[i,1]))
    t <- rep(1:col, col)
    df <- data.frame(t1 = t, t2 = t[order(t)])
    df <- df[df$t1 < df$t2,]
    if(nrow(df) > 0) {
      # df is empty if resource is only used by one train, so thats fine and we can skip this
      data.frame(T1 = tt(names[df$t1]), T1inSeq = row[df$t1]-1, T1outSeq = endrow[df$t1], 
                 T2 = tt(names[df$t2]), T2inSeq = row[df$t2]-1, T2outSeq = endrow[df$t2], 
                 release_time=releaseTime)
    } else {
      data.frame()
    }
  }))
  cat("end creating constraints for resource exclusion\n")
  exprs <- unique(exprs);
  
  times <- do.call(rbind.data.frame, lapply(allTrains, function(train) {
    list(
      "train" = train,
      "minTime" = min(allTrainSchedule[allTrainSchedule$route==train,"entry_earliest"], na.rm = T),
      "maxTime" = max(allTrainSchedule[allTrainSchedule$route==train,"exit_latest"], na.rm = T) + local_max_delay
    );
  }))
  l <- length(allTrains)
  t <- rep(1:l, l)
  df <- data.frame(t1 = t, t2 = t[order(t)])
  #df <- df[df$t1 < df$t2,]
  combinations <- cbind(times[df$t1,],times[df$t2,])
  names(combinations) <- c("train1","minTime1","maxTime1", "train2","minTime2","maxTime2")
  combinations$overlap <- !(combinations$maxTime1 < combinations$minTime2 | combinations$maxTime2 < combinations$minTime1)
  row.names(combinations) <- paste(tt(combinations$train1), tt(combinations$train2))
  exprs <- exprs[combinations[paste(exprs$T1, exprs$T2),"overlap"],]
  
  if(problem_instance == '07_V1.22_FWA') {
    # workaround for a bug in minizinc that only allows to read in 65'000 Lines: https://github.com/MiniZinc/libminizinc/issues/245#issuecomment-426482826
    cat(paste0("constraint (T",exprs$T1,"_",exprs$T1inSeq," >= T",exprs$T2,"_",exprs$T2outSeq," + ",exprs$release_time,") \\/ (T",exprs$T2,"_",exprs$T2inSeq," >= T",exprs$T1,"_",exprs$T1outSeq," + ",exprs$release_time,");"), sep = '   ', file=out, append = T);
  } else {
    cat(paste0("constraint (T",exprs$T1,"_",exprs$T1inSeq," >= T",exprs$T2,"_",exprs$T2outSeq," + ",exprs$release_time,") \\/ (T",exprs$T2,"_",exprs$T2inSeq," >= T",exprs$T1,"_",exprs$T1outSeq," + ",exprs$release_time,");"), sep = '\n', file=out, append = T);
  }
  cat("end writing creating constraints for resource exclusion\n")
  
  connections <- data.frame()
  for(i in 1:nrow(data$service_intentions)) {
    secReq <- data$service_intentions[i,"section_requirements"][[1]]
    locConnections <- secReq[!is.na.nullSave(secReq$connections),"connections"]
    if(length(locConnections) > 0) {
      for(j in 1:length(locConnections)) {
        if(is.data.frame(locConnections[[j]]))
          locConnections[[j]]$from_section_marker <- secReq$section_marker[j]
      }
      dfLocConnections <- do.call("rbind",locConnections[!unlist(lapply(locConnections, is.null))])
      dfLocConnections$from_service_intention <- data$service_intentions[i, "route"]
      connections <- rbind(connections, dfLocConnections)
    }
  }
  
  # write constraints for the connections
  if(nrow(connections) > 0) {
    for(i in 1:nrow(connections)) {
      TS1 <- connections[i,"from_service_intention"]
      TS2 <- connections[i,"onto_service_intention"]
      TS1entry <- -1 + allTrainSchedule[allTrainSchedule$route == TS1 & allTrainSchedule$section_marker == connections[i,"from_section_marker"],"sequence_number"]
      TS2exit <- allTrainSchedule[allTrainSchedule$route == TS2 & allTrainSchedule$section_marker == connections[i,"onto_section_marker"],"sequence_number"]
      cat("constraint T",tt(TS2),"_",TS2exit," - T",tt(TS1),"_",TS1entry," >= ",as.numeric(duration(connections[i,"min_connection_time"])),";\n",sep = "", file=out, append = T)
    }
  }
  
  return(allTrainSchedule)
}

# read model and transform to output format
writeModels <- function(problem_instance, allTrainSchedule, data) {
  print(problem_instance)
  
  if(!file.exists(paste0(dir_model_results, 'model_', problem_instance, '_result.txt'))) {
    print("result file for problem instance not found")
    return(-2)
  }
  
  resultsFromSolver <- lastResult(paste0(dir_model_results, 'model_', problem_instance, '_result.txt'));
  
  if(length(resultsFromSolver) < 10) return(-1)
  
  #in problem instance 5 there is a train called "TINT_01", change pattern
  pattern <- "T(.*)_([0-9]*) = ([0-9]*);"
  patReverseTT <- "_(.*)_(.*)"
  # in problem instance six there is a route named 77]877 this has to has a seperate naming
  patReverseTT2 <- "^([0-9]*)_(.*)"
  df <- data.frame(train = sub(pattern, "\\1", resultsFromSolver),
                   seq = as.numeric(sub(pattern, "\\2", resultsFromSolver)),
                   time = as.numeric(sub(pattern, "\\3", resultsFromSolver)))
  df[grepl(patReverseTT, df$train),"train"] <- sub(patReverseTT, "[\\1]\\2", df[grepl(patReverseTT, df$train),"train"])
  df[grepl(patReverseTT2, df$train),"train"] <- sub(patReverseTT2, "\\1]\\2", df[grepl(patReverseTT2, df$train),"train"])
  
  tM <- allTrainSchedule$route == df[df$seq > 0,"train"]
  allTrainSchedule[tM & allTrainSchedule$sequence_number == df[df$seq > 0,"seq"],"exit_time"] <- df[df$seq > 0,"time"]
  maxByTrain <- aggregate(seq ~ train, data = df, max)
  row.names(maxByTrain) <- maxByTrain$train
  df$seq.max <- maxByTrain[df$train, "seq"]
  exceptMaxByTrain <- df$seq != df$seq.max
  allTrainSchedule[tM & allTrainSchedule$sequence_number == (df[exceptMaxByTrain,"seq"]+1),"entry_time"] <- df[exceptMaxByTrain,"time"];
  
  allTrains <- unique(allTrainSchedule$route)
  result <- list(problem_instance_label = unbox(data$label), problem_instance_hash = unbox(data$hash[[1]]), hash = unbox(trunc(as.numeric(Sys.time()))), train_runs=data.frame())
  for(train in allTrains){
    df <- data.frame(service_intention_id = train)
    resultPath <- allTrainSchedule[allTrainSchedule$route == train, c("entry_time","exit_time","route","route_section_id","sequence_number","route_path","section_requirement")]
    resultPath$entry_time <- secondsToHms(resultPath$entry_time)
    resultPath$exit_time <- secondsToHms(resultPath$exit_time)
    df$train_run_sections <- list(resultPath)
    result$train_runs <- rbind(result$train_runs, df)
  }
  
  writeLines(toJSON(result, pretty=T, na='null'),paste0("./submision-result/model_",problem_instance,"_result.json"));
  
  all_exit_latest <- allTrainSchedule[!is.na(allTrainSchedule$exit_latest),]
  all_entry_latest <- allTrainSchedule[!is.na(allTrainSchedule$entry_latest),]
  (sum(all_exit_latest$exit_delay_weight * pmax(0,as.numeric(all_exit_latest$exit_time) - all_exit_latest$exit_latest)) + 
      sum(all_entry_latest$entry_delay_weight * pmax(0, as.numeric(all_entry_latest$entry_time) - all_entry_latest$entry_latest)) + 
      sum(allTrainSchedule$penalty, na.rm = T)
  )/60;
}

problem_instances <- c('01_dummy', '02_a_little_less_dummy', '03_FWA_0.125',
                       '04_V1.02_FWA_without_obstruction', '05_V1.02_FWA_with_obstruction', 
                       '06_V1.20_FWA', '07_V1.22_FWA', '08_V1.30_FWA', '09_ZUE-ZG-CH_0600-1200')
max_delay <- 60 * c(60,60,60,60,120,60,60,60,600) 
baseUrl <- 'https://raw.githubusercontent.com/crowdAI/train-schedule-optimisation-challenge-starter-kit/master/problem_instances/'
lapply(problem_instances, function(inst) if(!file.exists(paste0(dir_raw_data,inst,'.json'))) download.file(paste0(baseUrl,inst,'.json'),paste0(dir_raw_data,inst,'.json')))

log <- data.frame()
for(i in 1:length(problem_instances)) {
  problem_instance <- problem_instances[i]
  start <- Sys.time()
  data <- fromJSON(paste0(dir_raw_data, problem_instance, '.json'))
  model_file <- paste0(dir_minizinc_model,"model_",problem_instance,'.mzn')
  allTrainSchedule <- createMinizincModel(problem_instance, data, model_file, max_delay[i])
  end_model <- Sys.time()
  
  ## call minizinc
  fzn_file <- paste0(dir_flatzinc_model,"model_",problem_instance,'.fzn')
  system(paste0("MiniZincIDE-2.2.1-bundle-linux/bin/minizinc -c \"",model_file,"\" -O0 --fzn \"",fzn_file,"\""))
  end_compiling <- Sys.time()
  
  ## call or-tools flatzinc solver
  fzn_timelimit <- round(15*60 - as.numeric(difftime(Sys.time(), start, units = "sec")) - 15) 
  result_file <- paste0(dir_model_results, 'model_', problem_instance, '_result.txt')
  cat(paste0("or-tools_flatzinc_Ubuntu-18.04-64bit_v6.9.999/bin/fzn-or-tools -time-limit ",fzn_timelimit * 980," -r 123 -p ",number_of_cpu_cores," \"",fzn_file,"\" > \"",result_file,"\"\n"))
  system(paste0("or-tools_flatzinc_Ubuntu-18.04-64bit_v6.9.999/bin/fzn-or-tools -time-limit ",fzn_timelimit * 980," -r 123 -p ",number_of_cpu_cores," \"",fzn_file,"\" > \"",result_file,"\""))
  end_solver <- Sys.time()
  
  ## read result
  decision_result <- writeModels(problem_instance,allTrainSchedule, data)
  end_writing_result <- Sys.time()
  log <- rbind(log, data.frame(problem_instance,decision_result,start,end_model,end_compiling,end_solver,end_writing_result, total_time=as.numeric(difftime(end_writing_result, start, units = "sec")), solver_time=as.numeric(difftime(end_solver, end_compiling, units = "sec"))))
  
  write.csv(log, file="all_instances_log.csv")
}
