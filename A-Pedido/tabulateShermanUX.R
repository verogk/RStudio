experienceDF <- data.frame()

for (i in seq(7200000, 7198000, -1000)){
  
  print(paste("The interval is ", i, "to ", i-1000))
  
  url <- paste0("http://10.2.7.6/v3/reviews?type=experience&states=all&limit=", i, "&offset=", i - 1000)
  
  json <- try(fromJSON(getURL(url, httpheader= c('X-Client' = "RM-analytics"))),silent = T)
  
  if(class(json) == "try-error" | class(json) == "AsIs" | length(json) == 0 ) {
    return(NA)  
  } 
  
  else{
    json.un <- unlist(json)
    
    start.obj <- "items.id"
    columns <- unique(names(json.un))[1:28]
    colnames <- gsub("items.","",columns)
    
    start.ind <- grep(start.obj, names(json.un))
    
    col.indexes <- lapply(columns, grep, names(json.un))
    # col.indexes es una lista de indices. hay un indice por variable que pido al user history. 
    # y los indices ubican las posiciones de esas variables en el json unlisteado.
    
    col.position <- lapply(1:length(columns), function(x) findInterval(col.indexes[[x]], start.ind))
    # Para cada variable: me dice, si cortáramos el user history unlisteado en segmentos (intervalos) usando los indices de userId 
    # (los del vector start.ind), adentro de cuales intervalos aparece esa variable. (las variable de fecha retiro del auto no aparece en un trackeo de hoteles, por ejemplo)
    # intervalo es uno por trackeo / accion del usuario
    
    temp.frames <- lapply(1:length(columns), function(x) data.frame(pos = col.position[[x]], ind = json.un[col.indexes[[x]]], stringsAsFactors = F))
    # temp.frames es una lista de dataframes: cada dataframe corresponde a una variable del user history. 
    # cada dataframe me dice en qué intervalos aparece cada variable y cual es su valor. 
    
    # el problema es que en cada intervalo una variable podria aparecer mas de una vez (POR UN ERROR DE TRACKEO. ejemplo: que un trackeo de un detail de un hotel tenga dos veces el campo ci)
    
    collapse.cols <- which(sapply(temp.frames, nrow) > length(start.ind))
    print(paste("The columns with errors in interval ", i, "to ", i-1000, "are ", collapse.cols))
    
    #### agregado Vero para evitar los warnings del merge (porque se duplican los nombres de las columnas)
    for (i in seq_along(temp.frames)){
      colnames(temp.frames[[i]]) <- c("pos",colnames[i])
    }
    #######
    
    matr <- Reduce(function(...) merge(...,all=T,by="pos"),temp.frames)
    matr$pos <- NULL
    names(matr) <- colnames
    matr <- as.matrix(matr)
    colnames(matr) <- colnames
    matr <- as.data.frame(matr)
    matr$interval <- i
    experienceDF<- rbind(experienceDF,matr)
  }
}

#experienceDF = do.call(rbind, experienceList)

experienceDF$date <- as.Date(strptime(as.character(experienceDF$reviewed_date), "%Y-%m-%dT%H:%M:%SZ"))

max(experienceDF$date)
min(experienceDF$date)




