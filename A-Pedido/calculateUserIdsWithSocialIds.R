setwd("/home/despegar/rstudio_data/vseminario/A pedido")

# Levanto tabla con userids de todos los que se loggearon en el sitio el 18/julio/2016
data <- read.delim("/home/despegar/rstudio_data/vseminario/A pedido/users_logged_in_1Day.csv", sep=",", header =T)
data <- subset(data, !duplicated(data$userid))
data$userid <- as.character(data$userid)

# Me quedo con 10000 usuarios que se loggearon 
smp.size <- 10000
smp.ind <- sample(seq_len(nrow(data)), size = smp.size)
data_sample <- data[smp.ind, ]

# Consulto el GRAFO 
# Busco en el grafo los social id, mails y otros tracker ids asociados que tengo de esa persona
source("/home/despegar/rstudio_data/vseminario/Funciones/funcion get from grafo.R")

rel_type1 <- "SOCIAL"

clus <- makeCluster(10)
clusterExport(clus, list("getGrafoRelationships", "fromJSON", "getURL", "rel_type1"), envir=environment())
data.grafo <- parLapply(clus, data_sample$userid, function(x) getGrafoRelationships(x, relationship_type=rel_type1))
stopCluster(clus)
data.grafo <- do.call(rbind.data.frame, data.grafo)
data.grafo <- data.frame(lapply(data.grafo, as.character), stringsAsFactors=FALSE)
data.grafo <- subset(data.grafo, is.na(persona)==F)

save(data.grafo, file="grafo 10k users - count social ids.Rda")

data.grafo <- data.table(data.grafo)
count.socialids <-  data.grafo[ , lapply(.SD , function(x) length(unique(na.omit(x))) ), by=persona , .SDcols=c(4) ]

data.grafo <- merge(data.grafo, count.socialids, by="persona", all.x=T)
setnames(data.grafo, c("persona", "user.consultado", "type", "id", "socialid.count"))

data.grafo <- merge(data_sample, data.grafo, , by.y="user.consultado", by.x="userid", all.x=T)


