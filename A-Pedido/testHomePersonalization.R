setwd("/home/despegar/rstudio_data/vseminario/A pedido")

#### Tomo muestra de visitas a HOME ####
# funcion para bajar reducciones de BI
source("/home/despegar/rstudio_data/vseminario/Funciones/funcion get reduction Vero.R")

# Descargamos la data
getReduction2("FLOW","HOME",20160628)

# Levantamos la data
dataR <- read.delim("/home/despegar/rstudio_data/vseminario/A pedido/20160628-DATA_UPA_FLOW_HOME.txt", sep="\t", header =T)

# pongo formato adecuado a las columnas
dataR <- data.frame(lapply(dataR, as.character), stringsAsFactors=FALSE)

# descarto trackerids duplicados
dataR <- subset(dataR, !duplicated(dataR$userid)) 

# veo de sources que 17% es appmobile, 82,2% es Despegar (web + mobile web, asumo) 
table(dataR$source)

# Me quedo con 10000 usuarios que pasaron por la home (excluyendo app)
desktop <- subset(dataR, source=="Despegar")
smp.size <- 10000
smp.ind <- sample(seq_len(nrow(desktop)), size = smp.size)
desktop_sample <- desktop[smp.ind, ]

# me quedo con menos columnas
desktop_sample <- subset(desktop_sample, select=c("FECHA", "HORA", "userid", "product", "country"))
setnames(desktop_sample, c("FECHA_visit", "HORA_visit", "userid", "product_visit", "country_visit"))

# necesito saber el timestamp del momento en que entraron a la home para pedir la user history anterior a eso
# pido a hestia que use la user history hasta el dia anterior
max.ts <- 1466985601000 # 27/06/2016 00:00:01 GMT

#### 1) Consulto USER HISTORY ####
# Busco en el user history de los tracker ids originales
source("/home/despegar/rstudio_data/vseminario/Funciones/tabulate user history.R")

variables1 <- c("datetime", "pr", "fl", "dc", "pul", "reg")
amount1 <- 100

clus <- makeCluster(10)
clusterExport(clus, list("tabulateJSON", "fromJSON", "getURL", "tabulateUserHistory", "variables1", "amount1", "max.ts"), envir=environment())
data.uhist <- parLapply(clus, desktop_sample$userid, function(x) tabulateUserHistory(x, variables=variables1, amount=amount1, to=max.ts))
stopCluster(clus)
data.uhist <- do.call(rbind.data.frame, data.uhist)
data.uhist <- subset(data.uhist, is.na(user)==F)
data.uhist <- data.frame(lapply(data.uhist, as.character), stringsAsFactors=FALSE)

save(data.uhist, file="uhist_desktop 10k personalizados.Rda")

data.uhist$destino <- toupper(ifelse(is.na(data.uhist$dc)==FALSE, data.uhist$dc,
                                     ifelse(is.na(data.uhist$pul)==FALSE, data.uhist$pul, data.uhist$reg)))

data.uhist$dc <- NULL
data.uhist$pul <- NULL
data.uhist$reg <- NULL

save(data.uhist, file="uhist_desktop 10k personalizados.Rda")

#### 2) Consulto a HESTIA #### 
# Busco cual seria el contenido de la home para ese usuario con data de user history hasta max.ts
source("/home/despegar/rstudio_data/vseminario/Funciones/funcion get home personalization.R")

clus <- makeCluster(10)
clusterExport(clus, list("fromJSON", "getURL", "getHomePersonalization", "amount1", "max.ts"), envir=environment())
data.home <- parLapply(clus, desktop_sample$userid, function(x) getHomePersonalization(x, amount=amount1, to=max.ts))
stopCluster(clus)
data.home <- do.call(rbind.data.frame, data.home)
data.home <- data.frame(lapply(data.home, as.character), stringsAsFactors=FALSE)

write.csv(data.home, file="home_desktop 10k users personalizados.csv")

# analizo la home que se dibujaria para cada muestra de 10k
table(data.home$expectedHome)
table(data.home$timeout)

#### 3) Consulto el GRAFO ####
# Busco en el grafo los social id, mails y otros tracker ids asociados que tengo de esa persona
# puede que se hayan creado asociaciones posteriores a la navegacion de home pero no puedo filtrar eso
source("/home/despegar/rstudio_data/vseminario/Funciones/funcion get from grafo.R")

clus <- makeCluster(10)
clusterExport(clus, list=c("getIdsFromGrafo", "fromJSON", "getURL"))
data.grafo <- parLapply(clus, desktop_sample$userid, getIdsFromGrafo)
stopCluster(clus)
data.grafo <- do.call(rbind.data.frame, data.grafo)
data.grafo <- data.frame(lapply(data.grafo, as.character), stringsAsFactors=FALSE)
data.grafo <- subset(data.grafo, is.na(persona)==F)

save(data.grafo, file="grafo_desktop 10k personalizados.Rda")



#### 4) ANALISIS ####
# sacar los trackeames
# sacar uniques de mail y userid
# hacer un dcast con count
#load("/home/despegar/rstudio_data/vseminario/A pedido/uhist 10k personalizados.Rda")
#load("/home/despegar/rstudio_data/vseminario/A pedido/data grafo 10k personalizados.Rda")

library(reshape2)
count.id.types <- dcast(data.grafo, persona ~ type, fun.aggregate = length, value.var = "id")
count.id.types <- subset(count.id.types , !duplicated(count.id.types$persona)) 


# hacer un count de la cantidad de flujos, destinos, productos UNICOS para cada userid con el uhist
data.uhist <- data.table(data.uhist)
count.diff.prfldc <-  data.uhist[ , lapply(.SD , function(x) length(unique(na.omit(x))) ), by=user , .SDcols=c(3,4,5) ]

# agregar la cantidad de acciones (veces que aparece cada userid)
count.diff.actions <- as.data.frame(table(data.uhist$user))
count.diff.actions <- merge(count.diff.actions, count.diff.prfldc, by.x="Var1", by.y="user", all.x=T)

setnames(count.diff.actions, c("user", "actions.count", "unique.products", "unique.flow", "unique.destinations"))

# cuantos usuarios tienen grafo y cuantos user history?
users_grafo <- subset(data.grafo , !duplicated(data.grafo$user.consultado), select=c("user.consultado", "persona")) 
users_uhist <- subset(data.uhist , !duplicated(data.uhist$user), , select=c("user")) 

#### Armado TABLA FINAL ####
# merge de: los 10k users iniciales con la data dcasteada de grafo con la data dcasteada de uhist
#count.id.types <- merge(users_grafo, count.id.types, by="persona" )

# identifico el id de persona de cada user id de mi muerstra de 10k
desktop_sample <- merge(desktop_sample, users_grafo, by.x="userid", by.y="user.consultado", all.x=T)

# a partir del id de persona, hago merge con count.id.types
desktop_sample <- merge(desktop_sample, count.id.types, by="persona", all.x=T)

# a partir del userid, hago merge con count.actions
desktop_sample <- merge(desktop_sample, count.diff.actions, by.x="userid", by.y="user", all.x=T)

# agrego la home que se dibujaria para mi muerstra de 10k
desktop_sample <- merge(desktop_sample, data.home, by="userid", all.x=T)

# tiene history?
desktop_sample$tiene.history <- ifelse(is.na(desktop_sample$actions.count)==T, "no history", "tiene history")

# aparece en el grafo?
desktop_sample$tiene.grafo <- ifelse(desktop_sample$UPA==0 | is.na(desktop_sample$persona)==T, "no grafo", "tiene grafo")

# tiene email?
desktop_sample$tiene.email <- ifelse(desktop_sample$EMAIL==0 | is.na(desktop_sample$persona)==T, "no tiene email", "tiene email")

# tiene social?
desktop_sample$tiene.social <- ifelse(desktop_sample$SOCIAL==0 | is.na(desktop_sample$persona)==T, "no tiene social", "tiene social")

# busco al menos un destino?
desktop_sample$unomas.destinos <- ifelse(desktop_sample$unique.destinations==0  | is.na(desktop_sample$unique.destinations)==T, "0 destinos", "1 o mas destinos")


write.csv(desktop_sample, file="desktop 10k users personalizados.csv")


desktop_sample$"NA" <- NULL



# Busco cual seria el contenido de la home para ese usuario con data de user history hasta max.tx
source("/home/despegar/rstudio_data/vseminario/Funciones/funcion get home personalization.R")

amount1 <- 100
max.ts2 <- 1463976000000

clus <- makeCluster(10)
clusterExport(clus, list("fromJSON", "getURL", "getHomePersonalization", "amount1", "max.ts2"), envir=environment())
data.home5 <- parLapply(clus, sample$userid, function(x) getHomePersonalization(x, amount=amount1, to=max.ts2))
stopCluster(clus)
data.home5 <- do.call(rbind.data.frame, data.home5)
data.home <- data.frame(lapply(data.home, as.character), stringsAsFactors=FALSE)

# agrego la home que se dibujaria para mi muerstra de 10k
sample <- merge(sample, data.home, by="userid", all.x=T)

write.csv(sample, file="10k users personalizados.csv")

#### Next Step ####
# que pasaria si la home consultara todos los upa o emails asociados a cada persona?
data.grafo2 <- subset(data.grafo, type!="TRACKEAME")
data.grafo2 <- subset(data.grafo2, !duplicated(data.grafo2$id)) 

# Consulto USER HISTORY #
# Busco en el user history de los tracker ids originales
source("/home/despegar/rstudio_data/vseminario/Funciones/tabulate user history.R")

variables1 <- c("datetime", "pr", "fl", "dc", "pul", "reg")
amount1 <- 100

clus <- makeCluster(10)
clusterExport(clus, list("tabulateJSON", "fromJSON", "getURL", "tabulateUserHistory", "variables1", "amount1", "max.ts"), envir=environment())
data.uhist2 <- parLapply(clus, data.grafo2$id, function(x) tabulateUserHistory(x, variables=variables1, amount=amount1, to=max.ts))
stopCluster(clus)
data.uhist2 <- do.call(rbind.data.frame, data.uhist2)
data.uhist2 <- subset(data.uhist2, is.na(user)==F)
data.uhist2 <- data.frame(lapply(data.uhist2, as.character), stringsAsFactors=FALSE)

save(data.uhist2, file="uhist_allgrafo 10k personalizados.Rda")

data.uhist2$destino <- toupper(ifelse(is.na(data.uhist2$dc)==FALSE, data.uhist$dc,
                                      ifelse(is.na(data.uhist2$pul)==FALSE, data.uhist$pul, data.uhist$reg)))

data.uhist2$dc <- NULL
data.uhist2$pul <- NULL
data.uhist2$reg <- NULL

save(data.uhist, file="uhist_desktop 10k personalizados.Rda")
