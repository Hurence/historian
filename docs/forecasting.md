

# Forecasting

## Methodology
état de l’art des algorithmes et méthodes
implémentation
déploiement & test
datviz
Chunk Méta informations
calcul des FFT d’un chunk et d’une série de chunks
trouver la fréquence temporelle la plus appropriée (1/Y 1/M, 1/10D, 1/D, 1/12H , 1/H, ….)
trouver une saisonnalité
trouver un bruit

## Workflow
workflow de prédiction (select metric, compute meta-inf, propose algo + params, time range/window, remove outliers, intervalle de confiance) => job conf
implémentation dans l’api SPARK => job implem
run de la prédiction (LSTM, TensorFlow, ARIMA, reg linéaire) & stockage des prédictions dans solr
affichage des prédictions dans Grafana
suppression des prévisions obsolètes
affichage de la performance des prévisions
ajout d’alertes sur seuils de valeurs réelles ou de prévisions
Compaction
lancer la compaction en fonction de la fréquence temporelle de la métrique


## Time series topologies

patterns and morphologies of time series
ex : trending, cyclic, monotonic, noisy, big gaps

## State of the art
Algorithm & methods

add columns :

- paper link (bilbliography)
- existing implementations ( ex tensorflow v1.23)
- applicability to wich topologies
- inference live or pre-computed ?
- training cost (gpus ? many cpus ? big memory)
- parrallelized ?
- inference time ratio for n points
- ease of use (hyper-parameters settings ?)
- one day's game
- a-priori complexity score (1 to 10)
- max points precision (the number of acceptable forecated values)