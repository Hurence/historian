

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
ex : trending, cyclic, monotonic, noisy, big gaps, seasonal; bounding

Even if a dataset seems to be chaotic, we can study its morphology to find some patterns or some specificities. The three patterns we can encounter are:

**Trending**: when there is a long-term increase or decrease in the data (e.g. exponential, logarithm, linear...)  
**Seasonal or Periodic**: series are influenced by seasonal factors. We always know the seasonality, it is a fixed period.  
**Cyclic**: data are redundant over periods that are not fixed. We do not know the length of the current period (but in general it is cycles of more than  years).

[comment]: ![](https://otexts.com/fpp2/fpp_files/figure-html/a10-1.png)

[comment]: <> (Here, there is a clear and increasing trend. There is also a strong seasonal pattern that increases in size as the level of the series increases.)

[comment]: ![](https://robjhyndman.com/hyndsight/2011-12-14-cyclicts_files/figure-html/unnamed-chunk-2-1.png)

[comment]: <> (There is strong seasonality within each year, as well as some strong cyclic behaviour with period about 6–10 years.)

We can also define the time series with other adjectives.

**Noisy**: data with some unwanted modifications due to the capture/storage/transmission of the data.  
**Monotonic**: data is always evolving in the same way (increasing or decreasing).  
**Big Gaps**: there is some big missing gaps in the data, because data are missing or because there is no data during some periods (e.g. animal hibernation, migration...)  
**Big Changes**: 

[comment]: ![](https://www.researchgate.net/publication/333437211/figure/fig1/AS:763708615700481@1559093731898/The-noisy-signal-and-the-noiseless-signal.png)
[comment]: ![](https://upload.wikimedia.org/wikipedia/commons/3/32/Monotonicity_example1.png)



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


### Linear Regression
[**Linear regression**](https://en.wikipedia.org/wiki/Linear_regression) is a linear approach to modelling the relationship between the variable(s) and a scalar response.

|  Pros  | Cons | Existing implementations | topologies | inference live ? | training cost | parallelization | inference time | ease to use | one day's game | complexity score | point precision |
|---------|------|--------------------------|------------|------------------|---------------|-----------------|----------------|-------------|----------------|------------------|-----------------|
|<li>Linear combinations are easy to implement <li>Guarantee to find optimal weights<li>A lot of information on the Internet|The linear combinations oversimplify the reality|[Tensorflow Core v2.5.0](https://www.tensorflow.org/api_docs/python/tf/estimator/LinearRegressor) <p>[sklearn v0.24.2](https://scikit-learn.org/stable/modules/generated/sklearn.linear_model.LinearRegression.html)||||||||||



### Support-Vector Machine (SVM)
[**Support-Vector Machine**](https://en.wikipedia.org/wiki/Support-vector_machine) is a supervised machine learning algorithm, it is based on the idea of finding a hyperplane that best divides a dataset into two classes.

|  Pros  | Cons | Existing implementations | topologies | inference live ? | training cost | parallelization | inference time | ease to use | one day's game | complexity score | point precision |
|--------|------|--------------------------|------------|------------------|---------------|-----------------|----------------|-------------|----------------|------------------|-----------------|
|<li>Work well if there is a clear separation between the classes <li>Is more effective in high dimensional spaces|<li>Is not suitable for large dataset <li>Does not perform well when data are noisy|[Tensorflow Core v2.5.0](https://stackoverflow.com/questions/49465891/building-an-svm-with-tensorflow) <p>[sklearn v0.24.2](https://scikit-learn.org/stable/modules/generated/sklearn.svm.SVC.html)||||||||||


### Simple Exponential Smoothing (SES)
[**Simple Exponential Smoothing**](https://en.wikipedia.org/wiki/Exponential_smoothing) is a technique for smoothing time series using the exponential window function.

|  Pros  | Cons | Existing implementations | topologies | inference live ? | training cost | parallelization | inference time | ease to use | one day's game | complexity score | point precision |
|--------|------|--------------------------|------------|------------------|---------------|-----------------|----------------|-------------|----------------|------------------|-----------------|
|Used of a trend projection technique|<li> Does not perform well on seasonal dataset and short-term dataset <li>Smoothing process looses some data|[Tensorflow Core v2.5.0](https://www.tensorflow.org/api_docs/python/tf/estimator/LinearRegressor) <p>[statsmodels v0.12.2](https://www.statsmodels.org/stable/examples/notebooks/generated/exponential_smoothing.html) ||||||||||


### Convolutional Neural Networks (CNN)
[**Convolutional Neural Networks**](https://en.wikipedia.org/wiki/Convolutional_neural_networks) is a neural network with not all its neurons fully connected with these of the adjacent layer, it uses convolution.

|  Pros  | Cons | Existing implementations | topologies | inference live ? | training cost | parallelization | inference time | ease to use | one day's game | complexity score | point precision |
|--------|------|--------------------------|------------|------------------|---------------|-----------------|----------------|-------------|----------------|------------------|-----------------|
|Very useful for visual imagery data|<li>Requires a large dataset to train <li>Slow inference due to the convolution|[Tensorflow Core v2.5.0](https://www.tensorflow.org/tutorials/images/cnn) ||||||||||


### Radial Basis Function Neural Networks (RBFNN)
[**Radial Basis Function Neural Networks**](https://en.wikipedia.org/wiki/Radial_basis_function_network) is a neural network that uses radial basis functions as activation functions. The **RBFNN** transforms the input signal into another form, which can be then feed into the network to get linear separability.  
A radial basis function is a function whose value depends only on the distance between the input, and a fixed point ( f(x) = f(||x – c||) ).

|  Pros  | Cons | Existing implementations | topologies | inference live ? | training cost | parallelization | inference time | ease to use | one day's game | complexity score | point precision |
|--------|------|--------------------------|------------|------------------|---------------|-----------------|----------------|-------------|----------------|------------------|-----------------|
|<li>Strong tolerance to input noise <li>


### Long Short-Term Memory (LSTM)
[**LSTM**](https://penseeartificielle.fr/comprendre-lstm-gru-fonctionnement-schema/) is a recurrent neural network using feedback connections to extend to a long-term memory in addition with the short-term memory of the RNNs. It is a cell composed by three gates: the forget gate, the input gate, and the output gate, there are also a cell state and a hidden state.

|  Pros  | Cons | Existing implementations | topologies | inference live ? | training cost | parallelization | inference time | ease to use | one day's game | complexity score | point precision |
|--------|------|--------------------------|------------|------------------|---------------|-----------------|----------------|-------------|----------------|------------------|-----------------|


### Gated Recurrent Unit (GRU)
[**GRU**](https://penseeartificielle.fr/comprendre-lstm-gru-fonctionnement-schema/) is like a LSTM but with only two gates, a reset gate, and an update gate.

|  Pros  | Cons | Existing implementations | topologies | inference live ? | training cost | parallelization | inference time | ease to use | one day's game | complexity score | point precision |
|--------|------|--------------------------|------------|------------------|---------------|-----------------|----------------|-------------|----------------|------------------|-----------------|


#### (Seasonal) AutoRegressive Integrated Moving Average (SARIMA)

The ARIMA process is a combination of different simpler processes:  
- the AutoRegressive (AR) can explain a time series at t time using the previous terms,
- the Moving Average (MA)  can explain a time series at t time like a linear combination of random errors,
- the Integration permit to stationarize the series.

The combination of the AR and the MA process is called the ARMA and thanks to the integration process we can generalize this process to the non-stationary time series to obtain the ARIMA model.

ARIMA needs three input values, ARIMA(p, d, q):
- p is the number of autoregressive terms,
- d is the number of nonseasonal differences needed for stationarity,
- q is the number of lagged forecast errors in the prediction equation.

If there is some seasonal element in the data set we can also extend to the SARIMA model whose needs three more input values, SARIMA(p, d, q, P, D, Q):
- P is the number of seasonal autoregressive terms,
- D is the number of seasonal differences,
- Q is the number of seasonal moving-average terms.


|  Pros  | Cons | Existing implementations | topologies | inference live ? | training cost | parallelization | inference time | ease to use | one day's game | complexity score | point precision |
|--------|------|--------------------------|------------|------------------|---------------|-----------------|----------------|-------------|----------------|------------------|-----------------|


### Prophet

|  Pros  | Cons | Existing implementations | topologies | inference live ? | training cost | parallelization | inference time | ease to use | one day's game | complexity score | point precision |
|--------|------|:--------------------------:|------------|------------------|---------------|-----------------|----------------|-------------|----------------|------------------|-----------------|
| | |[Prophet](https://facebook.github.io/prophet/docs/quick_start.html#python-api) | Facebook : <li>big changes<li>big gaps