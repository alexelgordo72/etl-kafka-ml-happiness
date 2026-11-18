# ETL y Machine Learning: World Happiness Report

**Autor:** Alexander Mosquera Fajardo

## Descripción del Proyecto
Este proyecto implementa un pipeline de datos de extremo a extremo que extrae datos históricos del *World Happiness Report* (2015-2019) desde un clúster de Apache Kafka, realiza un análisis exploratorio (EDA), entrena un modelo predictivo de regresión y carga los resultados en una base de datos PostgreSQL remota. Se priorizó el aislamiento de entornos desplegando la base de datos en una máquina virtual dedicada en Google Cloud Platform.

## Arquitectura y Tecnologías
* **Extracción:** Apache Kafka (Azure) mediante `confluent_kafka`.
* **Procesamiento y EDA:** Python (Pandas, Matplotlib, Seaborn).
* **Machine Learning:** Scikit-Learn (`RandomForestRegressor`, `Pipeline`, `StandardScaler`).
* **Carga de Datos:** PostgreSQL alojado de forma aislada en instancia de GCP (`etl-ws3`) conectado vía `SQLAlchemy`.

## Fases de Ejecución

### 1. Data Extraction
Se desarrolló un consumidor conectado al broker de Kafka en Azure mediante autenticación SASL_SSL/SCRAM-SHA-512. Se extrajeron exitosamente 782 registros acumulando los mensajes JSON en memoria y exportándolos a un archivo crudo para evitar sobrecargar el broker durante la fase de análisis.

### 2. Data Preparation y EDA
El dataset original presentó 33 variables dispersas generadas por cambios de nomenclatura en los esquemas a través de los años (ej. de `Family` a `Social support`).
* **Unificación:** Se implementó una lógica de mapeo de esquemas (*Schema Mapping*) para consolidar las columnas históricas en 9 características estandarizadas.
* **Imputación:** Se detectó 1 único valor nulo en la variable `Corruption`, el cual fue imputado utilizando la mediana de la columna para preservar la distribución estadística.
* **Hallazgos (EDA):** Las matrices de correlación confirmaron que variables como el PIB per cápita (`GDP_per_Capita`), el apoyo social (`Social_Support`) y la esperanza de vida (`Life_Expectancy`) poseen una fuerte correlación positiva (>0.70) con el puntaje de felicidad, confirmándolas como predictoras robustas.

### 3. Model Training
Se estructuró el entrenamiento separando los datos en 80% (entrenamiento) y 20% (prueba).
* **Algoritmo:** Se eligió un modelo basado en árboles (`RandomForestRegressor`) debido a su capacidad para capturar relaciones complejas e interacciones no estrictamente lineales evidenciadas en el EDA.
* **Prevención de Data Leakage:** Se implementó un `Pipeline` de Scikit-Learn que integra un `StandardScaler`, garantizando que la transformación de variables se ajuste exclusivamente utilizando los datos del conjunto de entrenamiento.

### 4. Model Evaluation
El modelo fue evaluado utilizando el conjunto de prueba (datos nunca antes vistos por el algoritmo), obteniendo un excelente rendimiento predictivo:
* **MAE:** 0.4051
* **MSE:** 0.2816
* **RMSE:** 0.5307
* **R²:** 0.7817

El modelo explica el 78.17% de la varianza en los niveles de felicidad basándose únicamente en los 6 factores socioeconómicos principales. Como trabajo futuro, se recomienda implementar validación cruzada para ajuste fino de hiperparámetros e incluir variables categóricas regionales mediante *One-Hot Encoding*.

### 5. Data Loading
Las predicciones consolidadas junto con los valores reales se cargaron automáticamente mediante un motor `SQLAlchemy` a la tabla `happiness_predictions` en la base de datos PostgreSQL alojada en Google Cloud Platform, permitiendo que la información quede disponible para su consumo por plataformas analíticas de inteligencia de negocios.o

