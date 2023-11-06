# Introduction 
This is a datasummary repo. 

# How It Works
1. Config has connections to MongoDB and SQL databases which are used to fetch the required data for summarization
2. Start processing for summarization.

# Architecture
![Architectural Flow](postprocessing/images/postprocess.png)

1. summarization API is hosted using uvicorn
2. To make the alertsummarization fast, we are running multiple thread pool to process the request faster
3. Summarization is performed, and the results are saved into a final DataFrame and saved in the db.

# Dependency
1. This Module is dependent on the https://tatacommiot@dev.azure.com/tatacommiot/Video%20Based%20IoT/_git/vd-iot-dataapiservice
2. This module will be dependent on post process service

# Installation
1. Install Python3.9 
2. poetry install

# Run App
1. python app.py

# Docker 
1. Containirization is enabled
2. change the config.yaml
2. Navigate to the Dockerfile level
2. build the container (sudo docker build -t "summarization")
3. Run the container (sudo oocker run -t "summarization")