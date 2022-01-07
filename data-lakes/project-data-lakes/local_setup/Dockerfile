FROM jupyter/pyspark-notebook
USER root

# add addtionally needed packages
# RUN apt-get update && apt-get install python3-boto3 -y

# Install Python requirements
COPY requirements.txt /home/jovyan/
RUN pip install -r /home/jovyan/requirements.txt

COPY jupyter_lab_config.json /home/jovyan/

# Run the notebook
#CMD ["/opt/conda/bin/jupyter", "lab", "--config", "/jupyter_lab_config.json"]
