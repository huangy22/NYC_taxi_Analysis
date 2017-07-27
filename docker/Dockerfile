# Use an official Python runtime as a parent image
FROM ubuntu
#FROM 

# Set the working directory to /app
#WORKDIR /notebook

# Copy the current directory contents into the container at /app
#ADD . /notebook

# Install any needed packages specified in requirements.txt

#RUN /opt/conda/bin/conda install --quiet --yes \
    #'jupyter' \
    #'pandas' \
    #'seaborn' \
    #'shapely' \
    #'fiona' \
    #'bokeh' \
    #'sqlite3' \
    #'sqlalchemy' \
    #&& conda clean -tipsy

RUN apt-get update \
 && apt-get install -yq --no-install-recommends \
    wget \
    bzip2 \
    ca-certificates \
    build-essential \
    python \
    python-dev \
    libgeos-dev \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

RUN wget https://bootstrap.pypa.io/get-pip.py \
    && python get-pip.py \
    && rm get-pip.py

RUN pip install jupyter pandas seaborn shapely fiona bokeh sqlalchemy geopandas

# Make port 80 available to the world outside this container
#EXPOSE 8888

# Define environment variable
ENV NAME jupyter

# Run app.py when the container launches
#CMD ["juypter", "notebook","--allow-root"]
#CMD ["./start.sh"]
