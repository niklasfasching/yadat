FROM openjdk:8
# http://subhasingh.com/blog/How-to-Setup-Datomic-Free/

ENV DATOMIC_VERSION 0.9.5697

# RUN curl rather than ADD to cache download
RUN curl https://my.datomic.com/downloads/free/${DATOMIC_VERSION} --location --output /tmp/datomic.zip
RUN unzip /tmp/datomic.zip && rm /tmp/datomic.zip
WORKDIR datomic-free-${DATOMIC_VERSION}

RUN cp ./config/samples/free-transactor-template.properties transactor.properties
RUN sed "s/host=localhost/host=0.0.0.0/" -i transactor.properties
RUN sed "/host=0.0.0.0/a alt-host=172.17.0.1" -i transactor.properties

EXPOSE 4334 4335 4336

CMD "bin/transactor" "transactor.properties"
