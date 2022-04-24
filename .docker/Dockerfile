# Adapted from https://github.com/docker-library/cassandra/blob/master/4.0/Dockerfile

FROM eclipse-temurin:11-jre-focal

RUN groupadd -r cassandra --gid=999 \
 && useradd -r -g cassandra --uid=999 cassandra

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
     wget \
     libjemalloc2 \
     procps \
     iproute2 \
     numactl \
 && rm -rf /var/lib/apt/lists/* \
 && libjemalloc=$(readlink -e /usr/lib/*/libjemalloc.so.2) \
 && ln -sT $libjemalloc /usr/local/lib/libjemalloc.so \
 && ldconfig

ENV GOSU_VERSION 1.12
RUN dpkgArch="$(dpkg --print-architecture | awk -F- '{ print $NF }')" \
 && wget -O /usr/local/bin/gosu "https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-$dpkgArch" \
 && chmod +x /usr/local/bin/gosu \
 && gosu nobody true

ENV CASSANDRA_HOME /opt/cassandra
ENV CASSANDRA_CONF /etc/cassandra
ENV PATH $CASSANDRA_HOME/bin:$PATH

RUN mkdir -p $CASSANDRA_HOME \
     $CASSANDRA_CONF \
     /var/lib/cassandra \
     /var/log/cassandra

COPY ./dist $CASSANDRA_HOME

RUN chmod +x $CASSANDRA_HOME/bin/cassandra $CASSANDRA_HOME/bin/nodetool \
 && chown -R cassandra:cassandra $CASSANDRA_CONF /var/lib/cassandra /var/log/cassandra \
 && chmod 777 $CASSANDRA_CONF /var/lib/cassandra /var/log/cassandra \
 && chmod -R a+rwX $CASSANDRA_CONF \
 && mv $CASSANDRA_HOME/conf/* $CASSANDRA_CONF \
 && rm -rf $CASSANDRA_HOME/conf \
 && ln -sT $CASSANDRA_CONF $CASSANDRA_HOME/conf \
 && ln -sT /var/lib/cassandra $CASSANDRA_HOME/data \
 && ln -sT /var/log/cassandra $CASSANDRA_HOME/logs

COPY ./.docker/entrypoint.sh /usr/local/bin/
ENTRYPOINT ["entrypoint.sh"]

EXPOSE 7000 7001 7199 9042 9160
CMD ["cassandra", "-f"]
