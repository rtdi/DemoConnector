FROM rtdi/connectorrootapp

LABEL maintainer="info@rtdi.io"

ADD https://raw.githubusercontent.com/rtdi/DemoConnector/master/docker/global.properties conf/rtdiconfig/DemoConnector/
ADD https://github.com/rtdi/DemoConnector/releases/latest/download/democonnector.war ./webapps/

EXPOSE 8080
EXPOSE 8443


CMD ["appcontainer.sh"]
