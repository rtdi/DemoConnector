FROM rtdi/connectorrootapp

LABEL maintainer="info@rtdi.io"

ADD https://raw.githubusercontent.com/rtdi/RTDIDemoConnector/master/docker/global.properties conf/rtdiconfig/DemoConnector/
ADD https://github.com/rtdi/RTDIDemoConnector/releases/latest/download/democonnector.war ./webapps/

EXPOSE 8080
EXPOSE 8443


CMD ["appcontainer.sh"]
