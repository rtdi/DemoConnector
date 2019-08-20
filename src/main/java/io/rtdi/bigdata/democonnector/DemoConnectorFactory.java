package io.rtdi.bigdata.democonnector;

import java.io.IOException;

import io.rtdi.bigdata.connector.connectorframework.BrowsingService;
import io.rtdi.bigdata.connector.connectorframework.ConnectorFactory;
import io.rtdi.bigdata.connector.connectorframework.Consumer;
import io.rtdi.bigdata.connector.connectorframework.Producer;
import io.rtdi.bigdata.connector.connectorframework.Service;
import io.rtdi.bigdata.connector.connectorframework.controller.ConnectionController;
import io.rtdi.bigdata.connector.connectorframework.controller.ConsumerInstanceController;
import io.rtdi.bigdata.connector.connectorframework.controller.ProducerInstanceController;
import io.rtdi.bigdata.connector.connectorframework.controller.ServiceController;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.properties.ServiceProperties;

public class DemoConnectorFactory extends ConnectorFactory<DemoConnectionProperties, DemoProducerProperties, DemoConsumerProperties> {

	public DemoConnectorFactory() {
		super("DemoConnector");
	}

	@Override
	public Consumer<DemoConnectionProperties, DemoConsumerProperties> createConsumer(ConsumerInstanceController instance) throws IOException {
		return null;
	}

	@Override
	public Producer<DemoConnectionProperties, DemoProducerProperties> createProducer(ProducerInstanceController instance) throws IOException {
		return new DemoProducer(instance);
	}

	@Override
	public DemoConnectionProperties createConnectionProperties(String name) throws PropertiesException {
		return new DemoConnectionProperties(name);
	}

	@Override
	public DemoConsumerProperties createConsumerProperties(String name) throws PropertiesException {
		return new DemoConsumerProperties(name);
	}

	@Override
	public DemoProducerProperties createProducerProperties(String name) throws PropertiesException {
		return new DemoProducerProperties(name);
	}

	@Override
	public BrowsingService<DemoConnectionProperties> createBrowsingService(ConnectionController controller) throws IOException {
		return new DemoBrowse(controller);
	}

	@Override
	public Service createService(ServiceController instance) throws PropertiesException {
		return null;
	}

	@Override
	public ServiceProperties<?> createServiceProperties(String servicename) throws PropertiesException {
		return null;
	}

	@Override
	public boolean supportsConnections() {
		return true;
	}

	@Override
	public boolean supportsServices() {
		return false;
	}

	@Override
	public boolean supportsProducers() {
		return true;
	}

	@Override
	public boolean supportsConsumers() {
		return true;
	}

	@Override
	public boolean supportsBrowsing() {
		return true;
	}

}
