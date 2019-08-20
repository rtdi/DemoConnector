package io.rtdi.bigdata.democonnector;

import io.rtdi.bigdata.connector.pipeline.foundation.TopicName;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.properties.ConsumerProperties;

public class DemoConsumerProperties extends ConsumerProperties {

	public DemoConsumerProperties(String name) throws PropertiesException {
		super(name);
	}

	public DemoConsumerProperties(String name, TopicName topic) throws PropertiesException {
		super(name, topic);
	}

	public DemoConsumerProperties(String name, String pattern) throws PropertiesException {
		super(name, pattern);
	}

}
