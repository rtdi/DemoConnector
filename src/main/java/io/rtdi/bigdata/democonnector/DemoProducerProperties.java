package io.rtdi.bigdata.democonnector;

import java.io.File;

import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.properties.ProducerProperties;

public class DemoProducerProperties extends ProducerProperties {

	private static final String TARGETTOPIC_SALES = "demo.topic.sales";
	private static final String TARGETTOPIC_HR = "demo.topic.hr";

	public DemoProducerProperties(String name) throws PropertiesException {
		super(name);
		properties.addStringProperty(TARGETTOPIC_SALES, "Sales topic name", "The topic name for sales data", "sap-icon://batch-payments", "SALES", false);
		properties.addStringProperty(TARGETTOPIC_HR, "HR topic name", "The topic name for HR data", "sap-icon://batch-payments", "HR", false);
	}

	public DemoProducerProperties(File dir, String name) throws PropertiesException {
		super(dir, name);
	}

	public String getSalesTopic() {
		return properties.getStringPropertyValue(TARGETTOPIC_SALES);
	}

	public String getHRTopic() {
		return properties.getStringPropertyValue(TARGETTOPIC_HR);
	}

}
