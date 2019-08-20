package io.rtdi.bigdata.democonnector;

import io.rtdi.bigdata.connector.properties.ConnectionProperties;

public class DemoConnectionProperties extends ConnectionProperties {

	private static final String MESSAGE_FRQUENCY = "demo.frequency";

	public DemoConnectionProperties(String name) {
		super(name);
		properties.addIntegerProperty(MESSAGE_FRQUENCY, "Messages per minute", "How many messages should be created per minute", "sap-icon://target-group", 1, false);
	}

	public int getRowsPerMinute() {
		return properties.getIntPropertyValue(MESSAGE_FRQUENCY);
	}
}
