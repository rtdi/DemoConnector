package io.rtdi.bigdata.democonnector;

import io.rtdi.bigdata.connector.connectorframework.JerseyApplication;

public class JerseyApplicationSettings extends JerseyApplication {

	public JerseyApplicationSettings() {
		super();
	}

	@Override
	protected String[] getPackages() {
		return null;
		// return new String[] {"io.rtdi.bigdata.democonnector.service"};
	}

}
