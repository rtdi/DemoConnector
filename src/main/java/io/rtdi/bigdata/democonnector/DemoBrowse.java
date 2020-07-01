package io.rtdi.bigdata.democonnector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;

import io.rtdi.bigdata.connector.connectorframework.BrowsingService;
import io.rtdi.bigdata.connector.connectorframework.controller.ConnectionController;
import io.rtdi.bigdata.connector.connectorframework.entity.TableEntry;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroDate;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroDecimal;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroInt;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroNVarchar;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroTimestamp;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroVarchar;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.TableType;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.SchemaException;
import io.rtdi.bigdata.connector.pipeline.foundation.recordbuilders.AvroRecordArray;
import io.rtdi.bigdata.connector.pipeline.foundation.recordbuilders.ValueSchema;

public class DemoBrowse extends BrowsingService<DemoConnectionProperties> {
	public static final String EMPLOYEE = "Employee";
	public static final String CUSTOMER = "Customer";
	public static final String MATERIAL = "Material";
	public static final String SALES_ORDER = "SalesOrder";
	public static List<TableEntry> sourceobjects = new ArrayList<>();
	public static ValueSchema salesorder;
	public static ValueSchema material;
	public static ValueSchema customer;
	public static ValueSchema employee;
	
	static {
		sourceobjects.add(new TableEntry(SALES_ORDER, TableType.TABLE, "Business Object for Sales Orders"));
		sourceobjects.add(new TableEntry(MATERIAL, TableType.TABLE, "Business Object for Material"));
		sourceobjects.add(new TableEntry(CUSTOMER, TableType.TABLE, "Business Object for Customer"));
		sourceobjects.add(new TableEntry(EMPLOYEE, TableType.TABLE, "Business Object for Employee"));
	}
	
	static {
		try {
			salesorder = new ValueSchema(SALES_ORDER, "Business Object for Sales Order");
			salesorder.add("OrderNumber", AvroVarchar.getSchema(10), "The sales order number", false).setPrimaryKey();
			salesorder.add("ChangeTimestamp", AvroTimestamp.getSchema(), "The timestamp (Unix epoch format) the order was changed", false).setPrimaryKey();
			salesorder.add("OrderDate", AvroDate.getSchema(), "The date the order was created", false);
			salesorder.add("SoldTo", AvroVarchar.getSchema(10), "The sold-to party", false);
			salesorder.add("ShipTo", AvroVarchar.getSchema(10), "The ship-to party", false);
			salesorder.add("BillTo", AvroVarchar.getSchema(10), "The bill-to party", false);
			salesorder.add("OrderStatus", AvroVarchar.getSchema(1), "Status of the order Entered, Fullfilled, Shipped, Completed, Terminated", true);
			AvroRecordArray items = salesorder.addColumnRecordArray("SalesItems", "Bought items", "SalesItems", "Sales order Items");
			items.add("OrderLine", AvroInt.getSchema(), "The order line", false);
			items.add("MaterialNumber", AvroVarchar.getSchema(20), "Material Number", false);
			items.add("Quantity", AvroDecimal.getSchema(20,3), "Sold quantity", true);
			items.add("UoM", AvroVarchar.getSchema(3), "Unit of Measure", false);
			items.add("Value", AvroDecimal.getSchema(20,3), "Overall item value", true);
			salesorder.build();
			
			material = new ValueSchema(MATERIAL, "Masterdata about Materials");
			material.add("MaterialNumber", AvroVarchar.getSchema(20), "Material Number", false).setPrimaryKey();
			material.add("ChangeTimestamp", AvroTimestamp.getSchema(), "The timestamp (Unix epoch format) the record was changed", false).setPrimaryKey();
			material.add("UoM", AvroVarchar.getSchema(3), "Unit of Measure", false);
			material.add("Color", AvroVarchar.getSchema(9), "Color", false);
			material.add("Size", AvroVarchar.getSchema(10), "Size", false);
			AvroRecordArray desc = material.addColumnRecordArray("MaterialName", "Material name", "MaterialName", "Material name");
			desc.add("Language", AvroVarchar.getSchema(3), "Language as ISO code", false);
			desc.add("Name", AvroNVarchar.getSchema(40), "Material name", false);
			desc.add("Text", AvroNVarchar.getSchema(200), "Material text", true);
			material.add("Type", AvroVarchar.getSchema(4), "Material type", true);
			material.add("Group", AvroVarchar.getSchema(3), "Material group", true);
			material.build();
			
			customer = new ValueSchema(CUSTOMER, "Masterdata about Customers");
			customer.add("CustomerNumber", AvroVarchar.getSchema(10), "Material Number", false).setPrimaryKey();
			customer.add("ChangeTimestamp", AvroTimestamp.getSchema(), "The timestamp (Unix epoch format) the record was changed", false).setPrimaryKey();
			customer.add("CompanyName", AvroNVarchar.getSchema(40), "Company name", true);

			AvroRecordArray address = customer.addColumnRecordArray("CompanyAddress", "Company address", "CompanyAddress", "Company address");
			address.add("City", AvroVarchar.getSchema(40), "City", true);
			address.add("Country", AvroNVarchar.getSchema(40), "Country", true);
			address.add("Street", AvroNVarchar.getSchema(40), "Street", true);
			address.add("PostCode", AvroVarchar.getSchema(10), "PostCode", true);
			customer.build();

			employee = new ValueSchema(EMPLOYEE, "Masterdata about Employees");
			employee.add("EmployeeNumber", AvroVarchar.getSchema(10), "Employee Number", false).setPrimaryKey();
			employee.add("ChangeTimestamp", AvroTimestamp.getSchema(), "The timestamp (Unix epoch format) the record was changed", false).setPrimaryKey();
			employee.add("Firstname", AvroNVarchar.getSchema(40), "Firstname", true);
			employee.add("Lastname", AvroNVarchar.getSchema(40), "Lastname", true);
			employee.add("Title", AvroNVarchar.getSchema(20), "Title", true);
			employee.add("CurrentDepartment", AvroVarchar.getSchema(20), "Current department", true);
			employee.add("CurrentPosition", AvroVarchar.getSchema(40), "Current position", true);

			AvroRecordArray employeeaddress = employee.addColumnRecordArray("EmployeeAddress", "Employee address", "EmployeeAddress", "Employee address");
			employeeaddress.add("AddressType", AvroVarchar.getSchema(10), "Address type", true);
			employeeaddress.add("City", AvroVarchar.getSchema(40), "City", true);
			employeeaddress.add("Country", AvroNVarchar.getSchema(40), "Country", true);
			employeeaddress.add("Street", AvroNVarchar.getSchema(40), "Street", true);
			employeeaddress.add("PostCode", AvroVarchar.getSchema(10), "PostCode", true);
			employee.build();

		} catch (SchemaException e) {
			e.printStackTrace();
		}
	}

	public DemoBrowse(ConnectionController controller) throws IOException {
		super(controller);
	}

	@Override
	public void open() throws IOException {
	}

	@Override
	public void close() {
	}

	@Override
	public List<TableEntry> getRemoteSchemaNames() throws IOException {
		return sourceobjects;
	}

	@Override
	public Schema getRemoteSchemaOrFail(String remotename) throws IOException {
		try {
			switch (remotename) {
			case SALES_ORDER: return salesorder.getSchema();
			case MATERIAL: return material.getSchema();
			case CUSTOMER: return customer.getSchema();
			case EMPLOYEE: return employee.getSchema();
			}
		} catch (SchemaException e) {
			e.printStackTrace();
		}
		return null;
	}

}
