package io.rtdi.bigdata.democonnector;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.apache.avro.Schema;

import io.rtdi.bigdata.connector.connectorframework.Producer;
import io.rtdi.bigdata.connector.connectorframework.controller.ProducerInstanceController;
import io.rtdi.bigdata.connector.pipeline.foundation.SchemaHandler;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicHandler;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.RowType;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.SchemaException;

public class DemoProducer extends Producer<DemoConnectionProperties, DemoProducerProperties> {

	private SchemaHandler salesorder;
	private SchemaHandler material;
	private SchemaHandler customer;
	private SchemaHandler employee;
	private TopicHandler sales;
	private TopicHandler hr;
	private Random random = new Random();
	
	private Set<String> customers = new HashSet<>();
	private Set<String> materials = new HashSet<>();
	private long pollinterval;
	private int rows_per_poll;
	
	public DemoProducer(ProducerInstanceController instance) throws PropertiesException {
		super(instance);
	}

	@Override
	public void startProducerChangeLogging() throws IOException {
	}

	@Override
	public void startProducerCapture() throws IOException {
		double d = getConnectionProperties().getRowsPerMinute();
		pollinterval = (long) (60.0 / d);
		rows_per_poll = (getConnectionProperties().getRowsPerMinute() / 60) + 1;
	}

	@Override
	public void createTopiclist() throws IOException {
		salesorder = getSchemaHandler(DemoBrowse.salesorder.getName());
		material = getSchemaHandler(DemoBrowse.material.getName());
		customer = getSchemaHandler(DemoBrowse.customer.getName());
		employee = getSchemaHandler(DemoBrowse.employee.getName());
		sales = getPipelineAPI().getTopicOrCreate(this.getProducerProperties().getSalesTopic(), 1, (short) 1);
		hr = getPipelineAPI().getTopicOrCreate(this.getProducerProperties().getHRTopic(), 1, (short) 1);
		addTopicSchema(sales, salesorder);
		addTopicSchema(sales, material);
		addTopicSchema(sales, customer);
		addTopicSchema(hr, employee);
	}

	@Override
	public String getLastSuccessfulSourceTransaction() throws IOException {
		return null;
	}

	@Override
	public void initialLoad() throws IOException {
	}

	@Override
	public void restartWith(String lastsourcetransactionid) throws IOException {
	}

	@Override
	public long getPollingInterval() {
		return pollinterval;
	}

	@Override
	public void closeImpl() {
	}

	@Override
	protected Schema createSchema(String sourceschemaname) throws SchemaException, IOException {
		try {
			switch (sourceschemaname) {
			case DemoBrowse.SALES_ORDER: return DemoBrowse.salesorder.getSchema();
			case DemoBrowse.MATERIAL: return DemoBrowse.material.getSchema();
			case DemoBrowse.CUSTOMER: return DemoBrowse.customer.getSchema();
			case DemoBrowse.EMPLOYEE: return DemoBrowse.employee.getSchema();
			}
		} catch (SchemaException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public void poll() throws IOException {
		LocalDateTime today = LocalDateTime.now();
		beginTransaction(String.valueOf(System.currentTimeMillis()));
		for (int counter=0; counter < rows_per_poll; counter++) {
			int trigger = random.nextInt(100000);
			int orderno = random.nextInt(100000);
			String orderno_string = String.valueOf(orderno);
			int customerno = orderno % companynames.length;
			String customerno_string = String.valueOf(customerno);
			int materialno1 = orderno % 67;
			String materialno1_string = String.valueOf(materialno1);
			int materialno2 = materialno1 + 1;
			String materialno2_string = String.valueOf(materialno2);
			
			boolean newcustomer = !customers.contains(customerno_string);
			if (newcustomer || trigger % 135 == 0) {
				JexlRecord r = new JexlRecord(customer.getValueSchema());
				r.put("CustomerNumber", customerno_string);
				r.put("ChangeTimestamp", System.currentTimeMillis());
				r.put("CompanyName", companynames[customerno]);
				JexlRecord address = r.addChild("CompanyAddress");
				address.put("City", "New York");
				address.put("Country", "US");
				address.put("Street", "2nd Street Building " + customerno_string);
				RowType type;
				if (newcustomer) {
					type = RowType.UPSERT;
				} else {
					type = RowType.UPDATE;
				}
				addRow(sales, null, customer, r, type, null, "DemoConnector");
				customers.add(customerno_string);
			}
		
			boolean newmaterial = !materials.contains(materialno1_string);
			if (newmaterial || trigger % 203 == 0) {
				JexlRecord r = new JexlRecord(material.getValueSchema());
				r.put("MaterialNumber", materialno1_string);
				r.put("ChangeTimestamp", System.currentTimeMillis());
				r.put("UoM", "pc");
				r.put("Color", "black");
				r.put("Size", "XL");
				r.put("Type", "SI");
				r.put("Group", "TEX");
				JexlRecord text = r.addChild("MaterialName");
				text.put("Language", "EN");
				text.put("Name", "T-Shirt Type " + materialno1_string);
				addRow(sales, null, material, r, RowType.UPSERT, null, "DemoConnector");
				materials.add(materialno1_string);
			}
			if (!materials.contains(materialno2_string)) {
				JexlRecord r = new JexlRecord(material.getValueSchema());
				r.put("MaterialNumber", materialno2_string);
				r.put("ChangeTimestamp", System.currentTimeMillis());
				r.put("UoM", "pc");
				r.put("Color", "grey");
				r.put("Size", "L");
				r.put("Type", "SI");
				r.put("Group", "TEX");
				JexlRecord text = r.addChild("MaterialName");
				text.put("Language", "EN");
				text.put("Name", "Jean " + materialno2_string);
				addRow(sales, null, material, r, RowType.UPSERT, null, "DemoConnector");
				materials.add(materialno1_string);
			}
			
			if (trigger % 37 == 0) {
				JexlRecord r = new JexlRecord(employee.getValueSchema());
				r.put("EmployeeNumber", customerno_string);
				r.put("ChangeTimestamp", System.currentTimeMillis());
				r.put("Firstname", "Fritz");
				r.put("Lastname", "Lang");
				r.put("CurrentDepartment", "SALES");
				r.put("CurrentPosition", "Sales Lead");
				JexlRecord address = r.addChild("EmployeeAddress");
				address.put("AddressType", "Home");
				address.put("City", "New York");
				address.put("Country", "US");
				address.put("Street", "Main Street " + customerno_string);
				addRow(hr, null, employee, r, RowType.UPSERT, null, "DemoConnector");
			}
	
			
			JexlRecord r = new JexlRecord(salesorder.getValueSchema());
			r.put("OrderNumber", orderno_string);
			r.put("ChangeTimestamp", System.currentTimeMillis());
			r.put("OrderDate", today);
			r.put("SoldTo", customerno_string);
			r.put("ShipTo", customerno_string);
			r.put("BillTo", customerno_string);
			r.put("OrderStatus", "C");
			JexlRecord item1 = r.addChild("SalesItems");
			item1.put("OrderLine", 1);
			item1.put("MaterialNumber", materialno1_string);
			item1.put("Quantity", 4.0);
			item1.put("UoM", "pc");
			item1.put("Value", 100.45);
			JexlRecord item2 = r.addChild("SalesItems");
			item2.put("OrderLine", 2);
			item2.put("MaterialNumber", materialno2_string);
			item2.put("Quantity", 7.0);
			item2.put("UoM", "pc");
			item2.put("Value", 213.6);
			addRow(sales, null, salesorder, r, RowType.UPSERT, null, "DemoConnector");
		}
		commitTransaction();
	}

	
	private static String[] companynames = {"3 Round Stones, Inc.",
			"48 Factoring Inc.",
			"5PSolutions",
			"Abt Associates",
			"Accela",
			"Accenture",
			"AccuWeather",
			"Acxiom",
			"Adaptive",
			"Adobe Digital Government",
			"Aidin",
			"Alarm.com",
			"Allianz",
			"Allied Van Lines",
			"AllState Insurance Group",
			"Alltuition",
			"Altova",
			"Amazon Web Services",
			"American Red Ball Movers",
			"Amida Technology Solutions",
			"Analytica",
			"Apextech LLC",
			"Appallicious",
			"Aquicore",
			"Archimedes Inc.",
			"AreaVibes Inc.",
			"Arpin Van Lines",
			"Arrive Labs",
			"ASC Partners",
			"Asset4",
			"Atlas Van Lines",
			"AtSite",
			"Aunt Bertha, Inc.",
			"Aureus Sciences (*Now part of Elsevier)",
			"AutoGrid Systems",
			"Avalara",
			"Avvo",
			"Ayasdi",
			"Azavea",
			"BaleFire Global",
			"Barchart",
			"Be Informed",
			"Bekins",
			"Berkery Noyes MandASoft",
			"Berkshire Hathaway",
			"BetterLesson",
			"BillGuard",
			"Bing",
			"Biovia",
			"BizVizz",
			"BlackRock",
			"Bloomberg",
			"Booz Allen Hamilton",
			"Boston Consulting Group",
			"Boundless",
			"Bridgewater",
			"Brightscope",
			"BuildFax",
			"Buildingeye",
			"BuildZoom",
			"Business and Legal Resources",
			"Business Monitor International",
			"Calcbench, Inc.",
			"Cambridge Information Group",
			"Cambridge Semantics",
			"CAN Capital",
			"Canon",
			"Capital Cube",
			"Cappex",
			"Captricity",
			"CareSet Systems",
			"Careset.com",
			"CARFAX",
			"Caspio",
			"Castle Biosciences",
			"CB Insights",
			"Ceiba Solutions",
			"Center for Responsive Politics",
			"Cerner",
			"Certara",
			"CGI",
			"Charles River Associates",
			"Charles Schwab Corp.",
			"Chemical Abstracts Service",
			"Child Care Desk",
			"Chubb",
			"Citigroup",
			"CityScan",
			"CitySourced",
			"Civic Impulse LLC",
			"Civic Insight",
			"Civinomics",
			"Civis Analytics",
			"Clean Power Finance",
			"ClearHealthCosts",
			"ClearStory Data",
			"Climate Corporation",
			"CliniCast",
			"Cloudmade",
			"Cloudspyre",
			"Code for America",
			"Code-N",
			"Collective IP",
			"College Abacus, an ECMC initiative",
			"College Board",
			"Compared Care",
			"Compendia Bioscience Life Technologies",
			"Compliance and Risks",
			"Computer Packages Inc",
			"CONNECT-DOT LLC.",
			"ConnectEDU",
			"Connotate",
			"Construction Monitor LLC",
			"Consumer Reports",
			"CoolClimate",
			"Copyright Clearance Center",
			"CoreLogic",
			"CostQuest",
			"Credit Karma",
			"Credit Sesame",
			"CrowdANALYTIX",
			"Dabo Health",
			"DataLogix",
			"DataMade",
			"DataMarket",
			"Datamyne",
			"DataWeave",
			"Deloitte",
			"DemystData",
			"Department of Better Technology",
			"Development Seed",
			"Docket Alarm, Inc.",
			"Dow Jones & Co.",
			"Dun & Bradstreet",
			"Earth Networks",
			"EarthObserver App",
			"Earthquake Alert!",
			"Eat Shop Sleep",
			"Ecodesk",
			"eInstitutional",
			"Embark",
			"EMC",
			"Energy Points, Inc.",
			"Energy Solutions Forum",
			"Enervee Corporation",
			"Enigma.io",
			"Ensco",
			"Environmental Data Resources",
			"Epsilon",
			"Equal Pay for Women",
			"Equifax",
			"Equilar",
			"Ernst & Young LLP",
			"eScholar LLC.",
			"Esri",
			"Estately",
			"Everyday Health",
			"Evidera",
			"Experian",
			"Expert Health Data Programming, Inc.",
			"Exversion",
			"Ez-XBRL",
			"Factset",
			"Factual",
			"Farmers",
			"FarmLogs",
			"Fastcase",
			"Fidelity Investments",
			"FindTheBest.com",
			"First Fuel Software",
			"FirstPoint, Inc.",
			"Fitch",
			"FlightAware",
			"FlightStats",
			"FlightView",
			"Food+Tech Connect",
			"Forrester Research",
			"Foursquare",
			"Fujitsu",
			"Funding Circle",
			"FutureAdvisor",
			"Fuzion Apps, Inc.",
			"Gallup",
			"Galorath Incorporated",
			"Garmin",
			"Genability",
			"GenoSpace",
			"Geofeedia",
			"Geolytics",
			"Geoscape",
			"GetRaised",
			"GitHub",
			"Glassy Media",
			"Golden Helix",
			"GoodGuide",
			"Google Maps",
			"Google Public Data Explorer",
			"Government Transaction Services",
			"Govini",
			"GovTribe",
			"Govzilla, Inc.",
			"gRadiant Research LLC",
			"Graebel Van Lines",
			"Graematter, Inc.",
			"Granicus",
			"GreatSchools",
			"GuideStar",
			"H3 Biomedicine",
			"Harris Corporation",
			"HDScores, Inc",
			"Headlight",
			"Healthgrades",
			"Healthline",
			"HealthMap",
			"HealthPocket, Inc.",
			"HelloWallet",
			"HERE",
			"Honest Buildings",
			"HopStop",
			"Housefax",
			"How's My Offer?",
			"IBM",
			"ideas42",
			"iFactor Consulting",
			"IFI CLAIMS Patent Services",
			"iMedicare",
			"Impact Forecasting (Aon)",
			"Impaq International",
			"Import.io",
			"IMS Health",
			"InCadence",
			"indoo.rs",
			"InfoCommerce Group",
			"Informatica",
			"InnoCentive",
			"Innography",
			"Innovest Systems",
			"Inovalon",
			"Inrix Traffic",
			"Intelius",
			"Intermap Technologies",
			"Investormill",
			"Iodine",
			"IPHIX",
			"iRecycle",
			"iTriage",
			"IVES Group Inc",
			"IW Financial",
			"JJ Keller",
			"J.P. Morgan Chase",
			"Junar, Inc.",
			"Junyo",
			"Jurispect",
			"Kaiser Permanante",
			"karmadata",
			"Keychain Logistics Corp.",
			"KidAdmit, Inc.",
			"Kimono Labs",
			"KLD Research",
			"Knoema",
			"Knowledge Agency",
			"KPMG",
			"Kroll Bond Ratings Agency",
			"Kyruus",
			"Lawdragon",
			"Legal Science Partners",
			"(Leg)Cyte",
			"LegiNation, Inc.",
			"LegiStorm",
			"Lenddo",
			"Lending Club",
			"Level One Technologies",
			"LexisNexis",
			"Liberty Mutual Insurance Cos.",
			"Lilly Open Innovation Drug Discovery",
			"Liquid Robotics",
			"Locavore",
			"LOGIXDATA, LLC",
			"LoopNet",
			"Loqate, Inc.",
			"LoseIt.com",
			"LOVELAND Technologies",
			"Lucid",
			"Lumesis, Inc.",
			"Mango Transit",
			"Mapbox",
			"Maponics",
			"MapQuest",
			"Marinexplore, Inc.",
			"MarketSense",
			"Marlin & Associates",
			"Marlin Alter and Associates",
			"McGraw Hill Financial",
			"McKinsey",
			"MedWatcher",
			"Mercaris",
			"Merrill Corp.",
			"Merrill Lynch",
			"MetLife",
			"mHealthCoach",
			"MicroBilt Corporation",
			"Microsoft Windows Azure Marketplace",
			"Mint",
			"Moody's",
			"Morgan Stanley",
			"Morningstar, Inc.",
			"Mozio",
			"MuckRock.com",
			"Munetrix",
			"Municode",
			"National Van Lines",
			"Nationwide Mutual Insurance Company",
			"Nautilytics",
			"Navico",
			"NERA Economic Consulting",
			"NerdWallet",
			"New Media Parents",
			"Next Step Living",
			"NextBus",
			"nGAP Incorporated",
			"Nielsen",
			"Noesis",
			"NonprofitMetrics",
			"North American Van Lines",
			"Noveda Technologies",
			"NuCivic",
			"Numedii",
			"Oliver Wyman",
			"OnDeck",
			"OnStar",
			"Ontodia, Inc",
			"Onvia",
			"Open Data Nation",
			"OpenCounter",
			"OpenGov",
			"OpenPlans",
			"OpportunitySpace, Inc.",
			"Optensity",
			"optiGov",
			"OptumInsight",
			"Orlin Research",
			"OSIsoft",
			"OTC Markets",
			"Outline",
			"Oversight Systems",
			"Overture Technologies",
			"Owler",
			"Palantir Technologies",
			"Panjiva",
			"Parsons Brinckerhoff",
			"Patently-O",
			"PatientsLikeMe",
			"Pave",
			"Paxata",
			"PayScale, Inc.",
			"PeerJ",
			"People Power",
			"Persint",
			"Personal Democracy Media",
			"Personal, Inc.",
			"Personalis",
			"Peterson's",
			"PEV4me.com",
			"PIXIA Corp",
			"PlaceILive.com",
			"PlanetEcosystems",
			"PlotWatt",
			"Plus-U",
			"PolicyMap",
			"Politify",
			"Poncho App",
			"POPVOX",
			"Porch",
			"PossibilityU",
			"PowerAdvocate",
			"Practice Fusion",
			"Predilytics",
			"PricewaterhouseCoopers (PWC)",
			"ProgrammableWeb",
			"Progressive Insurance Group",
			"Propeller Health",
			"ProPublica",
			"PublicEngines",
			"PYA Analytics",
			"Qado Energy, Inc.",
			"Quandl",
			"Quertle",
			"Quid",
			"R R Donnelley",
			"RAND Corporation",
			"Rand McNally",
			"Rank and Filed",
			"Ranku",
			"Rapid Cycle Solutions",
			"realtor.com",
			"Recargo",
			"ReciPal",
			"Redfin",
			"RedLaser",
			"Reed Elsevier",
			"REI Systems",
			"Relationship Science",
			"Remi",
			"Retroficiency",
			"Revaluate",
			"Revelstone",
			"Rezolve Group",
			"Rivet Software",
			"Roadify Transit",
			"Robinson + Yu",
			"Russell Investments",
			"Sage Bionetworks",
			"SAP",
			"SAS",
			"Scale Unlimited",
			"Science Exchange",
			"Seabourne",
			"SeeClickFix",
			"SigFig",
			"Simple Energy",
			"SimpleTuition",
			"SlashDB",
			"Smart Utility Systems",
			"SmartAsset",
			"SmartProcure",
			"Smartronix",
			"SnapSense",
			"Social Explorer",
			"Social Health Insights",
			"SocialEffort Inc",
			"Socrata",
			"Solar Census",
			"SolarList",
			"Sophic Systems Alliance",
			"S&P Capital IQ",
			"SpaceCurve",
			"SpeSo Health",
			"Spikes Cavell Analytic Inc",
			"Splunk",
			"Spokeo",
			"SpotCrime",
			"SpotHero.com",
			"Stamen Design",
			"Standard and Poor's",
			"State Farm Insurance",
			"Sterling Infosystems",
			"Stevens Worldwide Van Lines",
			"STILLWATER SUPERCOMPUTING INC",
			"StockSmart",
			"Stormpulse",
			"StreamLink Software",
			"StreetCred Software, Inc",
			"StreetEasy",
			"Suddath",
			"Symcat",
			"Synthicity",
			"T. Rowe Price",
			"Tableau Software",
			"TagniFi",
			"Telenav",
			"Tendril",
			"Teradata",
			"The Advisory Board Company",
			"The Bridgespan Group",
			"The DocGraph Journal",
			"The Govtech Fund",
			"The Schork Report",
			"The Vanguard Group",
			"Think Computer Corporation",
			"Thinknum",
			"Thomson Reuters",
			"TopCoder",
			"TowerData",
			"TransparaGov",
			"TransUnion",
			"TrialTrove",
			"TrialX",
			"Trintech",
			"TrueCar",
			"Trulia",
			"TrustedID",
			"TuvaLabs",
			"Uber",
			"Unigo LLC",
			"United Mayflower",
			"Urban Airship",
			"Urban Mapping, Inc",
			"US Green Data",
			"U.S. News Schools",
			"USAA Group",
			"USSearch",
			"Verdafero",
			"Vimo",
			"VisualDoD, LLC",
			"Vital Axiom | Niinja",
			"VitalChek",
			"Vitals",
			"Vizzuality",
			"Votizen",
			"Walk Score",
			"WaterSmart Software",
			"WattzOn",
			"Way Better Patents",
			"Weather Channel",
			"Weather Decision Technologies",
			"Weather Underground",
			"WebFilings",
			"Webitects",
			"WebMD",
			"Weight Watchers",
			"WeMakeItSafer",
			"Wheaton World Wide Moving",
			"Whitby Group",
			"Wolfram Research",
			"Wolters Kluwer",
			"Workhands",
			"Xatori",
			"Xcential",
			"xDayta",
			"Xignite",
			"Yahoo",
			"Zebu Compliance Solutions",
			"Yelp",
			"YourMapper",
			"Zillow",
			"ZocDoc",
			"Zonability",
			"Zoner",
			"Zurich Insurance (Risk Room)"
};
	
}
