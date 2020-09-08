import scala.xml.XML
object readPNRFile {
	def main(args : Array[String]) : 
		Unit = {
			//var conf = new SparkConf().setAppName("Read XML File in Spark").setMaster("local[*]")

			//val sc = new SparkContext(conf)
			val xmlRDD = XML.loadFile(args(0))

			//println(xmlRDD)

			// Read RDD
			val l_PnrHandling = (xmlRDD \\ "BatchFeed" \\ "ForPnrHandling")
			println(l_PnrHandling.length)
			var l_activitiReports_xml=""
			var l_activityReport_xml=""
			var l_delimeter =","
			for (each_PNRHandling <- l_PnrHandling)
			{
				val l_activePNRimage = (each_PNRHandling \\ "activePNRimage" )
				println(l_activePNRimage.length)
				val l_activityReport = (each_PNRHandling \\ "activityReport" )
				println(l_activityReport.length)
				if (l_activityReport.length > 0)  
				{
					val l_control_number = (l_activePNRimage \ "amadeusId" \ "reservation" \ "controlNumber")
					val l_pnrPurgeDate  = (l_activePNRimage \ "pnrHeader" \"technicalData"\ "pnrPurgeDate")
					for (each_activity <- l_activityReport)
					{
					    l_activityReport_xml = "<activityReport>"
					    l_activityReport_xml=l_activityReport_xml+l_control_number.toString()
					    l_activityReport_xml = l_activityReport_xml.concat(l_pnrPurgeDate.toString())	
					    val l_referenceType = (each_activity \"activityId" \"referenceType")
					    l_activityReport_xml=l_activityReport_xml.concat(l_referenceType.toString())	
					    val l_uniqueReference = (each_activity \"activityId" \"uniqueReference")
					    l_activityReport_xml=l_activityReport_xml.concat(l_uniqueReference.toString())
					    val l_actionRequestCode = (each_activity \"action" \"actionRequestCode")
					    l_activityReport_xml=l_activityReport_xml.concat(l_actionRequestCode.toString())	
					    val l_ele_qualifier = (each_activity \"activityReference"\"elementReference" \ "qualifier")
					    l_activityReport_xml=l_activityReport_xml.concat(l_ele_qualifier.toString())
					    val l_ele_number = (each_activity \"activityReference" \"elementReference" \ "number")
					    l_activityReport_xml=l_activityReport_xml.concat(l_ele_number.toString())
					    val l_segmentName = (each_activity \"activityReference"\ "segmentName")
					    l_activityReport_xml=l_activityReport_xml.concat(l_segmentName.toString())
					    val l_lineNumber = (each_activity \"activityReference" \"lineNumber")
					    l_activityReport_xml=l_activityReport_xml.concat(l_lineNumber.toString())					    
					    l_activityReport_xml=l_activityReport_xml.concat("</activityReport>")
					    l_activitiReports_xml=l_activitiReports_xml.concat(l_activityReport_xml)
					}
				}
			}
			l_activitiReports_xml="<xml>"+l_activitiReports_xml.concat("</xml>")
			println(l_activitiReports_xml)
			val l_activityXMLNode = XML.loadString(l_activitiReports_xml)
			XML.save("/home/hduser/PNR/activitiReports.xml", l_activityXMLNode)
			val sqlContext = new org.apache.spark.sql.SQLContext(sc)
			val df = sqlContext.read.format("com.databricks.spark.xml").option("inferSchema", "True").option("rowTag", "activityReport").load("/home/hduser/PNR/activitiReports.xml")
			df.registerTempTable("activitiReportsTBL");
			println(df.schema);
			df.printSchema()
			sqlContext.sql("select * from activitiReportsTBL").show();
			df.write.mode("overwrite").saveAsTable("dbpnr.activitiReports")
		}
  }
