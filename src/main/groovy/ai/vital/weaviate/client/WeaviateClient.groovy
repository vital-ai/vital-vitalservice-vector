package ai.vital.weaviate.client

import groovy.json.JsonSlurper
import org.apache.commons.httpclient.util.URIUtil
import org.apache.http.client.methods.CloseableHttpResponse
import org.apache.http.client.methods.HttpDelete
import org.apache.http.client.methods.HttpPost
import org.apache.http.client.methods.HttpPut
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.methods.HttpHead
import org.apache.http.client.methods.HttpPatch
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.impl.client.HttpClients
import org.apache.http.NameValuePair
import org.apache.http.util.EntityUtils
import ai.vital.httputils.HttpDeleteBody
import ai.vital.vitalsigns.VitalSigns
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import groovy.json.JsonOutput
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class WeaviateClient {
	
	private final static Logger log = LoggerFactory.getLogger( WeaviateClient.class )
	
	private String endpoint
	
	public static WeaviateClient getClient(String endpoint) {
		
		WeaviateClient client = new WeaviateClient()
	
		client.endpoint = endpoint
			
		return client			
	}
	
	private WeaviateClient() {
		
			
	}
	
	// query
	
	public String query(String graphQL) {
		
		String url = endpoint + "/v1/graphql"
		
		return post(url, graphQL)	
	}
	
	// schema
	
	public String getSchema() {
		
		String url = endpoint + "/v1/schema"
		
		return get(url)	
	}
	
	public String postSchema(String schema) {
		
		String url = endpoint + "/v1/schema"
		
		return post(url, schema)
	}
	
	public String deleteSchemaClass(String className) {
		
		String url = endpoint + "/v1/schema/" + className
		
		return delete(url)
	}
	
	//// objects
	
	// get objects
	
	// offset
	
	// sort=propName
	// order = asc, desc
	
	public String getObjects(Integer limit = null, String className = null) {
		
		String params = ""
				
		if(limit == null && className != null) {
			
			params = "?class=${className}"
		}
		
		if(limit != null && className == null) {
		
			params = "?limit=${limit}"
		}
	
		if(limit != null && className != null) {
		
			params = "?class=${className}&limit=${limit}"
		}
	
		String url = endpoint + "/v1/objects" + params
		
		return get(url)
	}
	
	
	
	public String getObjects(String className, Integer limit, Integer offset, String sortProp, String sortOrder) {
		
		String params = "?class=${className}&limit=${limit}&offset=${offset}&sort=${sortProp}&order=${sortOrder}"
						
		String url = endpoint + "/v1/objects" + params
				
		return get(url)
	}
	
	
	
	
	
	
	// get object
	
	public String getObject(String className, String objectIdentifier) {
		
		String url = endpoint + "/v1/objects/${className}/${objectIdentifier}"
		
		return get(url)
	}
	
	// create object
	
	public Map createObject(String objectJSON) {
		
		Map resultMap = [:]
		
		JsonSlurper parser = new JsonSlurper()
		
		String url = endpoint + "/v1/objects"
		
		String insertResult = post(url, objectJSON)
		
		if(insertResult == null) {
			
			resultMap = [:]
			
			resultMap["status"] = "error"
			
			return resultMap
		}
		
		Map insertMap = parser.parseText(insertResult)
		
		return insertMap
	}

	// should check exists first
	
	// update object
	
	public Map updateObject(String className, String objectIdentifier, String objectJSON) {
		
		Map resultMap = [:]
		
		JsonSlurper parser = new JsonSlurper()
	
		String url = endpoint + "/v1/objects/${className}/${objectIdentifier}"
	
		String updateResult = put(url, objectJSON)
			
		if(updateResult == null) {
			
			resultMap = [:]
			
			resultMap["status"] = "error"
			
			return resultMap
		}
		
		Map updateMap = parser.parseText(updateResult)
		
		return updateMap
	}
	
	// delete object
	
	public boolean deleteObject(String className, String objectIdentifier) {
		
		JsonSlurper parser = new JsonSlurper()
		
		String url = endpoint + "/v1/objects/${className}/${objectIdentifier}"
		
		String deleteResult = delete( url )
		
		Map deleteMap = parser.parseText( deleteResult )
		
		Integer status = deleteMap["status"]
		
		if(status == 204 || status == 200) {
			
			return true	
		}	
		
		return false
	}
	
	// validate object
	
	// add cross reference
	
	public String addCrossReference(String className, String objectIdentifier, String propertyName, String beaconIdentifier) {
		
		String url = endpoint + "/v1/objects/${className}/${objectIdentifier}/references/${propertyName}"
		
		Map bMap = [
			"beacon": "${beaconIdentifier}"
			]

		String json = JsonOutput.toJson(bMap)
					
		return post(url, json)
	}
		
	// update cross reference
	
	public String updateCrossReference(String className, String objectIdentifier, String propertyName, List<String> beaconIdentifierList) {
		
		String url = endpoint + "/v1/objects/${className}/${objectIdentifier}/references/${propertyName}"
		
		List<Map> beaconList = []
		
		for(b in beaconIdentifierList) {
			
			Map bMap = [
				"beacon": "${b}"
				]
			beaconList.add(bMap)
			
		}
		
		String json = JsonOutput.toJson(beaconList)
		
		return put(url, json)
	}
	
	
	// delete cross reference
	
	public String deleteCrossReference(String className, String objectIdentifier, String propertyName, String beaconIdentifier) {
		
		String url = endpoint + "/v1/objects/${className}/${objectIdentifier}/references/${propertyName}"
		
		Map bMap = [
			"beacon": "${beaconIdentifier}"
			]

		String json = JsonOutput.toJson(bMap)
					
		return delete(url, json)
	}
	
	//// batch
	
	// batch data objects
	
	public String batchObjects(String objectListJSON) {
		
		String url = endpoint + "/v1/batch/objects"
		
		return post(url, objectListJSON)
	}
	
	// batch references
	
	public String batchReferences(String referenceListJSON) {
		
		String url = endpoint + "/v1/batch/references"
		
		return post(url, referenceListJSON)
	}
	
	// batch delete by query
	
	// DELETE /v1/batch/objects[?consistency_level=ONE|QUORUM|ALL]


	
	//// backups
	
	//// classification
	
	//// meta
	
	//// nodes
	
	//// well known
	
	//// modules
	
	//// http
	
	private String get(String url) {
		
		String results = null
		
		CloseableHttpClient httpclient = null
		
		try {

			httpclient = HttpClients.createDefault()

			HttpGet httpget = new HttpGet ( url )

			httpget.setHeader("Content-type", "application/json")

			CloseableHttpResponse httpResponse = httpclient.execute(httpget)
			
			Integer statusCode = httpResponse.statusLine.statusCode
			
			if(statusCode == 200) {
				
				log.info("get succeeded: " + url)
				
			}
			
			if(statusCode == 404) {
				
				// not found
				
				httpResponse.close()
				
				return null
				
			}
			
			String json_string = EntityUtils.toString( httpResponse.getEntity() )

			httpResponse.close()

			if(json_string == null || json_string == "") {
				
				return null
				
			}
			
			def pretty = JsonOutput.prettyPrint(json_string)

			// log.info( "Result:\n" + pretty )

			results = pretty

		} catch(Exception ex) {

			log.error( "Exception: " + ex.localizedMessage )

		} finally {
			httpclient?.close()
		}

		return results
	}
	
	private String post(String url, String payloadJSON) {
		
		CloseableHttpClient httpclient = HttpClients.createDefault()
		
		String results = null
				
		try {
				
			HttpPost httppost = new HttpPost ( url )
												
			// log.info( "JSON:\n" +  payloadJSON )
				
			StringEntity entity = new StringEntity(payloadJSON, "utf-8")
							
			httppost.setEntity(entity)
									
			httppost.setHeader("Content-type", "application/json")
							
			CloseableHttpResponse response = httpclient.execute(httppost)
				
			// println "Response: " + response.getStatusLine()
						
			String json_string = EntityUtils.toString( response.getEntity() )
						
			def pretty = JsonOutput.prettyPrint(json_string)
							
			// println "Result:\n" + pretty
						
			// Map result = jsonParser.parse(json_string.toCharArray())

			results = pretty
		
		} catch(Exception ex) {
			
			log.error( "Exception: " + ex.localizedMessage )
			
			ex.printStackTrace()
			
		} finally {
			httpclient?.close()
		}
			
		return results

	}
	
	
	private String put(String url, String payloadJSON) {
		
		CloseableHttpClient httpclient = HttpClients.createDefault()
		
		String results = null
				
		try {
				
			HttpPut httpput = new HttpPut ( url )
												
			// log.info( "JSON:\n" +  payloadJSON )
				
			StringEntity entity = new StringEntity(payloadJSON, "utf-8")
							
			httpput.setEntity(entity)
									
			httpput.setHeader("Content-type", "application/json")
							
			CloseableHttpResponse response = httpclient.execute(httpput)
							
			String json_string = EntityUtils.toString( response.getEntity() )
						
			def pretty = JsonOutput.prettyPrint(json_string)
							
			// println "Result:\n" + pretty
						
			// Map result = jsonParser.parse(json_string.toCharArray())

			results = pretty
		
		} catch(Exception ex) {
			
			log.error( "Exception: " + ex.localizedMessage )
			
		} finally {
			httpclient?.close()
		}
			
		return results

	}
	
	
	private String delete(String url) {
		
		CloseableHttpClient httpclient = HttpClients.createDefault()
		
		String results = null
				
		try {
				
			HttpDelete httpdelete = new HttpDelete ( url )
												
			httpdelete.setHeader("Content-type", "application/json")
							
			CloseableHttpResponse response = httpclient.execute(httpdelete)
				
			int status = response.getStatusLine().statusCode
						
			// log.info("Delete Status: " + status)
			
			// String json_string = EntityUtils.toString( response.getEntity() )
						
			// def pretty = JsonOutput.prettyPrint(json_string)
							
			// println "Result:\n" + pretty
						
			// Map result = jsonParser.parse(json_string.toCharArray())

			results = """{
"status": ${status}
}"""
			
			// results = pretty
		
		} catch(Exception ex) {
			
			log.error( "Exception: " + ex.localizedMessage )
			
		} finally {
			httpclient?.close()
		}
			
		return results	
	}
	
	private String delete(String url, String payloadJSON) {
		
		CloseableHttpClient httpclient = HttpClients.createDefault()
		
		String results = null
				
		try {
				
			HttpDeleteBody httpdelete = new HttpDeleteBody( url )
												
			StringEntity entity = new StringEntity(payloadJSON, "utf-8")
			
			httpdelete.setEntity(entity)

			httpdelete.setHeader("Content-type", "application/json")
							
			CloseableHttpResponse response = httpclient.execute(httpdelete)
				
			int status = response.getStatusLine().statusCode
						
			// log.info("Delete Status: " + status)
			
			// String json_string = EntityUtils.toString( response.getEntity() )
						
			// def pretty = JsonOutput.prettyPrint(json_string)
							
			// println "Result:\n" + pretty
						
			// Map result = jsonParser.parse(json_string.toCharArray())

			results = """{
"status": ${status}
}"""
			
			// results = pretty
		
		} catch(Exception ex) {
			
			log.error( "Exception: " + ex.localizedMessage )
			
		} finally {
			httpclient?.close()
		}
			
		return results		
	}
		
}
