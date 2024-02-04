package ai.vital.service.vector

class VitalVectorService {
	
	
	String endpoint
	
	
	public VitalVectorService(String endpoint) {
		
		this.endpoint = endpoint
		
		
		
	}
	
	VitalVectorServiceStatus getStatus() {
		
		VitalVectorServiceStatus vitalVectorServiceStatus = new VitalVectorServiceStatus()
		
		vitalVectorServiceStatus.statusType = VitalVectorServiceStatusType.OK
		vitalVectorServiceStatus.statusCode = 0
		vitalVectorServiceStatus.statusMessage = ""
					
		return vitalVectorServiceStatus
	}
	
	
	
	// divide schema into global classes and tenancy classes
	// each tenant gets all of the tenancy classes
	// adding a tenant involves updating the schema for all the tenancy classes
	
	// tenants are tracked in external database such as graph service
	
	// list tenants for a class
	// should only be needed for testing and confirming tenant list synced
	
	
	// update schema
	// add tenant
	// remove tenant
	
	// caller composes GraphQL
	// query
	
	// insert/update
	
	// delete
	
	// get
	
	// add cross reference
	
	
	
	
	
	
}
