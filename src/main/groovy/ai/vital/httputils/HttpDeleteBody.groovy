package ai.vital.httputils

import org.apache.http.client.methods.HttpPost

class HttpDeleteBody extends HttpPost {
	
    public HttpDeleteBody(String url) {
		super(url) 
    }
	
    @Override
    public String getMethod() {
        return "DELETE";
    }
}
